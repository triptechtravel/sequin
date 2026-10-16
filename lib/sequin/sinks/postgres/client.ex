defmodule Sequin.Sinks.Postgres.Client do
  @moduledoc """
  Writes rows to tables in a destination Postgres database, for `Sequin.Consumers.PostgresSink`.

  Rows are maps of column name to value, as in a message's record. Postgres converts each value
  into its destination column's type (via `jsonb_populate_recordset/2`), so a value is written
  the way Postgres itself would parse its text or JSON form.
  """
  alias Sequin.Consumers.PostgresSink
  alias Sequin.Error
  alias Sequin.Postgres
  alias Sequin.Sinks.Postgres.ConnectionCache

  defmodule Table do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :schema, String.t()
      field :name, String.t()
      # Writable columns (not generated), in table order: %{name: String.t(), type: String.t()}
      field :columns, [map()]
      field :primary_key, [String.t()]
    end

    def quoted_name(%__MODULE__{} = table), do: Postgres.quote_name(table.schema, table.name)
  end

  @doc """
  Checks that Sequin can connect to the sink's database and, for a static table, that the table
  exists with a primary key. Uses a connection of its own, so it can check unsaved settings.
  """
  @spec test_connection(PostgresSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%PostgresSink{} = sink) do
    opts = Keyword.merge(PostgresSink.connection_opts(sink), pool_size: 1, backoff_type: :stop, timeout: 10_000)

    # Connecting directly (not via a pool) reports bad settings synchronously
    with :ok <- check_connect(opts) do
      if sink.table_name, do: check_table(opts, sink.table_name), else: :ok
    end
  end

  defp check_connect(opts) do
    case opts |> Postgrex.Utils.default_opts() |> Postgrex.Protocol.connect() do
      {:ok, state} ->
        Postgrex.Protocol.disconnect(RuntimeError.exception("disconnect"), state)
        :ok

      {:error, error} ->
        {:error, wrap_error(error, "Failed to connect to Postgres")}
    end
  end

  # In an unlinked task, as a pool that fails to connect exits, taking linked processes with it
  defp check_table(opts, table_name) do
    task =
      Task.Supervisor.async_nolink(Sequin.TaskSupervisor, fn ->
        {:ok, conn} = Postgrex.start_link(opts)

        try do
          with {:ok, _table} <- fetch_table(conn, table_name), do: :ok
        after
          GenServer.stop(conn)
        end
      end)

    case Task.yield(task, to_timeout(second: 15)) || Task.shutdown(task) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, wrap_error(reason, "Failed to check table #{table_name}")}
      nil -> {:error, Error.service(service: :postgres_sink, message: "Timed out checking table #{table_name}")}
    end
  end

  @spec connection(PostgresSink.t()) :: {:ok, pid()} | {:error, Error.t()}
  def connection(%PostgresSink{} = sink) do
    case ConnectionCache.connection(sink) do
      {:ok, conn} -> {:ok, conn}
      {:error, error} -> {:error, wrap_error(error, "Failed to connect to Postgres")}
    end
  end

  @doc """
  The destination table's writable columns and primary key.
  """
  @spec fetch_table(pid(), String.t()) :: {:ok, Table.t()} | {:error, Error.t()}
  def fetch_table(conn, table_name) do
    with {:ok, {schema, name}} <- parse_table_name(table_name),
         {:ok, %Postgrex.Result{rows: rows}} <-
           query(
             conn,
             """
             select a.attname, format_type(a.atttypid, a.atttypmod), a.attgenerated <> '',
                    coalesce(a.attnum = any(i.indkey), false)
             from pg_attribute a
             left join pg_index i on i.indrelid = a.attrelid and i.indisprimary
             where a.attrelid = to_regclass(format('%I.%I', $1::text, $2::text))
               and a.attnum > 0 and not a.attisdropped
             order by a.attnum
             """,
             [schema, name]
           ) do
      primary_key = for [column, _type, _generated?, true] <- rows, do: column

      cond do
        rows == [] ->
          {:error, Error.service(service: :postgres_sink, message: "Table #{table_name} does not exist")}

        primary_key == [] ->
          {:error,
           Error.service(
             service: :postgres_sink,
             message: "Table #{table_name} has no primary key, which is needed to upsert and delete rows"
           )}

        true ->
          columns = for [column, type, false, _pk?] <- rows, do: %{name: column, type: type}
          {:ok, %Table{schema: schema, name: name, columns: columns, primary_key: primary_key}}
      end
    end
  end

  @doc """
  Inserts the rows, or updates them where a row with the same primary key exists. When a batch
  has several rows with the same key, the last one wins.

  Only columns present in both a row and the table are written. Columns a row lacks are left as
  they are, rather than set to null.
  """
  @spec upsert(pid(), Table.t(), [map()]) :: :ok | {:error, Error.t()}
  def upsert(conn, %Table{} = table, rows) do
    with {:ok, rows} <- prepare_rows(table, rows) do
      rows
      |> Enum.group_by(&(&1 |> Map.keys() |> Enum.sort()))
      |> Enum.reduce_while(:ok, fn {columns, rows}, :ok ->
        case query(conn, upsert_sql(table, columns), [encode_rows(table, rows)]) do
          {:ok, _} -> {:cont, :ok}
          {:error, error} -> {:halt, {:error, error}}
        end
      end)
    end
  end

  @doc """
  Deletes the rows with the given rows' primary keys.
  """
  @spec delete(pid(), Table.t(), [map()]) :: :ok | {:error, Error.t()}
  def delete(conn, %Table{} = table, rows) do
    with {:ok, rows} <- prepare_rows(table, rows) do
      keys = Enum.map(rows, &Map.take(&1, table.primary_key))
      match = Enum.map_join(table.primary_key, " and ", &"t.#{q(&1)} = d.#{q(&1)}")

      sql = """
      delete from #{Table.quoted_name(table)} as t
      using jsonb_populate_recordset(null::#{Table.quoted_name(table)}, $1::jsonb) as d
      where #{match}
      """

      with {:ok, _} <- query(conn, sql, [encode_rows(table, keys)]), do: :ok
    end
  end

  defp upsert_sql(%Table{} = table, columns) do
    column_list = Enum.map_join(columns, ", ", &q/1)
    updates = columns -- table.primary_key

    on_conflict =
      if updates == [],
        do: "do nothing",
        else: "do update set " <> Enum.map_join(updates, ", ", &"#{q(&1)} = excluded.#{q(&1)}")

    # OVERRIDING SYSTEM VALUE: a mirror keeps the source's values for identity columns
    """
    insert into #{Table.quoted_name(table)} (#{column_list}) overriding system value
    select #{column_list} from jsonb_populate_recordset(null::#{Table.quoted_name(table)}, $1::jsonb)
    on conflict (#{Enum.map_join(table.primary_key, ", ", &q/1)}) #{on_conflict}
    """
  end

  # Keeps the table's columns, drops values replication couldn't supply, and dedupes by key
  defp prepare_rows(%Table{} = table, rows) do
    column_names = Enum.map(table.columns, & &1.name)

    rows =
      Enum.map(rows, fn row ->
        row
        |> Map.take(column_names)
        |> Map.reject(fn {_column, value} -> value in [:unchanged_toast, "unchanged_toast"] end)
      end)

    case Enum.find(rows, &(not Enum.all?(table.primary_key, fn column -> Map.has_key?(&1, column) end))) do
      nil ->
        {:ok, rows |> Enum.reverse() |> Enum.uniq_by(&Map.take(&1, table.primary_key)) |> Enum.reverse()}

      _row ->
        {:error,
         Error.service(
           service: :postgres_sink,
           message:
             "Rows for #{table.schema}.#{table.name} must include its primary key " <>
               "(#{Enum.join(table.primary_key, ", ")})"
         )}
    end
  end

  # Values arrive as Sequin emits them in records. Postgres parses nearly all of them from their
  # JSON form; intervals, which records carry as maps, need their text form.
  defp encode_rows(%Table{} = table, rows) do
    types = Map.new(table.columns, &{&1.name, &1.type})

    # Postgrex encodes the list as the jsonb parameter
    Enum.map(rows, fn row -> Map.new(row, fn {column, value} -> {column, encode_value(types[column], value)} end) end)
  end

  defp encode_value(type, values) when is_list(values) and type in ["interval[]"],
    do: Enum.map(values, &encode_value("interval", &1))

  defp encode_value("interval" <> _, %{"months" => months, "days" => days, "microseconds" => microseconds}),
    do: "#{months} mons #{days} days #{microseconds} microseconds"

  defp encode_value(_type, value), do: value

  defp parse_table_name(table_name) do
    case PostgresSink.parse_table_name(table_name) do
      {:ok, parsed} -> {:ok, parsed}
      :error -> {:error, Error.service(service: :postgres_sink, message: "Invalid table name: #{inspect(table_name)}")}
    end
  end

  defp q(identifier), do: Postgres.quote_name(identifier)

  defp query(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, result} -> {:ok, result}
      {:error, error} -> {:error, wrap_error(error, "Postgres query failed")}
    end
  end

  defp wrap_error(%Postgrex.Error{} = error, message) do
    Error.service(service: :postgres_sink, message: "#{message}: #{Exception.message(error)}")
  end

  defp wrap_error(%DBConnection.ConnectionError{} = error, message) do
    Error.service(service: :postgres_sink, message: "#{message}: #{Exception.message(error)}")
  end

  defp wrap_error(error, message) do
    Error.service(service: :postgres_sink, message: "#{message}: #{inspect(error)}")
  end
end
