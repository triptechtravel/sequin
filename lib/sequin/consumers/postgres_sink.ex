defmodule Sequin.Consumers.PostgresSink do
  @moduledoc """
  Mirrors rows into tables in another Postgres database: inserts and updates are upserted by the
  destination table's primary key, and deletes delete.
  """
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:host, :port, :database, :username, :ssl, :table_name, :batch_size, :routing_mode]}
  @derive {Inspect, except: [:password]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:postgres], default: :postgres
    field :host, :string
    field :port, :integer, default: 5432
    field :database, :string
    field :username, :string
    field :password, Sequin.Encrypted.Binary
    field :ssl, :boolean, default: false
    # A table, optionally schema-qualified ("schema.table"), for static routing
    field :table_name, :string
    field :batch_size, :integer, default: 100
    field :routing_mode, Ecto.Enum, values: [:dynamic, :static]
    # Identifies this sink's connection in the connection cache
    field :connection_id, :string
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :host,
      :port,
      :database,
      :username,
      :password,
      :ssl,
      :table_name,
      :batch_size,
      :routing_mode
    ])
    |> validate_required([:host, :port, :database, :username])
    |> validate_number(:port, greater_than: 0, less_than: 65_536)
    |> validate_number(:batch_size, greater_than: 0, less_than_or_equal_to: 10_000)
    |> validate_length(:host, max: 1024)
    |> validate_length(:database, max: 255)
    |> validate_routing()
    |> put_connection_id()
  end

  defp validate_routing(changeset) do
    case get_field(changeset, :routing_mode) do
      :dynamic ->
        put_change(changeset, :table_name, nil)

      :static ->
        changeset
        |> validate_required([:table_name])
        |> validate_change(:table_name, fn :table_name, table_name ->
          case parse_table_name(table_name) do
            {:ok, _} -> []
            :error -> [table_name: "must be a table name, optionally schema-qualified, like public.users"]
          end
        end)

      _ ->
        add_error(changeset, :routing_mode, "is required")
    end
  end

  defp put_connection_id(changeset) do
    if get_field(changeset, :connection_id),
      do: changeset,
      else: put_change(changeset, :connection_id, Ecto.UUID.generate())
  end

  @doc """
  Splits a table name into `{schema, table}`, defaulting the schema to "public". Names are taken
  literally (they're always quoted in SQL), so "Users" and "users" are different tables.
  """
  @spec parse_table_name(String.t()) :: {:ok, {String.t(), String.t()}} | :error
  def parse_table_name(table_name) when is_binary(table_name) do
    case String.split(table_name, ".") do
      [table] when table != "" -> {:ok, {"public", table}}
      [schema, table] when schema != "" and table != "" -> {:ok, {schema, table}}
      _ -> :error
    end
  end

  def parse_table_name(_), do: :error

  def connection_opts(%__MODULE__{} = sink) do
    [
      hostname: sink.host,
      port: sink.port,
      database: sink.database,
      username: sink.username,
      password: sink.password,
      ssl: sink.ssl,
      ssl_opts: [verify: :verify_none],
      pool_size: 5
    ]
  end
end
