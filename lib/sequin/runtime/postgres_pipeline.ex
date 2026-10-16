defmodule Sequin.Runtime.PostgresPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.Trace
  alias Sequin.Sinks.Postgres.Client
  alias Sequin.Transforms.Message

  # Destination table metadata is re-read after this long, picking up schema changes
  @table_ttl_ms to_timeout(minute: 1)

  @impl SinkPipeline
  def init(context, _opts), do: Map.put(context, :postgres_tables, %{})

  @impl SinkPipeline
  def batchers_config(consumer) do
    [
      default: [
        concurrency: min(System.schedulers_online() * 2, 20),
        batch_size: consumer.sink.batch_size || 100,
        batch_timeout: 5
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %Routing.Consumers.Postgres{action: action, table_name: table_name} =
      Routing.route_message(context.consumer, message.data)

    {:ok, Broadway.Message.put_batch_key(message, {action, table_name}), context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: {action, table_name}}, context) do
    %{consumer: %SinkConsumer{} = consumer} = context

    with {:ok, conn} <- Client.connection(consumer.sink),
         {:ok, table, context} <- table(conn, table_name, context),
         :ok <- write(action, conn, table, Enum.map(messages, &row(action, consumer, &1.data))) do
      Trace.info(consumer.id, %Trace.Event{
        message: "#{if action == :delete, do: "Deleted", else: "Upserted"} #{length(messages)} rows in #{table_name}"
      })

      {:ok, messages, context}
    else
      {:error, error} ->
        Trace.error(consumer.id, %Trace.Event{message: "Failed to write rows to #{table_name}", error: error})
        {:error, error}
    end
  end

  defp write(:upsert, conn, table, rows), do: Client.upsert(conn, table, rows)
  defp write(:delete, conn, table, rows), do: Client.delete(conn, table, rows)

  # A delete only needs the key, which the record always carries. Otherwise the row is the
  # record, or the output of the consumer's transform when it has one.
  defp row(:delete, _consumer, message), do: message.data.record
  defp row(:upsert, %SinkConsumer{transform: nil}, message), do: message.data.record

  defp row(:upsert, %SinkConsumer{} = consumer, message) do
    case Message.to_external(consumer, message) do
      %{} = row ->
        row

      other ->
        raise Error.invariant(
                message: "A Postgres sink's transform must return a map of column to value, got: #{inspect(other)}"
              )
    end
  end

  defp table(conn, table_name, context) do
    now = System.monotonic_time(:millisecond)

    case context.postgres_tables[table_name] do
      {table, fetched_at} when now - fetched_at < @table_ttl_ms ->
        {:ok, table, context}

      _missing_or_expired ->
        with {:ok, table} <- Client.fetch_table(conn, table_name) do
          {:ok, table, put_in(context, [:postgres_tables, table_name], {table, now})}
        end
    end
  end
end
