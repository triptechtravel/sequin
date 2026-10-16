defmodule Sequin.Runtime.PostgresPipelineTest do
  # Destination tables are created outside the sandbox, as the sink writes through its own
  # connections
  use Sequin.DataCase, async: false

  alias Sequin.Consumers
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Test.UnboxedRepo

  @schema "postgres_pipeline_test"

  @moduletag :capture_log

  setup do
    UnboxedRepo.query!("drop schema if exists #{@schema} cascade")
    UnboxedRepo.query!("create schema #{@schema}")
    UnboxedRepo.query!("create table #{@schema}.users (id int primary key, name text)")
    on_exit(fn -> UnboxedRepo.query!("drop schema if exists #{@schema} cascade") end)

    consumer =
      ConsumersFactory.insert_sink_consumer!(
        type: :postgres,
        sink: %{table_name: "#{@schema}.users", routing_mode: :static},
        message_grouping: false
      )

    %{consumer: consumer}
  end

  test "upserts and deletes rows", %{consumer: consumer} do
    start_pipeline!(consumer)

    deliver!(consumer, [event(consumer, :insert, %{"id" => 1, "name" => "a"}), event(consumer, :insert, %{"id" => 2})])
    deliver!(consumer, [event(consumer, :update, %{"id" => 1, "name" => "a2"}), event(consumer, :delete, %{"id" => 2})])

    assert users() == [[1, "a2"]]
  end

  test "routes rows to tables chosen by a routing function", %{consumer: consumer} do
    UnboxedRepo.query!("create table #{@schema}.accounts (id int primary key, name text)")

    consumer =
      with_function(consumer, :routing, """
      def route(action, record, changes, metadata) do
        %{table_name: "#{@schema}." <> metadata.table_name}
      end
      """)

    start_pipeline!(consumer)

    deliver!(consumer, [
      event(consumer, :insert, %{"id" => 1, "name" => "user"}, "users"),
      event(consumer, :insert, %{"id" => 1, "name" => "account"}, "accounts")
    ])

    assert users() == [[1, "user"]]
    assert UnboxedRepo.query!("select id, name from #{@schema}.accounts").rows == [[1, "account"]]
  end

  test "writes a transform's output as the row", %{consumer: consumer} do
    consumer =
      with_function(consumer, :transform, """
      def transform(action, record, changes, metadata) do
        %{"id" => record["id"], "name" => String.upcase(record["name"])}
      end
      """)

    start_pipeline!(consumer)
    deliver!(consumer, [event(consumer, :insert, %{"id" => 1, "name" => "shout"})])

    assert users() == [[1, "SHOUT"]]
  end

  test "fails the batch, without crashing, when the destination can't take it", %{consumer: consumer} do
    UnboxedRepo.query!("drop table #{@schema}.users")
    start_pipeline!(consumer)

    ref = Broadway.test_batch(SinkPipeline.via_tuple(consumer.id), [event(consumer, :insert, %{"id" => 1})])
    assert_receive {:ack, ^ref, [], [failed]}, 3_000
    assert {:failed, %{message: message}} = failed.status
    assert message =~ "does not exist"
  end

  defp users, do: UnboxedRepo.query!("select id, name from #{@schema}.users order by id").rows

  defp event(consumer, action, record, table_name \\ "users") do
    data = ConsumersFactory.consumer_event_data(record: record, action: action)

    [consumer_id: consumer.id]
    |> ConsumersFactory.insert_deliverable_consumer_event!()
    |> Map.put(:data, %{data | metadata: %{data.metadata | table_schema: "public", table_name: table_name}})
  end

  defp deliver!(consumer, events) do
    ref = Broadway.test_batch(SinkPipeline.via_tuple(consumer.id), events)
    await_acks(ref, length(events))
  end

  # Broadway acks a test batch in as many messages as it splits it into batches
  defp await_acks(_ref, 0), do: :ok

  defp await_acks(ref, remaining) do
    assert_receive {:ack, ^ref, delivered, []}, 3_000
    await_acks(ref, remaining - length(delivered))
  end

  defp with_function(consumer, type, code) do
    {:ok, function} =
      Consumers.create_function(consumer.account_id, %{
        name: "#{type}-#{System.unique_integer([:positive])}",
        function: %{type: to_string(type), sink_type: :postgres, code: code}
      })

    MiniElixir.create(function.id, function.function.code)
    # A routing function comes with dynamic routing
    attrs = if type == :routing, do: %{"routing_mode" => "dynamic"}, else: %{}
    {:ok, consumer} = Consumers.update_sink_consumer(consumer, Map.put(attrs, "#{type}_id", function.id))
    consumer
  end

  defp start_pipeline!(consumer) do
    start_supervised!({SinkPipeline, [consumer_id: consumer.id, producer: Broadway.DummyProducer, test_pid: self()]})
  end
end
