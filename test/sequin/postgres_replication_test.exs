defmodule Sequin.PostgresReplicationTest do
  @moduledoc """
  This test file contains both unit tests for the Replication extension as well as integration
  tests with PostgresReplicationSlot. The reason for combining the two is interference: when the two
  test suites are run side-by-side, they interfere with each other. We didn't get to the bottom of
  the interference (issues starting the replication connection?), but it definitely seems to occur
  when the two tests are running and connecting to slots at the same time.

  We're making this test async: false to avoid interference with other tests, as they need to use
  character tables un-sandboxed (to produce WAL). We can make these async true with a clever trick
  like isolating tests with partitioned tables. (Cost is low, at time of writing these tests take 0.6s)
  """
  use Sequin.DataCase, async: false
  use AssertEventually, interval: 1

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Factory.TestEventLogFactory
  alias Sequin.Functions.TestMessages
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Runtime
  alias Sequin.Runtime.KeysetCursor
  alias Sequin.Runtime.MessageHandlerMock
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.SlotProcessorServer
  alias Sequin.Runtime.SlotProducer.Supervisor
  alias Sequin.Runtime.TableReader
  alias Sequin.Sinks.RedisMock
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterDetailed
  alias Sequin.TestSupport.Models.CharacterIdentFull
  alias Sequin.TestSupport.Models.CharacterMultiPK
  alias Sequin.TestSupport.Models.TestEventLogPartitioned
  alias Sequin.TestSupport.ReplicationSlots
  alias Sequin.TestSupport.SimpleHttpServer

  @moduletag :unboxed
  @moduletag :capture_log

  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    # Fast-forward the replication slot to the current WAL position
    :ok = ReplicationSlots.reset_slot(UnboxedRepo, replication_slot())
    TestMessages.create_ets_table()

    :ok
  end

  describe "PostgresReplicationSlot end-to-end" do
    setup ctx do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: account_id,
          status: :active
        )

      # Create consumers for each table type (event)
      event_character_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_consumer",
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: Character.table_oid(),
              group_column_attnums: [Character.column_attnum("id")]
            )
          ]
        )

      event_character_ident_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_ident_consumer",
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: CharacterIdentFull.table_oid(),
              group_column_attnums: [CharacterIdentFull.column_attnum("id")]
            )
          ]
        )

      event_character_multi_pk_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_multi_pk_consumer",
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: CharacterMultiPK.table_oid(),
              group_column_attnums: [
                CharacterMultiPK.column_attnum("id_integer"),
                CharacterMultiPK.column_attnum("id_string"),
                CharacterMultiPK.column_attnum("id_uuid")
              ]
            )
          ]
        )

      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          account_id: account_id,
          scheme: :http,
          host: Application.get_env(:sequin, :jepsen_http_host),
          port: Application.get_env(:sequin, :jepsen_http_port),
          path: "/",
          headers: %{"Content-Type" => "application/json"}
        )

      test_event_log_partitioned_consumer_http =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account_id,
          type: :http_push,
          source: %{
            include_table_oids: [TestEventLogPartitioned.table_oid()]
          },
          sink: %{
            type: :http_push,
            http_endpoint_id: http_endpoint.id,
            http_endpoint: http_endpoint,
            batch: true
          },
          replication_slot_id: pg_replication.id,
          status: :active
        )

      test_event_log_partitioned_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "test_event_log_partitioned_consumer",
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: TestEventLogPartitioned.table_oid(),
              group_column_attnums: [TestEventLogPartitioned.column_attnum("id")]
            )
          ]
        )

      event_character_consumer = Repo.preload(event_character_consumer, :postgres_database)
      event_character_ident_consumer = Repo.preload(event_character_ident_consumer, :postgres_database)
      event_character_multi_pk_consumer = Repo.preload(event_character_multi_pk_consumer, :postgres_database)

      test_event_log_partitioned_consumer = Repo.preload(test_event_log_partitioned_consumer, :postgres_database)
      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))
      start_opts = ctx |> Map.get(:start_opts, []) |> Keyword.put(:test_pid, self())
      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, start_opts)

      %{
        sup: sup,
        pg_replication: pg_replication,
        source_db: source_db,
        event_character_consumer: event_character_consumer,
        event_character_ident_consumer: event_character_ident_consumer,
        event_character_multi_pk_consumer: event_character_multi_pk_consumer,
        test_event_log_partitioned_consumer: test_event_log_partitioned_consumer,
        test_event_log_partitioned_consumer_http: test_event_log_partitioned_consumer_http
      }
    end

    test "inserts are replicated to consumer events", %{event_character_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      assert_maps_equal(data.record, Map.from_struct(character), ["id", "name", "house", "planet", "is_active", "tags"],
        indifferent_keys: true
      )

      assert is_nil(data.changes)
      assert data.action == :insert

      assert_maps_equal(
        data.metadata,
        %{table_name: "Characters", table_schema: "public", database_name: consumer.postgres_database.name},
        [:table_name, :table_schema, :database_name]
      )

      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "updates are replicated to consumer events when replica identity default", %{event_character_consumer: consumer} do
      # Insert a character
      character =
        CharacterFactory.insert_character!([name: "Leto Atreides", house: "Atreides", planet: "Caladan"],
          repo: UnboxedRepo
        )

      # Wait for the insert message to be handled
      await_messages(1)

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled and fetch consumer events
      await_messages(1)
      events = list_messages(consumer)
      update_event = Enum.find(events, &(&1.data.action == :update))

      # Assert the consumer event details
      assert update_event.consumer_id == consumer.id
      assert update_event.table_oid == Character.table_oid()
      assert update_event.record_pks == [to_string(character.id)]
      %{data: data} = update_event

      character = Repo.reload(character)

      assert data.record == %{
               "id" => character.id,
               "name" => character.name,
               "house" => character.house,
               "planet" => "Arrakis",
               "is_active" => character.is_active,
               "tags" => character.tags,
               "metadata" => character.metadata,
               "inserted_at" => character.inserted_at,
               "updated_at" => character.updated_at
             }

      assert data.changes == %{}
      assert data.action == :update

      assert_maps_equal(
        data.metadata,
        %{table_name: "Characters", table_schema: "public", database_name: consumer.postgres_database.name},
        [:table_name, :table_schema, :database_name]
      )

      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "updates are replicated to consumer events when replica identity full", %{
      event_character_ident_consumer: consumer
    } do
      # Insert a character with full replica identity
      character =
        CharacterFactory.insert_character_ident_full!(
          [
            name: "Paul Atreides",
            house: "Atreides",
            planet: "Caladan",
            is_active: true,
            tags: ["heir", "kwisatz haderach"]
          ],
          repo: UnboxedRepo
        )

      # Wait for the insert message to be handled
      await_messages(1)

      # Update the character
      UnboxedRepo.update!(
        Ecto.Changeset.change(character, %{
          house: "Emperor",
          planet: "Arrakis",
          is_active: false,
          tags: ["emperor", "kwisatz haderach"]
        })
      )

      # Wait for the update message to be handled and fetch consumer events
      await_messages(1)
      events = list_messages(consumer)
      update_event = Enum.find(events, &(&1.data.action == :update))

      # Assert the consumer event details
      %{data: data} = update_event

      assert_maps_equal(
        data.changes,
        %{
          "house" => "Atreides",
          "planet" => "Caladan",
          "is_active" => true,
          "tags" => ["heir", "kwisatz haderach"]
        },
        ["house", "planet", "is_active", "tags"]
      )

      assert data.action == :update
    end

    test "deletes are replicated to consumer events when replica identity default", %{event_character_consumer: consumer} do
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      await_messages(1)

      UnboxedRepo.delete!(character)

      await_messages(1)
      events = list_messages(consumer)
      delete_event = Enum.find(events, &(&1.data.action == :delete))

      %{data: data} = delete_event

      assert data.record == %{
               "house" => nil,
               "id" => character.id,
               "is_active" => nil,
               "name" => nil,
               "planet" => nil,
               "tags" => nil,
               "metadata" => nil,
               "inserted_at" => nil,
               "updated_at" => nil
             }

      assert data.changes == nil
      assert data.action == :delete
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "deletes are replicated to consumer events when replica identity full", %{
      event_character_ident_consumer: consumer
    } do
      character = CharacterFactory.insert_character_ident_full!([], repo: UnboxedRepo)

      await_messages(1)

      UnboxedRepo.delete!(character)

      await_messages(1)
      events = list_messages(consumer)
      delete_event = Enum.find(events, &(&1.data.action == :delete))

      %{data: data} = delete_event

      assert_maps_equal(data.record, Map.from_struct(character), ["id", "name", "house", "planet", "is_active", "tags"],
        indifferent_keys: true
      )

      assert data.changes == nil
      assert data.action == :delete

      assert_maps_equal(data.metadata, %{table_name: "characters_ident_full", table_schema: "public"}, [
        :table_name,
        :table_schema
      ])

      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "replication with multiple primary key columns", %{
      event_character_multi_pk_consumer: consumer
    } do
      # Insert
      character = CharacterFactory.insert_character_multi_pk!([], repo: UnboxedRepo)

      [insert_message] = receive_messages(consumer, 1)

      # Assert the consumer message details
      assert insert_message.consumer_id == consumer.id
      assert insert_message.table_oid == CharacterMultiPK.table_oid()

      assert insert_message.record_pks == [
               to_string(character.id_integer),
               character.id_string,
               to_string(character.id_uuid)
             ]
    end

    test "consumer with column filter only receives relevant messages", %{event_character_consumer: consumer} do
      source = ConsumersFactory.source_attrs(include_table_oids: [Character.table_oid()])
      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{actions: [:insert, :update], source: source})

      :ok = Runtime.Supervisor.refresh_message_handler_ctx(consumer.replication_slot_id)

      # Insert a character that doesn't match the filter
      CharacterFactory.insert_character_detailed!([], repo: UnboxedRepo)

      # Insert a character that matches the filter
      matching_character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer messages
      [consumer_message] = receive_messages(consumer, 2)

      # Assert the consumer message details
      assert consumer_message.consumer_id == consumer.id
      assert consumer_message.table_oid == Character.table_oid()
      assert consumer_message.record_pks == [to_string(matching_character.id)]

      UnboxedRepo.delete!(matching_character)

      # Wait for the message to be handled and fetch consumer messages, no new message should be created
      messages = receive_messages(consumer, 1)
      assert [^consumer_message] = messages
    end

    test "empty array fields are replicated correctly", %{event_character_consumer: consumer} do
      # Insert a character with an empty array field
      character = CharacterFactory.insert_character!([tags: []], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      # Check that the tags field is an empty list, not [""]
      assert data.record["tags"] == [], "Expected empty array, got: #{inspect(data.record["tags"])}"
    end

    test "transaction annotations are propagated correctly", %{event_character_consumer: consumer} do
      # Insert two characters in the same transaction with annotations
      {:ok, {character1, character2}} =
        UnboxedRepo.transaction(fn ->
          # Set initial transaction annotations
          {:ok, _} =
            UnboxedRepo.query(
              ~s|select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', '{ "username": "yahya" }')|
            )

          c1 = CharacterFactory.insert_character!([name: "Paul"], repo: UnboxedRepo)
          c2 = CharacterFactory.insert_character!([name: "Leto"], repo: UnboxedRepo)
          {c1, c2}
        end)

      # Wait for the annotated messages to be handled
      await_messages(2)

      # Insert a character without annotations
      character3 = CharacterFactory.insert_character!([name: "Duncan"], repo: UnboxedRepo)

      # Wait for the message to be handled
      await_messages(1)

      # Insert final character with new annotations
      {:ok, character4} =
        UnboxedRepo.transaction(fn ->
          # Set new annotations
          {:ok, _} =
            UnboxedRepo.query(
              ~s|select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', '{ "spice": "flow" }')|
            )

          CharacterFactory.insert_character!([name: "Chani"], repo: UnboxedRepo)
        end)

      # Wait for final messages to be handled
      await_messages(1)

      # Fetch all consumer events
      events = list_messages(consumer)

      # Find events for each character
      event1 = Enum.find(events, &(hd(&1.record_pks) == to_string(character1.id)))
      event2 = Enum.find(events, &(hd(&1.record_pks) == to_string(character2.id)))
      event3 = Enum.find(events, &(hd(&1.record_pks) == to_string(character3.id)))
      event4 = Enum.find(events, &(hd(&1.record_pks) == to_string(character4.id)))

      # First two events should have the same annotations
      assert event1.data.metadata.transaction_annotations == %{"username" => "yahya"}
      assert event2.data.metadata.transaction_annotations == %{"username" => "yahya"}

      # Third event should have no annotations
      assert event3.data.metadata.transaction_annotations == nil

      # Fourth event should have new annotations
      assert event4.data.metadata.transaction_annotations == %{"spice" => "flow"}
    end

    @tag capture_log: true
    test "invalid transaction annotations are ignored", %{event_character_consumer: consumer} do
      # Insert a character with invalid JSON annotations
      {:ok, character1} =
        UnboxedRepo.transaction(fn ->
          # Set invalid JSON as transaction annotations
          {:ok, _} =
            UnboxedRepo.query(
              "select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', '{ invalid json }')"
            )

          CharacterFactory.insert_character!([name: "Paul"], repo: UnboxedRepo)
        end)

      # Wait for the message to be handled
      await_messages(1)

      # Insert another character with valid annotations
      {:ok, character2} =
        UnboxedRepo.transaction(fn ->
          # Set valid annotations
          {:ok, _} =
            UnboxedRepo.query(
              ~s|select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', '{ "username": "leto" }')|
            )

          CharacterFactory.insert_character!([name: "Leto"], repo: UnboxedRepo)
        end)

      # Wait for the message to be handled
      await_messages(1)

      # Fetch all consumer events
      events = list_messages(consumer)

      # Find events for each character
      event1 = Enum.find(events, &(hd(&1.record_pks) == to_string(character1.id)))
      event2 = Enum.find(events, &(hd(&1.record_pks) == to_string(character2.id)))

      # First event should have no annotations due to parse error
      assert event1.data.metadata.transaction_annotations == nil

      # Second event should have valid annotations
      assert event2.data.metadata.transaction_annotations == %{"username" => "leto"}
    end

    # Postgres quirk - the logical decoding process does not distinguish between an empty array and an array with an empty string.
    # https://chatgpt.com/share/6707334f-0978-8006-8358-ec2300d759a4
    test "array fields with empty string are returned as empty list", %{event_character_consumer: consumer} do
      # Insert a character with an array containing an empty string
      CharacterFactory.insert_character!([tags: [""]], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      %{data: data} = consumer_event

      # Check that the tags field contains an empty string
      # Postgres quirk - the logical decoding slot will return `{}` for both `{}` and `{""}`.
      assert data.record["tags"] == [], "Expected array with empty string, got: #{inspect(data.record["tags"])}"
    end

    test "array fields are updated correctly from non-empty to empty", %{event_character_ident_consumer: consumer} do
      # Insert a character with a non-empty array field
      character = CharacterFactory.insert_character_ident_full!([tags: ["tag1", "tag2"]], repo: UnboxedRepo)

      await_messages(1)

      # Update the character with an empty array
      UnboxedRepo.update!(Ecto.Changeset.change(character, tags: []))

      # Wait for the message to be handled and fetch consumer events
      await_messages(1)
      events = list_messages(consumer)
      update_event = Enum.find(events, &(&1.data.action == :update))

      # Assert the consumer event details
      %{data: data} = update_event

      # Check that the changes field shows the previous non-empty array
      assert data.changes["tags"] == ["tag1", "tag2"],
             "Expected non-empty array in changes, got: #{inspect(data.changes["tags"])}"

      # Check that the record field shows the new empty array
      assert data.record["tags"] == [], "Expected empty array in record, got: #{inspect(data.record["tags"])}"
    end

    test "changes to partitioned tables are replicated", %{test_event_log_partitioned_consumer: consumer} do
      # Insert a record into the partitioned table
      TestEventLogFactory.insert_test_event_log_partitioned!(
        [
          seq: 1,
          source_table_name: "test_table",
          action: "insert",
          record: %{"field1" => "test value"}
        ],
        repo: UnboxedRepo
      )

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == TestEventLogPartitioned.table_oid()
    end

    @tag start_opts: [
           heartbeat_interval: 1,
           slot_producer_opts: [ack_interval: 5, restart_wal_cursor_update_interval: 5]
         ]
    test "replication slot advances even when database is idle", %{source_db: db} do
      {:ok, init_lsn} = Postgres.confirmed_flush_lsn(db, replication_slot())
      assert_lsn_progress(init_lsn, db)
    end

    @tag start_opts: [
           heartbeat_interval: to_timeout(minute: 1),
           slot_producer_opts: [ack_interval: 5, restart_wal_cursor_update_interval: 5]
         ]
    test "replication slot advances after insert", %{source_db: db, event_character_consumer: consumer} do
      {:ok, init_lsn} = Postgres.confirmed_flush_lsn(db, replication_slot())

      CharacterFactory.insert_character!([], repo: UnboxedRepo)

      assert_lsn_progress(init_lsn, db)
      [consumer_event] = receive_messages(consumer, 1)
      commit_lsn = consumer_event.commit_lsn

      assert {:ok, ^commit_lsn} = Postgres.confirmed_flush_lsn(db, replication_slot())
    end

    @tag :jepsen
    @tag capture_log: true
    test "batch updates are delivered in sequence order via HTTP", %{sup: sup} do
      {:ok, pid} = SimpleHttpServer.start_link(%{caller: self()})

      on_exit(fn ->
        try do
          GenServer.stop(pid)
        catch
          _, _ -> :ok
        end
      end)

      ref = make_ref()
      initial_seq = 0
      event = [seq: initial_seq, source_table_schema: inspect(ref)]
      event = TestEventLogFactory.insert_test_event_log_partitioned!(event, repo: UnboxedRepo)

      transactions_count = Application.get_env(:sequin, :jepsen_transactions_count)
      transaction_queries_count = Application.get_env(:sequin, :jepsen_transaction_queries_count)

      Enum.reduce(1..transactions_count, initial_seq, fn _, seq ->
        {events, seq} =
          Enum.reduce(1..transaction_queries_count, {[], seq}, fn _, {acc, seq} ->
            seq = seq + 1
            {[%{seq: seq} | acc], seq}
          end)

        TestEventLogFactory.update_test_event_log_partitioned!(event, Enum.reverse(events), repo: UnboxedRepo)
        seq
      end)

      assert wait_and_validate_data(inspect(ref), -1, transactions_count * transaction_queries_count)
      stop_supervised!(sup)
    end
  end

  describe "PostgresReplication end-to-end with http push" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: account_id,
          status: :active
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "consumer",
          type: :http_push,
          status: :active,
          partition_count: 1,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: Character.table_oid(),
              group_column_attnums: [Character.column_attnum("id")]
            )
          ]
        )

      consumer = Repo.preload(consumer, :postgres_database)

      %{consumer: consumer, pg_replication: pg_replication}
    end

    test "messages are successfully delivered to HTTP", %{consumer: consumer, pg_replication: pg_replication} do
      test_pid = self()

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} =
        Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: test_pid, req_opts: [adapter: adapter])

      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:http_request, req}, 500
      assert to_string(req.body) =~ "Characters"
      assert to_string(req.body) =~ "insert"
      assert to_string(req.body) =~ to_string(character.id)

      assert_receive {SinkPipeline, :ack_finished, [_ack_id], []}, 500

      assert [] == list_messages(consumer)
    end

    @tag capture_log: true
    test "failed messages are written to disk", %{consumer: consumer, pg_replication: pg_replication} do
      test_pid = self()

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 500)}
      end

      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} =
        Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: test_pid, req_opts: [adapter: adapter])

      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:http_request, req}, 600
      assert to_string(req.body) =~ "Characters"
      assert to_string(req.body) =~ "insert"
      assert to_string(req.body) =~ to_string(character.id)

      assert_receive {SinkPipeline, :ack_finished, [], [_ack_id]}, 600

      assert [failed_message] = list_messages(consumer)
      assert failed_message.deliver_count == 1
    end
  end

  describe "replication in isolation" do
    setup do
      # Create a real account and configured database for isolation tests
      account = AccountsFactory.insert_account!()

      postgres_database =
        DatabasesFactory.insert_configured_postgres_database!(
          account_id: account.id,
          pg_major_version: 17
        )

      ConnectionCache.cache_connection(postgres_database, UnboxedRepo)

      # Create a real PostgresReplicationSlot
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: postgres_database.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          status: :active
        )

      test_pid = self()

      stub(MessageHandlerMock, :before_handle_messages, fn _ctx, _msgs -> :ok end)
      stub(MessageHandlerMock, :put_high_watermark_wal_cursor, fn _id, _cursor -> :ok end)

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:changes, msgs})
        {:ok, 0}
      end)

      {:ok, %{postgres_database: postgres_database, pg_replication: pg_replication}}
    end

    test "changes are buffered in the WAL, even if the listener is not up", %{pg_replication: pg_replication} do
      record =
        []
        |> CharacterFactory.insert_character!(repo: UnboxedRepo)
        |> Sequin.Map.from_ecto()
        |> Sequin.Map.stringify_keys()

      start_replication!(pg_replication, slot_processor_opts: [message_handler_module: MessageHandlerMock])
      assert_receive {:changes, [change]}, to_timeout(second: 5)

      assert action?(change, :insert), "Expected change to be an insert, got: #{inspect(change)}"

      assert fields_equal?(change.fields, record)

      assert change.table_name == "Characters"
      assert change.table_schema == "public"
    end

    test "changes in a transaction are buffered then delivered to message handler in order", %{
      pg_replication: pg_replication
    } do
      start_replication!(pg_replication, slot_processor_opts: [message_handler_module: MessageHandlerMock])

      # Create three characters in sequence
      UnboxedRepo.transaction(fn ->
        CharacterFactory.insert_character!([name: "Paul Atreides"], repo: UnboxedRepo)
        CharacterFactory.insert_character!([name: "Leto Atreides"], repo: UnboxedRepo)
        CharacterFactory.insert_character!([name: "Chani"], repo: UnboxedRepo)
      end)

      changes = receive_messages_from_mock(3)
      [insert1, insert2, insert3] = changes

      # Assert seq values increase within transaction
      assert insert1.commit_idx == 0
      assert insert2.commit_idx == 1
      assert insert3.commit_idx == 2

      assert action?(insert1, :insert)
      assert get_field_value(insert1.fields, "name") == "Paul Atreides"

      assert action?(insert2, :insert)
      assert get_field_value(insert2.fields, "name") == "Leto Atreides"

      assert action?(insert3, :insert)
      assert get_field_value(insert3.fields, "name") == "Chani"

      # Insert another character
      CharacterFactory.insert_character!([name: "Duncan Idaho"], repo: UnboxedRepo)

      assert_receive {:changes, [insert4]}, to_timeout(second: 1)
      # commit_idx resets but seq should be higher than previous transaction
      assert insert4.commit_lsn > insert3.commit_lsn
      assert insert4.commit_idx == 0
    end

    @tag capture_log: true
    test "changes are delivered at least once", %{pg_replication: pg_replication} do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
        raise "Simulated crash"
      end)

      start_replication!(pg_replication, slot_processor_opts: [message_handler_module: MessageHandlerMock])

      record =
        []
        |> CharacterFactory.insert_character!(repo: UnboxedRepo)
        |> Sequin.Map.from_ecto()
        |> Sequin.Map.stringify_keys()

      assert_receive {:change, _}, to_timeout(second: 1)

      stop_replication!(pg_replication)

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
        {:ok, length(msgs)}
      end)

      start_replication!(pg_replication, slot_processor_opts: [message_handler_module: MessageHandlerMock])

      assert_receive {:change, [change]}, to_timeout(second: 1)
      assert action?(change, :insert)

      # Should have received the record (it was re-delivered)
      assert fields_equal?(change.fields, record)
    end

    test "creates, updates, and deletes are captured", %{pg_replication: pg_replication} do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
        {:ok, length(msgs)}
      end)

      start_replication!(pg_replication, slot_processor_opts: [message_handler_module: MessageHandlerMock])

      # Test create
      character = CharacterFactory.insert_character_ident_full!([planet: "Caladan"], repo: UnboxedRepo)
      record = character |> Sequin.Map.from_ecto() |> Sequin.Map.stringify_keys()

      assert_receive {:change, [create_change]}, to_timeout(second: 1)
      assert action?(create_change, :insert)
      assert is_integer(create_change.commit_lsn)
      assert create_change.commit_idx == 0

      assert fields_equal?(create_change.fields, record)
      assert create_change.action == :insert

      # Test update
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))
      record = Map.put(record, "planet", "Arrakis")

      assert_receive {:change, [update_change]}, to_timeout(second: 1)
      assert action?(update_change, :update)
      assert update_change.commit_lsn > create_change.commit_lsn
      assert update_change.commit_idx == 0

      assert fields_equal?(update_change.fields, record)
      refute is_nil(update_change.old_fields)
      assert Enum.find(update_change.old_fields, &(&1.column_name == "planet")).value == "Caladan"
      assert update_change.action == :update

      # Test delete
      UnboxedRepo.delete!(character)

      assert_receive {:change, [delete_change]}, to_timeout(second: 1)
      assert action?(delete_change, :delete)
      assert delete_change.commit_lsn > update_change.commit_lsn
      assert delete_change.commit_idx == 0

      assert fields_equal?(delete_change.old_fields, record)
      assert delete_change.action == :delete
    end

    @tag capture_log: true
    test "messages are processed exactly once, even after crash and reboot", %{pg_replication: pg_replication} do
      start_replication!(pg_replication, slot_processor_opts: [message_handler_module: MessageHandlerMock])

      # Insert a record
      character1 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:changes, [change]}, to_timeout(second: 1)
      assert action?(change, :insert)
      assert get_field_value(change.fields, "id") == character1.id

      # Insert another record
      character2 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:changes, [change]}, to_timeout(second: 1)
      assert action?(change, :insert)
      assert get_field_value(change.fields, "id") == character2.id

      # write the low watermark for character2
      Replication.put_restart_wal_cursor!(pg_replication.id, %{
        commit_lsn: change.commit_lsn,
        commit_idx: change.commit_idx
      })

      # Stop the replication - likely before the message was acked, but there is a race here
      stop_replication!(pg_replication)

      # Restart the replication
      start_replication!(pg_replication, slot_processor_opts: [message_handler_module: MessageHandlerMock])

      # Insert another record to verify replication is working
      character3 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the new message to be handled
      [change2, change3] = receive_messages_from_mock(2)

      # Verify we only get the records >= low watermark
      assert action?(change2, :insert)
      assert get_field_value(change2.fields, "id") == character2.id

      assert action?(change3, :insert)
      assert get_field_value(change3.fields, "id") == character3.id
    end

    @tag capture_log: true
    test "retries flushing when payload size limit exceeded", %{pg_replication: pg_replication} do
      test_pid = self()
      # First call succeeds, updating the last_flushed_wal_cursor
      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        {:ok, length(msgs)}
      end)

      # Start replication with our custom reconnect interval
      start_replication!(
        pg_replication,
        slot_processor_opts: [message_handler_module: MessageHandlerMock],
        slot_producer_opts: [batch_flush_interval: 50, reconnect_interval: 5]
      )

      # Insert a character to generate a message
      _character1 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      await_messages(1)

      # Second call will fail with payload_size_limit_exceeded
      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        if length(msgs) == 1 do
          send(test_pid, :sent_error)
        end

        # Return the error that should trigger disconnection
        {:error, Sequin.Error.invariant(code: :payload_size_limit_exceeded, message: "Payload size limit exceeded")}
      end)

      # Insert another character to trigger the payload size limit exceeded error
      character2 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      assert_receive :sent_error, 100

      # Last call succeeds
      expect(MessageHandlerMock, :handle_messages, fn _ctx, messages ->
        assert length(messages) == 1
        message = List.first(messages)
        send(test_pid, {:changes, [message]})
        {:ok, length(messages)}
      end)

      stub(MessageHandlerMock, :handle_messages, fn _ctx, messages ->
        {:ok, length(messages)}
      end)

      # Should process ONLY the second message
      assert_receive {:changes, [change]}, to_timeout(second: 1)
      assert get_field_value(change.fields, "id") == character2.id
    end

    @tag capture_log: true
    test "fails to start when replication slot does not exist", %{pg_replication: pg_replication} do
      # Use a non-existent slot name
      non_existent_slot = "non_existent_slot"
      pg_replication = %{pg_replication | slot_name: non_existent_slot}

      # Attempt to start replication with the non-existent slot
      test_pid = self()
      on_connect_fail = fn _, _ -> send(test_pid, :connect_fail) end

      start_replication!(
        pg_replication,
        slot_producer_opts: [on_connect_fail_fn: on_connect_fail]
      )

      assert_receive :connect_fail, 1000
    end

    test "emits heartbeat messages for latest postgres version", %{pg_replication: pg_replication} do
      # Attempt to start replication with the non-existent slot
      start_replication!(pg_replication, slot_processor_opts: [heartbeat_interval: 5])

      assert_receive {SlotProcessorServer, :heartbeat_received}, 1000
      assert_receive {SlotProcessorServer, :heartbeat_received}, 1000

      # Verify that the Health status was updated
      {:ok, health} =
        Sequin.Health.health(%PostgresReplicationSlot{id: pg_replication.id, inserted_at: DateTime.utc_now()})

      check = Enum.find(health.checks, &(&1.slug == :replication_messages))
      assert check.status == :healthy
    end

    test "emits heartbeat messages for older postgres version", %{pg_replication: pg_replication} do
      # Attempt to start replication with the non-existent slot
      start_replication!(pg_replication, slot_processor_opts: [heartbeat_interval: 5])

      assert_receive {SlotProcessorServer, :heartbeat_received}, 1000
      assert_receive {SlotProcessorServer, :heartbeat_received}, 1000

      # Verify that the Health status was updated
      {:ok, health} =
        Sequin.Health.health(%PostgresReplicationSlot{id: pg_replication.id, inserted_at: DateTime.utc_now()})

      check = Enum.find(health.checks, &(&1.slug == :replication_messages))
      assert check.status == :healthy
    end
  end

  describe "PostgresReplicationSlot end-to-end with sequences" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: account_id,
          status: :active
        )

      # Create a consumer for this replication slot (event)
      event_consumer =
        ConsumersFactory.insert_sink_consumer!(
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: Character.table_oid(),
              group_column_attnums: [Character.column_attnum("id")]
            )
          ]
        )

      # Start replication
      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{
        sup_name: sup,
        pg_replication: pg_replication,
        source_db: source_db,
        event_consumer: event_consumer
      }
    end

    test "inserts are replicated to consumer events", %{event_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      assert_maps_equal(data.record, Map.from_struct(character), ["id", "name", "house", "planet", "is_active", "tags"],
        indifferent_keys: true
      )

      assert is_nil(data.changes)
      assert data.action == :insert
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "updates are replicated to consumer events when replica identity default", %{event_consumer: consumer} do
      # Insert a character
      character =
        CharacterFactory.insert_character!([name: "Leto Atreides", house: "Atreides", planet: "Caladan"],
          repo: UnboxedRepo
        )

      # Wait for the message to be handled
      await_messages(1)
      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled and fetch consumer events
      await_messages(1)
      events = list_messages(consumer)
      update_event = Enum.find(events, &(&1.data.action == :update))

      # Assert the consumer event details
      assert update_event.consumer_id == consumer.id
      assert update_event.table_oid == Character.table_oid()
      assert update_event.record_pks == [to_string(character.id)]
      %{data: data} = update_event

      character = Repo.reload(character)

      assert data.record == %{
               "id" => character.id,
               "name" => character.name,
               "house" => character.house,
               "planet" => "Arrakis",
               "is_active" => character.is_active,
               "tags" => character.tags,
               "metadata" => character.metadata,
               "inserted_at" => character.inserted_at,
               "updated_at" => character.updated_at
             }

      assert data.changes == %{}
      assert data.action == :update
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "deletes are replicated to consumer events when replica identity default", %{event_consumer: consumer} do
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      await_messages(1)

      UnboxedRepo.delete!(character)

      await_messages(1)
      events = list_messages(consumer)
      delete_event = Enum.find(events, &(&1.data.action == :delete))

      %{data: data} = delete_event

      assert data.record == %{
               "house" => nil,
               "id" => character.id,
               "is_active" => nil,
               "name" => nil,
               "planet" => nil,
               "tags" => nil,
               "metadata" => nil,
               "inserted_at" => nil,
               "updated_at" => nil
             }

      assert data.changes == nil
      assert data.action == :delete
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "consumer fans in events from multiple tables", %{event_consumer: consumer} do
      # Attach a schema filter to the consumer
      Consumers.update_sink_consumer(consumer, %{source: %{}}, skip_lifecycle: true)

      # Restart the consumer to apply the changes
      Consumers.update_sink_consumer(consumer, %{})
      Runtime.Supervisor.refresh_message_handler_ctx(consumer.replication_slot_id)

      # Verify no consumer messages yet
      assert list_messages(consumer) == []

      # Insert characters in two different tables and wait for each to be flushed
      matching_character = CharacterFactory.insert_character!([], repo: UnboxedRepo)
      await_messages(1)

      matching_character_detailed = CharacterFactory.insert_character_detailed!([], repo: UnboxedRepo)
      await_messages(1)

      # Fetch consumer messages
      messages = list_messages(consumer)
      assert length(messages) == 2
      [consumer_message1, consumer_message2] = messages

      # Assert the consumer message details
      assert consumer_message1.consumer_id == consumer.id
      assert consumer_message2.consumer_id == consumer.id

      assert consumer_message1.table_oid == Character.table_oid()
      assert consumer_message2.table_oid == CharacterDetailed.table_oid()

      assert consumer_message1.record_pks == [to_string(matching_character.id)]
      assert consumer_message2.record_pks == [to_string(matching_character_detailed.id)]
    end

    test "empty array fields are replicated correctly", %{event_consumer: consumer} do
      # Insert a character with an empty array field
      character = CharacterFactory.insert_character!([tags: []], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      # Check that the tags field is an empty list, not [""]
      assert data.record["tags"] == [], "Expected empty array, got: #{inspect(data.record["tags"])}"
    end

    # Postgres quirk - the logical decoding process does not distinguish between an empty array and an array with an empty string.
    # https://chatgpt.com/share/6707334f-0978-8006-8358-ec2300d759a4
    test "array fields with empty string are returned as empty list", %{event_consumer: consumer} do
      # Insert a character with an array containing an empty string
      CharacterFactory.insert_character!([tags: [""]], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      %{data: data} = consumer_event

      # Check that the tags field contains an empty string
      # Postgres quirk - the logical decoding slot will return `{}` for both `{}` and `{""}`.
      assert data.record["tags"] == [], "Expected array with empty string, got: #{inspect(data.record["tags"])}"
    end

    @tag capture_log: true
    test "schema changes are detected and database update worker is enqueued", %{
      pg_replication: pg_replication,
      sup_name: sup,
      source_db: source_db
    } do
      on_exit(fn ->
        UnboxedRepo.query!(
          "alter table #{Character.quoted_table_name()} drop column if exists test_column_for_schema_change",
          []
        )
      end)

      do_migration = fn ->
        UnboxedRepo.query!(
          "alter table #{Character.quoted_table_name()} drop column if exists test_column_for_schema_change",
          []
        )

        UnboxedRepo.query!(
          "alter table #{Character.quoted_table_name()} add column if not exists test_column_for_schema_change text",
          []
        )
      end

      do_migration.()

      # Insert a record to trigger WAL processing
      CharacterFactory.insert_character!([], repo: UnboxedRepo)

      await_messages(1)

      # Verify that DatabaseUpdateWorker was enqueued with the correct args
      assert_enqueued(worker: DatabaseUpdateWorker, args: %{postgres_database_id: source_db.id})

      GenServer.stop(sup)

      do_migration.()

      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      await_messages(1)

      # Verify that DatabaseUpdateWorker was enqueued with the correct args
      # twice now
      [_, _] = all_enqueued(worker: DatabaseUpdateWorker, args: %{postgres_database_id: source_db.id})
    end
  end

  describe "PostgresReplicationSlot end-to-end with sequences for characters_detailed" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      Mox.stub(RedisMock, :send_messages, fn _sink, _redis_messages ->
        :ok
      end)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          status: :active
        )

      # Create a consumer for this replication slot (record)
      consumer =
        ConsumersFactory.insert_sink_consumer!(
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: CharacterDetailed.table_oid(),
              group_column_attnums: [CharacterDetailed.column_attnum("id")]
            )
          ]
        )

      # Start replication
      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{
        source_db: source_db,
        consumer: consumer
      }
    end

    test "columns flow through properly", %{consumer: consumer} do
      # Insert a character with specific enum value, daterange and domain type
      character =
        CharacterFactory.insert_character_detailed!(
          [
            status: :retired,
            active_period: [~D[2010-01-01], ~D[2020-12-31]],
            # It's over 9000!
            power_level: 9001,
            embedding: [1.0, 2.0, 3.0]
          ],
          repo: UnboxedRepo
        )

      # Fetch consumer records
      [message] = receive_messages(consumer, 1)

      # Assert the consumer record details
      assert message.consumer_id == consumer.id
      assert message.table_oid == CharacterDetailed.table_oid()
      assert message.record_pks == [to_string(character.id)]
      assert message.data.record["status"] == "retired"
      assert message.data.record["active_period"] == "[2010-01-01,2020-12-31)"
      assert message.data.record["power_level"] == 9001
      assert message.data.record["embedding"] == [1.0, 2.0, 3.0]
    end
  end

  describe "backfill and replication parity" do
    setup ctx do
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)
      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      {:ok, tables} = Databases.tables(source_db)
      table_name = ctx[:table] || "all_column_types"
      table = Sequin.Enum.find!(tables, &(&1.name == table_name))
      attnums = Map.new(table.columns, &{&1.name, &1.attnum})
      table = %{table | sort_column_attnum: attnums["updated_at"]}

      Mox.stub(RedisMock, :send_messages, fn _sink, _redis_messages -> :ok end)

      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          status: :active
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: table.oid,
              group_column_attnums:
                if(ctx[:group_columns],
                  do: Enum.map(ctx.group_columns, &attnums[&1]),
                  else: table.columns |> Enum.filter(& &1.is_pk?) |> Enum.map(& &1.attnum)
                )
            )
          ]
        )

      consumer = Repo.preload(consumer, [:filter, replication_slot: :postgres_database])

      backfill =
        ConsumersFactory.insert_active_backfill!(
          account_id: account_id,
          sink_consumer_id: consumer.id,
          table_oid: table.oid,
          sort_column_attnum: table.sort_column_attnum,
          initial_min_cursor: KeysetCursor.min_cursor(table, ~N[1970-01-01 00:00:00])
        )

      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))
      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{source_db: source_db, table: table, consumer: consumer, backfill: backfill}
    end

    test "a backfilled row is identical to the same row streamed from the WAL", ctx do
      %{rows: [[populated_id]]} =
        UnboxedRepo.query!(~S"""
        insert into all_column_types (
          c_bool, c_int2, c_int4, c_int8, c_numeric, c_numeric_scaled, c_numeric_nan,
          c_float4, c_float8, c_float8_special, c_money, c_char, c_varchar, c_text, c_name, c_qchar,
          c_citext, c_bytea, c_date, c_date_infinity, c_time, c_timetz, c_timestamp,
          c_timestamp_infinity, c_timestamptz, c_interval, c_uuid, c_json, c_jsonb, c_inet, c_cidr,
          c_macaddr, c_macaddr8, c_bit, c_varbit, c_point, c_line, c_lseg, c_box, c_path, c_polygon,
          c_circle, c_tsvector, c_tsquery, c_xml, c_pg_lsn, c_oid, c_regclass, c_int4range,
          c_int8range, c_numrange, c_tsrange, c_tstzrange, c_daterange, c_empty_range,
          c_unbounded_range, c_int4multirange, c_hstore, c_vector, c_halfvec, c_sparsevec, c_enum,
          c_domain, c_composite, c_composite_tricky, c_bool_arr, c_int4_arr, c_int4_2d_arr,
          c_int8_arr, c_numeric_arr, c_float8_arr, c_text_arr, c_varchar_arr, c_bytea_arr,
          c_date_arr, c_time_arr, c_timetz_arr, c_timestamp_arr, c_timestamptz_arr, c_interval_arr, c_uuid_arr,
          c_json_arr, c_jsonb_arr, c_inet_arr, c_money_arr, c_xml_arr, c_int4range_arr, c_enum_arr,
          c_domain_arr, c_composite_arr
        ) values (
          true, -32768, 2147483647, 9223372036854775807, 12345678901234567890.123456789, 3.14159, 'NaN',
          1.5, -2.25e-10, 'Infinity', 1234.56, 'ab', 'hello, world', E'line\nbreak "quoted" \\ back',
          'a_name', 'q', 'MiXeD', '\xdeadbeef', '2024-02-29', 'infinity', '13:45:30.123456',
          '13:45:30+05:30', '2024-02-29 13:45:30.123456', '-infinity', '2024-02-29 13:45:30.123456+00',
          '1 year 2 mons 3 days 04:05:06.789', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
          '{"b": 1, "a": [1, 2.5, null]}', '{"b": 1, "a": [1, 2.5, null]}', '192.168.0.1/24',
          '10.0.0.0/8', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05', B'1010', B'101',
          '(1.5,2)', '{1,-1,0}', '[(0,0),(1,1)]', '((0,0),(1,1))', '[(0,0),(1,1),(2,0)]',
          '((0,0),(1,1),(2,0))', '<(1,1),5>', 'the quick brown fox', 'quick & fox',
          '<hobbit name="Frodo"/>', '16/B374D848', 12345, 'all_column_types', '[1,10)',
          '[1,9223372036854775807)', '(1.5,2.5]', '[2024-01-01 00:00,2024-12-31 23:59)',
          '[2024-01-01 00:00+00,2024-12-31 23:59+00)', '[2024-01-01,2024-12-31)', 'empty', '[5,)',
          '{[1,3), [5,8)}', 'a=>1, "b c"=>NULL', '[1,2,3]', '[1.5,2.5,3.5]', '{1:1.5,3:2}/5',
          'retired', 9001, row('EUR', 42.00), row('1 Bag End, "Hobbiton"', null, 3),
          '{t,f,NULL}', '{1,NULL,3}', '{{1,2},{3,4}}', '{9223372036854775807}', '{1.5,NaN,Infinity,-Infinity}',
          '{1.5,-Infinity,NaN}', '{"a,b","c\"d",NULL,"NULL",""}', '{x,y}', '{"\\xdead","\\xbeef"}',
          '{2024-02-29,infinity,-infinity}', '{13:45:30,23:59:59.999999}', '{13:45:30.5-03,00:30:00+01}', '{"2024-02-29 13:45:30.5",infinity}',
          '{"2024-02-29 13:45:30.5+00"}', '{"1 day","-04:05:06"}',
          '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}', E'{"{\\"a\\": 1}"}', E'{"{\\"a\\": [1, 2]}"}',
          '{10.0.0.1,::1}', '{1.50,2}', '{"<a/>"}', '{"[1,2)","empty"}', '{active,retired}',
          '{1,9001}', array[row('EUR', 1.50), row('U,S"D', 2)]::money_with_currency[]
        )
        returning id
        """)

      %{rows: [[null_id]]} = UnboxedRepo.query!("insert into all_column_types default values returning id")

      streamed = Map.new(receive_messages(ctx.consumer, 2), &{&1.data.record["id"], &1.data.record})

      {:ok, %{messages: backfilled}} =
        TableReader.fetch_batch(
          ctx.source_db,
          ctx.consumer,
          ctx.backfill,
          ctx.table,
          ctx.backfill.initial_min_cursor,
          include_min: true
        )

      backfilled = Map.new(backfilled, &{&1.data.record["id"], &1.data.record})

      assert streamed |> Map.keys() |> Enum.sort() == Enum.sort([populated_id, null_id])
      assert backfilled |> Map.keys() |> Enum.sort() == Enum.sort([populated_id, null_id])

      # Guard against both paths agreeing by both dropping a value
      assert for({column, nil} <- streamed[populated_id], do: column) == []
      assert for({column, value} <- streamed[null_id], column not in ["id", "updated_at"], value != nil, do: column) == []

      mismatches =
        for {id, streamed_record} <- streamed,
            {column, streamed_value} <- streamed_record,
            backfilled[id][column] !== streamed_value do
          {id, column, backfilled: backfilled[id][column], streamed: streamed_value}
        end

      assert mismatches == [], """
      backfill and replication disagree on #{length(mismatches)} column(s):
      #{Enum.map_join(Enum.sort(mismatches), "\n", &inspect(&1, limit: :infinity, printable_limit: :infinity))}
      """

      for record <- Map.values(streamed) ++ Map.values(backfilled), do: Jason.encode!(record)
    end

    @tag group_columns:
           ~w(c_timestamp c_timestamptz c_date c_time c_numeric c_text c_jsonb c_int4_arr c_bool c_uuid c_float8)
    test "a message's group is the same live, backfilled, and reloaded from persistence", ctx do
      UnboxedRepo.query!(~S"""
      insert into all_column_types
        (c_timestamp, c_timestamptz, c_date, c_time, c_numeric, c_text, c_jsonb, c_int4_arr, c_bool, c_uuid, c_float8)
      values
        ('2024-02-29 13:45:30.5', '2024-02-29 13:45:30+00', '2024-02-29', '13:45:30', 0.00000001, null,
         '{"b": 1, "a": [1, 2]}', '{65,66}', true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1.5)
      """)

      [streamed] = receive_messages(ctx.consumer, 1)

      {:ok, %{messages: [backfilled]}} =
        TableReader.fetch_batch(
          ctx.source_db,
          ctx.consumer,
          ctx.backfill,
          ctx.table,
          ctx.backfill.initial_min_cursor,
          include_min: true
        )

      # Messages are persisted when a consumer's store overflows, and reloaded on restart
      {:ok, 1} = Consumers.upsert_consumer_messages(ctx.consumer, [streamed])

      [reloaded] =
        ctx.consumer
        |> Consumers.stream_consumer_messages_for_consumer()
        |> Enum.map(&Consumers.put_dynamic_fields(&1, ctx.consumer, ctx.source_db))

      assert streamed.group_id ==
               Enum.join(
                 [
                   "2024-02-29 13:45:30.500000",
                   "2024-02-29 13:45:30.000000Z",
                   "2024-02-29",
                   "13:45:30.000000",
                   "1E-8",
                   "",
                   ~s({"a":[1,2],"b":1}),
                   "[65,66]",
                   "true",
                   "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                   "1.5"
                 ],
                 ":"
               )

      assert backfilled.group_id == streamed.group_id
      assert reloaded.group_id == streamed.group_id
    end

    test "an update that leaves a TOASTed value unchanged carries the value", ctx do
      # Large and incompressible, so Postgres stores it out of line and, under the default replica
      # identity, replicates an update that doesn't touch it as "unchanged"
      %{rows: [[id, toasted]]} =
        UnboxedRepo.query!("""
        insert into all_column_types (c_text, c_int4)
        select string_agg(md5(random()::text), '') , 1 from generate_series(1, 400)
        returning id, c_text
        """)

      UnboxedRepo.query!("update all_column_types set c_int4 = 2 where id = $1", [id])

      [insert, update] = ctx.consumer |> receive_messages(2) |> Enum.sort_by(& &1.commit_lsn)

      assert insert.data.record["c_text"] == toasted
      assert update.data.action == :update
      assert update.data.record["c_int4"] == 2
      assert update.data.record["c_text"] == toasted
    end

    test "an unchanged TOASTed value of a row deleted since is left as is, without failing", ctx do
      %{rows: [[id]]} =
        UnboxedRepo.query!("""
        insert into all_column_types (c_text, c_int4)
        select string_agg(md5(random()::text), ''), 1 from generate_series(1, 400)
        returning id
        """)

      # Committed together, so the row is gone by the time the update is processed
      UnboxedRepo.transaction(fn ->
        UnboxedRepo.query!("update all_column_types set c_int4 = 2 where id = $1", [id])
        UnboxedRepo.query!("delete from all_column_types where id = $1", [id])
      end)

      messages = ctx.consumer |> receive_messages(3) |> Enum.sort_by(&{&1.commit_lsn, &1.commit_idx})

      assert [:insert, :update, :delete] = Enum.map(messages, & &1.data.action)
      assert Enum.at(messages, 1).data.record["c_text"] == :unchanged_toast
    end

    @tag table: "pk_types"
    test "a backfilled row has the same identity as the same row streamed from the WAL", ctx do
      UnboxedRepo.query!(~S"""
      insert into pk_types (pk_timestamp, pk_timestamptz, pk_bool, pk_numeric, name)
      values ('2024-02-29 13:45:30.5', '2024-02-29 13:45:30+00', true, 0.00000001, 'Frodo')
      """)

      [streamed] = receive_messages(ctx.consumer, 1)

      {:ok, %{messages: [backfilled]}} =
        TableReader.fetch_batch(
          ctx.source_db,
          ctx.consumer,
          ctx.backfill,
          ctx.table,
          ctx.backfill.initial_min_cursor,
          include_min: true
        )

      {:ok, %{pks: pks}} =
        TableReader.fetch_batch_pks(ctx.source_db, ctx.table, ctx.backfill.initial_min_cursor, include_min: true)

      # The WAL side uses Postgres's text for each key column
      assert streamed.record_pks == ["2024-02-29 13:45:30.5", "2024-02-29 13:45:30+00", "t", "0.00000001"]

      # Backfilled messages are only emitted if fetch_batch_pks/4 found the same key
      assert pks == [streamed.record_pks]
      assert backfilled.record_pks == streamed.record_pks
      # Conflict detection and ordering between backfill and CDC key off the group
      assert backfilled.group_id == streamed.group_id
      assert backfilled.data.record == streamed.data.record
    end
  end

  describe "PostgresReplicationSlot with no attached consumers/message stores" do
    setup ctx do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: account_id,
          status: :active
        )

      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))
      start_opts = ctx |> Map.get(:start_opts, []) |> Keyword.put(:test_pid, self())
      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, start_opts)

      %{
        sup: sup,
        pg_replication: pg_replication,
        source_db: source_db
      }
    end

    @tag start_opts: [
           heartbeat_interval: 1,
           slot_producer_opts: [ack_interval: 5, restart_wal_cursor_update_interval: 5]
         ]
    test "replication slot advances even when database is idle", %{source_db: db} do
      {:ok, init_lsn} = Postgres.confirmed_flush_lsn(db, replication_slot())
      assert_lsn_progress(init_lsn, db)
    end

    @tag start_opts: [
           heartbeat_interval: to_timeout(minute: 1),
           slot_producer_opts: [ack_interval: 5, restart_wal_cursor_update_interval: 5]
         ]
    test "replication slot advances after insert", %{source_db: db} do
      {:ok, init_lsn} = Postgres.confirmed_flush_lsn(db, replication_slot())

      CharacterFactory.insert_character!([], repo: UnboxedRepo)

      assert_lsn_progress(init_lsn, db)
    end
  end

  defp start_replication!(replication_slot, opts) do
    {slot_processor_opts, opts} = Keyword.pop(opts, :slot_processor_opts, [])
    # Merge supervisor options
    supervisor_opts =
      Keyword.merge(
        [
          replication_slot: replication_slot,
          slot_processor_opts: Keyword.merge([message_handler_module: MessageHandlerMock], slot_processor_opts),
          test_pid: self()
        ],
        opts
      )

    start_supervised!({Supervisor, supervisor_opts})
  end

  defp action?(change, action) do
    is_struct(change, Message) and change.action == action
  end

  defp stop_replication!(pg_replication) do
    # Stop the supervisor using its via_tuple
    stop_supervised!(Supervisor.via_tuple(pg_replication.id))
    # Wait for :syn to clean up the process registration asynchronously
    wait_for_syn_cleanup({Supervisor, pg_replication.id})
  end

  defp wait_for_syn_cleanup(key, attempts \\ 50) do
    if :syn.lookup(:replication, key) == :undefined do
      :ok
    else
      if attempts > 0 do
        Process.sleep(10)
        wait_for_syn_cleanup(key, attempts - 1)
      else
        raise "Timed out waiting for :syn to clean up registration for #{inspect(key)}"
      end
    end
  end

  # defp config do
  #   :sequin
  #   |> Application.get_env(Sequin.Repo)
  #   |> Keyword.take([:username, :password, :hostname, :database, :port])
  # end

  # Helper functions

  defp get_field_value(fields, column_name) do
    Enum.find_value(fields, fn %{column_name: name, value: value} ->
      if name == column_name, do: value
    end)
  end

  defp fields_equal?(fields, record) do
    record_fields = Enum.map(record, fn {key, value} -> %{column_name: key, value: value} end)

    assert_lists_equal(fields, record_fields, fn field1, field2 ->
      if field1.column_name == "updated_at" and field2.column_name == "updated_at" do
        # updated_at timestamp may not be exactly the same
        NaiveDateTime.diff(field1.value, field2.value, :second) < 2
      else
        field1.column_name == field2.column_name && field1.value == field2.value
      end
    end)
  end

  defp await_messages(count, acc_count \\ 0) do
    assert_receive {SlotProcessorServer, :flush_messages, flush_count}, 1_000
    acc_count = acc_count + flush_count

    case count - acc_count do
      0 -> :ok
      total when total < 0 -> flunk("Expected #{count} messages but got #{acc_count}")
      _ -> await_messages(count, acc_count)
    end
  rescue
    err ->
      case err do
        %ExUnit.AssertionError{message: "Assertion failed, no matching message after" <> _rest} ->
          flunk("Did not receive remaining #{count - acc_count} messages")

        err ->
          reraise err, __STACKTRACE__
      end
  end

  defp receive_messages(consumer, count) do
    await_messages(count)

    list_messages(consumer)
  end

  defp receive_messages_from_mock(count, acc \\ []) do
    assert_receive {:changes, changes}, 1_000
    acc = acc ++ changes

    case count - length(acc) do
      0 -> acc
      total when total < 0 -> flunk("Expected #{count} messages but got #{length(acc)}")
      _ -> receive_messages_from_mock(count, acc)
    end
  rescue
    err ->
      case err do
        %ExUnit.AssertionError{message: "Assertion failed, no matching message after" <> _rest} ->
          flunk("Did not receive remaining #{count - length(acc)} messages")

        err ->
          reraise err, __STACKTRACE__
      end
  end

  defp list_messages(consumer) do
    SlotMessageStore.peek_messages(consumer, 1000)
  end

  @spec wait_and_validate_data(binary(), non_neg_integer(), non_neg_integer()) :: any()
  defp wait_and_validate_data(_, _, 0), do: true

  defp wait_and_validate_data(ref, prev_seq, expected_count) do
    receive do
      %{"action" => "update", "record" => %{"source_table_schema" => ^ref, "seq" => current_seq}} ->
        if current_seq > prev_seq,
          do: wait_and_validate_data(ref, current_seq, expected_count - 1),
          else: flunk("Received message with seq #{current_seq} which is less than prev_seq #{prev_seq}")

      _ ->
        wait_and_validate_data(ref, prev_seq, expected_count)
    after
      5_000 ->
        flunk("Did not receive :simple_http_server_loaded within 5 seconds")
    end
  end

  defp assert_lsn_progress(init_lsn, db) do
    # Verify LSN progressed after heartbeats
    assert_eventually(
      (fn ->
         case Postgres.confirmed_flush_lsn(db, replication_slot()) do
           {:ok, current_lsn} ->
             if current_lsn > init_lsn do
               current_lsn
             else
               false
             end

           error ->
             false
         end
       end).(),
      1000
    )
  end
end
