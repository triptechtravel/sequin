defmodule Sequin.Runtime.DatabaseLifecycleEventWorkerTest do
  use Sequin.DataCase, async: false

  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.DatabaseLifecycleEventWorker

  describe "deleting a database" do
    test "closes its cached connection" do
      db = DatabasesFactory.insert_configured_postgres_database!()
      ReplicationFactory.insert_postgres_replication!(account_id: db.account_id, postgres_database_id: db.id)

      # A real connection to the database, as replication and backfills open
      {:ok, conn} = ConnectionCache.connection(db)
      ref = Process.monitor(conn)

      :ok = Databases.delete_db_with_replication_slot(db)
      assert_enqueued(worker: DatabaseLifecycleEventWorker, args: %{event: "delete", entity_id: db.id})
      assert %{failure: 0, discard: 0} = Oban.drain_queue(queue: :lifecycle)

      assert_receive {:DOWN, ^ref, :process, ^conn, _reason}, 1000
      assert {:error, _not_found} = ConnectionCache.existing_connection(db)
    end
  end
end
