defmodule Sequin.Sinks.PostgresSinkTest do
  # Destination tables are created outside the sandbox, as the sink writes through its own
  # connections
  use Sequin.DataCase, async: false

  alias Sequin.Consumers.PostgresSink
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Sinks.Postgres.Client
  alias Sequin.Test.UnboxedRepo

  @schema "postgres_sink_test"

  setup do
    UnboxedRepo.query!("drop schema if exists #{@schema} cascade")
    UnboxedRepo.query!("create schema #{@schema}")
    on_exit(fn -> UnboxedRepo.query!("drop schema if exists #{@schema} cascade") end)

    sink = sink()
    {:ok, conn} = Client.connection(sink)
    %{sink: sink, conn: conn}
  end

  defp sink(attrs \\ []) do
    %{
      type: :postgres,
      host: "localhost",
      port: 5432,
      database: "sequin_test",
      username: "postgres",
      password: "postgres",
      routing_mode: :static,
      table_name: "#{@schema}.t"
    }
    |> Map.merge(Map.new(attrs))
    |> then(&PostgresSink.changeset(%PostgresSink{}, &1))
    |> Ecto.Changeset.apply_action!(:insert)
  end

  defp rows(table, order_by \\ "1") do
    List.flatten(UnboxedRepo.query!("select to_jsonb(t) from #{@schema}.#{table} t order by #{order_by}").rows)
  end

  describe "upsert/3 and delete/3" do
    test "insert, update and delete by primary key", %{conn: conn} do
      UnboxedRepo.query!("create table #{@schema}.t (id int primary key, name text, tags text[], meta jsonb)")
      {:ok, table} = Client.fetch_table(conn, "#{@schema}.t")

      :ok =
        Client.upsert(conn, table, [
          %{"id" => 1, "name" => "a", "tags" => ["x", "NULL"], "meta" => %{"k" => [1]}},
          %{"id" => 2, "name" => "b", "tags" => [], "meta" => nil}
        ])

      :ok = Client.upsert(conn, table, [%{"id" => 1, "name" => "a2", "tags" => nil, "meta" => %{}}])
      :ok = Client.delete(conn, table, [%{"id" => 2}])

      assert rows("t") == [%{"id" => 1, "name" => "a2", "tags" => nil, "meta" => %{}}]
    end

    test "the last of several rows with the same key wins", %{conn: conn} do
      UnboxedRepo.query!("create table #{@schema}.t (id int primary key, name text)")
      {:ok, table} = Client.fetch_table(conn, "#{@schema}.t")

      :ok =
        Client.upsert(conn, table, [
          %{"id" => 1, "name" => "first"},
          %{"id" => 2, "name" => "other"},
          %{"id" => 1, "name" => "last"}
        ])

      assert rows("t") == [%{"id" => 1, "name" => "last"}, %{"id" => 2, "name" => "other"}]
    end

    test "writes only columns both sides have, leaving the rest as they are", %{conn: conn} do
      UnboxedRepo.query!("create table #{@schema}.t (id int primary key, name text, local_note text)")
      UnboxedRepo.query!("insert into #{@schema}.t values (1, 'old', 'kept')")
      {:ok, table} = Client.fetch_table(conn, "#{@schema}.t")

      # "source_only" isn't in the destination; "local_note" isn't in the row
      :ok = Client.upsert(conn, table, [%{"id" => 1, "name" => "new", "source_only" => "ignored"}])

      assert rows("t") == [%{"id" => 1, "name" => "new", "local_note" => "kept"}]
    end

    test "keeps the value of a column replication couldn't supply", %{conn: conn} do
      UnboxedRepo.query!("create table #{@schema}.t (id int primary key, big text, n int)")
      UnboxedRepo.query!("insert into #{@schema}.t values (1, 'large value', 1)")
      {:ok, table} = Client.fetch_table(conn, "#{@schema}.t")

      :ok = Client.upsert(conn, table, [%{"id" => 1, "big" => :unchanged_toast, "n" => 2}])

      assert rows("t") == [%{"id" => 1, "big" => "large value", "n" => 2}]
    end

    test "mirrors identity values and skips generated columns", %{conn: conn} do
      UnboxedRepo.query!("""
      create table #{@schema}.t (
        id int generated always as identity primary key,
        price numeric,
        doubled numeric generated always as (price * 2) stored
      )
      """)

      {:ok, table} = Client.fetch_table(conn, "#{@schema}.t")

      :ok = Client.upsert(conn, table, [%{"id" => 42, "price" => "1.5", "doubled" => "ignored"}])

      assert rows("t") == [%{"id" => 42, "price" => 1.5, "doubled" => 3.0}]
    end

    test "handles quoted identifiers and composite primary keys", %{conn: conn} do
      UnboxedRepo.query!(
        ~s{create table #{@schema}."Order Lines" ("Order" int, "select" text, qty int, primary key ("Order", "select"))}
      )

      {:ok, table} = Client.fetch_table(conn, ~s{#{@schema}.Order Lines})

      :ok =
        Client.upsert(conn, table, [
          %{"Order" => 1, "select" => "a", "qty" => 1},
          %{"Order" => 1, "select" => "b", "qty" => 2}
        ])

      :ok = Client.delete(conn, table, [%{"Order" => 1, "select" => "a", "qty" => 1}])

      assert rows(~s{"Order Lines"}) == [%{"Order" => 1, "select" => "b", "qty" => 2}]
    end

    test "writes intervals, which records carry as maps", %{conn: conn} do
      UnboxedRepo.query!("create table #{@schema}.t (id int primary key, i interval, ia interval[])")
      {:ok, table} = Client.fetch_table(conn, "#{@schema}.t")

      interval = %{"months" => 14, "days" => 3, "microseconds" => -14_706_000_001}
      :ok = Client.upsert(conn, table, [%{"id" => 1, "i" => interval, "ia" => [interval, nil]}])

      assert %{rows: [["1 year 2 mons 3 days -04:05:06.000001", ["1 year 2 mons 3 days -04:05:06.000001", nil]]]} =
               UnboxedRepo.query!("select i::text, ia::text[] from #{@schema}.t")
    end

    test "requires every row to carry the primary key", %{conn: conn} do
      UnboxedRepo.query!("create table #{@schema}.t (id int primary key, name text)")
      {:ok, table} = Client.fetch_table(conn, "#{@schema}.t")

      assert {:error, error} = Client.upsert(conn, table, [%{"name" => "no key"}])
      assert error.message =~ "must include its primary key (id)"
    end
  end

  describe "fetch_table/2" do
    test "explains a missing table or primary key", %{conn: conn} do
      assert {:error, error} = Client.fetch_table(conn, "#{@schema}.missing")
      assert error.message =~ "does not exist"

      UnboxedRepo.query!("create table #{@schema}.no_pk (id int)")
      assert {:error, error} = Client.fetch_table(conn, "#{@schema}.no_pk")
      assert error.message =~ "has no primary key"
    end
  end

  describe "test_connection/1" do
    test "succeeds for reachable settings and an existing table" do
      UnboxedRepo.query!("create table #{@schema}.t (id int primary key)")
      assert :ok = Client.test_connection(sink())
    end

    test "returns, rather than crashes on, bad settings" do
      # The test database trusts local connections, so a rejected login is an unknown role
      assert {:error, error} = Client.test_connection(sink(username: "no_such_role"))
      assert error.message =~ ~s(role "no_such_role" does not exist)

      assert {:error, error} = Client.test_connection(sink(port: 1))
      assert error.message =~ "Failed to connect"

      assert {:error, error} = Client.test_connection(sink(table_name: "#{@schema}.missing"))
      assert error.message =~ "does not exist"
    end
  end

  describe "changeset/2" do
    test "static routing requires a valid table name; dynamic routing drops it" do
      assert %{valid?: false} =
               PostgresSink.changeset(%PostgresSink{}, %{
                 host: "h",
                 database: "d",
                 username: "u",
                 routing_mode: :static
               })

      assert %{errors: [table_name: _]} =
               PostgresSink.changeset(%PostgresSink{}, %{
                 host: "h",
                 database: "d",
                 username: "u",
                 routing_mode: :static,
                 table_name: "a.b.c"
               })

      changeset =
        PostgresSink.changeset(%PostgresSink{}, %{
          host: "h",
          database: "d",
          username: "u",
          routing_mode: :dynamic,
          table_name: "ignored"
        })

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :table_name) == nil
    end
  end

  test "a consumer can be created with a Postgres sink" do
    consumer = ConsumersFactory.insert_sink_consumer!(type: :postgres)
    assert %PostgresSink{connection_id: connection_id} = consumer.sink
    assert is_binary(connection_id)
  end
end
