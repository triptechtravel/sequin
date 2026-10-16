defmodule SequinWeb.SinkConsumersLive.FormTest do
  # Creates a destination table outside the sandbox, as connection tests use connections of their
  # own
  use SequinWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias Sequin.Consumers
  alias Sequin.Consumers.PostgresSink
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Test.UnboxedRepo

  setup :logged_in_user

  setup %{account: account} do
    database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)
    ReplicationFactory.insert_postgres_replication!(account_id: account.id, postgres_database_id: database.id)

    UnboxedRepo.query!("drop table if exists public.form_test_mirror")
    UnboxedRepo.query!("create table public.form_test_mirror (id int primary key)")
    on_exit(fn -> UnboxedRepo.query!("drop table if exists public.form_test_mirror") end)

    %{database: database}
  end

  describe "a Postgres sink" do
    test "tests its connection from the form", %{conn: conn, database: database} do
      {:ok, view, _html} = live(conn, ~p"/sinks/new?kind=postgres")
      form = with_target(view, "#consumer-form")

      render_hook(form, "form_updated", %{"form" => form_params(database)})
      render_hook(form, "test_connection", %{})
      assert_reply(view, %{ok: true})

      render_hook(form, "form_updated", %{"form" => form_params(database, %{"username" => "no_such_role"})})
      render_hook(form, "test_connection", %{})
      assert_reply(view, %{ok: false, error: error})
      assert error =~ ~s(role "no_such_role" does not exist)
    end

    test "is created from the form", %{conn: conn, database: database} do
      {:ok, view, _html} = live(conn, ~p"/sinks/new?kind=postgres")

      # Creating a sink navigates to its page
      assert {:error, {:live_redirect, %{to: "/sinks/postgres/" <> _id = path}}} =
               view
               |> with_target("#consumer-form")
               |> render_hook("form_submitted", %{"form" => form_params(database)})

      assert [consumer] = Consumers.list_sink_consumers_for_account(database.account_id)

      assert %PostgresSink{
               host: "localhost",
               database: "sequin_test",
               username: "postgres",
               password: "postgres",
               table_name: "public.form_test_mirror",
               routing_mode: :static
             } = consumer.sink

      {:ok, _view, html} = live(conn, path)
      assert html =~ "postgres-mirror"
      assert html =~ "public.form_test_mirror"
      refute html =~ ~s("password")
    end
  end

  defp form_params(database, sink \\ %{}) do
    %{
      "name" => "postgres-mirror",
      "postgresDatabaseId" => database.id,
      "actions" => ["insert", "update", "delete"],
      "batchSize" => 100,
      "messageGrouping" => true,
      "routingMode" => "static",
      "routingId" => "none",
      "transform" => "none",
      "filterId" => "none",
      "enrichmentId" => "none",
      "maxMemoryMb" => 128,
      "ackWaitMs" => 30_000,
      "maxAckPending" => 10_000,
      "maxWaiting" => 20,
      "timestampFormat" => "iso8601",
      "source" => %{"include_schemas" => nil, "exclude_schemas" => nil},
      "backfill" => %{"selectedTables" => []},
      "sink" =>
        Map.merge(
          %{
            "host" => "localhost",
            "port" => 5432,
            "database" => "sequin_test",
            "username" => "postgres",
            "password" => "postgres",
            "ssl" => false,
            "table_name" => "public.form_test_mirror",
            "batch_size" => 100
          },
          sink
        )
    }
  end
end
