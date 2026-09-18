defmodule Sequin.RedisOutageTest do
  use Sequin.Case, async: false

  alias Sequin.Error.ServiceError
  alias Sequin.Redis
  alias Sequin.TestSupport.RedisOutage

  @moduletag :capture_log
  @key "sequin:test:redis_outage:#{System.unique_integer([:positive])}"

  describe "when Redis is unreachable" do
    test "command/2 returns a no_connection error" do
      RedisOutage.with_outage(:no_connection, fn ->
        assert {:error, %ServiceError{service: :redis, code: "no_connection"}} = Redis.command(["GET", @key])
      end)

      assert {:ok, nil} = Redis.command(["GET", @key])
    end

    test "pipeline/2 returns a no_connection error" do
      RedisOutage.with_outage(:no_connection, fn ->
        assert {:error, %ServiceError{service: :redis, code: "no_connection"}} = Redis.pipeline([["GET", @key]])
      end)
    end

    test "command!/2 raises a ServiceError" do
      RedisOutage.with_outage(:no_connection, fn ->
        assert_raise ServiceError, ~r/no.connection/i, fn -> Redis.command!(["GET", @key]) end
      end)
    end
  end

  describe "when the cluster client exits the caller" do
    test "command/2 returns a no_connection error instead of exiting" do
      RedisOutage.with_outage(:exit, fn ->
        assert {:error, %ServiceError{service: :redis, code: "no_connection"}} = Redis.command(["GET", @key])
      end)
    end

    test "pipeline/2 returns a no_connection error instead of exiting" do
      RedisOutage.with_outage(:exit, fn ->
        assert {:error, %ServiceError{service: :redis, code: "no_connection"}} = Redis.pipeline([["GET", @key]])
      end)
    end

    test "command!/2 raises a ServiceError instead of exiting" do
      RedisOutage.with_outage(:exit, fn ->
        assert_raise ServiceError, ~r/no.connection/i, fn -> Redis.command!(["GET", @key]) end
      end)
    end
  end
end
