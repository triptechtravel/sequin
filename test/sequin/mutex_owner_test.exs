defmodule Sequin.MutexOwnerTest do
  # These tests swap the global Sequin.Redis.RedisClient app env, so they must
  # never run concurrently with other tests that touch Redis.
  use Sequin.Case, async: false
  use AssertEventually, interval: 10

  alias Sequin.Mutex
  alias Sequin.MutexOwner
  alias Sequin.Redis.RedisClient

  defmodule DownClient do
    @moduledoc """
    Simulates Redis being unreachable. eredis keeps its connection process alive
    across outages and returns {:error, :no_connection} for queries, which
    Sequin.Redis.command/2 maps to a ServiceError and Sequin.Mutex maps to :error.
    """
    def q(_connection, _command), do: {:error, :no_connection}
    def qp(_connection, commands), do: Enum.map(commands, fn _ -> {:error, :no_connection} end)
  end

  defmodule TakenClient do
    @moduledoc "Simulates another node holding the mutex: EVAL returns the other owner's token."
    def q(_connection, _command), do: {:ok, "another-node-token"}
    def qp(_connection, commands), do: Enum.map(commands, fn _ -> {:ok, "another-node-token"} end)
  end

  defp swap_redis_client(client) do
    original = Application.get_env(:sequin, RedisClient)
    Application.put_env(:sequin, RedisClient, client)
    on_exit(fn -> Application.put_env(:sequin, RedisClient, original) end)
    original
  end

  defp with_redis_client(client, fun) do
    original = Application.get_env(:sequin, RedisClient)
    Application.put_env(:sequin, RedisClient, client)

    try do
      fun.()
    after
      Application.put_env(:sequin, RedisClient, original)
    end
  end

  defp unique_key(label), do: "test:mutex_owner:#{label}:#{System.unique_integer([:positive])}"

  describe "handle_event/4 {:timeout, :keep_mutex} in :has_mutex" do
    setup do
      data = %MutexOwner.State{
        lock_expiry: 5_000,
        mutex_key: unique_key("handle_event"),
        mutex_token: "test-token-#{System.unique_integer([:positive])}",
        on_acquired: fn -> :ok end
      }

      {:ok, data: data}
    end

    test "when Redis is unreachable, keeps state and schedules a backoff retry instead of stopping", %{data: data} do
      with_redis_client(DownClient, fn ->
        assert {:keep_state, new_data, [{{:timeout, :keep_mutex}, retry_ms, nil}]} =
                 MutexOwner.handle_event({:timeout, :keep_mutex}, nil, :has_mutex, data)

        assert new_data.consecutive_redis_errors == 1
        assert retry_ms == data.lock_expiry * 2
      end)
    end

    test "backoff doubles with each consecutive error and caps at one hour", %{data: data} do
      with_redis_client(DownClient, fn ->
        retry_for = fn errors ->
          data = %{data | consecutive_redis_errors: errors}

          {:keep_state, new_data, [{{:timeout, :keep_mutex}, retry_ms, nil}]} =
            MutexOwner.handle_event({:timeout, :keep_mutex}, nil, :has_mutex, data)

          assert new_data.consecutive_redis_errors == errors + 1
          retry_ms
        end

        assert retry_for.(0) == 10_000
        assert retry_for.(1) == 20_000
        assert retry_for.(2) == 40_000
        assert retry_for.(30) == to_timeout(hour: 1)
        assert retry_for.(1_000) == to_timeout(hour: 1)
      end)
    end

    test "when the mutex is taken by another owner, stops with :lost_mutex", %{data: data} do
      with_redis_client(TakenClient, fn ->
        assert {:stop, {:shutdown, :lost_mutex}} =
                 MutexOwner.handle_event({:timeout, :keep_mutex}, nil, :has_mutex, data)
      end)
    end

    test "a successful keep resets the consecutive error count and schedules the next keep", %{data: data} do
      data = %{data | consecutive_redis_errors: 7}

      assert {:keep_state, new_data, [{{:timeout, :keep_mutex}, keep_ms, nil}]} =
               MutexOwner.handle_event({:timeout, :keep_mutex}, nil, :has_mutex, data)

      assert new_data.consecutive_redis_errors == 0
      assert keep_ms == round(data.lock_expiry * 0.80)

      Mutex.release(data.mutex_key, data.mutex_token)
    end
  end

  describe "State struct" do
    test "includes consecutive_redis_errors field defaulting to 0" do
      state =
        MutexOwner.State.new(
          mutex_key: unique_key("state"),
          on_acquired: fn -> :ok end
        )

      assert state.consecutive_redis_errors == 0
      assert is_binary(state.mutex_token)
    end
  end

  describe "MutexOwner process resilience to a Redis outage" do
    test "survives the outage and re-acquires when Redis returns" do
      test_pid = self()

      {:ok, pid} =
        MutexOwner.start_link(
          name: :"test_mutex_owner_#{System.unique_integer([:positive])}",
          mutex_key: unique_key("outage"),
          lock_expiry: 50,
          on_acquired: fn -> send(test_pid, :mutex_acquired) end
        )

      assert_receive :mutex_acquired, 5_000
      ref = Process.monitor(pid)

      # Redis goes down. Pre-fix, the next keep_mutex tick crashed the process with
      # {:bad_return_from_state_function, {:shutdown, :err_keeping_mutex}} and the
      # :one_for_all MutexedSupervisor cascade took down every consumer with it.
      original = swap_redis_client(DownClient)

      # Wait until the keep_mutex tick has actually failed at least once.
      assert_eventually(
        match?(
          {:has_mutex, %MutexOwner.State{consecutive_redis_errors: errors}} when errors >= 1,
          :sys.get_state(pid)
        ),
        5_000
      )

      assert Process.alive?(pid)
      refute_received {:DOWN, ^ref, :process, ^pid, _reason}

      # Redis comes back. The held key has long expired (PX 50), so recovery must
      # re-acquire it fresh, reset the error count, and resume normal keeps.
      Application.put_env(:sequin, RedisClient, original)

      assert_eventually(
        match?({:has_mutex, %MutexOwner.State{consecutive_redis_errors: 0}}, :sys.get_state(pid)),
        5_000
      )

      assert Process.alive?(pid)
      refute_received {:DOWN, ^ref, :process, ^pid, _reason}

      GenStateMachine.stop(pid, :normal)
    end
  end
end
