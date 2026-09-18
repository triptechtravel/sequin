defmodule Sequin.TestSupport.RedisOutage do
  @moduledoc """
  Integration harness for simulating a Redis outage against the real runtime.

  Sequin talks to Redis through a client module resolved from `Application.get_env(:sequin, Sequin.Redis.RedisClient)`.
  `install/0` (called from `test_helper.exs`, after the app has booted and picked its real client) wraps that client
  with this module. By default every call is delegated to the real client, so the rest of the suite is unaffected.

  Inside `with_outage/2`, calls instead fail exactly the way eredis fails when Redis is unreachable:

    * `:no_connection` — the connection was closed / refused. `q/2` and `qp/2` return `{:error, :no_connection}`.
      This is what a Dragonfly/Redis restart looks like from the BEAM (`tcp_closed` -> `econnrefused`).
    * `:exit` — the eredis_cluster slot-map monitor is blocked trying to reach Redis and the caller's
      `gen_server.call` times out, so the calling process is exited with `{:timeout, {GenServer, :call, ...}}`.

  Tests using this harness must be `async: false` — the fault is process-global.
  """

  alias Sequin.Redis.RedisClient

  @delegate_key {__MODULE__, :delegate}
  @mode_key {__MODULE__, :mode}

  @type mode :: :no_connection | :exit

  @doc "Wraps the currently configured Redis client with this fault injector. Idempotent."
  def install do
    case Application.get_env(:sequin, RedisClient) do
      __MODULE__ ->
        :ok

      real when is_atom(real) and not is_nil(real) ->
        :persistent_term.put(@delegate_key, real)
        Application.put_env(:sequin, RedisClient, __MODULE__)
        :ok
    end
  end

  @doc "Runs `fun` while Redis is unreachable, then restores connectivity (even if `fun` raises)."
  @spec with_outage(mode(), (-> result)) :: result when result: any()
  def with_outage(mode \\ :no_connection, fun) when mode in [:no_connection, :exit] do
    start_outage(mode)

    try do
      fun.()
    after
      end_outage()
    end
  end

  @spec start_outage(mode()) :: :ok
  def start_outage(mode \\ :no_connection) when mode in [:no_connection, :exit] do
    :persistent_term.put(@mode_key, mode)
  end

  @spec end_outage() :: :ok
  def end_outage do
    :persistent_term.erase(@mode_key)
    :ok
  end

  # Client interface (mirrors Sequin.Redis.ClusterClient / Sequin.Redis.Client)

  def connect(index, opts), do: delegate().connect(index, opts)

  def q(connection, command) do
    case current_mode() do
      nil -> delegate().q(connection, command)
      mode -> fail(mode, connection)
    end
  end

  def qp(connection, commands) do
    case current_mode() do
      nil -> delegate().qp(connection, commands)
      mode -> fail(mode, connection)
    end
  end

  defp fail(:no_connection, _connection), do: {:error, :no_connection}

  defp fail(:exit, connection) do
    # Same shape as `GenServer.call/3` timing out against the eredis_cluster monitor.
    exit({:timeout, {GenServer, :call, [connection, {:reload_slots_map, 0}, 5000]}})
  end

  defp current_mode, do: :persistent_term.get(@mode_key, nil)

  defp delegate, do: :persistent_term.get(@delegate_key)
end
