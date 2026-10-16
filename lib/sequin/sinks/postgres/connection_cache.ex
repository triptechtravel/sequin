defmodule Sequin.Sinks.Postgres.ConnectionCache do
  @moduledoc """
  Caches a Postgrex connection pool per Postgres sink.

  The cache owns the pools and stops them when they're invalidated: when the sink's connection
  options change, when a pool is found dead, or when the cache stops. Callers must not stop
  them.
  """
  use GenServer

  alias Sequin.Consumers.PostgresSink

  require Logger

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      # connection_id => %{conn: pid(), options_hash: integer()}
      field :cache, map(), default: %{}
      field :start_fn, (PostgresSink.t() -> {:ok, pid()} | {:error, term()})
    end
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec connection(GenServer.server(), PostgresSink.t()) :: {:ok, pid()} | {:error, term()}
  def connection(server \\ __MODULE__, %PostgresSink{} = sink) do
    GenServer.call(server, {:connection, sink}, to_timeout(second: 30))
  end

  @spec invalidate_connection(GenServer.server(), PostgresSink.t()) :: :ok
  def invalidate_connection(server \\ __MODULE__, %PostgresSink{} = sink) do
    GenServer.cast(server, {:invalidate_connection, sink})
  end

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    {:ok, %State{start_fn: Keyword.get(opts, :start_fn, &default_start/1)}}
  end

  @impl GenServer
  def handle_call({:connection, sink}, _from, %State{} = state) do
    hash = options_hash(sink)

    case Map.get(state.cache, sink.connection_id) do
      %{conn: conn, options_hash: ^hash} = entry when is_pid(conn) ->
        if Process.alive?(conn) do
          {:reply, {:ok, entry.conn}, state}
        else
          start_and_cache(state, sink, hash)
        end

      _missing_or_stale ->
        state = invalidate(state, sink)
        start_and_cache(state, sink, hash)
    end
  end

  @impl GenServer
  def handle_cast({:invalidate_connection, sink}, state) do
    {:noreply, invalidate(state, sink)}
  end

  # Pools are linked, so we hear when one exits; the next lookup replaces it
  @impl GenServer
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, %State{} = state) do
    Enum.each(state.cache, fn {_id, %{conn: conn}} -> stop(conn) end)
  end

  defp start_and_cache(%State{} = state, sink, hash) do
    case state.start_fn.(sink) do
      {:ok, conn} ->
        cache = Map.put(state.cache, sink.connection_id, %{conn: conn, options_hash: hash})
        {:reply, {:ok, conn}, %{state | cache: cache}}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp invalidate(%State{} = state, sink) do
    {entry, cache} = Map.pop(state.cache, sink.connection_id)
    if entry, do: stop(entry.conn)
    %{state | cache: cache}
  end

  defp stop(conn) do
    if Process.alive?(conn), do: GenServer.stop(conn, :normal, to_timeout(second: 5))
    :ok
  catch
    :exit, _ -> :ok
  end

  defp options_hash(%PostgresSink{} = sink), do: :erlang.phash2(PostgresSink.connection_opts(sink))

  defp default_start(%PostgresSink{} = sink), do: Postgrex.start_link(PostgresSink.connection_opts(sink))
end
