defmodule Kvasir.Kafka.OffsetTracker do
  use GenServer
  alias Kvasir.Offset

  @offset_refresh 5 * 60_000

  def offset(topic), do: GenServer.call(__MODULE__, {:offset, topic})
  def offset(topic, partition), do: GenServer.call(__MODULE__, {:offset, topic, partition})

  def child_spec(topics, servers, config) do
    %{
      id: __MODULE__,
      start: {GenServer, :start_link, [__MODULE__, {topics, servers, config}, [name: __MODULE__]]}
    }
  end

  @impl GenServer
  def init({topics, servers, config}) do
    offsets = fetch_offsets(topics, servers, config)
    Process.send_after(self(), :refresh, @offset_refresh)

    {:ok, {offsets, topics, servers, config}}
  end

  @impl GenServer
  def handle_call({:offset, topic}, _from, state = {offsets, _, _, _}) do
    {:reply, Map.get(offsets, topic), state}
  end

  def handle_call({:offset, topic, partition}, _from, state = {offsets, _, _, _}) do
    if t = Map.get(offsets, topic) do
      {:reply, Offset.get(t, partition), state}
    else
      {:reply, nil, state}
    end
  end

  @impl GenServer
  def handle_info(:refresh, state = {_, topics, servers, config}) do
    pid = self()
    spawn_link(fn -> send(pid, {:refresh, fetch_offsets(topics, servers, config)}) end)
    {:noreply, state}
  end

  def handle_info({:refresh, offsets}, {_, topics, servers, config}) do
    Process.send_after(self(), :refresh, @offset_refresh)
    {:noreply, {offsets, topics, servers, config}}
  end

  defp fetch_offsets(topics, servers, config) do
    topics
    |> Enum.map(fn {t, p} -> Task.async(fn -> fetch_offsets(t, p, servers, config) end) end)
    |> Enum.flat_map(&Task.await(&1, 30_000))
    |> Enum.reduce(%{}, fn {t, p, o}, acc ->
      Map.update(acc, t, Offset.create(p, o), &Offset.set(&1, p, o))
    end)
  end

  defp fetch_offsets(topic, 1, servers, config),
    do: [{topic, 0, earliest!(topic, 0, servers, config)}]

  defp fetch_offsets(topic, partitions, servers, config) do
    0..(partitions - 1)
    |> Enum.map(fn p -> Task.async(fn -> {topic, p, earliest!(topic, p, servers, config)} end) end)
    |> Enum.map(&Task.await/1)
  end

  defp earliest!(topic, partition, servers, config) do
    case :brod_utils.resolve_offset(servers, topic, partition, -2, config) do
      {:ok, offset} -> offset
      {:error, :timeout} -> 0
    end
  end
end
