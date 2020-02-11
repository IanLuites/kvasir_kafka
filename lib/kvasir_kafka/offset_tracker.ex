defmodule Kvasir.Kafka.OffsetTracker do
  use GenServer
  alias Kvasir.Offset

  @offset_timeout 25_000
  @leader_timeout @offset_timeout + 5_000
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
    case Map.fetch(offsets, topic) do
      {:ok, o} -> {:reply, o, state}
      _ -> {:reply, nil, state}
    end
  end

  def handle_call({:offset, topic, partition}, _from, state = {offsets, _, _, _}) do
    case Map.fetch(offsets, topic) do
      {:ok, o} -> {:reply, Offset.get(o, partition), state}
      _ -> {:reply, nil, state}
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
    {:ok, %{topic_metadata: metadata}} =
      :brod_utils.get_metadata(servers, Map.keys(topics), config)

    metadata
    |> leaders()
    |> Enum.map(fn {_leader, leader_topics} ->
      Task.async(fn ->
        topics = [{t, [p | _]} | _] = :maps.to_list(leader_topics)
        {:ok, conn} = :kpro.connect_partition_leader(servers, config, t, p)

        req =
          :kpro_req_lib.make(:list_offsets, 1,
            replica_id: -1,
            isolation_level: :read_committed,
            topics:
              Enum.map(topics, fn {k, v} ->
                [topic: k, partitions: Enum.map(v, &[partition: &1, timestamp: -2])]
              end)
          )

        result =
          case :kpro.request_sync(conn, req, @offset_timeout) do
            {:ok, {:kpro_rsp, _, :list_offsets, _, %{responses: r}}} -> r
            _ -> []
          end

        :kpro.close_connection(conn)

        Enum.reduce(result, %{}, fn %{topic: t, partition_responses: r}, acc ->
          Map.put(
            acc,
            t,
            Enum.reduce(r, Offset.create(), &Offset.set(&2, &1.partition, &1.offset))
          )
        end)
      end)
    end)
    |> Enum.map(&Task.await(&1, @leader_timeout))
    |> Enum.reduce(%{}, fn data, acc ->
      Map.merge(acc, data, fn _, v1, v2 -> Offset.merge(v1, v2) end)
    end)
  end

  defp leaders(metadata, acc \\ %{})
  defp leaders([], acc), do: acc

  defp leaders([%{topic: t, partition_metadata: pm} | metadata], acc) do
    leaders(
      metadata,
      Enum.reduce(pm, acc, fn %{leader: l, partition: p}, a ->
        Map.update(a, l, %{t => [p]}, fn la -> Map.update(la, t, [p], &[p | &1]) end)
      end)
    )
  end
end
