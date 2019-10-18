defmodule Kvasir.Source.Kafka do
  @moduledoc """
  Documentation for Kvasir.Kafka.
  """
  @behaviour Kvasir.Source
  import Kvasir.Kafka, only: [decode: 3]

  @impl Kvasir.Source
  def contains?(_name, _, _), do: :maybe

  @impl Kvasir.Source
  def child_spec(name, opts \\ []) do
    %{
      id: name,
      start: {__MODULE__, :start_link, [name, opts]}
    }
  end

  def start_link(name, opts \\ []) do
    servers = prepare_servers(opts[:servers])
    conn_config = Keyword.drop(opts, ~w(servers initialize)a)

    {:ok, _} = :application.ensure_all_started(:brod)
    :ok = :brod.start_client(servers, name, conn_config)

    if init = opts[:initialize] do
      Kvasir.Kafka.create_topics(name, init)

      Enum.each(init, fn {topic, partitions} ->
        Enum.each(0..(partitions - 1), &preconnect(name, topic, &1))
      end)
    end

    children = [
      Kvasir.Kafka.OffsetTracker.child_spec(opts[:initialize], servers, conn_config)
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Module.concat(name, Supervisor))
  end

  defp preconnect(name, topic, partition) do
    :brod.start_producer(name, topic, partition: partition)

    :brod.fetch(
      name,
      topic,
      partition,
      -2,
      %{max_bytes: 0, max_wait_time: 0}
    )
  end

  defp prepare_servers(nil), do: raise("Need to set `:servers` option.")
  defp prepare_servers([]), do: raise("Need to set `:servers` option.")

  defp prepare_servers(servers) when is_list(servers) do
    Enum.map(servers, fn
      {k, v} ->
        {k, if(is_integer(v), do: v, else: String.to_integer(v))}

      v ->
        case String.split(v, ":", parts: 2, trim: true) do
          [s, p] -> {String.trim(s), p |> String.trim() |> String.to_integer()}
          [s] -> {String.trim(s), 9092}
        end
    end)
  end

  ### Writing ###

  @impl Kvasir.Source
  def publish(client, topic, event) do
    with {:ok, d = %{meta: m}} <- Kvasir.Event.Encoding.encode(topic, event),
         {:ok, data} <- Jason.encode(Map.delete(d, :meta)) do
      {key, partition} =
        case m do
          %{key: k, partition: p} -> {k, p}
          _ -> {nil, nil}
        end

      do_publish(client, topic, event, partition, key, data)
    end
  end

  @impl Kvasir.Source
  def commit(client, topic, event) do
    with {:ok, d = %{meta: m}} <- Kvasir.Event.Encoding.encode(topic, event),
         {:ok, data} <- Jason.encode(Map.delete(d, :meta)) do
      {key, partition} =
        case m do
          %{key: k, partition: p} -> {k, p}
          _ -> {nil, nil}
        end

      do_commit(client, topic, event, partition, key, data)
    end
  end

  ### Reading ###

  @impl Kvasir.Source
  def subscribe(_client, _topic, _partition) do
    :ok
  end

  @impl Kvasir.Source
  def stream(client, topic, opts \\ []) do
    offset =
      cond do
        f = opts[:from] ->
          f

        k = opts[:key] || opts[:id] ->
          p = topic.key.partition(k, topic.partitions)

          Kvasir.Offset.create(
            p,
            Kvasir.Kafka.OffsetTracker.offset(topic.topic, p)
          )

        :all ->
          Kvasir.Kafka.OffsetTracker.offset(topic.topic)
      end

    filter =
      case opts[:key] do
        nil -> fn _ -> true end
        key -> fn {:kafka_message, _, k, _, _, _, _} -> k == key end
      end

    if Enum.count(offset.partitions) == 1 do
      # Single Read
      {:ok,
       Stream.resource(
         fn -> offset end,
         fn
           f = %{partitions: p} when p == %{} ->
             {:halt, f}

           f = %{partitions: p} ->
             p
             |> Enum.map(fn {p, o} -> {p, read(client, topic, filter, p, o)} end)
             |> Enum.reduce({[], f}, &reducer/2)
         end,
         fn _ -> :ok end
       )}
    else
      # Multiread
      {:ok,
       Stream.resource(
         fn -> offset end,
         fn
           f = %{partitions: p} when p == %{} ->
             {:halt, f}

           f = %{partitions: p} ->
             p
             |> Enum.map(fn {p, o} ->
               {p, Task.async(fn -> read(client, topic, filter, p, o) end)}
             end)
             |> Enum.map(fn {p, task} -> {p, Task.await(task)} end)
             |> Enum.reduce({[], f}, &reducer/2)
         end,
         fn _ -> :ok end
       )}
    end
  end

  defp reducer({p, {total, off, values}}, {acc, a}) do
    next = off + 1

    if total > next do
      {acc ++ values, Kvasir.Offset.set(a, p, next)}
    else
      {acc ++ values, %{a | partitions: Map.delete(a.partitions, p)}}
    end
  end

  defp read(client, topic, filter, partition, offset) do
    with {:ok, {total, m}} =
           :brod.fetch(
             client,
             topic.topic,
             partition,
             offset,
             %{max_bytes: 1024, max_wait_time: 0}
           ) do
      continue = if e = List.last(m), do: Kvasir.Kafka.offset(e), else: offset
      {total, continue, m |> Enum.filter(filter) |> Enum.map(&decode(&1, topic, partition))}
    end
  end

  ### Publishing ###

  defp do_publish(client, topic, event, partition, key, data) do
    with {:error, {:producer_not_found, _}} <-
           :brod.produce_sync(client, topic.topic, partition, key, data) do
      :brod.start_producer(client, topic.topic, [])
      do_publish(client, topic, event, partition, key, data)
    end
  end

  defp do_commit(client, topic, event, partition, key, data) do
    case :brod.produce_sync_offset(
           client,
           topic.topic,
           partition,
           key,
           data
         ) do
      {:ok, offset} ->
        {:ok, Kvasir.Event.set_offset(event, offset)}

      {:error, {:producer_not_found, _}} ->
        :brod.start_producer(client, topic.topic, [])
        do_commit(client, topic, event, partition, key, data)

      other ->
        other
    end
  end
end
