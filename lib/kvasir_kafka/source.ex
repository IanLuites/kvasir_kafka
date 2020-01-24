defmodule Kvasir.Source.Kafka do
  @moduledoc """
  Documentation for Kvasir.Kafka.
  """
  @behaviour Kvasir.Source
  alias Kvasir.Kafka.OffsetTracker
  alias Kvasir.Offset
  import Kvasir.Kafka, only: [decode: 3]

  @impl Kvasir.Source
  def contains?(_name, _topic, _offset) do
    :maybe
    # case Offset.compare(OffsetTracker.offset(topic), offset) do
    #   :eq -> true
    #   :lt -> true
    #   :gt -> false
    #   :mixed -> :maybe
    # end
  end

  @impl Kvasir.Source
  def child_spec(name, opts \\ []) do
    %{
      id: name,
      start: {__MODULE__, :start_link, [name, opts]}
    }
  end

  def start_link(name, opts \\ []) do
    servers = prepare_servers(opts[:servers])

    conn_config =
      [
        auto_start_producers: true,
        allow_topic_auto_creation: false,
        default_producer_config: [],
        reconnect_cool_down_seconds: 5
      ]
      |> Keyword.merge(opts)
      |> Keyword.drop(~w(servers initialize auto_create_topics auto_create_config)a)

    {:ok, _} = :application.ensure_all_started(:brod)
    :ok = :brod.start_client(servers, name, conn_config)

    if init = opts[:initialize] do
      if opts[:auto_create_topics] in [true, "TRUE", "1"],
        do: Kvasir.Kafka.create_topics(name, init, opts[:auto_create_config] || %{})

      init
      |> Enum.map(fn {topic, partitions} ->
        Task.async(fn ->
          0..(partitions - 1)
          |> Enum.map(fn p -> Task.async(fn -> preconnect(name, topic, p) end) end)
          |> Enum.each(&Task.await(&1, 15_000))
        end)
      end)
      |> Enum.each(&Task.await(&1, 60_000))
    end

    children = [
      OffsetTracker.child_spec(opts[:initialize], servers, conn_config)
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
          %{key: k, partition: p} -> {to_string(k), p}
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
          %{key: k, partition: p} -> {to_string(k), p}
          _ -> {nil, nil}
        end

      do_commit(client, topic, event, partition, key, data)
    end
  end

  ### Reading ###

  @impl Kvasir.Source
  def subscribe(client, topic, callback_module, opts \\ []) do
    offset =
      if f = opts[:from] do
        f
      else
        Offset.create(Map.new(0..(topic.partitions - 1), &{&1, :earliest}))
      end

    begin = offset.partitions |> Map.values() |> Enum.reduce(&Offset.min/2)

    pre_filter =
      case opts[:only] do
        nil ->
          fn _ -> true end

        events ->
          types = Enum.map(events, & &1.__event__(:type))
          &(&1 in types)
      end

    starter = self()

    fn ->
      with {:ok, c} <- client_start_link(client),
           {:ok, _} <-
             :brod_group_subscriber_v2.start_link(%{
               client: c,
               group_id: opts[:group],
               consumer_config: [begin_offset: begin],
               cb_module: Kvasir.Kafka.Subscriber,
               topics: [topic.topic],
               message_type: :message,
               init_data: {topic, offset, pre_filter, callback_module, opts[:state]}
             }) do
        send(starter, :subscriber_up)
        Process.sleep(:infinity)
      else
        err -> send(starter, {:subscriber_failed, err})
      end
    end
    |> spawn_link
    |> wait_for_subscribe()
  end

  @impl Kvasir.Source
  def listen(client, topic, callback, opts \\ []) do
    {partitions, resume_offsets} =
      if f = opts[:from] do
        p = f.partitions
        {Map.keys(p), Enum.map(p, fn {k, v} -> {k, v} end)}
      else
        {Enum.map(0..(topic.partitions - 1), & &1), []}
      end

    starter = self()

    fn ->
      with {:ok, c} <- client_start_link(client),
           {:ok, _} <-
             :brod_topic_subscriber.start_link(
               c,
               topic.topic,
               partitions,
               # [begin_offset: :earliest]
               _consumerConfig = [begin_offset: :latest],
               resume_offsets,
               :message,
               &listen_call/3,
               {topic, callback}
             ) do
        send(starter, :subscriber_up)
        Process.sleep(:infinity)
      else
        err -> send(starter, {:subscriber_failed, err})
      end
    end
    |> spawn_link
    |> wait_for_subscribe()
  end

  defp wait_for_subscribe(controller) do
    receive do
      :subscriber_up ->
        if Process.link(controller) do
          {:ok, controller}
        else
          {:error, :subscribe_failed}
        end

      {:subscriber_failed, err} ->
        err
    after
      5_000 ->
        Process.exit(controller, :kill)
        {:error, :subscribe_timeout}
    end
  end

  @spec client_start_link(atom) :: {:ok, atom} | {:error, term}
  def client_start_link(client) do
    {:state, _, hosts, _, _, _, _, c, _} = client |> Process.whereis() |> :sys.get_state()
    config = Keyword.take(c, ~w(reconnect_cool_down_seconds query_api_versions ssl)a)
    name = free_client()

    with {:ok, _} <- :brod.start_link_client(hosts, name, config) do
      {:ok, name}
    end
  end

  @spec free_client(non_neg_integer) :: atom
  defp free_client(id \\ 0) do
    client = :"Elixir.Kvasir.Kafka.Client#{id}"

    if Process.whereis(client) do
      free_client(id + 1)
    else
      client
    end
  end

  defp listen_call(partition, message, {topic, callback}) do
    :ok = callback.(Kvasir.Kafka.decode(message, topic, partition))
    {:ok, :ack, {topic, callback}}
  end

  @impl Kvasir.Source
  def stream(client, topic, opts \\ []) do
    offset =
      cond do
        f = opts[:from] ->
          f

        k = opts[:key] || opts[:id] ->
          {:ok, p} = topic.key.partition(k, topic.partitions)
          Offset.create(p, OffsetTracker.offset(topic.topic, p))

        p = opts[:partition] ->
          Offset.create(p, OffsetTracker.offset(topic.topic, p))

        :all ->
          OffsetTracker.offset(topic.topic)
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
      {acc ++ values, Offset.set(a, p, next)}
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
             %{max_bytes: 1024 * 1024, max_wait_time: 0}
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
