defmodule Kvasir.Source.Kafka do
  @moduledoc """
  Documentation for Kvasir.Kafka.
  """
  @behaviour Kvasir.Source
  alias Kvasir.Kafka.OffsetTracker
  alias Kvasir.Offset
  import Kvasir.Client
  import Kvasir.Kafka, only: [decode: 5]
  import Kvasir.Publisher
  require Logger

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
    Kvasir.Kafka.Metrics.create()

    servers = prepare_servers(opts[:servers])
    connect_timeout = opts[:connect_timeout] || 120_000
    start_producers = Keyword.get(opts, :start_producers, true)
    track_offsets = Keyword.get(opts, :track_offsets, true)

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
          |> Enum.map(fn p ->
            Task.async(fn -> preconnect(name, topic, p, start_producers) end)
          end)
          |> Enum.each(&Task.await(&1, connect_timeout))
        end)
      end)
      |> Enum.each(&Task.await(&1, connect_timeout))
    end

    children =
      if track_offsets do
        [
          OffsetTracker.child_spec(opts[:initialize], servers, conn_config)
        ]
      else
        []
      end

    Supervisor.start_link(children, strategy: :one_for_one, name: Module.concat(name, Supervisor))
  end

  defp preconnect(name, topic, partition, start) do
    if start, do: :brod.start_producer(name, topic, partition: partition)

    :ok
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

  # @impl Kvasir.Source
  # def publish(client, topic, event = %type{}) do
  #   key = Kvasir.Event.key(event)

  #   with {:ok, k} <- topic.key.dump(key, []),
  #        {:ok, p} <- topic.key.partition(key, topic.partitions),
  #        {:ok, data} <- topic.module.bin_encode(event) do
  #     t = topic.topic
  #     start = :erlang.monotonic_time()

  #     client
  #     |> do_publish(t, p, to_string(k), data)
  #     |> report_publish_metric(type, t, p, start)
  #   end
  # end

  @impl Kvasir.Source
  def commit(client, topic, event)

  def commit(client, topic, event = %type{__meta__: meta = %{key: key}}) do
    with {:ok, k} <- topic.key.dump(key, []),
         {:ok, p} <- topic.key.partition(key, topic.partitions),
         e = %{event | __meta__: %{meta | key: nil, topic: nil, partition: nil}},
         {:ok, data} <- topic.module.bin_encode(e) do
      t = topic.topic
      start = :erlang.monotonic_time()

      client
      |> do_commit(event, t, p, to_string(k), data)
      |> report_publish_metric(type, t, p, start)
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

    decoder = if(only = opts[:only], do: topic.module.filter(only), else: topic.module)

    consumer_config =
      opts
      |> Keyword.get(:consumer_config, [])
      |> Keyword.put(:begin_offset, begin)
      |> Keyword.put_new(:offset_reset_policy, :reset_to_earliest)

    {cb_module, message_type} =
      case opts[:mode] do
        :batch -> {Kvasir.Kafka.BatchSubscriber, :message_set}
        _ -> {Kvasir.Kafka.Subscriber, :message}
      end

    with {:ok, c, spec} <- client_child_spec(client) do
      children = [
        spec,
        %{
          id: :subscriber,
          type: :supervisor,
          start:
            {:brod_group_subscriber_v2, :start_link,
             [
               %{
                 client: c,
                 group_id: opts[:group],
                 group_config: Keyword.get(opts, :group_config, []),
                 consumer_config: consumer_config,
                 cb_module: cb_module,
                 topics: [topic.topic],
                 message_type: message_type,
                 init_data: {topic, offset, decoder, callback_module, opts[:state]}
               }
             ]}
        }
      ]

      Supervisor.start_link(children, strategy: :rest_for_one)
    end
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

    decoder = if(only = opts[:only], do: topic.module.filter(only), else: topic.module)

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
               {topic, callback, decoder}
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

  defp listen_call(partition, message, {topic, callback, decoder}) do
    listened =
      with {:ok, e} <- Kvasir.Kafka.decode?(decoder, message, topic, partition),
           do: callback.(e)

    if listened == :ok do
      {:ok, :ack, {topic, callback, decoder}}
    else
      listened
    end
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

    pre_filter =
      case opts[:key] do
        nil ->
          fn _ -> true end

        key ->
          {:ok, m} = topic.key.dump(key, [])
          m = to_string(m)
          fn {:kafka_message, _, k, _, _, _, _} -> k == m end
      end

    decoder = if(only = opts[:events], do: topic.module.filter(only), else: topic.module)

    read_timeout = Keyword.get(opts, :read_timeout, 60_000)

    if Enum.count(offset.partitions) == 1 do
      # Single Read
      {:ok,
       Stream.resource(
         fn -> offset end,
         fn
           f = %{partitions: p} when p == %{} ->
             {:halt, f}

           f = %{partitions: p} ->
             r =
               p
               |> Enum.map(fn {p, o} ->
                 {p, read(client, topic.topic, topic.key, decoder, pre_filter, p, o)}
               end)
               |> Enum.reduce({[], f}, &reducer/2)

             Logger.debug(fn ->
               "#{inspect(__MODULE__)}[#{inspect(client)}]: Read #{Enum.count(elem(r, 0))}"
             end)

             r
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
             r =
               p
               |> Enum.map(fn {p, o} ->
                 {p,
                  Task.async(fn ->
                    read(client, topic.topic, topic.key, decoder, pre_filter, p, o)
                  end)}
               end)
               |> Enum.map(fn {p, task} -> {p, Task.await(task, read_timeout)} end)
               |> Enum.reduce({[], f}, &reducer/2)

             Logger.debug(fn ->
               "#{inspect(__MODULE__)}[#{inspect(client)}]: Read #{Enum.count(elem(r, 0))}"
             end)

             r
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

  defp read(client, topic, key, decoder, filter, partition, offset) do
    case :brod.fetch(
           client,
           topic,
           partition,
           offset,
           %{max_bytes: 1024 * 1024, max_wait_time: 0}
         ) do
      {:ok, {total, m}} ->
        continue = if e = List.last(m), do: Kvasir.Kafka.offset(e), else: offset

        {total, continue,
         m
         |> Enum.filter(filter)
         |> Enum.map(&decode(&1, topic, key, decoder, partition))
         |> read_reduce([])}

      {:error, :offset_out_of_range} ->
        t = OffsetTracker.offset(topic, partition)

        if offset >= 0 and offset <= t,
          do: read(client, topic, key, decoder, filter, partition, t),
          else: {offset, offset - 1, []}
    end
  end

  defp read_reduce([], acc), do: :lists.reverse(acc)

  defp read_reduce([event | events], acc) do
    case event do
      {:ok, e} -> read_reduce(events, [e | acc])
      {:error, :unknown_event_type} -> read_reduce(events, acc)
      err = {:error, _} -> raise inspect(err)
    end
  end

  @impl Kvasir.Source
  def generate_dedicated_publisher(name, target, topic, opts)

  def generate_dedicated_publisher(
        name,
        target,
        %{key: key, module: module, partitions: partitions, topic: topic},
        opts
      ) do
    pool_size = Keyword.get(opts, :pool_size, 1)

    {client, extra} =
      if pool_size > 1 do
        diff = -(:erlang.unique_integer([:positive]) - :erlang.unique_integer([:positive]))
        clients = Enum.map(1..pool_size, &Module.concat([target, "C#{&1}"]))

        get =
          Enum.reduce(
            Enum.with_index(clients),
            quote do
              @spec client(non_neg_integer) :: module
              defp client(id)
            end,
            fn {w, i}, acc ->
              quote do
                unquote(acc)
                defp client(unquote(i)), do: unquote(w)
              end
            end
          )

        {quote do
           [:positive]
           |> :erlang.unique_integer()
           |> :erlang.div(unquote(diff))
           |> rem(unquote(Enum.count(clients)))
           |> client()
         end,
         quote do
           @doc false
           def start_link(opts \\ [])

           def start_link(_opts) do
             children =
               Enum.map(unquote(clients), fn c ->
                 {:ok, ^c, spec} = client_child_spec(unquote(name), name: c)
                 Map.put(spec, :id, c)
               end)

             Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
           end

           unquote(get)
         end}
      else
        {quote(do: __MODULE__),
         quote do
           @doc false
           def start_link(opts \\ [])

           def start_link(_opts),
             do: client_start_link(unquote(name), name: __MODULE__, named: false)
         end}
      end

    Code.compiler_options(ignore_module_conflict: true)

    compiled =
      Code.compile_quoted(
        quote do
          defmodule unquote(target) do
            @moduledoc false
            import Kvasir.Client
            import Kvasir.Publisher

            @doc false
            def child_spec(opts \\ [])

            def child_spec(opts) do
              %{
                id: __MODULE__,
                type: :supervisor,
                start: {__MODULE__, :start_link, [opts]}
              }
            end

            unquote(extra)

            @doc false
            @spec publish(Kvasir.Event.t()) :: {:ok, Kvasir.Event.t()} | {:error, term}
            def publish(event)

            def publish(event = %type{__meta__: meta = %{key: key}}) do
              with {:ok, k} <- unquote(key).dump(key, []),
                   {:ok, p} <- unquote(key).partition(key, unquote(partitions)),
                   e = %{event | __meta__: %{meta | key: nil, topic: nil, partition: nil}},
                   {:ok, data} <- unquote(module).bin_encode(e) do
                start = :erlang.monotonic_time()

                unquote(client)
                |> do_commit(event, unquote(topic), p, to_string(k), data)
                |> report_publish_metric(type, unquote(topic), p, start)
              end
            end
          end
        end
      )

    Code.compiler_options(ignore_module_conflict: false)

    if match?([{_, _}], compiled), do: :ok, else: {:error, :failed_to_compile_dedicated_publisher}
  end
end
