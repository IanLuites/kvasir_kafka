defmodule Kvasir.Kafka do
  @moduledoc """
  Documentation for Kvasir.Kafka.
  """

  def offset({:kafka_message, offset, _key, _payload, _, _timestamp, _meta}), do: offset

  def decode({:kafka_message, offset, key, payload, _, _timestamp, _meta}, topic, partition) do
    with {:ok, %{"type" => t, "version" => v, "payload" => p}} <- Jason.decode(payload) do
      Kvasir.Event.Encoding.decode(topic, %{
        type: t,
        version: v,
        meta: %{topic: topic.topic, offset: offset, partition: partition, key: key},
        payload: p
      })
      |> elem(1)
    end
  end

  def decode?(
        filter,
        {:kafka_message, offset, key, payload, _, _timestamp, _meta},
        topic,
        partition
      ) do
    with {:ok, %{"type" => t, "version" => v, "payload" => p}} <- Jason.decode(payload) do
      if filter.(t) do
        Kvasir.Event.Encoding.decode(topic, %{
          type: t,
          version: v,
          meta: %{topic: topic.topic, offset: offset, partition: partition, key: key},
          payload: p
        })
      else
        :ok
      end
    end
  end

  @base_topic_config %{
    "cleanup.policy" => "delete",
    "max.message.bytes" => "20485760",
    "retention.ms" => "2419200000",
    "delete.retention.ms" => "86400000"
  }

  def create_topics(client, topics, create_config, timeout \\ 5_000) do
    {:state, _, hosts, _, _, _, _, config, _} = client |> Process.whereis() |> :sys.get_state()
    {:ok, conn} = :kpro.connect_controller(hosts, config)

    config_entries =
      @base_topic_config
      |> Map.merge(create_config)
      |> Enum.map(fn {k, v} -> [config_name: k, config_value: v] end)

    topic_settings =
      Enum.map(topics, fn {topic, partitions} ->
        [
          topic: topic,
          num_partitions: partitions,
          replication_factor: Map.get(create_config, "replication_factor", 1),
          replica_assignment: [],
          config_entries: config_entries
        ]
      end)

    req = :kpro_req_lib.create_topics(0, topic_settings, %{timeout: timeout})

    with {:ok, {:kpro_rsp, _, :create_topics, _, %{topic_errors: errors}}} <-
           :kpro.request_sync(conn, req, timeout) do
      if err = Enum.find(errors, &(&1.error_code != :no_error)) do
        {:error, err.error_code}
      else
        # partition_settings =
        #   Enum.map(topics, fn {topic, partitions} ->
        #     [topic: topic, new_partitions: [count: partitions, assignment: [[_BrokerId = 0]]]]
        #   end)
        #
        # attempt_partitions(client, partition_settings, timeout)
        :ok
      end
    end
  end

  # defp attempt_partitions(client, config, timeout, attempt \\ 0)
  # defp attempt_partitions(client, config, timeout, 5), do: raise("Failed to create partitions.")

  # defp attempt_partitions(client, config, timeout, attempt) do
  #   {:state, _, hosts, _, _, _, _, c, _} = client |> Process.whereis() |> :sys.get_state()
  #   {:ok, conn} = :kpro.connect_controller(hosts, c)

  #   EnumX.each(config, fn p ->
  #     req = :kpro_req_lib.create_partitions(0, [p], %{timeout: timeout})

  #     with {:ok, {:kpro_rsp, _, _, _, %{topic_errors: errors}}} <-
  #            IO.inspect(:kpro.request_sync(conn, req, timeout)) do
  #       if err = Enum.find(errors, &(&1.error_code != :no_error)) do
  #         {:error, err.error_code}
  #       else
  #         :ok
  #       end
  #     end
  #   end)
  #   |> case do
  #     :ok ->
  #       :ok

  #     _ ->
  #       :timer.sleep(timeout)
  #       attempt_partitions(client, config, timeout, attempt + 1)
  #   end
  # end
end
