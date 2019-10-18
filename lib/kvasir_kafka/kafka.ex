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

  def create_topics(_client, _topics, _timeout \\ 5_000) do
    # {:state, _, hosts, _, _, _, _, config, _} =
    #   client |> Process.whereis() |> :sys.get_state() |> IO.inspect()

    # {:ok, conn} = :kpro.connect_controller(hosts, config) |> IO.inspect()

    # topic_settings =
    #   Enum.map(topics, fn {topic, partitions} ->
    #     [
    #       topic: topic,
    #       num_partitions: partitions,
    #       replication_factor: 3,
    #       replica_assignment: [],
    #       config_entries: [
    #         [config_name: "cleanup.policy", config_value: "delete"],
    #         [config_name: "max.message.bytes", config_value: "20485760"],
    #         [config_name: "retention.ms", config_value: "2419200000"],
    #         [config_name: "delete.retention.ms", config_value: "86400000"]
    #       ]
    #     ]
    #   end)

    # req = :kpro_req_lib.create_topics(0, topic_settings, %{timeout: timeout})

    # with {:ok, {:kpro_rsp, _, :create_topics, _, %{topic_errors: errors}}} <-
    #        IO.inspect(:kpro.request_sync(conn, req, timeout)) do
    #   if err = Enum.find(errors, &(&1.error_code != :no_error)) do
    #     {:error, err.error_code}
    #   else
    #     :ok
    #   end
    # end
  end
end
