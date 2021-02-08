defmodule Kvasir.Publisher do
  def do_commit(client, event, topic, partition, key, data) do
    case :brod.produce_sync_offset(
           client,
           topic,
           partition,
           key,
           data
         ) do
      {:ok, offset} ->
        {:ok,
         event
         |> Kvasir.Event.set_offset(offset)
         |> Kvasir.Event.set_key(key)
         |> Kvasir.Event.set_partition(partition)
         |> Kvasir.Event.set_timestamp(UTCDateTime.utc_now())
         |> Kvasir.Event.set_topic(topic)}

      {:error, {:producer_not_found, _}} ->
        :brod.start_producer(client, topic, [])
        do_commit(client, event, topic, partition, key, data)

      other ->
        other
    end
  end

  def report_publish_metric(result, event, topic, partition, start) do
    stop = :erlang.monotonic_time()
    ms = :erlang.convert_time_unit(stop - start, :native, :millisecond)

    success = if match?({:ok, _}, result), do: ",success:true", else: ",success:false"

    Kvasir.Kafka.Metrics.Sender.send([
      "kvasir.kafka.publish.timer:",
      to_string(ms),
      "|ms|#event:",
      event.__event__(:type),
      ",topic:",
      topic,
      ",partition:",
      to_string(partition),
      success
    ])

    result
  end
end
