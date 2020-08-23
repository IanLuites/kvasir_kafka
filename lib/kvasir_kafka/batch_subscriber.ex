defmodule Kvasir.Kafka.BatchSubscriber do
  require Logger

  def init(_group = %{partition: p}, {topic, offset, decoder, callback_module, state}) do
    {:ok, new_state} = callback_module.init(topic, p, state)

    {:ok,
     %{
       topic: topic,
       partition: p,
       offset: Kvasir.Offset.get(offset, p),
       decoder: decoder,
       subscriber: callback_module,
       state: new_state
     }}
  end

  def handle_message(
        {:kafka_message_set, _, _, _, messages},
        state = %{
          offset: offset,
          partition: partition,
          decoder: decoder,
          state: s,
          subscriber: sub,
          topic: topic
        }
      ) do
    case prepare_batch(messages, offset, {decoder, topic, partition}) do
      {:ok, []} ->
        {:ok, :commit, state}

      {:ok, events} ->
        case sub.event_batch(events, s) do
          :ok -> {:ok, :commit, state}
          {:ok, new_s} -> {:ok, :commit, %{state | state: new_s}}
          error -> error
        end

      err ->
        Logger.error("Kvasir Kafka: Subscriber Error: #{inspect(err)}", error: err)
        err
    end
  end

  defp prepare_batch(batch, offset, state, acc \\ [])
  defp prepare_batch([], offset, state, acc), do: {:ok, :lists.reverse(acc)}

  defp prepare_batch(
         [message = {:kafka_message, o, _, _, _, _, _} | batch],
         offset,
         s = {decoder, topic, partition},
         acc
       ) do
    if Kvasir.Offset.compare(o, offset) == :lt do
      prepare_batch(batch, offset, acc)
    else
      case Kvasir.Kafka.decode?(decoder, message, topic, partition) do
        {:ok, event} ->
          prepare_batch(batch, offset, s, [event | acc])

        :ok ->
          prepare_batch(batch, offset, s, acc)

        err ->
          err
      end
    end
  end
end
