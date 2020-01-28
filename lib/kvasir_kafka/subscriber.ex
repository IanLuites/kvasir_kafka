defmodule Kvasir.Kafka.Subscriber do
  def init(_group = %{partition: p}, {topic, offset, pre_filter, callback_module, state}) do
    {:ok, new_state} = callback_module.init(topic, p, state)

    if parent = :"$ancestors" |> Process.get([]) |> List.last() do
      Process.link(parent)
    end

    {:ok,
     %{
       topic: topic,
       partition: p,
       offset: Kvasir.Offset.get(offset, p),
       pre_filter: pre_filter,
       subscriber: callback_module,
       state: new_state
     }}
  end

  def handle_message(
        message = {:kafka_message, o, _, _, _, _, _},
        state = %{
          offset: offset,
          partition: partition,
          pre_filter: pre_filter,
          state: s,
          subscriber: sub,
          topic: topic
        }
      ) do
    if Kvasir.Offset.compare(o, offset) == :lt do
      {:ok, :commit, state}
    else
      with {:ok, event} <- Kvasir.Kafka.decode?(pre_filter, message, topic, partition) do
        case sub.event(event, s) do
          :ok -> {:ok, :commit, state}
          {:ok, new_s} -> {:ok, :commit, %{state | state: new_s}}
          error -> error
        end
      else
        :ok -> {:ok, :commit, state}
        err -> err
      end
    end
  end
end
