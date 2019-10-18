defmodule Kvasir.KafkaTest do
  use ExUnit.Case
  doctest Kvasir.Kafka

  test "greets the world" do
    assert Kvasir.Kafka.hello() == :world
  end
end
