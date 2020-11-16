defmodule Kvasir.Client do
  @spec client_start_link(atom, opts :: Keyword.t()) :: {:ok, atom} | {:error, term}
  def client_start_link(client, opts \\ []) do
    {:state, _, hosts, _, _, _, _, c, _} = client |> Process.whereis() |> :sys.get_state()
    config = Keyword.take(c, ~w(query_api_versions reconnect_cool_down_seconds sasl ssl)a)
    name = opts[:name] || free_client(opts[:prefix] || "Elixir.Kvasir.Kafka.Client")

    with {:ok, pid} <- :brod.start_link_client(hosts, name, config) do
      if opts[:named] != false do
        {:ok, name}
      else
        {:ok, pid}
      end
    end
  end

  @spec client_child_spec(atom, opts :: Keyword.t()) ::
          {:ok, atom, Supervisor.child_spec()} | {:error, term}
  def client_child_spec(client, opts \\ []) do
    {:state, _, hosts, _, _, _, _, c, _} = client |> Process.whereis() |> :sys.get_state()
    config = Keyword.take(c, ~w(query_api_versions reconnect_cool_down_seconds sasl ssl)a)
    name = opts[:name] || free_client(opts[:prefix] || "Elixir.Kvasir.Kafka.Client")

    {:ok, name,
     %{
       id: :client,
       type: :supervisor,
       start: {:brod, :start_link_client, [hosts, name, config]}
     }}
  end

  @spec free_client(String.t(), non_neg_integer) :: atom
  defp free_client(prefix, id \\ 0) do
    client = :"#{prefix}#{id}"

    if Process.whereis(client) do
      free_client(prefix, id + 1)
    else
      client
    end
  end
end
