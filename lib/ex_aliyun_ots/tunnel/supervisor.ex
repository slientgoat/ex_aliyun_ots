defmodule ExAliyunOts.Tunnel.Supervisor do
  @moduledoc false

  alias ExAliyunOts.Tunnel.Worker

  use Supervisor

  @typedoc "The supervisor specification"
  @type tunnel_config :: %{
          required(:instance) => atom(),
          required(:table_name) => binary(),
          required(:tunnel_name) => binary
        }
  @doc """
    arg:
      :client_config
        :instance
        :table_name
        :tunnel_name
      :worker_config
        :pool_size
        :channel_config
        :heartbeat_timeout
        :heartbeat_interval
        :customer_module
    arg = %{tunnel_config: %{instance: EDCEXTestInstance,table_name: "pxy_test",tunnel_name: "add2"},
            worker_config: %{pool_size: 2,heartbeat_timeout: 30_000,heartbeat_interval: 10_000,customer_module: CustomerExample}
           }

  """
  @spec start_link(arg :: tunnel_config, GenServer.Option) :: Supervisor.on_start()
  def start_link(arg, opt) do
    Supervisor.start_link(__MODULE__, arg, opt)
  end

  def workers() do
    tunnel_config = %{
      client_config: %{
        table_name: "pxy_test",
        tunnel_name: "add2",
        worker_size: 2
      },
      worker_config: %{
        heartbeat_timeout: 30,
        heartbeat_interval: 10_000,
        checkpoint_interval: 10_000,
        customer_module: CustomerExample,
        instance: EDCEXTestInstance,
        tunnel_id: nil,
        client_id: nil,
        index: nil
      }
    }

    tunnel_config2 = %{
      client_config: %{
        table_name: "pxy_test",
        tunnel_name: "add",
        worker_size: 2
      },
      worker_config: %{
        heartbeat_timeout: 30,
        heartbeat_interval: 10_000,
        checkpoint_interval: 10_000,
        customer_module: CustomerExample,
        instance: EDCEXTestInstance,
        tunnel_id: nil,
        client_id: nil,
        index: nil
      }
    }

    [
      supervisor_spec(tunnel_config)
      #      supervisor_spec(tunnel_config2)
    ]
  end

  @impl true
  def init(tunnel_config) do
    children = worker_spec(tunnel_config)
    Supervisor.init(children, strategy: :one_for_one)
  end

  defp supervisor_spec(tunnel_config) do
    id = gen_supervisor_name(tunnel_config)

    %{
      id: id,
      start: {__MODULE__, :start_link, [tunnel_config, [name: Module.concat(__MODULE__, id)]]}
    }
  end

  defp gen_supervisor_name(tunnel_config) do
    instance = tunnel_config.worker_config.instance
    client_config = tunnel_config.client_config
    "#{instance}_#{client_config.table_name}_#{client_config.tunnel_name}"
  end

  def worker_spec(tunnel_config) do
    worker_size = tunnel_config.client_config.worker_size

    f = fn x ->
      id = gen_id(tunnel_config, x)
      worker_config = %{tunnel_config.worker_config | index: x}
      tunnel_config = %{tunnel_config | worker_config: worker_config}

      %{
        id: id,
        start: {Worker, :start_link, [tunnel_config, [id: id]]},
        type: :worker
      }
    end

    Enum.map(1..worker_size, f)
  end

  defp gen_id(tunnel_config, x) do
    "#{gen_supervisor_name(tunnel_config)}_#{x}"
  end
end
