defmodule ExAliyunOts.Tunnel.Worker do
  @moduledoc false

  use GenServer
  alias ExAliyunOts.Tunnel.Channel

  # TODO xx
  alias ExAliyunOts.TableStoreTunnel.DescribeTunnelResponse
  alias ExAliyunOts.TableStoreTunnel.ClientConfig
  alias ExAliyunOts.TableStoreTunnel.ConnectResponse
  alias ExAliyunOts.TableStoreTunnel.HeartbeatResponse

  import ExAliyunOts.Mixin,
    only: [
      execute_describe_tunnel: 4,
      execute_connect_tunnel: 3,
      execute_heartbeat_tunnel: 4,
      execute_shutdown_tunnel: 3
    ]

  @client_tag Mix.Project.config()[:app]
              |> Atom.to_string()
  @status_open :OPEN
  @status_closing :CLOSING
  @status_close :CLOSE
  @status_terminated :TERMINATED
  def channel_status_open, do: @status_open
  def channel_status_closing, do: @status_closing
  def channel_status_close, do: @status_close
  def channel_status_terminated, do: @status_terminated

  def channels_merge([], new),
    do:
      new
      |> Enum.filter(&(&1.status == @status_open))

  def channels_merge(old, new) do
    filter_old(old, new)
    |> change_status_to_close()
    |> Enum.concat(filter_new(old, new))
  end

  def start_link(tunnel_config, opt) do
    GenServer.start_link(__MODULE__, tunnel_config, name: Module.concat(Worker, opt[:id]))
  end

  def init(tunnel_config) do
    Process.flag(:trap_exit, true)
    send(self(), :connect)

    worker_config =
      %{tunnel_id: nil, client_id: nil}
      |> Map.merge(tunnel_config)
      |> Map.put(:heartbeat_interval, tunnel_config.heartbeat_interval * 1000)

    state = %{working_channels: %{}, worker_config: worker_config}
    {:ok, state}
  end

  def handle_info(:connect, state) do
    %{worker_config: worker_config} = state
    {tunnel, client_id} = connect(worker_config)

    worker_config =
      Map.put(worker_config, :tunnel_id, tunnel.tunnel_id)
      |> Map.put(:client_id, client_id)

    send(self(), :heartbeat)
    {:noreply, %{state | worker_config: worker_config}}
  end

  # 一个producer 对应一个tunnel client
  # TODO heartbeat、readrecords，多个producer时考虑热点
  def handle_info(:heartbeat, %{worker_config: worker_config} = state) do
    heartbeat_interval = worker_config.heartbeat_interval
    channels = convert_to_channels(state.working_channels)

    case execute_heartbeat_tunnel(
           worker_config.instance,
           worker_config.tunnel_id,
           worker_config.client_id,
           channels
         ) do
      {:ok, %HeartbeatResponse{channels: []}} ->
        Process.send_after(self(), :heartbeat, heartbeat_interval)
        {:noreply, state}

      {:ok, %HeartbeatResponse{channels: new_channels}} ->
        state = batch_update_channel_state_machine(state, new_channels)
        Process.send_after(self(), :heartbeat, heartbeat_interval)
        {:noreply, state}

      {:error, reason} ->
        # 重启当前进程
        {:stop, inspect(reason)}
    end
  end

  #  def handle_info({:EXIT, _supervisor_pid, _reason}, state) do
  #    {:noreply, state}
  #  end

  def handle_call(_msg, _from, state) do
    {:reply, :ok, state}
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  defp connect(worker_config) do
    instance = worker_config.instance
    table_name = worker_config.table_name
    tunnel_name = worker_config.tunnel_name

    {:ok, %DescribeTunnelResponse{tunnel: tunnel}} =
      execute_describe_tunnel(instance, table_name, tunnel_name, nil)

    heartbeat_timeout = worker_config.heartbeat_timeout

    {:ok, %ConnectResponse{client_id: client_id}} =
      execute_connect_tunnel(instance, tunnel.tunnel_id, %ClientConfig{
        timeout: heartbeat_timeout,
        client_tag: @client_tag
      })

    {tunnel, client_id}
  end

  def shutdown(worker_config) do
    execute_shutdown_tunnel(
      worker_config.instance,
      worker_config.tunnel_id,
      worker_config.client_id
    )
  end

  @required_channel_config [:instance, :customer_module, :tunnel_id, :client_id]
  defp create_channel_broadway(channel, worker_config, working_channels) do
    channel_config =
      Map.take(worker_config, @required_channel_config)
      |> Map.put(:channel_id, channel.channel_id)

    {:ok, pid} = Channel.create(channel_config)
    Map.put(working_channels, channel.channel_id, %{info: channel, pid: pid})
  end

  defp remove_channel_broadway(identifier, working_channels) do
    {channel, rest} = Map.pop(working_channels, identifier)
    GenServer.stop(channel.pid)
    rest
  end

  defp batch_update_channel_state_machine(state, new_channels) do
    %{worker_config: worker_config, working_channels: working_channels} = state

    working_channels =
      convert_to_channels(working_channels)
      |> channels_merge(new_channels)
      |> do_batch_update(worker_config, working_channels)

    %{state | working_channels: working_channels}
  end

  defp do_batch_update([], _, working_channels), do: working_channels

  defp do_batch_update([%{status: status} = c | t], worker_config, working_channels)
       when status in [@status_close, @status_closing, @status_terminated] do
    working_channels = remove_channel_broadway(c.channel_id, working_channels)

    do_batch_update(t, worker_config, working_channels)
  end

  defp do_batch_update([%{status: status} = c | t], worker_config, working_channels)
       when status == @status_open do
    working_channels = create_channel_broadway(c, worker_config, working_channels)

    do_batch_update(t, worker_config, working_channels)
  end

  defp convert_to_channels(working_channels) do
    Enum.map(working_channels, &elem(&1, 1).info)
  end

  defp filter_new(old, new) do
    f = fn n ->
      o = Enum.find(old, fn o -> n.channel_id == o.channel_id end)

      if o == nil do
        n.status == @status_open
      else
        cond do
          n.version > o.version ->
            true

          n.version == o.version ->
            n.status != o.status

          true ->
            false
        end
      end
    end

    Enum.filter(new, f)
  end

  defp filter_old(old, new) do
    f = fn o ->
      n = Enum.find(new, fn n -> n.channel_id == o.channel_id end)

      if n == nil do
        o.status == @status_open
      else
        false
      end
    end

    Enum.filter(old, f)
  end

  defp change_status_to_close(old) do
    Enum.map(old, fn x -> %{x | status: @status_close, version: x.version + 1} end)
  end
end
