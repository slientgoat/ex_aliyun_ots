defmodule ExAliyunOts.TunnelProducer do
  @moduledoc """

    sdk implement by aliyun tunnel:

        * [tunnel sdk](https://help.aliyun.com/document_detail/107985.html?spm=a2c4g.11186623.4.1.34b41b50mN8eyX)

  """

  @instance_key EDCEXTestInstance
  @table_name "pxy_test"
  @tunnel_name "exampleTunnel"
  @client_tag :inet.gethostname()
              |> elem(1)

  # DefaultHeartbeatTimeout 300s
  @heartbeat_timeout 30

  # DefaultHeartbeatInterval 30s
  @heartbeat_interval 10 * 1000

  # DefaultCheckpointInterval 10s
  @checkpoint_interval 10 * 1000

  use ExAliyunOts, instance: @instance_key
  alias ExAliyunOts.TableStoreTunnel.DescribeTunnelResponse
  alias ExAliyunOts.TableStoreTunnel.ClientConfig
  alias ExAliyunOts.TableStoreTunnel.ConnectResponse
  alias ExAliyunOts.TableStoreTunnel.HeartbeatResponse
#  alias ExAliyunOts.TableStoreTunnel.ShutdownResponse
  #  alias ExAliyunOts.TableStoreTunnel.CheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.GetCheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.ReadRecordsResponse
  alias ExAliyunOts.TableStoreTunnel.Channel
  alias Broadway.Message

  @behaviour Broadway.Producer

  # 一个producer 对应一个tunnel client
  #TODO heartbeat、readrecords，多个producer时考虑热点
  def init(_args) do
    {:ok, %DescribeTunnelResponse{tunnel: tunnel, channels: _channels_info}} = describe_tunnel(
      @table_name,
      @tunnel_name,
      nil
    )
    result = connect_tunnel(tunnel.tunnel_id, %ClientConfig{timeout: @heartbeat_timeout, client_tag: @client_tag})
    {:ok, %ConnectResponse{client_id: client_id}} = result
    send(self(), :heartbeat)
    {:producer, %{tunnel: tunnel, client_id: client_id, channels: [], checkpoints: %{}}}
  end

  def prepare_for_draining(state) do
    IO.inspect(6, label: "---------------tag")
    shutdown_tunnel(state.tunnel_id, state.client_id)
  end

  # will be invoted by each processor
  def handle_demand(_demand, state) do
    events = []

    ref = make_ref()
    ack = {Broadway.CallerAcknowledger, {self(), ref}, :ok}
    events = Enum.map(events, &%Message{data: &1, acknowledger: ack})
    {:noreply, events, state}
  end


  def handle_info(:heartbeat, %{checkpoints: v} = state) do
    result2 = heartbeat_tunnel(state.tunnel.tunnel_id, state.client_id, state.channels)
    case result2 do
      {:ok, %HeartbeatResponse{channels: []}} ->
        Process.send_after(self(), :heartbeat, @heartbeat_interval)
        {:noreply, [], state}
      {:ok, %HeartbeatResponse{channels: channels}} ->
        if Map.size(v) == 0, do: send(self(), :getcheckpoint)
        channels = channels_merge(state.channels, channels)
        Process.send_after(self(), :heartbeat, @heartbeat_interval)
        {:noreply, [], %{state | channels: channels}}
      {:error, reason} ->
        #重启当前进程
        {:stop, inspect(reason), state}
    end
  end

  # TODO get_checkpoint的错误处理
  def handle_info(:getcheckpoint, state)  do
    v = get_checkpoints(state.tunnel.tunnel_id, state.client_id, state.channels, %{})
    state = %{state | checkpoints: v}
    send(self(), :readrecords)
    {:noreply, [], state}
  end

  def handle_info(:readrecords, state) do
    readrecords(state.tunnel.tunnel_id, state.client_id, state.checkpoints)
    {:noreply, [], state}
  end

  # TODO 寻找方案
  def handle_info(:checkpoint, state) do
    Process.send_after(self(), :checkpoint, @heartbeat_interval)
    {:noreply, [], state}
  end

  def handle_info(events, state) do
    IO.inspect(events, label: "------events")
    {:noreply, events, state}
  end

  def terminate(_reason, %{tunnel: nil}), do: :ok
  def terminate(_reason, state) do
    shutdown_tunnel(state.tunnel.tunnel_id, state.client_id)
  end

  def get_checkpoints(_tunnel_id, _client_id, [], checkpoints), do: checkpoints
  def get_checkpoints(tunnel_id, client_id, [channel | channels], checkpoints) do
    result2 = get_checkpoint(tunnel_id, client_id, channel.channel_id)
    case result2 do
      {:ok, resp} ->
        get_checkpoints(tunnel_id, client_id, channels, Map.put(checkpoints, channel.channel_id, resp))

      {:error, _} ->
        get_checkpoints(tunnel_id, client_id, channels, checkpoints)
    end
  end


  defp readrecords(tunnel_id, client_id, checkpoints) do
    prodocer_pid = self()
    f = fn ({channel_id, %GetCheckpointResponse{checkpoint: checkpoint}}) ->
      spawn_link(
        fn ->
          result = read_records(tunnel_id, client_id, channel_id, checkpoint)
          IO.inspect(result, label: "---------------result")
          {:ok, %ReadRecordsResponse{next_token: next_token, records: records} = _resp} = result
          ref = make_ref()
          ack = {Broadway.CallerAcknowledger, {self(), ref}, :ok}
          metadata = %{token: next_token}
          messages = Enum.map(records, &%Message{data: &1, acknowledger: ack, metadata: metadata})

          Broadway.Producer.push_messages(prodocer_pid, messages)
        end
      )
    end
    Enum.map(checkpoints, f)
  end

  defp channels_merge(old, cur) do
    Enum.sort(old ++ cur, &(&1.version > &2.version))
    |> Enum.uniq_by(fn %Channel{channel_id: channel_id} -> channel_id end)


  end

  defp check_sequence(tunnel_id, client_id, channel_id) do
    result2 = get_checkpoint(tunnel_id, client_id, channel_id)
    case result2 do
      {:ok, %GetCheckpointResponse{sequence_number: sequence_number} = resp} ->
        {:ok, %{resp | sequence_number: sequence_number + 1}}
      error ->
        error
    end
  end

#  def transformer(events, _opts) do
#    IO.inspect(events, label: "transformer events")
#
#  end

# TODO 存在问题：
#  processor 是否可以作为channel_connect?，固定间隔时间checkpoint？还是消费完毕后实时checkpoint？按批消费？如果允许多批同时消费，那么如果前面消费异常，则后面全部失效
#  producer根据channels动态管理channel（CUD），同一channel，保证checkpoint有序
  #  消费端数据消费完，异常中断、导致tunnel数据位点没有更新。下次会出现再次取到上次业务层消费完的数据。结论：需要在消费端业务层做数据踢重处理
  # 每个channel进程，readrecords产生的数据可以被多个consumer进程同时处理，
  # consumer进程处理完成后会把处理结果返回给channel进程进行汇总，汇总结果没问题，然后进行下次readrecords


  def channel_connect_create(_tunnel,_channel,_client_id) do
    :ok
  end

  def channel_connect_update(_tunnel,_channel,_client_id) do
    :ok
  end
end
