defmodule ExAliyunOtsTest.TunnelTest do
  use ExUnit.Case
  @instance_key EDCEXTestInstance
  @table_name "pxy_test"
  @tunnel_name "exampleTunnel"
  @client_tag :inet.gethostname()
              |> elem(1)
  @request_timeout 30
  use ExAliyunOts, instance: @instance_key
  alias ExAliyunOts.TableStoreTunnel.DescribeTunnelResponse
  alias ExAliyunOts.TableStoreTunnel.ClientConfig
  alias ExAliyunOts.TableStoreTunnel.ConnectResponse
  alias ExAliyunOts.TableStoreTunnel.HeartbeatResponse
  alias ExAliyunOts.TableStoreTunnel.Channel
  alias ExAliyunOts.TableStoreTunnel.ShutdownResponse
  alias ExAliyunOts.TableStoreTunnel.GetCheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.CheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.ReadRecordsResponse
  setup do
    {:ok, %DescribeTunnelResponse{tunnel: tunnel, channels: channels}} = describe_tunnel(@table_name, @tunnel_name, nil)
    {:ok, [tunnel: tunnel, channels: channels]}
  end

  test "tunnel/connect", context do
    result = connect_tunnel(context.tunnel.tunnel_id, %ClientConfig{})
    assert {:ok, %ConnectResponse{}} = result
  end

  test "tunnel/heartbeat", context do
    result = connect_tunnel(context.tunnel.tunnel_id, %ClientConfig{})
    {:ok, %ConnectResponse{client_id: client_id}} = result
    channels = patch_channels(context.channels)
    result2 = heartbeat_tunnel(context.tunnel.tunnel_id, client_id, channels)
    assert {:ok, %HeartbeatResponse{}} = result2
  end

  test "tunnel/shutdown", context do
    result = connect_tunnel(context.tunnel.tunnel_id, %ClientConfig{})
    {:ok, %ConnectResponse{client_id: client_id}} = result
    result2 = shutdown_tunnel(context.tunnel.tunnel_id, client_id)
    assert {:ok, %ShutdownResponse{}} = result2
  end

  test "tunnel/getcheckpoint", context do
    result = connect_tunnel(context.tunnel.tunnel_id, %ClientConfig{})
    {:ok, %ConnectResponse{client_id: client_id}} = result
    channel_id = context.channels
                 |> List.first()
                 |> Map.get(:channel_id)
    result2 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    assert {:ok, %GetCheckpointResponse{}} = result2
  end

  test "tunnel/readrecords", context do
    result = connect_tunnel(context.tunnel.tunnel_id, %ClientConfig{})
    {:ok, %ConnectResponse{client_id: client_id}} = result
    channel_id = context.channels
                 |> List.first()
                 |> Map.get(:channel_id)
    result2 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: _sequence_number}} = result2
    result3 = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    assert {:ok, %ReadRecordsResponse{}} = result3
  end

  test "tunnel/checkpoint", context do
    result = connect_tunnel(context.tunnel.tunnel_id, %ClientConfig{timeout: @request_timeout, client_tag: @client_tag})
    {:ok, %ConnectResponse{client_id: client_id}} = result
    channel_id = context.channels
                 |> List.first()
                 |> Map.get(:channel_id)
    result2 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: sequence_number}} = result2
    result3 = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    {:ok, %ReadRecordsResponse{next_token: next_token, records: records}} = result3

    cond do
      records == [] ->
        :ok
      true ->
        result4 = checkpoint(context.tunnel.tunnel_id, client_id, channel_id, next_token, sequence_number + 1)
        assert {:ok, %CheckpointResponse{}} = result4
    end
  end

  def patch_channels(channels) do
    Enum.map(
      channels,
      fn info ->
        version = Integer.floor_div(info.channel_rpo, 1000000)
        status = info.channel_status
                 |> String.to_atom()

        %Channel{channel_id: info.channel_id, version: version, status: status}
      end
    )


  end

end

