defmodule ExAliyunOtsTest.TunnelTest do
  @moduledoc """
  主要用来测试ailiyun的表格存储的通道服务sdk基础接口
  """
  use ExAliyunOtsTest.TunnelCase
  alias ExAliyunOtsTest.TunnelCase, as: Case
  alias ExAliyunOts.TableStoreTunnel.HeartbeatResponse
  alias ExAliyunOts.TableStoreTunnel.ShutdownResponse
  alias ExAliyunOts.TableStoreTunnel.GetCheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.ReadRecordsResponse


  test "tunnel/heartbeat", %{client_id: client_id} = context do
    result2 = heartbeat_tunnel(context.tunnel.tunnel_id, client_id, [])
    assert {:ok, %HeartbeatResponse{}} = result2
  end

  test "tunnel/shutdown", %{client_id: client_id} = context do
    result2 = shutdown_tunnel(context.tunnel.tunnel_id, client_id)
    assert {:ok, %ShutdownResponse{}} = result2
  end

  test "tunnel/getcheckpoint", %{client_id: client_id, channel_id: channel_id} = context do
    result2 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    assert {:ok, %GetCheckpointResponse{}} = result2
  end

  test "tunnel/readrecords", %{client_id: client_id, channel_id: channel_id} = context do
    result2 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: _sequence_number}} = result2
    result3 = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    assert {:ok, %ReadRecordsResponse{}} = result3
  end

  test "tunnel/checkpoint", context do
    Case.checkpoint(context)
  end


end

