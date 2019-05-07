defmodule ExAliyunOtsTest.TunnelCheckpointLogicTest do
  @moduledoc """
  主要用来测试ailiyun的表格存储的通道服务sdk的checkpoint机制的逻辑
  """
  use ExAliyunOtsTest.TunnelCase
  alias ExAliyunOtsTest.TunnelCase, as: Case

  alias ExAliyunOts.TableStoreTunnel.GetCheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.ReadRecordsResponse


  test "tunnel checkpoint logic", %{client_id: client_id, channel_id: channel_id} = context  do
    Case.checkpoint(context)
    Case.put_row()
    Process.sleep(5000)
    r2 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: _}} = r2
    r3 = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    {:ok, %ReadRecordsResponse{next_token: next_token, records: records}} = r3
    # r3 one record
    assert(length(records) == 1)

    r4 = read_records(context.tunnel.tunnel_id, client_id, channel_id, next_token)
    {:ok, %ReadRecordsResponse{next_token: next_token, records: records}} = r4
    # r4 zero record
    assert(length(records) == 0)

    r5 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    # r5.checkpoint == r2.checkpoint
    assert(elem(r5, 1).checkpoint == elem(r2, 1).checkpoint)


    Case.put_row()
    Process.sleep(5000)

    r6 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: _}} = r6
    # r6.checkpoint == r5.checkpoint
    assert(elem(r6, 1).checkpoint == elem(r5, 1).checkpoint)


    r7 = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    {:ok, %ReadRecordsResponse{records: records}} = r7
    # r7 two record
    assert(length(records) == 2)

    r8 = read_records(context.tunnel.tunnel_id, client_id, channel_id, next_token)
    {:ok, %ReadRecordsResponse{next_token: _, records: records}} = r8
    # r8 one record
    assert(length(records) == 1)

    true
  end



end

