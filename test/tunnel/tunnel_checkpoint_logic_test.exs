defmodule ExAliyunOtsTest.TunnelCheckpointLogicTest do
  @moduledoc """
  主要用来测试ailiyun的表格存储的通道服务sdk的checkpoint机制的逻辑
  """
  use ExAliyunOtsTest.TunnelCase
  alias ExAliyunOtsTest.TunnelCase, as: Case

  alias ExAliyunOts.TableStoreTunnel.GetCheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.ReadRecordsResponse
  alias ExAliyunOts.TableStoreTunnel.CheckpointResponse


  test "tunnel get_checkpoint and readrecords logic", %{client_id: client_id, channel_id: channel_id} = context  do
    Case.checkpoint(context)
    Case.put_row()

    r2 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: _}} = r2
    r3 = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    {:ok, %ReadRecordsResponse{next_token: next_token1, records: records}} = r3
    # r3 one record
    assert(length(records) == 1)

    r4 = read_records(context.tunnel.tunnel_id, client_id, channel_id, next_token1)
    {:ok, %ReadRecordsResponse{next_token: next_token2, records: records}} = r4
    # r4 zero record
    assert(length(records) == 0)

    r5 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    # r5.checkpoint == r2.checkpoint
    assert(elem(r5, 1).checkpoint == elem(r2, 1).checkpoint)


    Case.put_row()

    r6 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: _}} = r6
    # r6.checkpoint == r5.checkpoint
    assert(elem(r6, 1).checkpoint == elem(r5, 1).checkpoint)


    r7 = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    {:ok, %ReadRecordsResponse{records: records}} = r7
    # r7 two record
    assert(length(records) == 2)

    r8 = read_records(context.tunnel.tunnel_id, client_id, channel_id, next_token2)
    {:ok, %ReadRecordsResponse{next_token: _, records: records}} = r8
    # r8 one record
    assert(length(records) == 1)
    true
  end



  # 生产者不停readrecords数据给消费者，消费者可能存在checkpoint的情况（消费失败，延迟，可携带消费参数通知生产者）
  # 第一条记录写入后，处理进程P1在时间点1执行get_checkpoiont:得到seq-num=1，token=1。
  # P1在时间点2执行readrecords（1）得到length(rec)=1，token=2，把rec和token=2推送给消费进程C1
  # C1 在时间点3收到rec和token，休息至时间点12执行 checkpoint(token=2,seq-num=3)。
  # 第二条记录在时间点4写入。
  # P1在时间点5执行readrecords（2）得到length(rec)=1，token=3，把rec和token=3推送给消费进程C2
  # C2 在时间点6收到rec和token，在时间点7执行 checkpoint(token=3,seq-num=2)。

  # 现状：checkpoint(token=2,seq-num=3) 成功消费了readrecords(1)的rec，但是checkpoint(token=3,seq-num=2)消费readrecords(2)的rec失败了
  # 结论：checkpoint(token,seq-num)接口是用来更新token所对应的数据位点，该消费逻辑不可行，同一channel，只能按序消费

  # one tunnel client = one producer = multi channels。each channel is unique and  can be processed by individual,
  # channelconnect was in producer's processor,include checkpointer(TunnelWorkerConfig.CheckpointInterval)
  # heartbeat was in producer,

  # 非按序消费
  test "abnormal tunnel checkpointer logic", %{client_id: client_id, channel_id: channel_id} = context  do
    Case.checkpoint(context)
    Case.put_row()

    r1 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: token1, sequence_number: seq_num}} = r1

    r2 = read_records(context.tunnel.tunnel_id, client_id, channel_id, token1)
    {:ok, %ReadRecordsResponse{next_token: token2, records: [rec2]}} = r2
    Case.put_row()
    r3 = read_records(context.tunnel.tunnel_id, client_id, channel_id, token2)
    {:ok, %ReadRecordsResponse{next_token: token3, records: [rec3]}} = r3
    assert(rec2.record != rec3.record)

    #更新消费数据位点至消费token3所对应的位点，相当于两个数据状态改为已消费
    {:ok, %CheckpointResponse{}} = checkpoint(context.tunnel.tunnel_id, client_id, channel_id, token3, seq_num + 1)
    pointer = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: _}} = pointer
    r = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    {:ok, %ReadRecordsResponse{next_token: _, records: rec}} = r
    assert(length(rec)== 0)

    #回退消费数据位点至token1所对应的位点，之前的两条已消费数据，恢复成未消费状态
    {:ok, %CheckpointResponse{}} = checkpoint(context.tunnel.tunnel_id, client_id, channel_id, token1, seq_num + 2)
    pointer = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: _}} = pointer
    r = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    {:ok, %ReadRecordsResponse{next_token: _, records: rec}} = r
    assert(length(rec)== 2)
  end

  # 按序消费
  test "normal tunnel checkpointer logic", %{client_id: client_id, channel_id: channel_id} = context  do
    Case.checkpoint(context)
    Case.put_row()

    r1 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: token1, sequence_number: seq_num}} = r1

    r2 = read_records(context.tunnel.tunnel_id, client_id, channel_id, token1)
    {:ok, %ReadRecordsResponse{next_token: token2, records: [_]}} = r2

    Case.put_row()

    r3 = read_records(context.tunnel.tunnel_id, client_id, channel_id, token2)
    {:ok, %ReadRecordsResponse{next_token: token3, records: [_]}} = r3
    {:ok, %CheckpointResponse{}} = checkpoint(context.tunnel.tunnel_id, client_id, channel_id, token2, seq_num + 1)

    {:ok, %CheckpointResponse{}} = checkpoint(context.tunnel.tunnel_id, client_id, channel_id, token3, seq_num + 2)

    r5 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: token5, sequence_number: _}} = r5
    r6 = read_records(context.tunnel.tunnel_id, client_id, channel_id, token5)
    {:ok, %ReadRecordsResponse{next_token: _, records: rec6}} = r6
    assert(length(rec6) == 0)

  end
end

