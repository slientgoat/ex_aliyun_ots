defmodule ExAliyunOtsTest.TunnelTest do
  @moduledoc """
  主要用来测试ailiyun的表格存储的通道服务tunnel的重要函数逻辑
  """

  use ExUnit.Case

  alias ExAliyunOts.TableStoreTunnel.Channel, as: C
  alias ExAliyunOts.Tunnel.Worker, as: W

  def remote_source() do
    [
      %C{channel_id: "id0",version: 1,status: W.channel_status_close()},
      %C{channel_id: "id1",version: 1,status: W.channel_status_open()},
      %C{channel_id: "id2",version: 1,status: W.channel_status_open()},
      %C{channel_id: "id3",version: 1,status: W.channel_status_open()},
      %C{channel_id: "version_become_smaller",version: 2,status: W.channel_status_open()},
      %C{channel_id: "same_version",version: 2,status: W.channel_status_open()},
      %C{channel_id: "same_version_and_status",version: 2,status: W.channel_status_open()},
    ]
  end

  def working() do
    [
      %C{channel_id: "id1",version: 1,status: W.channel_status_open()},
      %C{channel_id: "id2",version: 1,status: W.channel_status_open()},
      %C{channel_id: "id3",version: 1,status: W.channel_status_open()},
      %C{channel_id: "version_become_smaller",version: 2,status: W.channel_status_open()},
      %C{channel_id: "same_version",version: 2,status: W.channel_status_open()},
      %C{channel_id: "same_version_and_status",version: 2,status: W.channel_status_open()},

    ]
  end

  def changed() do
    [
      %C{channel_id: "id2",version: 2,status: W.channel_status_open()},
      %C{channel_id: "id3",version: 2,status: W.channel_status_closing()},
      %C{channel_id: "id4",version: 1,status: W.channel_status_open()},
      %C{channel_id: "id5",version: 1,status: W.channel_status_close()},
      %C{channel_id: "id6",version: 1,status: W.channel_status_terminated()},
      %C{channel_id: "version_become_smaller",version: 1,status: W.channel_status_terminated()},
      %C{channel_id: "same_version",version: 2,status: W.channel_status_close()},
      %C{channel_id: "same_version_and_status",version: 2,status: W.channel_status_open()},

    ]
  end

  def merged() do
    [
      %C{channel_id: "id1",version: 1+1,status: W.channel_status_close()},
      %C{channel_id: "id2",version: 2,status: W.channel_status_open()},
      %C{channel_id: "id3",version: 2,status: W.channel_status_closing()},
      %C{channel_id: "id4",version: 1,status: W.channel_status_open()},
      %C{channel_id: "same_version",version: 2,status: W.channel_status_close()},
    ]
  end


  test "channel merge logic" do
    r = W.channels_merge([], remote_source())
    assert r == working()
    r2 = W.channels_merge(working(), changed())
    assert Enum.sort(r2) == Enum.sort(merged())
  end




end

