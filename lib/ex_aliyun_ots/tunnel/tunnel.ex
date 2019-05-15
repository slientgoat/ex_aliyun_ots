defmodule ExAliyunOts.Client.Tunnel do
  import ExAliyunOts.Logger, only: [debug: 1]
  alias ExAliyunOts.TableStoreTunnel.{CreateTunnelRequest, CreateTunnelResponse, Tunnel, DeleteTunnelRequest,
                                      DeleteTunnelResponse, ListTunnelRequest, ListTunnelResponse,
                                      DescribeTunnelRequest, DescribeTunnelResponse, ConnectRequest, ConnectResponse,
                                      HeartbeatRequest, HeartbeatResponse, ShutdownRequest, ShutdownResponse,
                                      GetCheckpointRequest, GetCheckpointResponse, CheckpointRequest,
                                      CheckpointResponse, ReadRecordsRequest, ReadRecordsResponse}
  alias ExAliyunOts.Http
  #TODO 错误处理
  def errCodeParamInvalid, do: "OTSParameterInvalid"
  def errCodeResourceGone, do: "OTSResourceGone"
  def errCodeServerUnavailable, do: "OTSTunnelServerUnavailable"
  def errCodeSequenceNotMatch, do: "OTSSequenceNumberNotMatch"
  def errCodeClientError, do: "OTSClientError"
  def errCodeTunnelExpired, do: "OTSTunnelExpired"
  def errCodePermissionDenied, do: "OTSPermissionDenied"
  def errCodeTunnelExist, do: "OTSTunnelExist"

  def request_to_create_tunnel(keywords) do
    CreateTunnelRequest.new(tunnel: Tunnel.new(keywords))
    |> CreateTunnelRequest.encode
  end

  def remote_create_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/create", request_body, &CreateTunnelResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "create_tunnel result: ",
          inspect(result)
        ]
      end
    )
    result
  end

  def request_to_delete_tunnel(keywords) do
    DeleteTunnelRequest.new(keywords)
    |> DeleteTunnelRequest.encode
  end

  def remote_delete_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/delete", request_body, &DeleteTunnelResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "delete_tunnel result: ",
          inspect(result)
        ]
      end
    )
    result
  end


  def request_to_list_tunnel(keywords) do
    ListTunnelRequest.new(keywords)
    |> ListTunnelRequest.encode
  end

  def remote_list_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/list", request_body, &ListTunnelResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "list_tunnel result: ",
          inspect(result)
        ]
      end
    )
    result
  end

  def request_to_describe_tunnel(keywords) do
    DescribeTunnelRequest.new(keywords)
    |> DescribeTunnelRequest.encode
  end

  def remote_describe_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/describe", request_body, &DescribeTunnelResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "describe_tunnel result: ",
          inspect(result)
        ]
      end
    )
    result
  end


  def request_to_connect(keywords) do
    ConnectRequest.new(keywords)
    |> ConnectRequest.encode
  end

  def remote_connect(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/connect", request_body, &ConnectResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "connect_tunnel result: ",
          inspect(result)
        ]
      end
    )
    result
  end

  def request_to_heartbeat(keywords) do
    HeartbeatRequest.new(keywords)
    |> HeartbeatRequest.encode
  end

  def remote_heartbeat(instance, request_body) do
    IO.inspect(request_body,label: "remote_heartbeat2")
    result =
      instance
      |> Http.client("/tunnel/heartbeat", request_body, &HeartbeatResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "heartbeat result: ",
          inspect(result)
        ]
      end
    )
    result
  end


  def request_to_shutdown(keywords) do
    ShutdownRequest.new(keywords)
    |> ShutdownRequest.encode
  end

  def remote_shutdown(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/shutdown", request_body, &ShutdownResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "shutdown result: ",
          inspect(result)
        ]
      end
    )
    result
  end


  def request_to_get_checkpoint(keywords) do
    GetCheckpointRequest.new(keywords)
    |> GetCheckpointRequest.encode
  end

  def remote_get_checkpoint(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/getcheckpoint", request_body, &GetCheckpointResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "get checkpoint result: ",
          inspect(result)
        ]
      end
    )
    result
  end


  def request_to_checkpoint(keywords) do
    CheckpointRequest.new(keywords)
    |> CheckpointRequest.encode
  end

  def remote_checkpoint(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/checkpoint", request_body, &CheckpointResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "checkpoint result: ",
          inspect(result)
        ]
      end
    )
    result
  end


  def request_to_read_records(keywords) do
    ReadRecordsRequest.new(keywords)
    |> ReadRecordsRequest.encode
  end

  def remote_read_records(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/readrecords", request_body, &ReadRecordsResponse.decode/1)
      |> Http.post()
    debug(
      fn ->
        [
          "read records result: ",
          inspect(result)
        ]
      end
    )
    result
  end


end