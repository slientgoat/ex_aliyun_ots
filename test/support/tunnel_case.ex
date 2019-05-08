defmodule ExAliyunOtsTest.TunnelCase do
  @moduledoc """
  This module defines the test case to be used by
  tests that require setting up a connection.

  Such tests rely on `Phoenix.ConnTest` and also
  import other functionality to make it easier
  to build common data structures and query the data layer.

  Finally, if the test case interacts with the database,
  it cannot be async. For this reason, every test runs
  inside a transaction which is reset at the beginning
  of the test unless the test case is marked as async.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      @instance_key EDCEXTestInstance
      use ExAliyunOts, instance: @instance_key
    end
  end


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
  alias ExAliyunOts.TableStoreTunnel.ShutdownResponse
  alias ExAliyunOts.TableStoreTunnel.GetCheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.CheckpointResponse
  alias ExAliyunOts.TableStoreTunnel.ReadRecordsResponse
  setup do
    {:ok, %DescribeTunnelResponse{tunnel: tunnel, channels: channels}} = describe_tunnel(@table_name, @tunnel_name, nil)
    %{client_id: client_id, channel_id: channel_id} = connect(tunnel, channels)
    on_exit(fn ->
      shutdown(%{client_id: client_id,tunnel: tunnel})
    end)
    {:ok, [tunnel: tunnel, channels: channels, client_id: client_id, channel_id: channel_id]}
  end

  defp connect(tunnel, channels) do
    result = connect_tunnel(tunnel.tunnel_id, %ClientConfig{timeout: @request_timeout, client_tag: @client_tag})
    {:ok, %ConnectResponse{client_id: client_id}} = result
    channel_id = channels
                 |> List.first()
                 |> Map.get(:channel_id)
    %{client_id: client_id, channel_id: channel_id}
  end

  def shutdown(%{client_id: client_id} = context) do
    shutdown_tunnel(context.tunnel.tunnel_id, client_id)
  end

  def checkpoint(%{client_id: client_id, channel_id: channel_id} = context) do
    result2 = get_checkpoint(context.tunnel.tunnel_id, client_id, channel_id)
    {:ok, %GetCheckpointResponse{checkpoint: checkpoint, sequence_number: sequence_number}} = result2
    result3 = read_records(context.tunnel.tunnel_id, client_id, channel_id, checkpoint)
    {:ok, %ReadRecordsResponse{next_token: next_token, records: records}} = result3

    cond do
      records == [] ->
        :ok
      true ->
        {:ok, %CheckpointResponse{}} = checkpoint(
          context.tunnel.tunnel_id,
          client_id,
          channel_id,
          next_token,
          sequence_number + 1
        )
        :ok
    end
  end

  def random_string(length) do
    :crypto.strong_rand_bytes(length)
    |> Base.url_encode64
    |> binary_part(0, length)
  end

  alias ExAliyunOts.Var
  alias ExAliyunOts.Const.ReturnType
  def put_row() do

    partition_key = random_string(10)
    condition = %Var.Condition{
      row_existence: :'IGNORE'
    }
    var_put_row = %Var.PutRow{
      table_name: @table_name,
      primary_keys: [{"str1", partition_key}],
      attribute_columns: [
        {"v", :rand.uniform(999999)}
      ],
      condition: condition,
      return_type: ReturnType.pk
    }
    {:ok, _result} = ExAliyunOts.Client.put_row(@instance_key, var_put_row)
  end
end
