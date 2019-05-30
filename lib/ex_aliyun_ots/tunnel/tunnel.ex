defmodule ExAliyunOts.Client.Tunnel do
  @moduledoc ~S"""
  A AliyunOTS producer for Broadway.
  It allows developers to consume data efficiently from Aliyun's OTS

  ## Features
    - Automatically acknowledges messages.
    - Handles request data sources using backoff.

  ## Starting to use Tunnel

  In order to use Tunnel, you need to:

    1. Define your instance configuration,you can see more in `ex_aliyun_ots/README.md`
    2. Define your tunnel configuration
    3. Define a customer module to process data
    4. Add it to your application's supervision tree

  ### Simple Example
  Here the simplest example to show how to use tunnel sdk.
  Please define required configuration in `your_project/configs/xxx.exs`,mostly like this:

  First, define your instance configuration
  ```elixir
  config :ex_aliyun_ots, MyInstance
    name: "MyInstanceName",
    endpoint: "MyInstanceEndpoint",
    access_key_id: "MyAliyunRAMKeyID",
    access_key_secret: "MyAliyunRAMKeySecret",
    pool_size: 100, # Optional
    pool_max_overflow: 20 # Optional

  config :ex_aliyun_ots,
    instances: [MyInstance],
    debug: false # Optional
  ```

  Second, define your tunnel configuration
  ```elixir
  config :ex_aliyun_ots,
    tunnels: [CustomerExample]

  config :ex_aliyun_ots, CustomerExample,
    tunnel_config: %{
      instance: EDCEXTestInstance,
      table_name: "pxy_test",
      tunnel_name: "add2"
    }
  ```

  Third,define a consumer module for process business logic of aliyunOts' records.
  like this:
    ```elixir
    defmodule CustomerExample do
      @moduledoc false

      alias Broadway.Message
      alias ExAliyunOts.PlainBuffer


      # called by Broadway.Processor
      def handle_message(_, message, _) do
        message
        |> Message.update_data(&process_data/1)
      end

      defp process_data(data) do
        # Do some calculations, generate a JSON representation, etc.
        PlainBuffer.deserialize_row(data.record)
      end

      # called by Broadway.Consumer
      def handle_batch(_batcher_key, messages, _batch_info, _context) do
        # do some batch operation
        messages
      end

    end
    ```



  ### Advanced Example
  Here the example to show how to use tunnel sdk with multi batcher_key.
  Please define required configuration in `your_project/configs/xxx.exs`

  First, the same as Simple Example


  Second, define your tunnel configuration, the batchers_config is optional,you can see more in `Broadway`'s Batchers options
  ```elixir
  config :ex_aliyun_ots,
    tunnels: [CustomerExample]

  config :ex_aliyun_ots, CustomerExample,
    tunnel_config: %{
      instance: EDCEXTestInstance,
      table_name: "pxy_test",
      tunnel_name: "add2",
      heartbeat_timeout: 30,
      heartbeat_interval: 10,
      worker_size: 2,
    },
    batchers_config: [
      sqs: [
        stages: 2,
        batch_size: 20
      ],
      s3: [
        stages: 1
      ]
    ]
  ```

  Third,define a consumer module for process business logic of aliyunOts' records.
  like this:
    ```elixir
    defmodule CustomerExample do
      @moduledoc false

      alias Broadway.Message
      alias ExAliyunOts.PlainBuffer

      import Integer

      # 被Broadway.Processor调用
      def handle_message(_, message, _) do
        message
        |> Message.update_data(&process_data/1)
        |> put_batcher()
      end

      defp process_data(data) do
        # Do some calculations, generate a JSON representation, etc.
        PlainBuffer.deserialize_row(data.record)
      end

      defp put_batcher(%Message{data: {_, [{_, v, _} | _]}} = message) when is_even(v) do
        Message.put_batcher(message, :sqs)
      end

      defp put_batcher(%Message{data: {_, [{_, v, _} | _]}} = message) when is_odd(v) do
        Message.put_batcher(message, :s3)
      end

      # 被Broadway.Consumer调用
      def handle_batch(:sqs, messages, _batch_info, _context) do
        IO.inspect(messages |> Enum.map(& &1.data), label: "length,#{length(messages)}-sqs")
        messages
        # Send batch of messages to SQS
      end

      def handle_batch(:s3, messages, _batch_info, _context) do
        IO.inspect(messages |> Enum.map(& &1.data), label: "length,#{length(messages)}-s3")

        messages
        # Send batch of messages to S3
      end
    end
    ```


  ### Make it work
  Then add your Tunnel pipeline above to your supervision tree
  (usually in `lib/my_app/application.ex`):

      children = ExAliyunOts.Tunnel.Supervisor.specs()

      Supervisor.start_link(children, strategy: :one_for_one)

  The advanced configuration above defines a pipeline with:

    * 1 instance
    * 2 worker(tunnel client with the same tunnel_id)
       * 1 producer
       * 2 processors
       * 1 batcher named `:sqs` with 2 consumers
       * 1 batcher named `:s3` with 1 consumer

  Here is how this pipeline would be represented:

  ```asciidoc

                              [instance]
                                 / \
                                /   \
                               /     \
                              /       \
                        [worker_1] [worker_2]
                            |          .
                            |          .
                            |          .
                       [producer_1]
                           / \
                          /   \
                         /     \
                        /       \
               [processor_1] [processor_2]   <- process each message
                        /\     /\
                       /  \   /  \
                      /    \ /    \
                     /      x      \
                    /      / \      \
                   /      /   \      \
                  /      /     \      \
             [batcher_sqs]    [batcher_s3]
                  /\                  \
                 /  \                  \
                /    \                  \
               /      \                  \
   [consumer_sqs_1] [consumer_sqs_2]  [consumer_s3_1] <- process each batch
  ```

  ## Tunnel's full configuration
  Include tunnel_config and batchers_config

  ### tunnel_config(Required)
    * `:instance` - Required. AliyunOts's Instacne
    * `:table_name` - Required. AliyunOts's Instance's table name
    * `:tunnel_name` - Required. AliyunOts's Instance's tunnel name
    * `:customer_module` - Required. The customer module name for consumer tunnel data
    * `:worker_size` - Optional. The tunnel client num with same tunnel name.default: 1
    * `:heartbeat_timeout` - Optional. The tunnel client heartbeat connect timeout.default: 300(second)
    * `:heartbeat_interval` - Optional. The tunnel client heartbeat interval.default: 30(second)

  ### batchers_config(Optional)
    Full detail,you can see `Batchers options` in the document of the `Broadway`
  """

  import ExAliyunOts.Logger, only: [debug: 1]

  alias ExAliyunOts.TableStoreTunnel.{
    CreateTunnelRequest,
    CreateTunnelResponse,
    Tunnel,
    DeleteTunnelRequest,
    DeleteTunnelResponse,
    ListTunnelRequest,
    ListTunnelResponse,
    DescribeTunnelRequest,
    DescribeTunnelResponse,
    ConnectRequest,
    ConnectResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    ShutdownRequest,
    ShutdownResponse,
    GetCheckpointRequest,
    GetCheckpointResponse,
    CheckpointRequest,
    CheckpointResponse,
    ReadRecordsRequest,
    ReadRecordsResponse
  }

  alias ExAliyunOts.Http
  # TODO 错误处理
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
    |> CreateTunnelRequest.encode()
  end

  def remote_create_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/create", request_body, &CreateTunnelResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "create_tunnel result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_delete_tunnel(keywords) do
    DeleteTunnelRequest.new(keywords)
    |> DeleteTunnelRequest.encode()
  end

  def remote_delete_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/delete", request_body, &DeleteTunnelResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "delete_tunnel result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_list_tunnel(keywords) do
    ListTunnelRequest.new(keywords)
    |> ListTunnelRequest.encode()
  end

  def remote_list_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/list", request_body, &ListTunnelResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "list_tunnel result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_describe_tunnel(keywords) do
    DescribeTunnelRequest.new(keywords)
    |> DescribeTunnelRequest.encode()
  end

  def remote_describe_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/describe", request_body, &DescribeTunnelResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "describe_tunnel result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_connect(keywords) do
    ConnectRequest.new(keywords)
    |> ConnectRequest.encode()
  end

  def remote_connect(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/connect", request_body, &ConnectResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "connect_tunnel result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_heartbeat(keywords) do
    HeartbeatRequest.new(keywords)
    |> HeartbeatRequest.encode()
  end

  def remote_heartbeat(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/heartbeat", request_body, &HeartbeatResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "heartbeat result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_shutdown(keywords) do
    ShutdownRequest.new(keywords)
    |> ShutdownRequest.encode()
  end

  def remote_shutdown(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/shutdown", request_body, &ShutdownResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "shutdown result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_get_checkpoint(keywords) do
    GetCheckpointRequest.new(keywords)
    |> GetCheckpointRequest.encode()
  end

  def remote_get_checkpoint(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/getcheckpoint", request_body, &GetCheckpointResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "get checkpoint result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_checkpoint(keywords) do
    CheckpointRequest.new(keywords)
    |> CheckpointRequest.encode()
  end

  def remote_checkpoint(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/checkpoint", request_body, &CheckpointResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "checkpoint result: ",
        inspect(result)
      ]
    end)

    result
  end

  def request_to_read_records(keywords) do
    ReadRecordsRequest.new(keywords)
    |> ReadRecordsRequest.encode()
  end

  def remote_read_records(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/readrecords", request_body, &ReadRecordsResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "read records result: ",
        inspect(result)
      ]
    end)

    result
  end
end
