defmodule ExAliyunOts.Tunnel.Channel do
  @moduledoc "管理tunnel的channel"

  use Broadway
  alias ExAliyunOts.Tunnel.Producer

  def create(channel_config) do
    Broadway.start_link(
      __MODULE__,
      name: get_channel_identifier(channel_config),
      context: %{real_customer: channel_config.customer_module},
      producers: [
        default: [
          module: {Producer, channel_config},
          stages: 1
        ]
      ],
      processors: [
        default: [
          stages: 2
        ]
      ],
      batchers: [
        sqs: [
          stages: 2,
          batch_size: 20
        ],
        s3: [
          stages: 2
        ]
      ]
    )
  end

  defp get_channel_identifier(channel_config) do
    Module.concat([Channel, channel_config.client_id])
  end

  def handle_message(processor_key, message, %{real_customer: customer_module} = context) do
    customer_module.handle_message(processor_key, message, context)
  end

  def handle_batch(batcher, messages, batch_info, %{real_customer: customer_module} = context) do
    customer_module.handle_batch(batcher, messages, batch_info, context)
  end
end
