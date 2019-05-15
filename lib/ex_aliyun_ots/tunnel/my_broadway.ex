defmodule MyBroadway do
  @moduledoc false

  use Broadway
  alias Broadway.Message

  def start_link(_opts) do

    Broadway.start_link(
      MyBroadway,
      name: MyBroadwayExample,
      producers: [
        default: [
          module: {ExAliyunOts.TunnelProducer, []},
          stages: 1,
#          transformer: {ExAliyunOts.TunnelProducer, :transformer, []}
        ]
      ],
      processors: [
        default: [
          stages: 2
        ]
      ],
      batchers: [
        default: [
          batch_size: 10,
          stages: 2
        ]
      ]
    )
  end

  def handle_message(_, message, _) do
#    IO.inspect(
#      Process.info(self(), :current_stacktrace)
#      |> elem(1)
#      |> Enum.fetch!(2),
#      label: "handler_message caller1"
#    )

    message
    |> Message.update_data(fn data -> {:data, data} end)
  end

  # TODO 寻找数据消费完后，返回消费结果给到对应的producer的途径
  def handle_batch(_, messages, _, _) do
    IO.inspect(messages)
#    IO.inspect(
#      Process.info(self(), :current_stacktrace)
#      |> elem(1)
#      |> Enum.fetch!(2),
#      label: "handler_message caller2"
#    )

#    list = messages
#           |> Enum.map(fn e -> e.data end)
    #    IO.inspect(messages, label: "Got batch")
    #    IO.inspect(self(), label: "pid")
    messages
  end
end
