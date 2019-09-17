defmodule ReadonlyReconnectTest do
  use ExUnit.Case
  import ExqTestUtil

  setup do
    Process.flag(:trap_exit, true)
    {:ok, redis} = Redix.start_link(host: "127.0.0.1", port: 6556)
    Process.register(redis, :testredis)

    on_exit(fn ->
      wait()
      TestRedis.teardown()
    end)

    {:ok, redis: redis}
  end

  test "pass through other errors" do
    assert {:error, %Redix.Error{}} = Exq.Redis.Connection.q(:testredis, ["GETS", "key"])
    assert {:ok, [%Redix.Error{}]} = Exq.Redis.Connection.qp(:testredis, [["GETS", "key"]])
  end
end
