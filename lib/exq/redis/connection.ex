defmodule Exq.Redis.Connection do
  @moduledoc """
  The Connection module encapsulates interaction with a live Redis connection or pool.

  """
  require Logger

  alias Exq.Support.Config

  def flushdb!() do
    qa(["flushdb"])
  end

  def incr!(key) do
    {:ok, count} = q(["INCR", key])
    count
  end

  def get!(key) do
    {:ok, val} = q(["GET", key])
    val
  end

  def del!(key) do
    q(["DEL", key])
  end

  def llen!(list) do
    {:ok, len} = q(["LLEN", list])
    len
  end

  def scard!(set) do
    {:ok, count} = q(["SCARD", set])
    count
  end

  def smembers!(set) do
    {:ok, members} = q(["SMEMBERS", set])
    members
  end

  def sadd!(set, member) do
    {:ok, res} = q(["SADD", set, member])
    res
  end

  def srem!(set, member) do
    {:ok, res} = q(["SREM", set, member])
    res
  end

  def lrange!(list, range_start \\ "0", range_end \\ "-1") do
    {:ok, items} = q(["LRANGE", list, range_start, range_end])
    items
  end

  def lrem!(list, value, count \\ 1) do
    {:ok, res} = q(["LREM", list, count, value])
    res
  end

  def lpop(key) do
    q(["LPOP", key])
  end

  def rpoplpush(key, backup) do
    q(["RPOPLPUSH", key, backup])
  end

  def zadd(set, score, member) do
    q(["ZADD", set, score, member])
  end

  def zadd!(set, score, member) do
    {:ok, res} = q(["ZADD", set, score, member])
    res
  end

  def zcard!(set) do
    {:ok, count} = q(["ZCARD", set])
    count
  end

  def zrangebyscore!(set, min \\ "0", max \\ "+inf") do
    {:ok, items} = q(["ZRANGEBYSCORE", set, min, max])
    items
  end

  def zrangebyscorewithscore!(set, min \\ "0", max \\ "+inf") do
    {:ok, items} = q(["ZRANGEBYSCORE", set, min, max, "WITHSCORES"])
    items
  end

  def zrange!(set, range_start \\ "0", range_end \\ "-1") do
    {:ok, items} = q(["ZRANGE", set, range_start, range_end])
    items
  end

  def zrem!(set, member) do
    {:ok, res} = q(["ZREM", set, member])
    res
  end

  def zrem(set, member) do
    q(["ZREM", set, member])
  end

  defdelegate q(command), to: :eredis_cluster

  defdelegate qa(command), to: :eredis_cluster

  defdelegate qp(commands), to: :eredis_cluster

  defdelegate qmn(commands), to: :eredis_cluster

  def qmn!(commands) do
    :eredis_cluster.qmn(commands)
    |> Enum.each({:ok, _any} -> :ok end)
  end
end
