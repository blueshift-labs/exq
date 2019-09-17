defmodule Exq.Redis.JobStat do
  @moduledoc """
  The JobStat module encapsulates storing system-wide stats on top of Redis
  It aims to be compatible with the Sidekiq stats format.
  """

  require Logger
  alias Exq.Support.{Binary, Process, Job, Time}
  alias Exq.Redis.{Connection, JobQueue}

  def record_processed_commands(namespace, _job, current_date \\ DateTime.utc_now()) do
    {time, date} = Time.format_current_date(current_date)

    [
      ["INCR", JobQueue.full_key(namespace, "stat:processed")],
      ["INCR", JobQueue.full_key(namespace, "stat:processed_rt:#{time}")],
      ["EXPIRE", JobQueue.full_key(namespace, "stat:processed_rt:#{time}"), 120],
      ["INCR", JobQueue.full_key(namespace, "stat:processed:#{date}")]
    ]
  end

  def record_processed(namespace, job, current_date \\ DateTime.utc_now()) do
    instr = record_processed_commands(namespace, job, current_date)
    {:ok, [count, _, _, _]} = Connection.qp(instr)
    {:ok, count}
  end

  def record_failure_commands(namespace, _error, _job, current_date \\ DateTime.utc_now()) do
    {time, date} = Time.format_current_date(current_date)

    [
      ["INCR", JobQueue.full_key(namespace, "stat:failed")],
      ["INCR", JobQueue.full_key(namespace, "stat:failed_rt:#{time}")],
      ["EXPIRE", JobQueue.full_key(namespace, "stat:failed_rt:#{time}"), 120],
      ["INCR", JobQueue.full_key(namespace, "stat:failed:#{date}")]
    ]
  end

  def record_failure(namespace, error, job, current_date \\ DateTime.utc_now()) do
    instr = record_failure_commands(namespace, error, job, current_date)
    {:ok, [count, _, _, _]} = Connection.qp(instr)
    {:ok, count}
  end

  def add_process_commands(namespace, process_info, serialized_process \\ nil) do
    serialized = serialized_process || Exq.Support.Process.encode(process_info)
    [["SADD", JobQueue.full_key(namespace, "processes"), serialized]]
  end

  def add_process(namespace, process_info, serialized_process \\ nil) do
    instr = add_process_commands(namespace, process_info, serialized_process)
    Connection.qmn!(instr)
    :ok
  end

  def remove_process_commands(namespace, process_info, serialized_process \\ nil) do
    serialized = serialized_process || Exq.Support.Process.encode(process_info)
    [["SREM", JobQueue.full_key(namespace, "processes"), serialized]]
  end

  def remove_process(namespace, process_info, serialized_process \\ nil) do
    instr = remove_process_commands(namespace, process_info, serialized_process)
    Connection.qmn!(instr)
    :ok
  end

  def cleanup_processes(namespace, host) do
    Connection.smembers!(JobQueue.full_key(namespace, "processes"))
    |> Enum.map(fn serialized -> {Process.decode(serialized), serialized} end)
    |> Enum.filter(fn {process, _} -> process.host == host end)
    |> Enum.each(fn {process, serialized} ->
      remove_process(namespace, process, serialized)
    end)

    :ok
  end

  def busy(namespace) do
    Connection.scard!(JobQueue.full_key(namespace, "processes"))
  end

  def processes(namespace) do
    list = Connection.smembers!(JobQueue.full_key(namespace, "processes")) || []
    Enum.map(list, &Process.decode/1)
  end

  def find_failed(namespace, jid) do
    Connection.zrange!(JobQueue.full_key(namespace, "dead"), 0, -1)
    |> JobQueue.search_jobs(jid)
  end

  def remove_queue(namespace, queue) do
    Connection.qp([
      ["SREM", JobQueue.full_key(namespace, "queues"), queue],
      ["DEL", JobQueue.queue_key(namespace, queue)]
    ])
  end

  def remove_failed(namespace, jid) do
    {:ok, failure} = find_failed(namespace, jid)

    Connection.qp([
      ["DECR", JobQueue.full_key(namespace, "stat:failed")],
      ["ZREM", JobQueue.full_key(namespace, "dead"), Job.encode(failure)]
    ])
  end

  def clear_failed(namespace) do
    Connection.qp([
      ["SET", JobQueue.full_key(namespace, "stat:failed"), 0],
      ["DEL", JobQueue.full_key(namespace, "dead")]
    ])
  end

  def clear_processes(namespace) do
    Connection.del!(JobQueue.full_key(namespace, "processes"))
  end

  def realtime_stats(namespace) do
    {:ok, [failure_keys, success_keys]} =
      Connection.qp([
        ["KEYS", JobQueue.full_key(namespace, "stat:failed_rt:*")],
        ["KEYS", JobQueue.full_key(namespace, "stat:processed_rt:*")]
      ])

    formatter = realtime_stats_formatter(namespace)
    failures = formatter.(failure_keys, "stat:failed_rt:")
    successes = formatter.(success_keys, "stat:processed_rt:")

    {:ok, failures, successes}
  end

  defp realtime_stats_formatter(namespace) do
    fn keys, ns ->
      if Enum.empty?(keys) do
        []
      else
        {:ok, counts} = Connection.qp(Enum.map(keys, &["GET", &1]))

        Enum.map(keys, &Binary.take_prefix(&1, JobQueue.full_key(namespace, ns)))
        |> Enum.zip(counts)
      end
    end
  end

  def get_count(namespace, key) do
    case Connection.get!(JobQueue.full_key(namespace, "stat:#{key}")) do
      :undefined ->
        0

      nil ->
        0

      count when is_integer(count) ->
        count

      count ->
        {val, _} = Integer.parse(count)
        val
    end
  end
end
