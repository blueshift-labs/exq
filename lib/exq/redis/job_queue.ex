defmodule Exq.Redis.JobQueue do
  @moduledoc """
  The JobQueue module is the main abstraction of a job queue on top of Redis.

  It provides functionality for:
    * Storing jobs in Redis
    * Fetching the next job(s) to be executed (and storing a backup of these).
    * Scheduling future jobs in Redis
    * Fetching scheduling jobs and moving them to current job list
    * Retrying or failing a job
    * Re-hydrating jobs from a backup queue
  """

  require Logger

  alias Exq.Redis.Connection
  alias Exq.Support.Job
  alias Exq.Support.Config
  alias Exq.Support.Time

  def enqueue(namespace, queue, worker, args, options) do
    {jid, job_serialized} = to_job_serialized(queue, worker, args, options)

    case enqueue(namespace, queue, job_serialized) do
      :ok -> {:ok, jid}
      other -> other
    end
  end

  def enqueue(namespace, job_serialized) do
    job = Config.serializer().decode_job(job_serialized)

    case enqueue(namespace, job.queue, job_serialized) do
      :ok -> {:ok, job.jid}
      error -> error
    end
  end

  def enqueue(namespace, queue, job_serialized) do
    try do
      response =
        Connection.qmn([
          ["SADD", full_key(namespace, "queues"), queue],
          ["LPUSH", queue_key(namespace, queue), job_serialized]
        ])

      case response do
        [{:error, _any}, {:error, _any_err}] = error -> error
        [{:error, _any}, _] = error -> error
        [_, {:error, _any}] = error -> error
        [_, _] -> :ok
        other -> other
      end
    catch
      :exit, e ->
        Logger.info("Error enqueueing -  #{Kernel.inspect(e)}")
        {:error, :timeout}
    end
  end

  def enqueue_in(namespace, queue, offset, worker, args, options)
      when is_integer(offset) do
    time = Time.offset_from_now(offset)
    enqueue_at(namespace, queue, time, worker, args, options)
  end

  def enqueue_at(namespace, queue, time, worker, args, options) do
    {jid, job_serialized} = to_job_serialized(queue, worker, args, options)
    enqueue_job_at(namespace, job_serialized, jid, time, scheduled_queue_key(namespace))
  end

  def enqueue_job_at(_namespace, job_serialized, jid, time, scheduled_queue) do
    score = Time.time_to_score(time)

    try do
      case Connection.zadd(scheduled_queue, score, job_serialized) do
        {:ok, _} -> {:ok, jid}
        other -> other
      end
    catch
      :exit, e ->
        Logger.info("Error enqueueing -  #{Kernel.inspect(e)}")
        {:error, :timeout}
    end
  end

  @doc """
  Dequeue jobs for available queues
  """
  def dequeue(namespace, node_id, queues) when is_list(queues) do
    dequeue_multiple(namespace, node_id, queues)
  end

  defp dequeue_multiple(_namespace, _node_id, []) do
    {:ok, {:none, nil}}
  end

  defp dequeue_multiple(namespace, node_id, queues) do
    deq_commands =
      Enum.map(queues, fn queue ->
        ["RPOPLPUSH", queue_key(namespace, queue), backup_queue_key(namespace, node_id, queue)]
      end)

    deq_commands
    |> Connection.qmn()
    |> Enum.zip(queues)
    |> Enum.map(fn {query_res, queue} ->
      case query_res do
        {:ok, :undefined} -> {:ok, {:none, queue}}
        {:ok, ""} -> {:ok, {:none, queue}}
        {:error, resp} -> {:error, {resp, queue}}
        {:ok, resp} -> {:ok, {resp, queue}}
      end
    end)
  end

  def re_enqueue_backup(namespace, node_id, queue) do
    resp =
      Connection.rpoplpush(
        backup_queue_key(namespace, node_id, queue),
        queue_key(namespace, queue)
      )

    case resp do
      {:ok, job} ->
        if String.valid?(job) do
          Logger.info(
            "Re-enqueueing job from backup for node_id [#{node_id}] and queue [#{queue}]"
          )

          re_enqueue_backup(namespace, node_id, queue)
        end

      _ ->
        nil
    end
  end

  def remove_job_from_backup(namespace, node_id, queue, job_serialized) do
    Connection.lrem!(backup_queue_key(namespace, node_id, queue), job_serialized)
  end

  def scheduler_dequeue(namespace) do
    scheduler_dequeue(namespace, Time.time_to_score())
  end

  def scheduler_dequeue(namespace, max_score) do
    queues = schedule_queues(namespace)

    queues
    |> Enum.map(&["ZRANGEBYSCORE", &1, 0, max_score])
    |> Connection.qmn()
    |> Enum.zip(queues)
    |> Enum.reduce(0, fn {response, queue}, acc ->
      case response do
        {:error, reason} ->
          Logger.error("Redis error scheduler dequeue #{Kernel.inspect(reason)}}.")
          acc
        {:ok, jobs} when is_list(jobs) ->
          deq_count = scheduler_dequeue_requeue(jobs, namespace, queue, 0)
          deq_count + acc
      end
    end)
  end

  def scheduler_dequeue_requeue([], __namespace, _schedule_queue, count), do: count

  def scheduler_dequeue_requeue([job_serialized | t], namespace, schedule_queue, count) do
    resp = Connection.zrem(schedule_queue, job_serialized)

    count =
      case resp do
        {:ok, 1} ->
          enqueue(namespace, job_serialized)
          count + 1

        {:ok, _} ->
          count

        {:error, reason} ->
          Logger.error("Redis error scheduler dequeue #{Kernel.inspect(reason)}}.")
          count
      end

    scheduler_dequeue_requeue(t, namespace, schedule_queue, count)
  end

  def full_key(namespace, key) do
    # below is a special-case for stat strings to be on one node
    if String.starts_with?(key, "stat:") do
      "#{namespace}:{stat}:#{key}"
    else
      "#{namespace}:{#{key}}"
    end
  end

  def full_key(namespace, node_id, "queue:backup::" <> queue = key) do
    "#{namespace}:{queue:#{queue}}#{key <> "::" <> node_id}"
  end

  def queue_key(namespace, queue) do
    full_key(namespace, "queue:#{queue}")
  end

  def backup_queue_key(namespace, node_id, queue) do
    full_key(namespace, node_id, "queue:backup::#{queue}")
  end

  def schedule_queues(namespace) do
    [scheduled_queue_key(namespace), retry_queue_key(namespace)]
  end

  def scheduled_queue_key(namespace) do
    full_key(namespace, "schedule")
  end

  def retry_queue_key(namespace) do
    full_key(namespace, "retry")
  end

  def failed_queue_key(namespace) do
    full_key(namespace, "dead")
  end

  def retry_or_fail_job(namespace, %{retry: retry} = job, error)
      when is_integer(retry) and retry > 0 do
    retry_or_fail_job(namespace, job, error, retry)
  end

  def retry_or_fail_job(namespace, %{retry: true} = job, error) do
    retry_or_fail_job(namespace, job, error, get_max_retries())
  end

  def retry_or_fail_job(namespace, job, error) do
    fail_job(namespace, job, error)
  end

  defp retry_or_fail_job(namespace, job, error, max_retries) do
    retry_count = (job.retry_count || 0) + 1

    if retry_count <= max_retries do
      retry_job(namespace, job, retry_count, error)
    else
      Logger.info("Max retries on job #{job.jid} exceeded")
      fail_job(namespace, job, error)
    end
  end

  def retry_job(namespace, job, retry_count, error) do
    job = %{job | failed_at: Time.unix_seconds(), retry_count: retry_count, error_message: error}

    offset = Config.backoff().offset(job)
    time = Time.offset_from_now(offset)
    Logger.info("Queueing job #{job.jid} to retry in #{offset} seconds")
    enqueue_job_at(namespace, Job.encode(job), job.jid, time, retry_queue_key(namespace))
  end

  def retry_job(namespace, job) do
    remove_retry(namespace, job.jid)
    enqueue(namespace, Job.encode(job))
  end

  def fail_job(namespace, job, error) do
    job = %{
      job
      | failed_at: Time.unix_seconds(),
        retry_count: job.retry_count || 0,
        error_class: "ExqGenericError",
        error_message: error
    }

    job_serialized = Job.encode(job)
    key = failed_queue_key(namespace)

    now = Time.unix_seconds()

    commands = [
      ["ZADD", key, Time.time_to_score(), job_serialized],
      ["ZREMRANGEBYSCORE", key, "-inf", now - Config.get(:dead_timeout_in_seconds)],
      ["ZREMRANGEBYRANK", key, 0, -Config.get(:dead_max_jobs) - 1]
    ]

    Connection.qmn!(commands)
  end

  def queue_size(namespace) do
    queues = list_queues(namespace)
    for q <- queues, do: {q, queue_size(namespace, q)}
  end

  def queue_size(namespace, :scheduled) do
    Connection.zcard!(scheduled_queue_key(namespace))
  end

  def queue_size(namespace, :retry) do
    Connection.zcard!(retry_queue_key(namespace))
  end

  def queue_size(namespace, queue) do
    Connection.llen!(queue_key(namespace, queue))
  end

  def delete_queue(namespace, queue) do
    Connection.del!(full_key(namespace, queue))
  end

  def jobs(namespace) do
    queues = list_queues(namespace)
    for q <- queues, do: {q, jobs(namespace, q)}
  end

  def jobs(namespace, queue) do
    Connection.lrange!(queue_key(namespace, queue))
    |> Enum.map(&Job.decode/1)
  end

  def scheduled_jobs(namespace, queue) do
    Connection.zrangebyscore!(full_key(namespace, queue))
    |> Enum.map(&Job.decode/1)
  end

  def scheduled_jobs_with_scores(namespace, queue) do
    Connection.zrangebyscorewithscore!(full_key(namespace, queue))
    |> Enum.chunk_every(2)
    |> Enum.map(fn [job, score] -> {Job.decode(job), score} end)
  end

  def failed(namespace) do
    Connection.zrange!(failed_queue_key(namespace))
    |> Enum.map(&Job.decode/1)
  end

  def retry_size(namespace) do
    Connection.zcard!(retry_queue_key(namespace))
  end

  def scheduled_size(namespace) do
    Connection.zcard!(scheduled_queue_key(namespace))
  end

  def failed_size(namespace) do
    Connection.zcard!(failed_queue_key(namespace))
  end

  def remove_job(namespace, queue, jid) do
    {:ok, job} = find_job(namespace, jid, queue, false)
    Connection.lrem!(queue_key(namespace, queue), job)
  end

  def remove_retry(namespace, jid) do
    {:ok, job} = find_job(namespace, jid, :retry, false)
    Connection.zrem!(retry_queue_key(namespace), job)
  end

  def remove_scheduled(namespace, jid) do
    {:ok, job} = find_job(namespace, jid, :scheduled, false)
    Connection.zrem!(scheduled_queue_key(namespace), job)
  end

  def list_queues(namespace) do
    Connection.smembers!(full_key(namespace, "queues"))
  end

  @doc """
  Find a current job by job id (but do not pop it)
  """
  def find_job(namespace, jid, queue) do
    find_job(namespace, jid, queue, true)
  end

  def find_job(namespace, jid, :scheduled, convert) do
    Connection.zrangebyscore!(scheduled_queue_key(namespace))
    |> search_jobs(jid, convert)
  end

  def find_job(namespace, jid, :retry, convert) do
    Connection.zrangebyscore!(retry_queue_key(namespace))
    |> search_jobs(jid, convert)
  end

  def find_job(namespace, jid, queue, convert) do
    Connection.lrange!(queue_key(namespace, queue))
    |> search_jobs(jid, convert)
  end

  def search_jobs(jobs_serialized, jid) do
    search_jobs(jobs_serialized, jid, true)
  end

  def search_jobs(jobs_serialized, jid, true) do
    found_job =
      jobs_serialized
      |> Enum.map(&Job.decode/1)
      |> Enum.find(fn job -> job.jid == jid end)

    {:ok, found_job}
  end

  def search_jobs(jobs_serialized, jid, false) do
    found_job =
      jobs_serialized
      |> Enum.find(fn job_serialized ->
        job = Job.decode(job_serialized)
        job.jid == jid
      end)

    {:ok, found_job}
  end

  def to_job_serialized(queue, worker, args, options) do
    to_job_serialized(queue, worker, args, options, Time.unix_seconds())
  end

  def to_job_serialized(queue, worker, args, options, enqueued_at) when is_atom(worker) do
    to_job_serialized(queue, to_string(worker), args, options, enqueued_at)
  end

  def to_job_serialized(queue, "Elixir." <> worker, args, options, enqueued_at) do
    to_job_serialized(queue, worker, args, options, enqueued_at)
  end

  def to_job_serialized(queue, worker, args, options, enqueued_at) do
    jid = UUID.uuid4()
    retry = Keyword.get_lazy(options, :max_retries, fn -> get_max_retries() end)

    job = %{
      queue: queue,
      retry: retry,
      class: worker,
      args: args,
      jid: jid,
      enqueued_at: enqueued_at
    }

    {jid, Config.serializer().encode!(job)}
  end

  defp get_max_retries do
    :max_retries
    |> Config.get()
    |> Exq.Support.Coercion.to_integer()
  end
end
