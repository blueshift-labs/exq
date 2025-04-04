defmodule Exq.Api.Server do
  @moduledoc """
  The Api deals with getting current stats for the UI / API.
  """

  alias Exq.Support.Config
  alias Exq.Redis.JobQueue
  alias Exq.Redis.JobStat

  use GenServer

  defmodule State do
    defstruct namespace: nil
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: server_name(opts[:name]))
  end

  ## ===========================================================
  ## gen server callbacks
  ## ===========================================================

  def init(opts) do
    {:ok, %State{namespace: opts[:namespace]}}
  end

  def handle_call(:processes, _from, state) do
    processes = JobStat.processes(state.namespace)
    {:reply, {:ok, processes}, state}
  end

  def handle_call(:busy, _from, state) do
    count = JobStat.busy(state.namespace)
    {:reply, {:ok, count}, state}
  end

  def handle_call({:stats, key}, _from, state) do
    count = JobStat.get_count(state.namespace, key)
    {:reply, {:ok, count}, state}
  end

  def handle_call({:stats, key, date}, _from, state) do
    count = JobStat.get_count(state.namespace, "#{key}:#{date}")
    {:reply, {:ok, count}, state}
  end

  def handle_call(:queues, _from, state) do
    queues = JobQueue.list_queues(state.namespace)
    {:reply, {:ok, queues}, state}
  end

  def handle_call(:failed, _from, state) do
    jobs = JobQueue.failed(state.namespace)
    {:reply, {:ok, jobs}, state}
  end

  def handle_call(:retries, _from, state) do
    jobs = JobQueue.scheduled_jobs(state.namespace, "retry")
    {:reply, {:ok, jobs}, state}
  end

  def handle_call(:jobs, _from, state) do
    jobs = JobQueue.jobs(state.namespace)
    {:reply, {:ok, jobs}, state}
  end

  def handle_call({:jobs, :scheduled}, _from, state) do
    jobs = JobQueue.scheduled_jobs(state.namespace, "schedule")
    {:reply, {:ok, jobs}, state}
  end

  def handle_call({:jobs, :scheduled_with_scores}, _from, state) do
    jobs = JobQueue.scheduled_jobs_with_scores(state.namespace, "schedule")
    {:reply, {:ok, jobs}, state}
  end

  def handle_call({:jobs, queue}, _from, state) do
    jobs = JobQueue.jobs(state.namespace, queue)
    {:reply, {:ok, jobs}, state}
  end

  def handle_call(:queue_size, _from, state) do
    sizes = JobQueue.queue_size(state.namespace)
    {:reply, {:ok, sizes}, state}
  end

  def handle_call({:queue_size, queue}, _from, state) do
    size = JobQueue.queue_size(state.namespace, queue)
    {:reply, {:ok, size}, state}
  end

  def handle_call(:scheduled_size, _from, state) do
    size = JobQueue.scheduled_size(state.namespace)
    {:reply, {:ok, size}, state}
  end

  def handle_call(:retry_size, _from, state) do
    size = JobQueue.retry_size(state.namespace)
    {:reply, {:ok, size}, state}
  end

  def handle_call(:failed_size, _from, state) do
    size = JobQueue.failed_size(state.namespace)
    {:reply, {:ok, size}, state}
  end

  def handle_call({:find_failed, jid}, _from, state) do
    {:ok, job} = JobStat.find_failed(state.namespace, jid)
    {:reply, {:ok, job}, state}
  end

  def handle_call({:find_job, queue, jid}, _from, state) do
    response = JobQueue.find_job(state.namespace, jid, queue)
    {:reply, response, state}
  end

  def handle_call({:find_scheduled, jid}, _from, state) do
    {:ok, job} = JobQueue.find_job(state.namespace, jid, :scheduled)
    {:reply, {:ok, job}, state}
  end

  def handle_call({:find_retry, jid}, _from, state) do
    {:ok, job} = JobQueue.find_job(state.namespace, jid, :retry)
    {:reply, {:ok, job}, state}
  end

  def handle_call({:remove_queue, queue}, _from, state) do
    JobStat.remove_queue(state.namespace, queue)
    {:reply, :ok, state}
  end

  def handle_call({:remove_job, queue, jid}, _from, state) do
    JobQueue.remove_job(state.namespace, queue, jid)
    {:reply, :ok, state}
  end

  def handle_call({:remove_retry, jid}, _from, state) do
    JobQueue.remove_retry(state.namespace, jid)
    {:reply, :ok, state}
  end

  def handle_call({:remove_scheduled, jid}, _from, state) do
    JobQueue.remove_scheduled(state.namespace, jid)
    {:reply, :ok, state}
  end

  def handle_call({:remove_failed, jid}, _from, state) do
    JobStat.remove_failed(state.namespace, jid)
    {:reply, :ok, state}
  end

  def handle_call(:clear_failed, _from, state) do
    JobStat.clear_failed(state.namespace)
    {:reply, :ok, state}
  end

  def handle_call(:clear_processes, _from, state) do
    JobStat.clear_processes(state.namespace)
    {:reply, :ok, state}
  end

  def handle_call(:clear_scheduled, _from, state) do
    JobQueue.delete_queue(state.namespace, "schedule")
    {:reply, :ok, state}
  end

  def handle_call(:clear_retries, _from, state) do
    JobQueue.delete_queue(state.namespace, "retry")
    {:reply, :ok, state}
  end

  def handle_call(:realtime_stats, _from, state) do
    {:ok, failures, successes} = JobStat.realtime_stats(state.namespace)
    {:reply, {:ok, failures, successes}, state}
  end

  def handle_call({:retry_job, jid}, _from, state) do
    {:ok, job} = JobQueue.find_job(state.namespace, jid, :retry)
    JobQueue.retry_job(state.namespace, job)
    {:reply, :ok, state}
  end

  def terminate(_reason, _state) do
    :ok
  end

  def server_name(name) do
    name = name || Config.get(:name)
    "#{name}.Api" |> String.to_atom()
  end
end
