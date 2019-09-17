defmodule Exq.Support.Mode do
  @moduledoc """
  This module defines several modes in which Exq can be used. These modes are:

  * `default` - starts the default processes
  * `enqueuer` - starts processes which are responsible for job enqueueing
  * `api` - starts processes which are responsible for API usage
  """

  @doc """
  Returns child list for the main Exq supervisor
  """

  import Exq.Support.Opts, only: [start_and_config_redis_cluster: 1]
  import Supervisor.Spec

  def children(opts) do
    # we're using redis_cluster which is booted earlier
    # due to it being an erlang lib
    opts = start_and_config_redis_cluster(opts)
    children(opts[:mode], opts)
  end

  def children(:default, opts) do
    shutdown_timeout = Keyword.get(opts, :shutdown_timeout)

    children = [
      worker(Exq.Worker.Metadata, [opts]),
      worker(Exq.Middleware.Server, [opts]),
      worker(Exq.Stats.Server, [opts]),
      supervisor(Exq.Worker.Supervisor, [opts]),
      worker(Exq.Manager.Server, [opts]),
      worker(Exq.WorkerDrainer.Server, [opts], shutdown: shutdown_timeout),
      worker(Exq.Enqueuer.Server, [opts]),
      worker(Exq.Api.Server, [opts])
    ]

    if opts[:scheduler_enable] do
      children ++ [worker(Exq.Scheduler.Server, [opts])]
    else
      children
    end
  end

  def children(:enqueuer, opts) do
    [worker(Exq.Enqueuer.Server, [opts])]
  end

  def children(:api, opts) do
    [worker(Exq.Api.Server, [opts])]
  end

  def children([:enqueuer, :api], opts) do
    [
      worker(Exq.Enqueuer.Server, [opts]),
      worker(Exq.Api.Server, [opts])
    ]
  end

  def children([:api, :enqueuer], opts), do: children([:enqueuer, :api], opts)
end
