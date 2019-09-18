defmodule Exq.Support.Opts do
  alias Exq.Support.Coercion
  alias Exq.Support.Config

  @doc """
   Return top supervisor's name default is Exq.Sup
  """
  def top_supervisor(name) do
    name = name || Config.get(:name)
    "#{name}.Sup" |> String.to_atom()
  end

  defp conform_opts(opts) do
    mode = opts[:mode] || Config.get(:mode)
    server_opts(mode, opts)
  end

  @doc """
   starts applications for redis_cluster
  """
  def start_and_config_redis_cluster(opts) do
    maybe_redis_cluster_configs = Config.get(:redis_cluster)
    opts = conform_opts(opts)
    # try to bootstrap redis cluster
    hosts = Enum.map(maybe_redis_cluster_configs[:hosts], &to_charlist/1)
    redis_host_info = Enum.zip(hosts, maybe_redis_cluster_configs[:ports])
    Application.put_env(:eredis_cluster, :init_nodes, redis_host_info)
    Application.put_env(:eredis_cluster, :pool_size, maybe_redis_cluster_configs[:pool_size] || 20)
    Application.put_env(:eredis_cluster, :pool_max_overflow, maybe_redis_cluster_configs[:pool_max_overflow] || 5)
    if maybe_redis_cluster_configs[:database], do: Application.put_env(:eredis_cluster, :database, maybe_redis_cluster_configs[:database])
    if maybe_redis_cluster_configs[:password], do: Application.put_env(:eredis_cluster, :password, maybe_redis_cluster_configs[:password])

    {:ok, _any} = Application.ensure_all_started(:eredis_cluster)
    opts
  end

  defp server_opts(:default, opts) do
    scheduler_enable =
      Coercion.to_boolean(opts[:scheduler_enable] || Config.get(:scheduler_enable))

    namespace = opts[:namespace] || Config.get(:namespace)

    scheduler_poll_timeout =
      Coercion.to_integer(opts[:scheduler_poll_timeout] || Config.get(:scheduler_poll_timeout))

    poll_timeout = Coercion.to_integer(opts[:poll_timeout] || Config.get(:poll_timeout))

    shutdown_timeout =
      Coercion.to_integer(opts[:shutdown_timeout] || Config.get(:shutdown_timeout))

    enqueuer = Exq.Enqueuer.Server.server_name(opts[:name])
    stats = Exq.Stats.Server.server_name(opts[:name])
    scheduler = Exq.Scheduler.Server.server_name(opts[:name])
    workers_sup = Exq.Worker.Supervisor.supervisor_name(opts[:name])
    middleware = Exq.Middleware.Server.server_name(opts[:name])
    metadata = Exq.Worker.Metadata.server_name(opts[:name])

    queue_configs = opts[:queues] || Config.get(:queues)
    per_queue_concurrency = opts[:concurrency] || get_config_concurrency()
    queues = get_queues(queue_configs)
    concurrency = get_concurrency(queue_configs, per_queue_concurrency)
    default_middleware = Config.get(:middleware)

    [
      scheduler_enable: scheduler_enable,
      namespace: namespace,
      scheduler_poll_timeout: scheduler_poll_timeout,
      workers_sup: workers_sup,
      poll_timeout: poll_timeout,
      enqueuer: enqueuer,
      metadata: metadata,
      stats: stats,
      name: opts[:name],
      scheduler: scheduler,
      queues: queues,
      concurrency: concurrency,
      middleware: middleware,
      default_middleware: default_middleware,
      mode: :default,
      shutdown_timeout: shutdown_timeout
    ]
  end

  defp server_opts(mode, opts) do
    namespace = opts[:namespace] || Config.get(:namespace)
    [name: opts[:name], namespace: namespace, mode: mode]
  end

  defp get_queues(queue_configs) do
    Enum.map(queue_configs, fn queue_config ->
      case queue_config do
        {queue, _concurrency} -> queue
        queue -> queue
      end
    end)
  end

  defp get_config_concurrency() do
    case Config.get(:concurrency) do
      x when is_atom(x) ->
        x

      x when is_integer(x) ->
        x

      x when is_binary(x) ->
        case x |> String.trim() |> String.downcase() do
          "infinity" -> :infinity
          x -> Coercion.to_integer(x)
        end
    end
  end

  defp get_concurrency(queue_configs, per_queue_concurrency) do
    Enum.map(queue_configs, fn queue_config ->
      case queue_config do
        {queue, concurrency} -> {queue, concurrency, 0}
        queue -> {queue, per_queue_concurrency, 0}
      end
    end)
  end
end
