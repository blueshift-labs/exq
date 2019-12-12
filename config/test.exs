use Mix.Config

config :logger, :console, format: "\n$date $time [$level]: $message \n"

config :exq,
  name: Exq,
  namespace: "test",
  queues: ["default"],
  concurrency: :infinite,
  scheduler_enable: false,
  scheduler_poll_timeout: 20,
  poll_timeout: 10,
  genserver_timeout: 5000,
  test_with_local_redis: true,
  max_retries: 0,
  stats_flush_interval: 5,
  stats_batch_size: 1,
  middleware: [Exq.Middleware.Stats, Exq.Middleware.Job, Exq.Middleware.Manager]
