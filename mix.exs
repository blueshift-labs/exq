defmodule Exq.Mixfile do
  use Mix.Project

  def project do
    [
      app: :exq,
      version: "0.13.3",
      elixir: "~> 1.6",
      elixirc_paths: ["lib"],
      package: [
        maintainers: [
          "Alex Kira",
          "zhongwencool",
          "Anantha Kumaran"
        ],
        links: %{"GitHub" => "https://github.com/akira/exq"},
        licenses: ["Apache 2.0"],
        files: ~w(lib test) ++ ~w(LICENSE mix.exs README.md)
      ],
      description: """
      Exq is a job processing library compatible with Resque / Sidekiq for the Elixir language.
      """,
      deps: deps(),
      aliases: [test: "test --no-start"],
      test_coverage: [tool: ExCoveralls],
      docs: [extras: ["README.md"]]
    ]
  end

  # Configuration for the OTP application
  def application do
    [
      mod: {Exq, []},
      applications: [:logger, :elixir_uuid]
    ]
  end

  # Returns the list of dependencies in the format:
  # { :foobar, "0.1", git: "https://github.com/elixir-lang/foobar.git" }
  defp deps do
    [
      {:elixir_uuid, ">= 1.2.0"},
      {:eredis_cluster, "~> 0.5.12", runtime: false},
      {:poison, ">= 1.2.0 or ~> 2.0", optional: true},
      {:jason, "~> 1.0", optional: true},
      {:excoveralls, "~> 0.6", only: :test},
      {:flaky_connection, git: "https://github.com/hamiltop/flaky_connection.git", only: :test},

      # docs
      {:ex_doc, "~> 0.19", only: :dev, override: true},
      {:earmark, "~> 1.0", only: :dev},
      {:ranch, "~> 1.6", only: :test, override: true}
    ]
  end
end
