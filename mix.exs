defmodule Kvasir.Kafka.MixProject do
  use Mix.Project
  @version "0.0.3"

  def project do
    [
      app: :kvasir_kafka,
      description: "Kafka [event] source for Kvasir.",
      version: @version,
      elixir: "~> 1.7",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),

      # Testing
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      # dialyzer: [ignore_warnings: "dialyzer.ignore-warnings", plt_add_deps: true],

      # Docs
      name: "kvasir_kafka",
      source_url: "https://github.com/IanLuites/kvasir_kafka",
      homepage_url: "https://github.com/IanLuites/kvasir_kafka",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ]
    ]
  end

  def package do
    [
      name: :kvasir_kafka,
      maintainers: ["Ian Luites"],
      licenses: ["MIT"],
      files: [
        # Elixir
        "lib/kvasir_kafka",
        ".formatter.exs",
        "mix.exs",
        "README*",
        "LICENSE*"
      ],
      links: %{
        "GitHub" => "https://github.com/IanLuites/kvasir_kafka"
      }
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:brod, "~> 3.14"},
      {:kvasir, git: "https://github.com/IanLuites/kvasir", branch: "release/v1.0"}
    ]
  end
end
