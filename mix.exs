defmodule BucketHydra.MixProject do
  use Mix.Project

  def project do
    [
      app: :bucket_hydra,
      version: "0.1.0",
      elixir: "~> 1.9",
      elixirc_paths: elixirc_paths(Mix.env),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: "A bucket-based rate limit algorithm with support for clusters",
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib","test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:telemetry, "~> 0.4.1"},
      {:phoenix_pubsub, "~> 1.1"},
      {:ex_doc, "~> 0.21.3", only: :dev}
    ]
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/ZennerIoT/bucket_hydra"
      },
    ]
  end
end
