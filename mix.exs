defmodule MuroPlug.MixProject do
  use Mix.Project

  def project do
    [
      app: :muro_plug,
      version: "0.1.0-dev",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "A Plug and Phoenix adapter for an Elixir-emitted muro-http server on Thousand Island.",
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger] ++ test_apps(Mix.env())
    ]
  end

  # The Plug and Phoenix tests drive the server with :httpc too.
  defp test_apps(:test), do: [:inets]
  defp test_apps(_), do: []

  defp deps do
    [
      {:muro, "~> 0.22.38"},
      {:thousand_island, "~> 1.0"},
      {:plug, "~> 1.16"},
      {:websock, "~> 0.5"},
      {:phoenix, "~> 1.8", only: :test},
      {:websock_adapter, "~> 0.5", only: :test}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/doctorcorral/muro_plug",
        "muro-http" => "https://github.com/doctorcorral/muro-http"
      },
      files: ~w(lib priv/muro mix.exs muro.exs README.md LICENSE .formatter.exs)
    ]
  end
end
