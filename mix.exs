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
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:muro, "~> 0.22.27"},
      {:thousand_island, "~> 1.0"},
      {:plug, "~> 1.16"}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/doctorcorral/muro_plug",
        "muro-http" => "https://github.com/doctorcorral/muro-http"
      },
      files: ~w(lib mix.exs README.md LICENSE .formatter.exs)
    ]
  end
end
