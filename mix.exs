defmodule MuroPlug.MixProject do
  use Mix.Project

  def project do
    [
      app: :muro_plug,
      version: "0.1.0-dev",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
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
      {:muro, "~> 0.22.36"},
      {:thousand_island, "~> 1.0"},
      {:plug, "~> 1.16"},
      {:phoenix, "~> 1.8", only: :test}
    ]
  end

  # muro 0.22.36 emits Slice/Bytes literals with a bare `inspect/1`, so a
  # literal past Inspect's 4096-byte printable limit is written as `...`
  # and the module does not compile. step.muro has three such fixtures
  # (capLine, capHdr, capHdrDone). `mix step.gen` runs `mix muro.gen`
  # with unlimited inspect limits, which is the output a fixed emitter
  # writes. `mix step.gen --check` compares. Drop this once muro ships
  # the fix.
  defp aliases do
    [
      "step.gen": [&unlimited_inspect/1, "muro.gen"]
    ]
  end

  defp unlimited_inspect(_args) do
    Inspect.Opts.default_inspect_fun(fn term, opts ->
      Inspect.inspect(term, %{opts | limit: :infinity, printable_limit: :infinity})
    end)
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
