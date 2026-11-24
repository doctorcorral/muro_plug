defmodule MuroPlug.Host do
  @moduledoc """
  A Thousand Island host for an Elixir-emitted muro-http step.

  This is a stub. `child_spec/1` already returns a Thousand Island
  spec so a host process can start; the handler does not yet parse
  or write HTTP.
  """

  @doc """
  A child spec for a Thousand Island listener.

  Options:

    * `:port` — listen port. `0` lets the OS pick one. Defaults to `0`.
    * `:plug` — a Plug module or function. Stored on the handler and
      unused until the adapter is wired up.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    ThousandIsland.child_spec(
      port: Keyword.get(opts, :port, 0),
      handler_module: MuroPlug.Handler,
      handler_options: %{plug: Keyword.get(opts, :plug)}
    )
  end
end
