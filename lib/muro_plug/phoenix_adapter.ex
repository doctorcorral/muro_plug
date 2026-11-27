defmodule MuroPlug.PhoenixAdapter do
  @moduledoc """
  A `Phoenix.Endpoint` adapter for MuroPlug, shaped like
  `Bandit.PhoenixAdapter`.

      config :my_app, MyAppWeb.Endpoint,
        adapter: MuroPlug.PhoenixAdapter,
        http: [ip: {127, 0, 0, 1}, port: 4000]

  The `:http` keyword list is passed to `MuroPlug.child_spec/1`
  (`:port`, `:ip`, `:max_body`, `:idle_timeout`,
  `:thousand_island_options`). `:https` is not served yet, and
  WebSockets (LiveView, channels) are a later milestone: `upgrade/3`
  answers `{:error, :not_supported}`.
  """

  @doc false
  def child_specs(endpoint, config) do
    if config[:https],
      do: raise(ArgumentError, "MuroPlug.PhoenixAdapter does not serve :https yet")

    plug = resolve_plug(config[:code_reloader], endpoint)

    for scheme <- [:http], opts = config[scheme] do
      opts
      |> Keyword.drop([:otp_app, :plug, :id])
      |> Keyword.merge(plug: plug, id: {endpoint, scheme})
      |> MuroPlug.child_spec()
    end
  end

  @doc "The MuroPlug (Thousand Island) process serving `scheme` for `endpoint`."
  @spec server_pid(module(), atom()) :: {:ok, pid()} | {:error, :no_server_found}
  def server_pid(endpoint, scheme \\ :http) do
    endpoint
    |> Supervisor.which_children()
    |> Enum.find(fn {id, _, _, _} -> id == {endpoint, scheme} end)
    |> case do
      {_, pid, _, _} when is_pid(pid) -> {:ok, pid}
      _ -> {:error, :no_server_found}
    end
  end

  @doc "The bound address and port, for `Phoenix.Endpoint.server_info/2`."
  def server_info(endpoint, scheme) do
    with {:ok, pid} <- server_pid(endpoint, scheme), do: ThousandIsland.listener_info(pid)
  end

  defp resolve_plug(code_reload?, endpoint) do
    if code_reload? &&
         Code.ensure_loaded?(Phoenix.Endpoint.SyncCodeReloadPlug) &&
         function_exported?(Phoenix.Endpoint.SyncCodeReloadPlug, :call, 2) do
      {Phoenix.Endpoint.SyncCodeReloadPlug, {endpoint, []}}
    else
      endpoint
    end
  end
end
