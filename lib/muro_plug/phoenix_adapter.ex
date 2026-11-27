defmodule MuroPlug.PhoenixAdapter do
  @moduledoc """
  A `Phoenix.Endpoint` adapter for MuroPlug, shaped like
  `Bandit.PhoenixAdapter`.

      config :my_app, MyAppWeb.Endpoint,
        adapter: MuroPlug.PhoenixAdapter,
        http: [ip: {127, 0, 0, 1}, port: 4000],
        https: [
          ip: {127, 0, 0, 1},
          port: 4040,
          certfile: "priv/cert/selfsigned.pem",
          keyfile: "priv/cert/selfsigned_key.pem"
        ]

  The `:http` and `:https` keyword lists are each passed to
  `MuroPlug.child_spec/1` (`:port`, `:ip`, `:max_body`,
  `:idle_timeout`, `:thousand_island_options`, and for `:https` the TLS
  options listed there). Either or both may be set: each one is its
  own MuroPlug server under the endpoint's supervisor, with child id
  `{endpoint, scheme}`. Relative TLS file paths in `:https` resolve
  against the endpoint's `:otp_app`, as in Plug.Cowboy and Bandit.

  WebSockets work on either scheme (`ws://` and `wss://`): Phoenix's
  socket transport calls `WebSockAdapter.upgrade/4`, which hands
  `{socket_module, state, opts}` to `MuroPlug.Adapter.upgrade/3`, so
  `Phoenix.Socket` channels and LiveView run on MuroPlug. The endpoint's
  `websocket:` options (`:timeout`, `:max_frame_size`, `:compress`,
  ...) are the ones listed in `MuroPlug.WebSocket`.
  """

  @doc false
  def child_specs(endpoint, config) do
    plug = resolve_plug(config[:code_reloader], endpoint)

    for scheme <- [:http, :https], opts = config[scheme] do
      opts
      |> Keyword.drop([:plug, :id, :scheme])
      |> otp_app(scheme, config[:otp_app])
      |> Keyword.merge(plug: plug, id: {endpoint, scheme}, scheme: scheme)
      |> MuroPlug.child_spec()
    end
  end

  defp otp_app(opts, :https, otp_app) when otp_app != nil,
    do: Keyword.put_new(opts, :otp_app, otp_app)

  defp otp_app(opts, :https, nil), do: opts
  defp otp_app(opts, :http, _), do: Keyword.delete(opts, :otp_app)

  @doc "The MuroPlug (Thousand Island) process serving `scheme` for `endpoint`."
  @spec server_pid(module(), :http | :https) :: {:ok, pid()} | {:error, :no_server_found}
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
