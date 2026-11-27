defmodule MuroPlug do
  @moduledoc """
  A Plug and Phoenix adapter that serves HTTP/1.1 from the muro-http
  book.

  `priv/muro/http/step.muro` (and the `request.muro` it imports) are
  pinned copies of the muro-http books. `mix muro.gen` emits them with
  muro's default Elixir backend into `MuroPlug.Step`. The Thousand
  Island handler (`MuroPlug.Handler`) feeds every socket event to that
  emitted step as a fact and does what the step's `Out` says. The
  `Plug.Conn.Adapter` (`MuroPlug.Adapter`) asks the same book how to
  frame each response. Octets stay binaries on the BEAM.

  ## Usage

      children = [
        {MuroPlug, plug: MyApp.Router, port: 4000}
      ]

  HTTPS, over `ThousandIsland.Transports.SSL`:

      children = [
        {MuroPlug,
         plug: MyApp.Router,
         scheme: :https,
         port: 4040,
         certfile: "/etc/my_app/cert.pem",
         keyfile: "/etc/my_app/key.pem"}
      ]

  Options:

    * `:plug` — a Plug module or `{module, opts}`. Required.
    * `:scheme` — `:http` (the default) or `:https`.
    * `:port` — listen port. Defaults to `4000` for `:http` and `4040`
      for `:https`; `0` lets the OS pick.
    * `:ip` — listen address tuple. Defaults to all interfaces.
    * `:max_body` — the book's `max`: a Content-Length or chunked
      total past this is 413. Defaults to `8_000_000`.
    * `:idle_timeout` — the book's idle budget in milliseconds, and
      the Thousand Island read timeout. Defaults to `60_000`.
    * `:thousand_island_options` — passed to `ThousandIsland`. Its
      `:transport_options` go to the transport (`:gen_tcp` or `:ssl`).

  With `scheme: :https` these TLS options are passed to `:ssl` after
  `Plug.SSL.configure/1`, the same checks Plug.Cowboy and Bandit run:
  `:keyfile`, `:certfile`, `:cacertfile`, `:dhfile`, `:key`, `:cert`,
  `:cacerts`, `:certs_keys`, `:sni_hosts`, `:sni_fun`, `:password`,
  `:otp_app` (relative file paths resolve against its app dir),
  `:cipher_suite` (`:strong` or `:compatible`), `:ciphers`,
  `:versions`, `:eccs`, `:verify`, `:fail_if_no_peer_cert`, and
  `:alpn_preferred_protocols`. A key and a certificate (or
  `:certs_keys`, or SNI) are required. The server speaks HTTP/1.1 only,
  so ALPN advertises `"http/1.1"` and no other protocol may be listed.

  TLS options may also sit in `thousand_island_options:
  [transport_options: ...]`; top-level ones win. `:ssl` is started
  before the listener.

  An invalid `:scheme`, missing TLS key or certificate, or TLS options
  given with `scheme: :http` (top-level or in `:transport_options`)
  raise `ArgumentError`.

  For Phoenix, see `MuroPlug.PhoenixAdapter`.
  """

  @tls_options [
    :keyfile,
    :certfile,
    :cacertfile,
    :dhfile,
    :key,
    :cert,
    :cacerts,
    :certs_keys,
    :sni_hosts,
    :sni_fun,
    :password,
    :otp_app,
    :cipher_suite,
    :ciphers,
    :versions,
    :eccs,
    :verify,
    :fail_if_no_peer_cert,
    :alpn_preferred_protocols
  ]

  @alpn ["http/1.1"]

  @doc "A child spec for a MuroPlug server."
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    {id, opts} = Keyword.pop(opts, :id, __MODULE__)

    opts
    |> thousand_island_options()
    |> ThousandIsland.child_spec()
    |> Supervisor.child_spec(id: id)
  end

  @doc "Start a MuroPlug server linked to the caller."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts),
    do: opts |> thousand_island_options() |> ThousandIsland.start_link()

  @doc false
  def thousand_island_options(opts) do
    scheme = scheme!(opts)

    plug =
      case Keyword.fetch!(opts, :plug) do
        {mod, plug_opts} when is_atom(mod) -> {mod, mod.init(plug_opts)}
        mod when is_atom(mod) -> {mod, mod.init([])}
      end

    idle = Keyword.get(opts, :idle_timeout, 60_000)
    ti = Keyword.get(opts, :thousand_island_options, [])

    transport =
      case Keyword.fetch(opts, :ip) do
        {:ok, ip} -> Keyword.put(Keyword.get(ti, :transport_options, []), :ip, ip)
        :error -> Keyword.get(ti, :transport_options, [])
      end

    {module, transport, port} = transport(scheme, opts, transport)

    Keyword.merge(ti,
      port: Keyword.get(opts, :port, port),
      read_timeout: idle,
      transport_module: module,
      transport_options: transport,
      handler_module: MuroPlug.Handler,
      handler_options: %{
        plug: plug,
        scheme: Atom.to_string(scheme),
        max_body: Keyword.get(opts, :max_body, 8_000_000),
        idle_timeout: idle
      }
    )
  end

  defp scheme!(opts) do
    case Keyword.get(opts, :scheme, :http) do
      scheme when scheme in [:http, :https] ->
        scheme

      other ->
        raise ArgumentError,
              "MuroPlug: invalid :scheme #{inspect(other)}, expected :http or :https"
    end
  end

  # TLS options under :http would serve plain HTTP, or fail in
  # :gen_tcp. A top-level :otp_app is harmless; in transport_options it
  # would reach :gen_tcp, so it is refused there too.
  defp transport(:http, opts, transport) do
    keys =
      Keyword.keys(Keyword.take(opts, @tls_options -- [:otp_app])) ++
        Keyword.keys(Keyword.take(transport, @tls_options))

    case Enum.uniq(keys) do
      [] ->
        {ThousandIsland.Transports.TCP, transport, 4000}

      keys ->
        raise ArgumentError,
              "MuroPlug: TLS options #{inspect(keys)} need scheme: :https"
    end
  end

  # Top-level TLS options override the same keys in transport_options.
  defp transport(:https, opts, transport) do
    # As Plug.Cowboy.https/3: :ssl must run before the SSL listener.
    {:ok, _} = Application.ensure_all_started(:ssl)
    tls = Keyword.merge(transport, Keyword.take(opts, @tls_options))

    case Keyword.get(tls, :alpn_preferred_protocols, @alpn) do
      @alpn ->
        :ok

      other ->
        raise ArgumentError,
              "MuroPlug serves HTTP/1.1 only: :alpn_preferred_protocols must be " <>
                "#{inspect(@alpn)}, got #{inspect(other)}"
    end

    case Plug.SSL.configure(Keyword.put(tls, :alpn_preferred_protocols, @alpn)) do
      {:ok, tls} ->
        {ThousandIsland.Transports.SSL, Keyword.delete(tls, :otp_app), 4040}

      {:error, message} ->
        raise ArgumentError, "MuroPlug: invalid TLS options: #{message}"
    end
  end
end
