defmodule MuroPlug do
  @moduledoc """
  A Plug and Phoenix adapter that serves HTTP/1.1 from the muro-http
  book.

  `priv/muro/http/step.muro` (and the `request.muro` it imports) are
  pinned copies of the muro-http books. `mix step.gen` emits them with
  muro's default Elixir backend into `MuroPlug.Step`. The Thousand
  Island handler (`MuroPlug.Handler`) feeds every socket event to that
  emitted step as a fact and does what the step's `Out` says. The
  `Plug.Conn.Adapter` (`MuroPlug.Adapter`) asks the same book how to
  frame each response. Octets stay binaries on the BEAM.

  ## Usage

      children = [
        {MuroPlug, plug: MyApp.Router, port: 4000}
      ]

  Options:

    * `:plug` — a Plug module or `{module, opts}`. Required.
    * `:port` — listen port. Defaults to `4000`; `0` lets the OS pick.
    * `:ip` — listen address tuple. Defaults to all interfaces.
    * `:max_body` — the book's `max`: a Content-Length or chunked
      total past this is 413. Defaults to `8_000_000`.
    * `:idle_timeout` — the book's idle budget in milliseconds, and
      the Thousand Island read timeout. Defaults to `60_000`.
    * `:thousand_island_options` — passed to `ThousandIsland`.

  For Phoenix, see `MuroPlug.PhoenixAdapter`.
  """

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

    Keyword.merge(ti,
      port: Keyword.get(opts, :port, 4000),
      read_timeout: idle,
      transport_options: transport,
      handler_module: MuroPlug.Handler,
      handler_options: %{
        plug: plug,
        scheme: "http",
        max_body: Keyword.get(opts, :max_body, 8_000_000),
        idle_timeout: idle
      }
    )
  end
end
