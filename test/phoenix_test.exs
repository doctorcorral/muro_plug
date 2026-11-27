defmodule MuroPlug.PhoenixTest do
  use ExUnit.Case, async: false

  defmodule PageController do
    use Phoenix.Controller, formats: [:html]

    def index(conn, _params), do: text(conn, "phoenix on muro")

    def echo(conn, params),
      do: text(conn, "#{params["id"]}:#{params["x"]}:#{conn.assigns[:seen]}")
  end

  defmodule ErrorText do
    def render(template, _assigns),
      do: Phoenix.Controller.status_message_from_template(template)
  end

  defmodule Router do
    use Phoenix.Router

    pipeline :api do
      plug(:mark)
    end

    scope "/" do
      pipe_through(:api)
      get("/", PageController, :index)
      post("/echo/:id", PageController, :echo)
    end

    def mark(conn, _), do: Plug.Conn.assign(conn, :seen, "piped")
  end

  defmodule Endpoint do
    use Phoenix.Endpoint, otp_app: :muro_plug

    plug(Plug.Parsers, parsers: [:urlencoded])
    plug(Router)
  end

  setup_all do
    {:ok, _} = Application.ensure_all_started(:inets)

    Application.put_env(:muro_plug, Endpoint,
      adapter: MuroPlug.PhoenixAdapter,
      server: true,
      http: [ip: {127, 0, 0, 1}, port: 0],
      secret_key_base: String.duplicate("a", 64),
      url: [host: "localhost"],
      render_errors: [formats: [text: ErrorText], layout: false]
    )

    start_supervised!(Endpoint)
    {:ok, {_ip, port}} = Endpoint.server_info(:http)
    {:ok, port: port}
  end

  test "the endpoint starts MuroPlug under its own supervisor" do
    assert {:ok, pid} = MuroPlug.PhoenixAdapter.server_pid(Endpoint, :http)
    assert is_pid(pid)
  end

  test "a Phoenix.Endpoint with a router and controller is served", %{port: port} do
    assert {:ok, {{_, 200, _}, headers, ~c"phoenix on muro"}} =
             :httpc.request(:get, {~c"http://127.0.0.1:#{port}/", []}, [], [])

    assert {~c"content-type", ~c"text/plain; charset=utf-8"} in headers

    assert {:ok, {{_, 200, _}, _, ~c"7:y:piped"}} =
             :httpc.request(
               :post,
               {~c"http://127.0.0.1:#{port}/echo/7", [], ~c"application/x-www-form-urlencoded",
                ~c"x=y"},
               [],
               []
             )
  end

  test "Phoenix render_errors answers a NoRouteError with 404, then the book closes", %{
    port: port
  } do
    s = MuroPlug.TestClient.connect(port)
    :ok = :gen_tcp.send(s, "GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n")
    {:ok, r, ""} = MuroPlug.TestClient.read(s)
    assert {r.status, r.body} == {404, "Not Found"}
    assert MuroPlug.TestClient.closed?(s)
  end

  test ":https is refused with a clear error" do
    assert_raise ArgumentError, ~r/https/, fn ->
      MuroPlug.PhoenixAdapter.child_specs(Endpoint, https: [port: 0], otp_app: :muro_plug)
    end
  end
end
