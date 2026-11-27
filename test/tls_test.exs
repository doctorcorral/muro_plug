defmodule MuroPlug.TLSTest do
  use ExUnit.Case, async: true

  import MuroPlug.TestClient

  alias MuroPlug.TestCert

  # 2.5 MiB: past ThousandIsland.Transports.SSL's 1 MiB sendfile chunk,
  # so the file goes out as three pread + :ssl.send rounds.
  @big_file 2_621_440

  defmodule App do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, opts), do: route(conn.method, conn.path_info, conn, opts)

    defp route(_, ["echo"], conn, _) do
      {:ok, body, conn} = read_all(conn, [])
      true = is_binary(body)
      send_resp(conn, 200, ["body=", body])
    end

    # Tells the test the plug runs, so the body is being streamed.
    defp route(_, ["size_tell"], conn, opts) do
      send(opts[:test], :streaming)
      route("POST", ["size"], conn, opts)
    end

    defp route(_, ["size"], conn, _) do
      {:ok, body, conn} = read_all(conn, [])
      send_resp(conn, 200, Integer.to_string(byte_size(body)) <> ":" <> hash(body))
    end

    defp route("GET", ["scheme"], conn, _) do
      %{address: addr, ssl_cert: cert} = get_peer_data(conn)
      ssl = get_ssl_data(conn)

      send_resp(
        conn,
        200,
        "#{conn.scheme} #{conn.host}:#{conn.port} #{:inet.ntoa(addr)} " <>
          "#{get_http_protocol(conn)} #{ssl[:protocol]} #{inspect(cert)}"
      )
    end

    defp route("GET", ["peer_cert"], conn, _),
      do: send_resp(conn, 200, hash(get_peer_data(conn).ssl_cert))

    defp route("GET", ["stream"], conn, _) do
      conn = send_chunked(conn, 200)
      {:ok, conn} = chunk(conn, "one,")
      {:ok, conn} = chunk(conn, "")
      {:ok, conn} = chunk(conn, ["two", ?,, "three"])
      conn
    end

    defp route("GET", ["file"], conn, _), do: send_file(conn, 200, __ENV__.file, 0, 100)
    defp route("GET", ["file_off"], conn, _), do: send_file(conn, 200, __ENV__.file, 10, :all)

    defp route("GET", ["file_past"], conn, _),
      do: send_file(conn, 200, __ENV__.file, 10_000_000, :all)

    defp route(_, ["big"], conn, opts), do: send_file(conn, 200, opts[:big], 0, :all)
    defp route("GET", ["big_mid"], conn, opts), do: send_file(conn, 200, opts[:big], 7, 1_500_000)

    defp route("GET", ["file_broken"], conn, opts) do
      {MuroPlug.Adapter, p} = conn.adapter
      :ssl.close(p.socket.socket)
      conn = send_file(conn, 200, __ENV__.file, 0, :all)
      {MuroPlug.Adapter, p} = conn.adapter
      send(opts[:test], {:file_broken, p.resp})
      conn
    end

    defp route("GET", ["hints"], conn, _) do
      conn
      |> inform!(103, [{"link", "</a.css>; rel=preload"}])
      |> send_resp(200, "after hints")
    end

    defp route("GET", ["teapot"], conn, _), do: send_resp(conn, 418, "short and stout")

    defp route(_, _, conn, _), do: send_resp(conn, 200, "#{conn.method} #{conn.request_path}")

    defp read_all(conn, acc) do
      case read_body(conn, length: 10_000) do
        {:ok, data, conn} -> {:ok, IO.iodata_to_binary([acc, data]), conn}
        {:more, data, conn} -> read_all(conn, [acc, data])
      end
    end

    def hash(b),
      do: :crypto.hash(:sha256, b) |> Base.encode16(case: :lower) |> binary_part(0, 16)
  end

  setup_all do
    big = Path.join(TestCert.dir(), "big.bin")
    File.write!(big, :crypto.strong_rand_bytes(@big_file))
    {:ok, big: big}
  end

  setup %{big: big} do
    port =
      serve({App, big: big, test: self()}, TestCert.server_options() ++ [max_body: 1_000_000])

    {:ok, port: port}
  end

  test "GET over TLS: scheme, host, port, peer, protocol, and TLS data", %{port: port} do
    r = request_tls(port, "GET /scheme HTTP/1.1\r\nHost: localhost:#{port}\r\n\r\n")
    assert r.status == 200
    assert r.body =~ ~r/^https localhost:#{port} 127\.0\.0\.1 HTTP\/1\.1 tlsv1\.[23] nil$/
    assert header(r, "content-length") == Integer.to_string(byte_size(r.body))
  end

  test "the TLS handshake advertises HTTP/1.1 only", %{port: port} do
    {:tls, s} = connect_tls(port, alpn_advertised_protocols: ["h2", "http/1.1"])
    assert :ssl.negotiated_protocol(s) == {:ok, "http/1.1"}
    :ok = :ssl.send(s, "GET /a HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{status: 200, body: "GET /a"}, ""} = read({:tls, s})
  end

  test "a plain TCP client on the TLS port gets no HTTP response", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET / HTTP/1.1\r\nHost: h\r\n\r\n")
    refute drain(s) =~ "HTTP/1.1"
  end

  test "POST with Content-Length and with a chunked body", %{port: port} do
    r = request_tls(port, "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello")
    assert {r.status, r.body} == {200, "body=hello"}

    req =
      "POST /echo HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n" <>
        "5;ext=1\r\nhello\r\n1\r\n \r\n5\r\nworld\r\n0\r\nX-Trailer: t\r\n\r\n"

    assert request_tls(port, req).body == "body=hello world"
  end

  test "a request split into one-byte TLS records", %{port: port} do
    s = connect_tls(port)

    for <<b <- "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabc">> do
      :ok = send_bytes(s, <<b>>)
    end

    assert {:ok, %{status: 200, body: "body=abc"}, ""} = read(s)
  end

  test "a Content-Length body past 16384 bytes streams from the TLS socket", %{port: port} do
    body = :crypto.strong_rand_bytes(200_000)
    s = connect_tls(port)
    :ok = send_bytes(s, "POST /size HTTP/1.1\r\nHost: h\r\nContent-Length: 200000\r\n\r\n")
    :ok = send_bytes(s, body)
    assert {:ok, %{body: got}, ""} = read(s)
    assert got == "200000:" <> App.hash(body)
    :ok = send_bytes(s, "GET /after HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: "GET /after"}, ""} = read(s)
  end

  test "a chunked body past 16384 bytes streams, then a pipelined request", %{port: port} do
    data = :crypto.strong_rand_bytes(60_000)
    chunks = for <<c::binary-size(6_000) <- data>>, into: "", do: "1770\r\n" <> c <> "\r\n"
    s = connect_tls(port)

    :ok =
      send_bytes(
        s,
        "POST /size HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n" <>
          chunks <> "0\r\n\r\nGET /next HTTP/1.1\r\nHost: h\r\n\r\n"
      )

    {:ok, r, rest} = read(s)
    assert r.body == "60000:" <> App.hash(data)
    assert {:ok, %{body: "GET /next"}, ""} = read(s, rest)
  end

  test "keep-alive reuses one TLS connection", %{port: port} do
    s = connect_tls(port)

    for i <- 1..20 do
      :ok = send_bytes(s, "GET /n/#{i} HTTP/1.1\r\nHost: h\r\n\r\n")
      assert {:ok, %{status: 200, body: body}, ""} = read(s)
      assert body == "GET /n/#{i}"
    end
  end

  test "pipelined requests answer in order over TLS", %{port: port} do
    s = connect_tls(port)

    :ok =
      send_bytes(
        s,
        "GET /a HTTP/1.1\r\nHost: h\r\n\r\n" <>
          "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\nhi" <>
          "POST /echo HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nyo\r\n0\r\n\r\n" <>
          "GET /b HTTP/1.1\r\nHost: h\r\n\r\n"
      )

    {:ok, r1, rest} = read(s)
    {:ok, r2, rest} = read(s, rest)
    {:ok, r3, rest} = read(s, rest)
    {:ok, r4, ""} = read(s, rest)
    assert Enum.map([r1, r2, r3, r4], & &1.body) == ["GET /a", "body=hi", "body=yo", "GET /b"]
  end

  test "Expect: 100-continue gets the interim over TLS", %{port: port} do
    {:tls, ssl} = s = connect_tls(port)

    :ok =
      send_bytes(
        s,
        "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n"
      )

    assert {:ok, "HTTP/1.1 100 Continue\r\n\r\n"} = :ssl.recv(ssl, 0, 1_000)
    :ok = send_bytes(s, "data")
    assert {:ok, %{status: 200, body: "body=data"}, ""} = read(s)
  end

  test "send_file over TLS: a length, an offset with :all, and past EOF", %{port: port} do
    file = File.read!(__ENV__.file)
    s = connect_tls(port)
    :ok = send_bytes(s, "GET /file HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, r, ""} = read(s)
    assert {header(r, "content-length"), r.body} == {"100", binary_part(file, 0, 100)}
    :ok = send_bytes(s, "GET /file_off HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: body}, ""} = read(s)
    assert body == binary_part(file, 10, byte_size(file) - 10)
    :ok = send_bytes(s, "GET /file_past HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{status: 200, body: ""}, ""} = read(s)
    :ok = send_bytes(s, "GET /next HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: "GET /next"}, ""} = read(s)
  end

  test "send_file over TLS past the 1 MiB SSL chunk, whole and from an offset", %{
    port: port,
    big: big
  } do
    data = File.read!(big)
    s = connect_tls(port)
    :ok = send_bytes(s, "GET /big HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, r, ""} = read(s, <<>>, timeout: 10_000)
    assert header(r, "content-length") == Integer.to_string(@big_file)
    assert r.body == data
    :ok = send_bytes(s, "GET /big_mid HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, r, ""} = read(s, <<>>, timeout: 10_000)
    assert r.body == binary_part(data, 7, 1_500_000)
    :ok = send_bytes(s, "HEAD /big HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, r, ""} = read(s, <<>>, head: true)
    assert header(r, "content-length") == Integer.to_string(@big_file)
    :ok = send_bytes(s, "GET /after HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: "GET /after"}, ""} = read(s)
  end

  test "send_file on a closed TLS socket does not crash and the handler closes", %{port: port} do
    s = connect_tls(port)

    {_, log} =
      ExUnit.CaptureLog.with_log(fn ->
        :ok = send_bytes(s, "GET /file_broken HTTP/1.1\r\nHost: h\r\n\r\n")
        assert_receive {:file_broken, {:failed, _out}}, 2_000
        assert closed?(s)
      end)

    # CaptureLog sees every async test's logs: refute only this module's.
    refute log =~ "MatchError"
    refute log =~ "TLSTest"
  end

  test "send_chunked over TLS, then keep-alive", %{port: port} do
    s = connect_tls(port)
    :ok = send_bytes(s, "GET /stream HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, r, ""} = read(s)
    assert header(r, "transfer-encoding") == "chunked"
    assert r.body == "one,two,three"
    :ok = send_bytes(s, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: "GET /x"}, ""} = read(s)
  end

  test "a chunked response to HTTP/1.0 over TLS is close-delimited", %{port: port} do
    r = request_tls(port, "GET /stream HTTP/1.0\r\n\r\n")
    refute header(r, "transfer-encoding")
    assert {header(r, "connection"), r.body} == {"close", "one,two,three"}
  end

  test "inform/3 writes a 103 before the response over TLS", %{port: port} do
    s = connect_tls(port)
    :ok = send_bytes(s, "GET /hints HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, info, rest} = read(s)
    assert info.status == 103
    assert header(info, "link") == "</a.css>; rel=preload"
    assert {:ok, %{status: 200, body: "after hints"}, ""} = read(s, rest)
  end

  test "the book's protocol statuses over TLS close the connection", %{port: port} do
    for {req, status} <- [
          {"GET / HTTP/1.1\r\n\r\n", 400},
          {"GET/HTTP/1.1\r\nHost: h\r\n\r\n", 400},
          {"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1000001\r\n\r\n", 413},
          {"GET /" <> String.duplicate("a", 17_000), 414},
          {"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nExpect: party\r\n\r\nx", 417},
          {"GET / HTTP/1.1\r\nHost: h\r\nX-Big: " <> String.duplicate("b", 17_000) <> "\r\n\r\n",
           431},
          {"TRACE / HTTP/1.1\r\nHost: h\r\n\r\n", 501}
        ] do
      s = connect_tls(port)
      :ok = send_bytes(s, req)
      assert {:ok, %{status: ^status} = r, ""} = read(s)
      assert header(r, "connection") == "close"
      assert closed?(s)
    end
  end

  test "a streamed chunked total past max_body is the book's 413 over TLS" do
    port = serve({App, test: self()}, TestCert.server_options() ++ [max_body: 30_000])
    s = connect_tls(port)
    hex = Integer.to_string(20_000, 16)

    :ok =
      send_bytes(
        s,
        "POST /size_tell HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n" <>
          hex <> "\r\n" <> String.duplicate("a", 20_000)
      )

    # The plug runs before the body ends: the book handed it to the
    # host to stream. Only then does the second chunk pass max_body.
    assert_receive :streaming, 2_000
    :ok = send_bytes(s, "\r\n" <> hex <> "\r\n" <> String.duplicate("b", 20_000))
    {{:ok, r, ""}, log} = ExUnit.CaptureLog.with_log(fn -> read(s) end)
    assert {r.status, header(r, "connection")} == {413, "close"}
    assert closed?(s)
    refute log =~ "TLSTest"
  end

  test "a 4xx from the plug closes over TLS, as the book decides", %{port: port} do
    s = connect_tls(port)
    :ok = send_bytes(s, "GET /teapot HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, r, ""} = read(s)
    assert {r.status, header(r, "connection")} == {418, "close"}
    assert closed?(s)
  end

  test "idle TLS connections close after idle_timeout" do
    port = serve({App, []}, TestCert.server_options() ++ [idle_timeout: 100])
    assert closed?(connect_tls(port), 2_000)
  end

  test "a client certificate reaches peer data" do
    opts = [verify: :verify_peer, cacertfile: TestCert.cacertfile(), fail_if_no_peer_cert: true]
    port = serve({App, []}, TestCert.server_options() ++ opts)
    s = connect_tls(port, certfile: TestCert.certfile(), keyfile: TestCert.keyfile())
    :ok = send_bytes(s, "GET /peer_cert HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{status: 200, body: body}, ""} = read(s)
    assert body == App.hash(TestCert.der().cert)
  end

  test "DER :cert and :key work in place of files" do
    %{cert: cert, key: key} = TestCert.der()
    port = serve({App, []}, scheme: :https, cert: cert, key: key)
    r = request_tls(port, "GET /scheme HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert r.body =~ ~r/^https localhost:#{port} /
  end

  test "TLS options pass through :thousand_island_options transport_options too" do
    ti = [transport_options: [certfile: TestCert.certfile(), keyfile: TestCert.keyfile()]]
    port = serve({App, []}, scheme: :https, thousand_island_options: ti)
    assert request_tls(port, "GET /ti HTTP/1.1\r\nHost: h\r\n\r\n").body == "GET /ti"
  end

  test "top-level TLS options override transport_options" do
    ti = [transport_options: [certfile: "/nope/cert.pem", keyfile: "/nope/key.pem"]]
    opts = TestCert.server_options() ++ [thousand_island_options: ti]
    tls = MuroPlug.thousand_island_options([plug: App] ++ opts)[:transport_options]
    assert tls[:certfile] == String.to_charlist(TestCert.certfile())
    assert tls[:keyfile] == String.to_charlist(TestCert.keyfile())
    assert length(Keyword.get_values(tls, :certfile)) == 1
    port = serve({App, []}, opts)
    assert request_tls(port, "GET /top HTTP/1.1\r\nHost: h\r\n\r\n").body == "GET /top"
  end

  test ":certs_keys serves, with relative files resolved against :otp_app" do
    app = Application.app_dir(:muro_plug)

    # A "../../.." path from the app dir to the temp cert.
    relative = Path.relative_to(TestCert.certfile(), app, force: true)
    assert Path.type(relative) == :relative
    certs_keys = [%{certfile: relative, keyfile: TestCert.keyfile()}]

    opts = [scheme: :https, otp_app: :muro_plug, certs_keys: certs_keys]
    tls = MuroPlug.thousand_island_options([plug: App] ++ opts)[:transport_options]
    refute Keyword.has_key?(tls, :otp_app)
    assert [%{certfile: certfile, keyfile: keyfile}] = tls[:certs_keys]
    assert certfile == String.to_charlist(TestCert.certfile())
    assert keyfile == String.to_charlist(TestCert.keyfile())

    port = serve({App, []}, opts)
    assert request_tls(port, "GET /ck HTTP/1.1\r\nHost: h\r\n\r\n").body == "GET /ck"
  end

  test ":sni_hosts and :sni_fun satisfy the key and certificate checks" do
    host = [certfile: TestCert.certfile(), keyfile: TestCert.keyfile()]
    sni_fun = fn _ -> host end

    for sni <- [[sni_hosts: [{~c"localhost", host}]], [sni_fun: sni_fun]] do
      opts = [scheme: :https, otp_app: :muro_plug] ++ sni
      ti = MuroPlug.thousand_island_options([plug: App] ++ opts)
      assert ti[:transport_module] == ThousandIsland.Transports.SSL
      tls = ti[:transport_options]
      refute Keyword.has_key?(tls, :otp_app)
      assert Keyword.take(tls, [:sni_hosts, :sni_fun]) == sni
      assert %{start: {ThousandIsland, :start_link, _}} = MuroPlug.child_spec([plug: App] ++ opts)
    end

    port = serve({App, []}, scheme: :https, sni_fun: sni_fun)

    s =
      connect_tls(port, server_name_indication: ~c"localhost")

    :ok = send_bytes(s, "GET /sni HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert {:ok, %{body: "GET /sni"}, ""} = read(s)
  end

  describe "option errors" do
    defp opts!(opts), do: MuroPlug.thousand_island_options([plug: App] ++ opts)

    test "an invalid :scheme" do
      assert_raise ArgumentError, ~r/invalid :scheme :ftp/, fn -> opts!(scheme: :ftp) end
      assert_raise ArgumentError, ~r/invalid :scheme "https"/, fn -> opts!(scheme: "https") end
    end

    test ":https without a key or a certificate" do
      assert_raise ArgumentError, ~r/missing option :key\/:keyfile/, fn ->
        opts!(scheme: :https)
      end

      assert_raise ArgumentError, ~r/missing option :cert\/:certfile/, fn ->
        opts!(scheme: :https, keyfile: TestCert.keyfile())
      end
    end

    test "a TLS file that does not exist, or a relative one without :otp_app" do
      assert_raise ArgumentError, ~r/nope\.pem.*does not exist/, fn ->
        opts!(scheme: :https, keyfile: "/nope.pem", certfile: TestCert.certfile())
      end

      assert_raise ArgumentError, ~r/:otp_app option is required/, fn ->
        opts!(scheme: :https, keyfile: "nope.pem", certfile: TestCert.certfile())
      end

      app = Regex.escape(Application.app_dir(:muro_plug))

      assert_raise ArgumentError, ~r/#{app}\/priv\/nope\.pem.*does not exist/, fn ->
        opts!(
          scheme: :https,
          otp_app: :muro_plug,
          keyfile: "priv/nope.pem",
          certfile: TestCert.certfile()
        )
      end
    end

    test "ALPN other than http/1.1 is refused" do
      assert_raise ArgumentError, ~r/HTTP\/1\.1 only/, fn ->
        opts!(TestCert.server_options() ++ [alpn_preferred_protocols: ["h2", "http/1.1"]])
      end
    end

    test "TLS options with scheme: :http" do
      assert_raise ArgumentError, ~r/\[:certfile, :keyfile\] need scheme: :https/, fn ->
        opts!(certfile: TestCert.certfile(), keyfile: TestCert.keyfile())
      end

      ti = [transport_options: [certfile: TestCert.certfile(), keyfile: TestCert.keyfile()]]

      assert_raise ArgumentError, ~r/\[:certfile, :keyfile\] need scheme: :https/, fn ->
        opts!(scheme: :http, thousand_island_options: ti)
      end

      assert_raise ArgumentError, ~r/\[:otp_app\] need scheme: :https/, fn ->
        opts!(thousand_island_options: [transport_options: [otp_app: :muro_plug]])
      end

      # A top-level :otp_app (as Phoenix configs carry) is fine under :http.
      assert opts!(otp_app: :muro_plug)[:transport_module] == ThousandIsland.Transports.TCP
    end

    test "the transport and its options" do
      http = opts!(port: 0)
      assert http[:transport_module] == ThousandIsland.Transports.TCP
      assert http[:port] == 0
      assert http[:handler_options].scheme == "http"
      assert opts!([])[:port] == 4000

      https = opts!(TestCert.server_options() ++ [ip: {127, 0, 0, 1}, cipher_suite: :strong])
      assert https[:transport_module] == ThousandIsland.Transports.SSL
      assert https[:port] == 4040
      assert https[:handler_options].scheme == "https"
      tls = https[:transport_options]
      assert tls[:ip] == {127, 0, 0, 1}
      assert tls[:alpn_preferred_protocols] == ["http/1.1"]
      assert tls[:versions] == [:"tlsv1.3"]
      assert tls[:certfile] == String.to_charlist(TestCert.certfile())
      refute Keyword.has_key?(tls, :otp_app)
      refute Keyword.has_key?(tls, :cipher_suite)
    end
  end

  defp drain(s, acc \\ <<>>) do
    case :gen_tcp.recv(s, 0, 2_000) do
      {:ok, data} -> drain(s, acc <> data)
      {:error, _} -> acc
    end
  end
end

defmodule MuroPlug.TLSPlugRouterTest do
  use ExUnit.Case, async: true

  alias MuroPlug.TestCert

  defmodule Router do
    use Plug.Router

    plug(Plug.Parsers, parsers: [:urlencoded])
    plug(:match)
    plug(:dispatch)

    get "/hello/:name" do
      send_resp(conn, 200, "hello #{name} over #{conn.scheme}")
    end

    post "/form" do
      send_resp(conn, 201, "#{conn.body_params["a"]}+#{conn.body_params["b"]}")
    end

    get "/static" do
      send_file(conn, 200, Path.expand("../mix.exs", __DIR__))
    end

    match _ do
      send_resp(conn, 404, "nope")
    end
  end

  setup_all do
    {:ok, _} = Application.ensure_all_started(:inets)
    :ok
  end

  test "a Plug.Router app served over HTTPS through :httpc" do
    port = MuroPlug.TestClient.serve(Router, TestCert.server_options())
    url = fn path -> ~c"https://127.0.0.1:#{port}#{path}" end
    http = [ssl: TestCert.client_options()]

    assert {:ok, {{_, 200, _}, _, ~c"hello tls over https"}} =
             :httpc.request(:get, {url.("/hello/tls"), []}, http, [])

    assert {:ok, {{_, 201, _}, _, ~c"1+two"}} =
             :httpc.request(
               :post,
               {url.("/form"), [], ~c"application/x-www-form-urlencoded", ~c"a=1&b=two"},
               http,
               []
             )

    assert {:ok, {{_, 404, _}, _, ~c"nope"}} =
             :httpc.request(:get, {url.("/missing"), []}, http, [])

    assert {:ok, {{_, 200, _}, _, body}} =
             :httpc.request(:get, {url.("/static"), []}, http, body_format: :binary)

    assert body == File.read!(Path.expand("../mix.exs", __DIR__))
  end
end

defmodule MuroPlug.TLSPhoenixTest do
  use ExUnit.Case, async: false

  alias MuroPlug.TestCert

  defmodule SchemeController do
    use Phoenix.Controller, formats: [:html]

    def index(conn, _params), do: text(conn, "#{conn.scheme} #{conn.port}")
  end

  defmodule Router do
    use Phoenix.Router

    get("/", SchemeController, :index)
  end

  defmodule Endpoint do
    use Phoenix.Endpoint, otp_app: :muro_plug

    plug(Router)
  end

  defmodule HTTPSOnly do
    use Phoenix.Endpoint, otp_app: :muro_plug

    plug(Router)
  end

  setup_all do
    {:ok, _} = Application.ensure_all_started(:inets)
    %{cert: cert, key: key} = TestCert.der()

    base = [
      adapter: MuroPlug.PhoenixAdapter,
      server: true,
      secret_key_base: String.duplicate("a", 64),
      url: [host: "localhost"]
    ]

    Application.put_env(
      :muro_plug,
      Endpoint,
      base ++
        [
          http: [ip: {127, 0, 0, 1}, port: 0],
          https: [
            ip: {127, 0, 0, 1},
            port: 0,
            certfile: TestCert.certfile(),
            keyfile: TestCert.keyfile()
          ]
        ]
    )

    Application.put_env(
      :muro_plug,
      HTTPSOnly,
      base ++ [https: [ip: {127, 0, 0, 1}, port: 0, cert: cert, key: key]]
    )

    start_supervised!(Endpoint)
    start_supervised!(HTTPSOnly)
    :ok
  end

  defp get(url), do: :httpc.request(:get, {url, []}, [ssl: TestCert.client_options()], [])

  test "one endpoint serves http: and https: together, each with server_info/2" do
    {:ok, {{127, 0, 0, 1}, http}} = Endpoint.server_info(:http)
    {:ok, {{127, 0, 0, 1}, https}} = Endpoint.server_info(:https)
    assert http != https

    assert {:ok, http_pid} = MuroPlug.PhoenixAdapter.server_pid(Endpoint, :http)
    assert {:ok, https_pid} = MuroPlug.PhoenixAdapter.server_pid(Endpoint, :https)
    assert http_pid != https_pid

    assert {:ok, {{_, 200, _}, _, body}} = get(~c"http://127.0.0.1:#{http}/")
    assert to_string(body) == "http #{http}"
    assert {:ok, {{_, 200, _}, _, body}} = get(~c"https://127.0.0.1:#{https}/")
    assert to_string(body) == "https #{https}"
  end

  test "an https:-only endpoint with DER :cert and :key" do
    assert {:error, :no_server_found} = HTTPSOnly.server_info(:http)
    {:ok, {_, port}} = HTTPSOnly.server_info(:https)
    assert {:ok, {{_, 200, _}, _, body}} = get(~c"https://127.0.0.1:#{port}/")
    assert to_string(body) == "https #{port}"
  end

  test "child_specs/2: ids, schemes, and :otp_app for relative https files" do
    config = [
      otp_app: :muro_plug,
      http: [port: 0, otp_app: :muro_plug],
      https: [port: 0, certfile: TestCert.certfile(), keyfile: TestCert.keyfile()]
    ]

    assert [%{id: {Endpoint, :http}}, %{id: {Endpoint, :https}}] =
             MuroPlug.PhoenixAdapter.child_specs(Endpoint, config)

    app = Regex.escape(Application.app_dir(:muro_plug))

    assert_raise ArgumentError, ~r/#{app}\/priv\/cert\/missing\.pem/, fn ->
      MuroPlug.PhoenixAdapter.child_specs(Endpoint,
        otp_app: :muro_plug,
        https: [port: 0, certfile: "priv/cert/missing.pem", keyfile: TestCert.keyfile()]
      )
    end

    assert [] = MuroPlug.PhoenixAdapter.child_specs(Endpoint, otp_app: :muro_plug, https: false)
  end
end
