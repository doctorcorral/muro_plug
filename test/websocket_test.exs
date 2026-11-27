defmodule MuroPlug.WebSocketTest do
  use ExUnit.Case,
    async: true,
    parameterize: [%{scheme: :ws}, %{scheme: :wss}]

  import ExUnit.CaptureLog
  import MuroPlug.WSClient

  alias MuroPlug.{TestCert, TestClient}

  defmodule Echo do
    @behaviour WebSock

    @impl true
    def init(%{test: test} = state) do
      send(test, {:init, self()})
      {:ok, state}
    end

    @impl true
    def handle_in({"stop", opcode: :text}, state), do: {:stop, :normal, state}

    def handle_in({"close_custom", opcode: :text}, state),
      do: {:stop, :normal, {4000, "bye"}, [{:text, "last"}], state}

    def handle_in({"close_code", opcode: :text}, state), do: {:stop, :normal, 4001, state}
    def handle_in({"boom", opcode: :text}, state), do: {:stop, :boom, state}
    def handle_in({"restart", opcode: :text}, state), do: {:stop, {:shutdown, :restart}, state}
    def handle_in({"crash", opcode: :text}, _state), do: raise("crash in handle_in")
    def handle_in({"reply", opcode: :text}, state), do: {:reply, :ok, {:text, "replied"}, state}

    def handle_in({"many", opcode: :text}, state),
      do: {:push, [{:text, "a"}, {:binary, <<0, 255>>}, {:ping, "p"}, {:text, nil}], state}

    def handle_in({data, opcode: op}, state) do
      send(state.test, {:in, op, byte_size(data)})
      {:push, {op, data}, state}
    end

    @impl true
    def handle_control({data, opcode: op}, state) do
      send(state.test, {:control, op, data})
      {:ok, state}
    end

    @impl true
    def handle_info({:push, frames}, state), do: {:push, frames, state}
    def handle_info(:stop, state), do: {:stop, :normal, state}
    def handle_info({:stop, detail}, state), do: {:stop, :normal, detail, state}

    @impl true
    def terminate(reason, state) do
      send(state.test, {:terminate, reason})
      :ok
    end
  end

  # init/handle_in/handle_info only: no handle_control or terminate.
  defmodule Minimal do
    @behaviour WebSock
    def init(state), do: {:ok, state}
    def handle_in({data, opcode: op}, state), do: {:push, {op, data}, state}
    def handle_info(_msg, state), do: {:ok, state}
  end

  defmodule InitPush do
    @behaviour WebSock
    def init(state), do: {:push, {:text, "hello from init"}, state}
    def handle_in({data, opcode: op}, state), do: {:push, {op, data}, state}
    def handle_info(_msg, state), do: {:ok, state}
  end

  defmodule App do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, opts), do: route(conn.path_info, conn, Map.new(opts))

    # Through WebSockAdapter, the way Phoenix upgrades.
    defp route(["ws"], conn, opts),
      do: WebSockAdapter.upgrade(conn, Echo, %{test: opts.test}, ws_opts(opts))

    # Straight to the adapter (no early validation in WebSockAdapter).
    defp route(["direct"], conn, opts),
      do: upgrade_adapter(conn, :websocket, {Echo, %{test: opts.test}, ws_opts(opts)})

    defp route(["headers"], conn, opts) do
      conn
      |> put_resp_cookie("session", "abc")
      |> put_resp_header("sec-websocket-protocol", "chat")
      |> put_resp_header("sec-websocket-extensions", "permessage-deflate")
      |> put_resp_header("x-custom", "yes")
      |> WebSockAdapter.upgrade(Echo, %{test: opts.test}, compress: true)
    end

    defp route(["small"], conn, opts),
      do:
        WebSockAdapter.upgrade(conn, Echo, %{test: opts.test},
          max_frame_size: 1_000,
          max_message_size: 2_000
        )

    defp route(["idle"], conn, opts),
      do: WebSockAdapter.upgrade(conn, Echo, %{test: opts.test}, timeout: 200)

    defp route(["nocheck"], conn, opts),
      do: WebSockAdapter.upgrade(conn, Echo, %{test: opts.test}, validate_utf8: false)

    defp route(["close_fast"], conn, opts),
      do: WebSockAdapter.upgrade(conn, Echo, %{test: opts.test}, close_timeout: 50)

    defp route(["bad_opts"], conn, opts),
      do: WebSockAdapter.upgrade(conn, Echo, %{test: opts.test}, close_timeout: :infinity)

    defp route(["bad_heap"], conn, opts),
      do: WebSockAdapter.upgrade(conn, Echo, %{test: opts.test}, max_heap_size: %{size: -1})

    defp route(["minimal"], conn, _opts), do: WebSockAdapter.upgrade(conn, Minimal, nil, [])
    defp route(["init_push"], conn, _opts), do: WebSockAdapter.upgrade(conn, InitPush, nil, [])

    defp route(["deflate"], conn, opts),
      do: WebSockAdapter.upgrade(conn, Echo, %{test: opts.test}, compress: true)

    defp route(["deflate_small"], conn, opts),
      do:
        WebSockAdapter.upgrade(conn, Echo, %{test: opts.test},
          compress: true,
          max_message_size: 2_000
        )

    defp route(["read_body"], conn, opts) do
      {:ok, _body, conn} = Plug.Conn.read_body(conn)
      WebSockAdapter.upgrade(conn, Echo, %{test: opts.test}, [])
    end

    defp route(["bad_spec"], conn, _opts), do: upgrade_adapter(conn, :websocket, :nope)

    defp route(_, conn, _opts), do: send_resp(conn, 200, "plain #{conn.request_path}")

    defp ws_opts(_opts), do: []
  end

  setup %{scheme: scheme} do
    tls = if scheme == :wss, do: TestCert.server_options(), else: []
    port = TestClient.serve({App, test: self()}, tls)
    {:ok, port: port}
  end

  defp up(ctx, path \\ "/ws", opts \\ []) do
    {s, resp, rest} = connect(ctx.port, ctx.scheme, path, opts)
    assert rest == <<>> or Keyword.has_key?(opts, :pipelined) or path == "/init_push"

    pid =
      if path in ["/minimal", "/init_push"] do
        nil
      else
        assert_receive {:init, pid}
        pid
      end

    %{s: s, resp: resp, pid: pid, rest: rest}
  end

  defp closed?(s), do: TestClient.closed?(s, 2_000)

  # The client answers a server close, then the server drops the socket.
  defp answer_close(s) do
    :ok = send_frame(s, :close, <<1000::16>>)
    assert closed?(s)
  end

  defp raw(ctx, data) do
    s = open(ctx.port, ctx.scheme)
    :ok = TestClient.send_bytes(s, data)
    {:ok, r, _} = TestClient.read(s)
    TestClient.close(s)
    r
  end

  describe "handshake" do
    test "101 with the RFC 6455 accept, then text and binary echo", ctx do
      %{s: s, resp: resp} = up(ctx)
      assert resp.version == "HTTP/1.1"
      assert TestClient.header(resp, "upgrade") == "websocket"
      assert TestClient.header(resp, "connection") == "Upgrade"
      refute TestClient.header(resp, "content-length")
      refute TestClient.header(resp, "sec-websocket-extensions")

      :ok = send_frame(s, :text, "héllo")
      assert {:text, "héllo", <<>>} = next(s)
      :ok = send_frame(s, :binary, <<0, 1, 2, 255>>)
      assert {:binary, <<0, 1, 2, 255>>, <<>>} = next(s)
      :ok = send_frame(s, :text, "")
      assert {:text, "", <<>>} = next(s)
      answer_close_after_client(s)
    end

    test "response headers set before the upgrade are sent once", ctx do
      %{s: s, resp: resp} =
        up(ctx, "/headers",
          headers: [
            {"Sec-WebSocket-Protocol", "chat, superchat"},
            {"Sec-WebSocket-Extensions", "permessage-deflate"}
          ]
        )

      assert TestClient.header(resp, "sec-websocket-protocol") == "chat"
      assert TestClient.header(resp, "x-custom") == "yes"
      assert TestClient.header(resp, "set-cookie") =~ "session=abc"
      # The Plug-set extensions header is not copied; the negotiated one is sent once.
      assert TestClient.header(resp, "sec-websocket-extensions") == "permessage-deflate"

      for name <- ~w(upgrade connection sec-websocket-accept sec-websocket-extensions) do
        assert Enum.count(resp.headers, fn {k, _} -> String.downcase(k) == name end) == 1
      end

      z = inflate_stream()
      :ok = send_frame(s, :text, "ok")
      assert {:text, "ok", _} = next_inflated(s, z)
      close_z(z)
    end

    test "Upgrade and Connection tokens are case-insensitive and may be in a list", ctx do
      %{s: s} =
        up(ctx, "/direct", upgrade: "WebSocket", connection: "keep-alive, UPGRADE")

      :ok = send_frame(s, :text, "tokens")
      assert {:text, "tokens", _} = next(s)
    end

    test "a first frame in the same packet as the upgrade request is read", ctx do
      first = frame(:text, "pipelined") <> frame(:binary, "second")
      %{s: s, rest: rest} = up(ctx, "/ws", pipelined: [first])
      assert {:text, "pipelined", rest} = next(s, rest)
      assert {:binary, "second", _} = next(s, rest)
    end

    test "init/1 may push the first frame", ctx do
      %{s: s, rest: rest} = up(ctx, "/init_push")
      assert {:text, "hello from init", _} = next(s, rest)
    end

    test "invalid upgrades through the adapter get 400 or 426 and a close", ctx do
      bad = [
        {[method: "POST"], 400, ~r/method POST/},
        {[drop: ["Upgrade"]], 400, ~r/'upgrade' header/},
        {[upgrade: "h2c"], 400, ~r/'upgrade' header/},
        {[connection: "keep-alive"], 400, ~r/'connection' header/},
        {[drop: ["Sec-WebSocket-Key"]], 400, ~r/sec-websocket-key/},
        {[key: "not base64!"], 400, ~r/16 octets/},
        {[key: Base.encode64("short")], 400, ~r/16 octets/},
        {[version: "HTTP/1.0"], 400, ~r/HTTP\/1.1/},
        {[ws_version: "8"], 426, ~r/must be 13/},
        {[drop: ["Sec-WebSocket-Version"]], 426, ~r/must be 13/}
      ]

      for {opts, status, body} <- bad do
        {_key, req} = upgrade_request("/direct", opts)
        s = open(ctx.port, ctx.scheme)
        :ok = TestClient.send_bytes(s, req)
        {:ok, r, ""} = TestClient.read(s)
        assert r.status == status, inspect(opts)
        assert r.body =~ body
        assert TestClient.header(r, "connection") == "close"

        if status == 426,
          do: assert(TestClient.header(r, "sec-websocket-version") == "13")

        assert closed?(s)
        refute_received {:init, _}
      end
    end

    test "an upgrade with an unread streamed body gets 400 and a close", ctx do
      {_key, cl} = upgrade_request("/ws", headers: [{"Content-Length", "100000"}])
      {_key, te} = upgrade_request("/ws", headers: [{"Transfer-Encoding", "chunked"}])

      for req <- [cl <> :binary.copy("x", 10), te <> "10000\r\n" <> :binary.copy("y", 20_000)] do
        s = open(ctx.port, ctx.scheme)
        :ok = TestClient.send_bytes(s, req)
        {:ok, r, _} = TestClient.read(s)
        assert r.status == 400
        assert TestClient.header(r, "connection") == "close"
        assert closed?(s)
        refute_received {:init, _}
      end
    end

    test "an upgrade with a fully-buffered unread body gets 400 and a close", ctx do
      {_key, req} = upgrade_request("/ws", headers: [{"Content-Length", "5"}])
      s = open(ctx.port, ctx.scheme)
      :ok = TestClient.send_bytes(s, req <> "hello")
      {:ok, r, _} = TestClient.read(s)
      assert r.status == 400
      assert TestClient.header(r, "connection") == "close"
      assert closed?(s)
      refute_received {:init, _}
    end

    test "an empty or already-read body still upgrades", ctx do
      {_key, empty} = upgrade_request("/ws", headers: [{"Content-Length", "0"}])
      s = open(ctx.port, ctx.scheme)
      :ok = TestClient.send_bytes(s, empty)
      {:ok, resp, _} = TestClient.read(s)
      assert resp.status == 101
      assert_receive {:init, _}
      TestClient.close(s)

      {_key, req} = upgrade_request("/read_body", headers: [{"Content-Length", "5"}])
      s = open(ctx.port, ctx.scheme)
      :ok = TestClient.send_bytes(s, req <> "hello")
      {:ok, resp, _} = TestClient.read(s)
      assert resp.status == 101
      assert_receive {:init, _}
      :ok = send_frame(s, :text, "after")
      assert {:text, "after", _} = next(s)
    end

    test "WebSockAdapter's own validation failure is a 400", ctx do
      {_key, req} = upgrade_request("/ws", drop: ["Sec-WebSocket-Key"])
      r = raw(ctx, req)
      assert r.status == 400
      assert r.body =~ "sec-websocket-key"
    end

    test "plain requests on the same server are unaffected", ctx do
      r = raw(ctx, "GET /hello HTTP/1.1\r\nHost: h\r\n\r\n")
      assert {r.status, r.body} == {200, "plain /hello"}
    end
  end

  describe "framing" do
    test "16-bit and 64-bit payload lengths both ways", ctx do
      %{s: s} = up(ctx)

      for size <- [125, 126, 65_535, 65_536, 300_000] do
        data = :crypto.strong_rand_bytes(size)
        :ok = send_frame(s, :binary, data)
        {:ok, {true, :binary, ^data, len7, 0}, <<>>} = recv(s, <<>>, 5_000)

        assert len7 == len_code(size)
      end
    end

    test "a fragmented message with pings between fragments", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "frag", fin: false)
      :ok = send_frame(s, :ping, "mid")
      assert {:pong, "mid", _} = next(s)
      :ok = send_frame(s, :continuation, "men", fin: false)
      :ok = send_frame(s, :pong, "unsolicited")
      :ok = send_frame(s, :continuation, "ted ✓")
      assert {:text, "fragmented ✓", _} = next(s)
      assert_receive {:control, :ping, "mid"}
      assert_receive {:control, :pong, "unsolicited"}

      # A binary message across fragments, all in one packet.
      :ok =
        TestClient.send_bytes(s, [
          frame(:binary, <<1, 2>>, fin: false),
          frame(:continuation, <<>>, fin: false),
          frame(:continuation, <<3>>)
        ])

      assert {:binary, <<1, 2, 3>>, _} = next(s)
    end

    test "2000 one-byte fragments reassemble exactly, with bounded state", ctx do
      %{s: s, pid: pid} = up(ctx)
      bytes = for i <- 1..2000, do: <<rem(i, 251)>>
      [first | rest] = bytes
      {middle, [last]} = Enum.split(rest, -1)

      # 1500 fragments, then look at the connection's state mid-message.
      {before, later} = Enum.split(middle, 1499)

      :ok =
        TestClient.send_bytes(s, [
          frame(:binary, first, fin: false)
          | Enum.map(before, &frame(:continuation, &1, fin: false))
        ])

      :ok = send_frame(s, :ping, "sync")
      assert {:pong, "sync", _} = next(s)

      {_socket, %MuroPlug.WebSocket.Connection{frag: {:binary, chunks, n, size, false}}} =
        :sys.get_state(pid)

      assert size == 1500
      assert n == length(chunks) and n <= 1024
      assert Enum.all?(chunks, &is_binary/1)

      :ok =
        TestClient.send_bytes(s, [
          Enum.map(later, &frame(:continuation, &1, fin: false)),
          frame(:continuation, last)
        ])

      expected = IO.iodata_to_binary(bytes)
      {:ok, {true, :binary, got, _, 0}, _} = recv(s, <<>>, 5_000)
      assert is_binary(got) and got == expected
      assert_receive {:in, :binary, 2000}
    end

    test "frames sent one byte at a time", ctx do
      %{s: s} = up(ctx)

      for <<b <- frame(:text, String.duplicate("drip", 40))>> do
        :ok = TestClient.send_bytes(s, <<b>>)
      end

      assert {:text, "drip" <> _ = text, _} = next(s)
      assert byte_size(text) == 160
    end

    test "ping without handle_control is still answered with a pong", ctx do
      %{s: s} = up(ctx, "/minimal")
      :ok = send_frame(s, :ping, "pp")
      assert {:pong, "pp", _} = next(s)
      :ok = send_frame(s, :ping, "")
      assert {:pong, "", _} = next(s)
      :ok = send_frame(s, :text, "still up")
      assert {:text, "still up", _} = next(s)
    end

    test "validate_utf8: false passes text through", ctx do
      %{s: s} = up(ctx, "/nocheck")
      :ok = send_frame(s, :text, <<0xFF, 0xFE>>)
      assert {:text, <<0xFF, 0xFE>>, _} = next(s)
    end
  end

  describe "protocol errors" do
    for {name, bytes, code} <- [
          {"an unmasked client frame", quote(do: frame(:text, "x", mask: false)), 1002},
          {"a reserved RSV bit", quote(do: frame(:text, "x", rsv: 4)), 1002},
          {"a reserved data opcode", quote(do: frame(3, "x")), 1002},
          {"a reserved control opcode", quote(do: frame(0xB, "x")), 1002},
          {"a fragmented ping", quote(do: frame(:ping, "x", fin: false)), 1002},
          {"a 126-byte ping", quote(do: frame(:ping, String.duplicate("x", 126))), 1002},
          {"a continuation with no message", quote(do: frame(:continuation, "x")), 1002},
          {"a text frame inside a fragmented message",
           quote(do: frame(:text, "a", fin: false) <> frame(:text, "b")), 1002},
          {"a 16-bit length under 126 (non-minimal)",
           quote(do: <<0x82, 0xFE, 5::16, 0::32, "hello">>), 1002},
          {"a 64-bit length under 65536 (non-minimal)",
           quote(do: <<0x82, 0xFF, 200::64, 0::32, :binary.copy("a", 200)::binary>>), 1002},
          {"a 64-bit length with the high bit set", quote(do: <<0x82, 0xFF, 1::1, 0::63, 0::32>>),
           1002},
          {"invalid UTF-8 text", quote(do: frame(:text, <<0xC3, 0x28>>)), 1007},
          {"invalid UTF-8 across fragments",
           quote(
             do:
               frame(:text, <<0xE2, 0x82>>, fin: false) <>
                 frame(:continuation, <<0x28>>)
           ), 1007},
          {"a 1-byte close", quote(do: frame(:close, <<3>>)), 1002},
          {"close code 999", quote(do: frame(:close, <<999::16>>)), 1002},
          {"close code 1004", quote(do: frame(:close, <<1004::16>>)), 1002},
          {"close code 1005", quote(do: frame(:close, <<1005::16>>)), 1002},
          {"close code 1006", quote(do: frame(:close, <<1006::16>>)), 1002},
          {"close code 1015", quote(do: frame(:close, <<1015::16>>)), 1002},
          {"close code 2000", quote(do: frame(:close, <<2000::16>>)), 1002},
          {"close code 5000", quote(do: frame(:close, <<5000::16>>)), 1002},
          {"a close reason that is not UTF-8", quote(do: frame(:close, <<1000::16, 0xFF>>)), 1007}
        ] do
      test "#{name} closes with #{code}", ctx do
        %{s: s} = up(ctx)
        :ok = TestClient.send_bytes(s, unquote(bytes))
        assert {unquote(code), ""} = close_frame(s)
        assert closed?(s)
        assert_receive {:terminate, {:error, _}}
      end
    end

    test "a frame past max_frame_size closes with 1009 before its payload arrives", ctx do
      %{s: s} = up(ctx, "/small")
      :ok = send_frame(s, :binary, String.duplicate("x", 1_000))
      assert {:binary, _, _} = next(s)
      # Only the header of a 1001-byte frame: the server need not wait.
      <<head::binary-size(8), _::binary>> = frame(:binary, String.duplicate("y", 1_001))
      :ok = TestClient.send_bytes(s, head)
      assert {1009, ""} = close_frame(s)
      assert closed?(s)
      assert_receive {:terminate, {:error, :frame_too_big}}
    end

    test "a fragmented message past max_message_size closes with 1009", ctx do
      %{s: s} = up(ctx, "/small")
      chunk = String.duplicate("z", 1_000)
      :ok = send_frame(s, :text, chunk, fin: false)
      :ok = send_frame(s, :continuation, chunk, fin: false)
      :ok = send_frame(s, :continuation, "!", fin: false)
      assert {1009, ""} = close_frame(s)
      assert closed?(s)
      assert_receive {:terminate, {:error, :message_too_big}}
    end
  end

  describe "WebSock lifecycle" do
    test "handle_info pushes frames from other processes", ctx do
      %{s: s, pid: pid} = up(ctx)
      send(pid, {:push, {:text, "from server"}})
      assert {:text, "from server", _} = next(s)
      send(pid, {:push, [{:binary, "one"}, {:text, "two"}]})
      {:binary, "one", rest} = next(s)
      assert {:text, "two", _} = next(s, rest)
    end

    test "{:push, list} and {:reply, ...}", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "reply")
      assert {:text, "replied", _} = next(s)
      :ok = send_frame(s, :text, "many")
      {:text, "a", rest} = next(s)
      {:binary, <<0, 255>>, rest} = next(s, rest)
      {:ping, "p", rest} = next(s, rest)
      assert {:text, "", _} = next(s, rest)
    end

    test "the client's close is echoed and terminate gets :remote", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :close, <<1000::16, "done">>)
      assert {1000, ""} = close_frame(s)
      assert closed?(s)
      assert_receive {:terminate, :remote}
    end

    test "an empty client close is echoed empty", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :close, <<>>)
      assert {nil, ""} = close_frame(s)
      assert closed?(s)
      assert_receive {:terminate, :remote}
    end

    test "{:stop, :normal} from handle_in closes with 1000", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "stop")
      assert {1000, ""} = close_frame(s)
      assert_receive {:terminate, :normal}
      answer_close(s)
    end

    test "a stop with frames, a code, and a reason", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "close_custom")
      {:text, "last", rest} = next(s)
      assert {4000, "bye"} = close_frame(s, rest)
      assert_receive {:terminate, :normal}
      answer_close(s)

      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "close_code")
      assert {4001, ""} = close_frame(s)
      answer_close(s)
    end

    test "{:stop, :normal} from handle_info closes with 1000 and a detail", ctx do
      %{s: s, pid: pid} = up(ctx)
      send(pid, {:stop, {4002, "server bye"}})
      assert {4002, "server bye"} = close_frame(s)
      assert_receive {:terminate, :normal}
      answer_close(s)

      %{s: s, pid: pid} = up(ctx)
      send(pid, :stop)
      assert {1000, ""} = close_frame(s)
      answer_close(s)
    end

    test "a client that never answers the server's close is dropped after close_timeout",
         ctx do
      %{s: s} = up(ctx, "/close_fast")
      :ok = send_frame(s, :text, "stop")
      t0 = System.monotonic_time(:millisecond)
      assert {1000, ""} = close_frame(s)
      assert {:error, :closed} = TestClient.recv(s, 0, 1_000)
      # close_timeout is 50 ms; the default (5 s) would fail the 1 s recv.
      assert System.monotonic_time(:millisecond) - t0 < 1_000
      assert_receive {:terminate, :normal}
    end

    test "after the server's close, frames are ignored until the client's close", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "stop")
      assert {1000, ""} = close_frame(s)
      :ok = send_frame(s, :text, "ignored")
      :ok = send_frame(s, :close, <<1000::16>>)
      assert {:error, :closed, <<>>} = recv(s)
    end

    test "an error stop closes with 1011, a restart with 1012", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "boom")
      assert {1011, ""} = close_frame(s)
      assert_receive {:terminate, {:error, :boom}}
      answer_close(s)

      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "restart")
      assert {1012, ""} = close_frame(s)
      assert_receive {:terminate, {:error, {:shutdown, :restart}}}
      answer_close(s)
    end

    test "an idle connection closes with 1001 and terminate(:timeout)", ctx do
      %{s: s} = up(ctx, "/idle")
      :ok = send_frame(s, :text, "keep")
      assert {:text, "keep", _} = next(s)
      assert {1001, ""} = close_frame(s)
      assert closed?(s)
      assert_receive {:terminate, :timeout}
    end

    test "a dropped socket calls terminate({:error, :closed})", ctx do
      %{s: s} = up(ctx)
      TestClient.close(s)
      assert_receive {:terminate, {:error, :closed}}
    end

    test "a server shutdown closes with 1001 and terminate(:shutdown)", ctx do
      tls = if ctx.scheme == :wss, do: TestCert.server_options(), else: []

      {:ok, server} =
        MuroPlug.start_link(Keyword.merge([plug: {App, test: self()}, port: 0], tls))

      Process.unlink(server)
      {:ok, {_, port}} = ThousandIsland.listener_info(server)
      {s, _resp, _} = connect(port, ctx.scheme)
      assert_receive {:init, _}
      :ok = ThousandIsland.stop(server, 2_000)
      assert {1001, ""} = close_frame(s)
      assert_receive {:terminate, :shutdown}
    end
  end

  describe "permessage-deflate" do
    @offer [{"Sec-WebSocket-Extensions", "permessage-deflate"}]

    defp deflate_up(ctx, path \\ "/deflate", opts \\ []) do
      headers = Keyword.get(opts, :headers, @offer)
      up(ctx, path, Keyword.put(opts, :headers, headers))
    end

    defp ext(resp), do: TestClient.header(resp, "sec-websocket-extensions")

    test "compress: true negotiates a bare permessage-deflate offer", ctx do
      %{s: s, resp: resp} = deflate_up(ctx)
      assert ext(resp) == "permessage-deflate"
      z_in = inflate_stream()
      z_out = deflate_stream()
      :ok = send_compressed(s, :text, "héllo", z_out)
      assert {:text, "héllo", _} = next_inflated(s, z_in)
      :ok = send_compressed(s, :binary, <<0, 1, 255>>, z_out)
      assert {:binary, <<0, 1, 255>>, _} = next_inflated(s, z_in)
      close_z(z_in)
      close_z(z_out)
    end

    test "compress: false never advertises Sec-WebSocket-Extensions", ctx do
      %{s: s, resp: resp} = up(ctx, "/ws", headers: @offer)
      refute ext(resp)
      :ok = send_frame(s, :text, "plain")
      assert {:text, "plain", _} = next(s)
    end

    test "no offer means no extension even when compress is true", ctx do
      %{s: s, resp: resp} = up(ctx, "/deflate")
      refute ext(resp)
      :ok = send_frame(s, :text, "plain")
      assert {:text, "plain", _} = next(s)
    end

    test "client_no_context_takeover and server_no_context_takeover are echoed", ctx do
      %{s: s, resp: resp} =
        deflate_up(ctx, "/deflate",
          headers: [
            {"Sec-WebSocket-Extensions",
             "permessage-deflate; client_no_context_takeover; server_no_context_takeover"}
          ]
        )

      assert ext(resp) ==
               "permessage-deflate; server_no_context_takeover; client_no_context_takeover"

      z_in = inflate_stream()
      z_out = deflate_stream()

      for msg <- ["alpha", "alpha", "beta"] do
        :ok = send_compressed(s, :text, msg, z_out, reset: true)
        assert {:text, ^msg, _} = next_inflated(s, z_in, <<>>, reset: true)
      end

      close_z(z_in)
      close_z(z_out)
    end

    test "context takeover keeps the inflate/deflate dictionary across messages", ctx do
      %{s: s, resp: resp} = deflate_up(ctx)
      assert ext(resp) == "permessage-deflate"
      z_in = inflate_stream()
      z_out = deflate_stream()
      payload = String.duplicate("the same dictionary phrase ", 20)
      :ok = send_compressed(s, :text, payload, z_out)
      assert {:text, ^payload, _} = next_inflated(s, z_in)
      :ok = send_compressed(s, :text, payload, z_out)
      assert {:text, ^payload, _} = next_inflated(s, z_in)
      close_z(z_in)
      close_z(z_out)
    end

    test "client_max_window_bits and server_max_window_bits are negotiated", ctx do
      %{s: s, resp: resp} =
        deflate_up(ctx, "/deflate",
          headers: [
            {"Sec-WebSocket-Extensions",
             "permessage-deflate; client_max_window_bits=12; server_max_window_bits=10"}
          ]
        )

      assert ext(resp) ==
               "permessage-deflate; server_max_window_bits=10; client_max_window_bits=12"

      z_in = inflate_stream(10)
      z_out = deflate_stream(12)
      :ok = send_compressed(s, :text, "windows", z_out)
      assert {:text, "windows", _} = next_inflated(s, z_in)
      close_z(z_in)
      close_z(z_out)
    end

    test "a valueless client_max_window_bits is accepted", ctx do
      %{resp: resp} =
        deflate_up(ctx, "/deflate",
          headers: [{"Sec-WebSocket-Extensions", "permessage-deflate; client_max_window_bits"}]
        )

      assert ext(resp) == "permessage-deflate; client_max_window_bits=15"
    end

    test "the first invalid offer is skipped in favour of a later valid one", ctx do
      %{s: s, resp: resp} =
        deflate_up(ctx, "/deflate",
          headers: [
            {"Sec-WebSocket-Extensions",
             "permessage-deflate; foo=1, permessage-deflate; client_no_context_takeover"}
          ]
        )

      assert ext(resp) == "permessage-deflate; client_no_context_takeover"
      z_in = inflate_stream()
      z_out = deflate_stream()
      :ok = send_compressed(s, :binary, <<1, 2, 3>>, z_out, reset: true)
      assert {:binary, <<1, 2, 3>>, _} = next_inflated(s, z_in, <<>>, reset: true)
      close_z(z_in)
      close_z(z_out)
    end

    test "an unknown extension is ignored and a later offer can still match", ctx do
      %{resp: resp} =
        deflate_up(ctx, "/deflate",
          headers: [
            {"Sec-WebSocket-Extensions", "mux"},
            {"Sec-WebSocket-Extensions", "permessage-deflate"}
          ]
        )

      assert ext(resp) == "permessage-deflate"
    end

    test "if no offer is acceptable the upgrade proceeds uncompressed", ctx do
      %{s: s, resp: resp} =
        deflate_up(ctx, "/deflate",
          headers: [
            {"Sec-WebSocket-Extensions",
             "permessage-deflate; server_max_window_bits=99, permessage-deflate; unknown=1"}
          ]
        )

      refute ext(resp)
      :ok = send_frame(s, :text, "fallback")
      assert {:text, "fallback", _} = next(s)
    end

    test "an uncompressed message is accepted alongside a negotiated extension", ctx do
      %{s: s, resp: resp} = deflate_up(ctx)
      assert ext(resp)
      z_in = inflate_stream()
      :ok = send_frame(s, :text, "not compressed")
      assert {:text, "not compressed", _} = next_inflated(s, z_in)
      close_z(z_in)
    end

    test "a large compressible payload round-trips", ctx do
      %{s: s} = deflate_up(ctx)
      payload = String.duplicate("0123456789abcdef", 8_000)
      z_in = inflate_stream()
      z_out = deflate_stream()
      :ok = send_compressed(s, :text, payload, z_out)
      assert {:text, ^payload, _} = next_inflated(s, z_in, <<>>)
      assert_receive {:in, :text, size} when size == byte_size(payload)
      close_z(z_in)
      close_z(z_out)
    end

    test "a fragmented compressed message with an interleaved ping", ctx do
      %{s: s} = deflate_up(ctx)
      z_in = inflate_stream()
      z_out = deflate_stream()
      payload = String.duplicate("frag-me", 50)
      compressed = deflate(z_out, payload)
      mid = div(byte_size(compressed), 2)
      <<first::binary-size(^mid), rest::binary>> = compressed
      :ok = send_frame(s, :text, first, fin: false, rsv: 4)
      :ok = send_frame(s, :ping, "mid")
      assert {:pong, "mid", _} = next(s)
      :ok = send_frame(s, :continuation, rest)
      assert {:text, ^payload, _} = next_inflated(s, z_in)
      close_z(z_in)
      close_z(z_out)
    end

    test "RSV1 without negotiation closes with 1002", ctx do
      %{s: s} = up(ctx)
      :ok = send_frame(s, :text, "x", rsv: 4)
      assert {1002, ""} = close_frame(s)
      assert_receive {:terminate, {:error, :reserved_bits}}
    end

    test "RSV1 on a continuation closes with 1002", ctx do
      %{s: s} = deflate_up(ctx)
      z_out = deflate_stream()
      data = deflate(z_out, "hello there")
      <<a::binary-size(2), b::binary>> = data
      :ok = send_frame(s, :text, a, fin: false, rsv: 4)
      :ok = send_frame(s, :continuation, b, rsv: 4)
      assert {1002, ""} = close_frame(s)
      assert_receive {:terminate, {:error, :reserved_bits}}
      close_z(z_out)
    end

    test "RSV1 on a ping or close closes with 1002", ctx do
      %{s: s} = deflate_up(ctx)
      :ok = send_frame(s, :ping, "x", rsv: 4)
      assert {1002, ""} = close_frame(s)
      assert_receive {:terminate, {:error, :reserved_bits}}

      %{s: s} = deflate_up(ctx)
      :ok = send_frame(s, :close, <<1000::16>>, rsv: 4)
      assert {1002, ""} = close_frame(s)
      assert_receive {:terminate, {:error, :reserved_bits}}
    end

    test "a zip bomb past max_message_size closes with 1009", ctx do
      %{s: s} = deflate_up(ctx, "/deflate_small")
      z_out = deflate_stream()
      :ok = send_compressed(s, :binary, :binary.copy(<<0>>, 100_000), z_out)
      assert {1009, ""} = close_frame(s)
      assert_receive {:terminate, {:error, :message_too_big}}
      close_z(z_out)
    end

    test "a corrupt deflate payload closes with 1007", ctx do
      %{s: s} = deflate_up(ctx)
      :ok = send_frame(s, :text, :crypto.strong_rand_bytes(32), rsv: 4)
      assert {1007, ""} = close_frame(s)
      assert_receive {:terminate, {:error, :bad_deflate}}
    end

    test "invalid UTF-8 after inflate closes with 1007", ctx do
      %{s: s} = deflate_up(ctx)
      z_out = deflate_stream()
      :ok = send_compressed(s, :text, <<0xFF, 0xFE>>, z_out)
      assert {1007, ""} = close_frame(s)
      assert_receive {:terminate, {:error, :invalid_utf8}}
      close_z(z_out)
    end
  end

  defp len_code(size) when size < 126, do: size
  defp len_code(size) when size < 65_536, do: 126
  defp len_code(_size), do: 127

  defp answer_close_after_client(s) do
    :ok = send_frame(s, :close, <<1000::16>>)
    assert {1000, ""} = close_frame(s)
    assert closed?(s)
  end
end

defmodule MuroPlug.WebSocketLogTest do
  # Tests that log errors run synchronously: capture_log sees every
  # process, so an async test would leak these logs into others.
  use ExUnit.Case,
    async: false,
    parameterize: [%{scheme: :ws}, %{scheme: :wss}]

  import ExUnit.CaptureLog
  import MuroPlug.WSClient

  alias MuroPlug.{TestCert, TestClient}

  setup %{scheme: scheme} do
    tls = if scheme == :wss, do: TestCert.server_options(), else: []
    port = TestClient.serve({MuroPlug.WebSocketTest.App, test: self()}, tls)
    {:ok, port: port}
  end

  test "a malformed upgrade spec raises, and the book answers 500", ctx do
    {_key, req} = upgrade_request("/bad_spec")

    log =
      capture_log(fn ->
        assert raw(ctx, req).status == 500
      end)

    assert log =~ "{websock_module, state, opts}"
  end

  test "bad WebSocket options raise before the 101", ctx do
    {_key, req} = upgrade_request("/bad_opts")

    log =
      capture_log(fn ->
        assert raw(ctx, req).status == 500
      end)

    assert log =~ ":close_timeout must be a non-negative integer"
    refute_received {:init, _}
  end

  test "an invalid max_heap_size map raises before the 101", ctx do
    {_key, req} = upgrade_request("/bad_heap")

    log =
      capture_log(fn ->
        assert raw(ctx, req).status == 500
      end)

    assert log =~ ":max_heap_size must be"
    refute_received {:init, _}
  end

  test "a raising handle_in closes with 1011 and logs", ctx do
    {s, _resp, <<>>} = connect(ctx.port, ctx.scheme)
    assert_receive {:init, _}

    log =
      capture_log(fn ->
        :ok = send_frame(s, :text, "crash")
        assert {1011, ""} = close_frame(s)
        assert TestClient.closed?(s, 2_000)
      end)

    assert log =~ "crash in handle_in"
    assert_receive {:terminate, {:error, %RuntimeError{}}}
  end

  defp raw(ctx, data) do
    s = open(ctx.port, ctx.scheme)
    :ok = TestClient.send_bytes(s, data)
    {:ok, r, _} = TestClient.read(s)
    TestClient.close(s)
    r
  end
end

defmodule MuroPlug.WebSocketFrameTest do
  use ExUnit.Case, async: true

  alias MuroPlug.WebSocket.Frame

  test "the accept value matches RFC 6455 §1.3's example" do
    assert MuroPlug.WebSocket.accept("dGhlIHNhbXBsZSBub25jZQ==") ==
             "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
  end

  test "encode writes unmasked final frames with minimal lengths" do
    assert IO.iodata_to_binary(Frame.encode(:text, "hi")) == <<0x81, 2, "hi">>

    assert <<0x82, 126, 126::16, _::binary>> =
             IO.iodata_to_binary(Frame.encode(:binary, String.duplicate("a", 126)))

    big = :binary.copy(<<7>>, 65_536)

    assert <<0x82, 127, 65_536::64, ^big::binary>> =
             IO.iodata_to_binary(Frame.encode(:binary, big))

    assert IO.iodata_to_binary(Frame.encode(:close, <<1000::16>>)) == <<0x88, 2, 1000::16>>
    assert IO.iodata_to_binary(Frame.encode(:text, "hi", rsv1: true)) == <<0xC1, 2, "hi">>
  end

  test "parse unmasks, waits for the whole frame, and keeps the tail" do
    f = MuroPlug.WSClient.frame(:text, "hello")
    assert {:ok, {true, :text, "hello", false}, "tail"} = Frame.parse(f <> "tail", 100)

    for n <- 0..(byte_size(f) - 1) do
      assert Frame.parse(binary_part(f, 0, n), 100) == :more
    end
  end

  test "parse returns RSV1 and rejects RSV2/RSV3" do
    f = MuroPlug.WSClient.frame(:text, "x", rsv: 4)
    assert {:ok, {true, :text, "x", true}, <<>>} = Frame.parse(f, 100)

    for rsv <- [1, 2, 3, 5, 6, 7] do
      assert {:error, 1002, :reserved_bits} =
               Frame.parse(MuroPlug.WSClient.frame(:text, "x", rsv: rsv), 100)
    end
  end

  test "unmask is its own inverse for every length mod 4" do
    key = <<1, 2, 3, 4>>

    for n <- [0, 1, 2, 3, 4, 5, 1_000_003] do
      data = :crypto.strong_rand_bytes(n)
      masked = Frame.unmask(data, key)
      assert is_binary(masked) and byte_size(masked) == n
      assert Frame.unmask(masked, key) == data
    end

    # RFC 6455 §5.7: "Hello" masked with 37 fa 21 3d.
    assert Frame.unmask("Hello", <<0x37, 0xFA, 0x21, 0x3D>>) == <<0x7F, 0x9F, 0x4D, 0x51, 0x58>>
  end

  test "WebSocket options are validated" do
    alias MuroPlug.WebSocket.Connection

    assert Connection.validate_opts!([]) == :ok

    assert Connection.validate_opts!(
             timeout: :infinity,
             max_frame_size: 0,
             max_message_size: :infinity,
             close_timeout: 0,
             validate_utf8: false,
             compress: true,
             deflate_options: [level: :best_speed, mem_level: 8, strategy: :default],
             fullsweep_after: nil,
             max_heap_size: %{
               size: 1_000_000,
               kill: true,
               error_logger: false,
               include_shared_binaries: true
             },
             connect_info: [:peer_data]
           ) == :ok

    assert Connection.validate_opts!(compress: [level: 6, mem_level: 1]) == :ok
    assert Connection.validate_opts!(max_heap_size: 0) == :ok

    for bad <- [
          close_timeout: :infinity,
          close_timeout: -1,
          close_timeout: "50",
          timeout: -5,
          timeout: 1.5,
          max_frame_size: :big,
          max_message_size: -1,
          validate_utf8: 1,
          compress: nil,
          compress: :yes,
          fullsweep_after: -1,
          max_heap_size: :lots,
          max_heap_size: %{size: -1},
          max_heap_size: %{foo: 1},
          max_heap_size: %{},
          max_heap_size: %{kill: true},
          max_heap_size: %{size: 1, unknown: true},
          max_heap_size: %{size: 1, kill: :yes}
        ] do
      assert_raise ArgumentError, ~r/WebSocket option/, fn -> Connection.validate_opts!([bad]) end
    end

    assert_raise ArgumentError, ~r/keyword list/, fn -> Connection.validate_opts!(%{}) end
  end

  test "close payloads" do
    assert Frame.close_payload(<<>>, true) == {:ok, nil, <<>>}
    assert Frame.close_payload(<<1000::16, "bye">>, true) == {:ok, 1000, "bye"}
    assert Frame.close_payload(<<4999::16>>, true) == {:ok, 4999, <<>>}
    assert Frame.close_payload(<<1012::16>>, true) == {:ok, 1012, <<>>}
    assert {:error, 1002, _} = Frame.close_payload(<<1>>, true)
    assert {:error, 1002, _} = Frame.close_payload(<<1016::16>>, true)
    assert {:error, 1007, _} = Frame.close_payload(<<1000::16, 0xFF>>, true)
    assert {:ok, 1000, <<0xFF>>} = Frame.close_payload(<<1000::16, 0xFF>>, false)
  end
end

defmodule MuroPlug.PhoenixWebSocketTest do
  # A real Phoenix.Endpoint on MuroPlug.PhoenixAdapter with `http:` and
  # `https:`: a raw Phoenix.Socket.Transport and a Phoenix.Socket with a
  # channel (the transport LiveView uses), over ws:// and wss://.
  use ExUnit.Case, async: false

  import MuroPlug.WSClient

  alias MuroPlug.{TestCert, TestClient}

  @test_key {__MODULE__, :test}

  defmodule RawSocket do
    @behaviour Phoenix.Socket.Transport

    @impl true
    def child_spec(_opts), do: :ignore

    @impl true
    def connect(%{params: params}), do: {:ok, %{name: params["name"] || "anon"}}

    @impl true
    def init(state) do
      send(:persistent_term.get({MuroPlug.PhoenixWebSocketTest, :test}), {:raw_init, self()})
      {:ok, state}
    end

    @impl true
    def handle_in({text, _opts}, state),
      do: {:reply, :ok, {:text, "#{state.name}:#{text}"}, state}

    @impl true
    def handle_info({:push, text}, state), do: {:push, {:text, text}, state}

    @impl true
    def terminate(reason, _state) do
      send(:persistent_term.get({MuroPlug.PhoenixWebSocketTest, :test}), {:raw_terminate, reason})
      :ok
    end
  end

  defmodule RoomChannel do
    use Phoenix.Channel

    def join("room:" <> _, payload, socket),
      do: {:ok, %{joined: true, got: payload, user: socket.assigns.user}, socket}

    def handle_in("ping", payload, socket), do: {:reply, {:ok, %{pong: payload}}, socket}
  end

  defmodule UserSocket do
    use Phoenix.Socket

    channel("room:*", RoomChannel)

    @impl true
    def connect(params, socket, _info), do: {:ok, assign(socket, :user, params["user"])}

    @impl true
    def id(_socket), do: nil
  end

  defmodule Endpoint do
    use Phoenix.Endpoint, otp_app: :muro_plug

    socket("/raw", RawSocket, websocket: true)
    socket("/socket", UserSocket, websocket: true)
    socket("/raw_c", RawSocket, websocket: [compress: true])
    socket("/socket_c", UserSocket, websocket: [compress: true])
  end

  setup_all do
    json = Application.get_env(:phoenix, :json_library)
    Application.put_env(:phoenix, :json_library, JSON)
    :persistent_term.put(@test_key, self())

    on_exit(fn ->
      if json,
        do: Application.put_env(:phoenix, :json_library, json),
        else: Application.delete_env(:phoenix, :json_library)
    end)

    Application.put_env(:muro_plug, Endpoint,
      adapter: MuroPlug.PhoenixAdapter,
      server: true,
      http: [ip: {127, 0, 0, 1}, port: 0],
      https: [
        ip: {127, 0, 0, 1},
        port: 0,
        certfile: TestCert.certfile(),
        keyfile: TestCert.keyfile()
      ],
      secret_key_base: String.duplicate("a", 64),
      pubsub_server: MuroPlug.PhoenixWebSocketTest.PubSub,
      url: [host: "localhost"]
    )

    start_supervised!({Phoenix.PubSub, name: MuroPlug.PhoenixWebSocketTest.PubSub})
    start_supervised!(Endpoint)
    {:ok, {_, http}} = Endpoint.server_info(:http)
    {:ok, {_, https}} = Endpoint.server_info(:https)
    {:ok, ports: %{ws: http, wss: https}}
  end

  setup do
    :persistent_term.put(@test_key, self())
    :ok
  end

  test "a Phoenix.Socket.Transport upgrades and exchanges messages", %{ports: ports} do
    for scheme <- [:ws, :wss] do
      {s, _resp, <<>>} = connect(ports[scheme], scheme, "/raw/websocket?name=ric")
      assert_receive {:raw_init, pid}
      :ok = send_frame(s, :text, "hi #{scheme}")
      expected = "ric:hi #{scheme}"
      assert {:text, ^expected, _} = next(s)
      send(pid, {:push, "pushed"})
      assert {:text, "pushed", _} = next(s)
      :ok = send_frame(s, :close, <<1000::16>>)
      assert {1000, ""} = close_frame(s)
      assert TestClient.closed?(s)
      assert_receive {:raw_terminate, :remote}
    end
  end

  test "a Phoenix.Socket joins a channel, replies, and gets broadcasts", %{ports: ports} do
    for scheme <- [:ws, :wss] do
      {s, _resp, <<>>} = connect(ports[scheme], scheme, "/socket/websocket?vsn=2.0.0&user=ric")
      topic = "room:#{scheme}"

      :ok = send_frame(s, :text, JSON.encode!(["1", "1", topic, "phx_join", %{"hello" => "w"}]))
      {:text, reply, _} = next(s)

      assert ["1", "1", ^topic, "phx_reply", %{"status" => "ok", "response" => response}] =
               JSON.decode!(reply)

      assert response == %{"joined" => true, "got" => %{"hello" => "w"}, "user" => "ric"}

      :ok = send_frame(s, :text, JSON.encode!(["1", "2", topic, "ping", %{"n" => 1}]))
      {:text, reply, _} = next(s)

      assert [
               "1",
               "2",
               ^topic,
               "phx_reply",
               %{"status" => "ok", "response" => %{"pong" => %{"n" => 1}}}
             ] =
               JSON.decode!(reply)

      Endpoint.broadcast!(topic, "news", %{"big" => String.duplicate("x", 70_000)})
      {:text, push, _} = next(s)
      assert [nil, nil, ^topic, "news", %{"big" => big}] = JSON.decode!(push)
      assert byte_size(big) == 70_000

      :ok = send_frame(s, :text, JSON.encode!([nil, "3", "phoenix", "heartbeat", %{}]))
      {:text, reply, _} = next(s)
      assert [nil, "3", "phoenix", "phx_reply", %{"status" => "ok"}] = JSON.decode!(reply)

      :ok = send_frame(s, :close, <<1000::16>>)
      assert {1000, ""} = close_frame(s)
      assert TestClient.closed?(s)
    end
  end

  test "a Phoenix.Socket channel works with compress: true over ws:// and wss://", %{
    ports: ports
  } do
    offer = [{"Sec-WebSocket-Extensions", "permessage-deflate"}]

    for scheme <- [:ws, :wss] do
      {s, resp, <<>>} =
        connect(ports[scheme], scheme, "/socket_c/websocket?vsn=2.0.0&user=ric", headers: offer)

      assert TestClient.header(resp, "sec-websocket-extensions") == "permessage-deflate"
      z = inflate_stream()
      topic = "room:#{scheme}-c"

      :ok = send_frame(s, :text, JSON.encode!(["1", "1", topic, "phx_join", %{"hello" => "w"}]))
      {:text, reply, _} = next_inflated(s, z)

      assert ["1", "1", ^topic, "phx_reply", %{"status" => "ok", "response" => response}] =
               JSON.decode!(reply)

      assert response == %{"joined" => true, "got" => %{"hello" => "w"}, "user" => "ric"}

      :ok = send_frame(s, :text, JSON.encode!(["1", "2", topic, "ping", %{"n" => 1}]))
      {:text, reply, _} = next_inflated(s, z)

      assert [
               "1",
               "2",
               ^topic,
               "phx_reply",
               %{"status" => "ok", "response" => %{"pong" => %{"n" => 1}}}
             ] = JSON.decode!(reply)

      Endpoint.broadcast!(topic, "news", %{"big" => String.duplicate("x", 70_000)})
      {:text, push, _} = next_inflated(s, z)
      assert [nil, nil, ^topic, "news", %{"big" => big}] = JSON.decode!(push)
      assert byte_size(big) == 70_000

      {s2, resp2, <<>>} =
        connect(ports[scheme], scheme, "/raw_c/websocket?name=ric", headers: offer)

      assert TestClient.header(resp2, "sec-websocket-extensions")
      assert_receive {:raw_init, _pid}
      :ok = send_frame(s2, :text, "hi #{scheme}")
      expected = "ric:hi #{scheme}"
      z2 = inflate_stream()
      assert {:text, ^expected, _} = next_inflated(s2, z2)
      close_z(z2)
      :ok = send_frame(s2, :close, <<1000::16>>)
      assert {1000, ""} = close_frame(s2)

      :ok = send_frame(s, :close, <<1000::16>>)
      assert {1000, ""} = close_frame(s)
      close_z(z)
    end
  end

  test "a socket path without an upgrade gets Phoenix's 400", %{ports: ports} do
    r = TestClient.request(ports.ws, "GET /socket/websocket HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert r.status == 400

    r =
      TestClient.request_tls(ports.wss, "GET /raw/websocket HTTP/1.1\r\nHost: localhost\r\n\r\n")

    assert r.status == 400
  end
end
