defmodule MuroPlug.HTTPTest do
  use ExUnit.Case, async: true

  import MuroPlug.TestClient

  defmodule App do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, _opts), do: route(conn.method, conn.path_info, conn)

    defp route(_, ["echo"], conn) do
      {:ok, body, conn} = read_all(conn, [])
      true = is_binary(body)

      conn
      |> put_resp_header("x-method", conn.method)
      |> put_resp_header("x-query", conn.query_string)
      |> send_resp(200, ["body=", body])
    end

    defp route(_, ["size"], conn) do
      {:ok, body, conn} = read_all(conn, [])
      send_resp(conn, 200, Integer.to_string(byte_size(body)) <> ":" <> hash(body))
    end

    defp route("GET", ["stream"], conn) do
      conn = send_chunked(conn, 200)
      {:ok, conn} = chunk(conn, "one,")
      {:ok, conn} = chunk(conn, "")
      {:ok, conn} = chunk(conn, ["two", ?,, "three"])
      conn
    end

    defp route("GET", ["file"], conn), do: send_file(conn, 200, __ENV__.file, 0, 100)
    defp route("GET", ["file_off"], conn), do: send_file(conn, 200, __ENV__.file, 10, :all)

    defp route("GET", ["file_past"], conn),
      do: send_file(conn, 200, __ENV__.file, 10_000_000, :all)

    defp route("GET", ["file_broken"], conn) do
      {MuroPlug.Adapter, p} = conn.adapter
      :gen_tcp.close(p.socket.socket)
      conn = send_file(conn, 200, __ENV__.file, 0, :all)
      {MuroPlug.Adapter, p} = conn.adapter
      send(conn.private[:test] || self(), {:file_broken, p.resp})
      conn
    end

    defp route("GET", ["teapot_keep"], conn) do
      conn |> put_resp_header("connection", "keep-alive") |> send_resp(418, "tea")
    end

    defp route("POST", ["ignore"], conn), do: send_resp(conn, 200, "did not read")

    defp route("GET", ["close"], conn),
      do: conn |> put_resp_header("connection", "close") |> send_resp(200, "bye")

    defp route("GET", ["teapot"], conn), do: send_resp(conn, 418, "short and stout")
    defp route("GET", ["nobody"], conn), do: send_resp(conn, 204, "")
    defp route("GET", ["raise"], _conn), do: raise("boom")
    defp route("GET", ["unsent"], conn), do: conn
    defp route("GET", ["set"], conn), do: resp(conn, 201, "set only")

    defp route("GET", ["hints"], conn) do
      conn
      |> inform!(103, [{"link", "</a.css>; rel=preload"}])
      |> send_resp(200, "after hints")
    end

    defp route("GET", ["upgrade"], conn) do
      result = conn.adapter |> elem(0) |> apply(:upgrade, [elem(conn.adapter, 1), :websocket, []])
      send_resp(conn, 200, inspect(result))
    end

    defp route("GET", ["peer"], conn) do
      %{address: addr} = get_peer_data(conn)

      send_resp(
        conn,
        200,
        "#{:inet.ntoa(addr)} #{get_http_protocol(conn)} #{conn.host}:#{conn.port}"
      )
    end

    defp route(_, _, conn), do: send_resp(conn, 200, "#{conn.method} #{conn.request_path}")

    defp read_all(conn, acc) do
      case read_body(conn, length: 10_000) do
        {:ok, data, conn} -> {:ok, IO.iodata_to_binary([acc, data]), conn}
        {:more, data, conn} -> read_all(conn, [acc, data])
      end
    end

    defp hash(b),
      do: :crypto.hash(:sha256, b) |> Base.encode16(case: :lower) |> binary_part(0, 16)
  end

  setup do
    {:ok, port: serve(App, max_body: 1_000_000)}
  end

  test "GET with a query", %{port: port} do
    r = request(port, "GET /echo?a=1&b=2 HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert r.status == 200
    assert r.body == "body="
    assert header(r, "x-query") == "a=1&b=2"
    assert header(r, "content-length") == "5"
    assert header(r, "date") =~ ~r/GMT$/
    refute header(r, "connection")
  end

  test "POST with Content-Length", %{port: port} do
    r = request(port, "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello")
    assert {200, "body=hello", "POST"} == {r.status, r.body, header(r, "x-method")}
  end

  test "chunked request body", %{port: port} do
    req =
      "POST /echo HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n" <>
        "5;ext=1\r\nhello\r\n1\r\n \r\n5\r\nworld\r\n0\r\nX-Trailer: t\r\n\r\n"

    assert request(port, req).body == "body=hello world"
  end

  test "a request split into one-byte packets", %{port: port} do
    s = connect(port)
    req = "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabc"

    for <<b <- req>> do
      :ok = :gen_tcp.send(s, <<b>>)
      Process.sleep(1)
    end

    assert {:ok, %{status: 200, body: "body=abc"}, ""} = read(s)
  end

  test "chunked body split across packets", %{port: port} do
    s = connect(port)
    :gen_tcp.send(s, "POST /echo HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n3\r")
    Process.sleep(20)
    :gen_tcp.send(s, "\nab")
    Process.sleep(20)
    :gen_tcp.send(s, "c\r\n0\r\n")
    Process.sleep(20)
    :gen_tcp.send(s, "\r\n")
    assert {:ok, %{status: 200, body: "body=abc"}, ""} = read(s)
  end

  test "keep-alive reuses one connection", %{port: port} do
    s = connect(port)

    for i <- 1..5 do
      :ok = :gen_tcp.send(s, "GET /n/#{i} HTTP/1.1\r\nHost: h\r\n\r\n")
      assert {:ok, %{status: 200, body: body}, ""} = read(s)
      assert body == "GET /n/#{i}"
    end
  end

  test "pipelined requests answer in order", %{port: port} do
    s = connect(port)

    :ok =
      :gen_tcp.send(
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

  test "a pipelined tail that is still partial waits for the rest", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET /a HTTP/1.1\r\nHost: h\r\n\r\nGET /b HTT")
    {:ok, r1, ""} = read(s)
    assert r1.body == "GET /a"
    :ok = :gen_tcp.send(s, "P/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: "GET /b"}, ""} = read(s)
  end

  test "Expect: 100-continue gets the interim before the body", %{port: port} do
    s = connect(port)

    :ok =
      :gen_tcp.send(
        s,
        "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n"
      )

    assert {:ok, "HTTP/1.1 100 Continue\r\n\r\n"} = :gen_tcp.recv(s, 0, 1_000)
    :ok = :gen_tcp.send(s, "data")
    assert {:ok, %{status: 200, body: "body=data"}, ""} = read(s)
  end

  test "Expect: 100-continue with the body already sent owes no interim", %{port: port} do
    r =
      request(
        port,
        "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\nExpect: 100-continue\r\n\r\nok"
      )

    assert {200, "body=ok"} == {r.status, r.body}
  end

  test "400 on a missing Host and the connection closes", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET / HTTP/1.1\r\n\r\n")
    {:ok, r, ""} = read(s)
    assert r.status == 400
    assert header(r, "connection") == "close"
    assert closed?(s)
  end

  test "400 on a malformed request line", %{port: port} do
    assert request(port, "GET/HTTP/1.1\r\nHost: h\r\n\r\n").status == 400
  end

  test "400 on Content-Length together with chunked", %{port: port} do
    req =
      "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"

    assert request(port, req).status == 400
  end

  test "400 on bad chunk framing", %{port: port} do
    req =
      "POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nhello\r\n0\r\n\r\n"

    assert request(port, req).status == 400
  end

  test "413 when Content-Length passes max_body", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1000001\r\n\r\n")
    {:ok, r, ""} = read(s)
    assert r.status == 413
    assert closed?(s)
  end

  test "413 when a buffered chunked total passes max_body" do
    port = serve(App, max_body: 10)

    req =
      "POST /echo HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\nb\r\nhello world\r\n0\r\n\r\n"

    assert request(port, req).status == 413
  end

  test "417 on an Expect other than 100-continue", %{port: port} do
    req = "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nExpect: party\r\n\r\nx"
    assert request(port, req).status == 417
  end

  test "501 on an unknown transfer coding", %{port: port} do
    req = "POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip\r\n\r\n"
    assert request(port, req).status == 501
  end

  test "501 on a method the book does not implement", %{port: port} do
    assert request(port, "TRACE / HTTP/1.1\r\nHost: h\r\n\r\n").status == 501
  end

  test "414 on a request line past 16384 bytes", %{port: port} do
    req = "GET /" <> String.duplicate("a", 17_000)
    assert request(port, req).status == 414
  end

  test "431 on a header block past 16384 bytes", %{port: port} do
    req = "GET / HTTP/1.1\r\nHost: h\r\nX-Big: " <> String.duplicate("b", 17_000) <> "\r\n\r\n"
    assert request(port, req).status == 431
  end

  test "a body past 16384 bytes streams from the socket", %{port: port} do
    body = :crypto.strong_rand_bytes(200_000)
    s = connect(port)
    :ok = :gen_tcp.send(s, "POST /size HTTP/1.1\r\nHost: h\r\nContent-Length: 200000\r\n\r\n")
    :ok = :gen_tcp.send(s, body)
    {:ok, r, ""} = read(s)
    hash = :crypto.hash(:sha256, body) |> Base.encode16(case: :lower) |> binary_part(0, 16)
    assert r.body == "200000:" <> hash
    # The connection is still good after a streamed body.
    :ok = :gen_tcp.send(s, "GET /after HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: "GET /after"}, ""} = read(s)
  end

  test "a chunked body past 16384 bytes streams, then the next request", %{port: port} do
    data = :crypto.strong_rand_bytes(60_000)
    chunks = for <<c::binary-size(6_000) <- data>>, into: "", do: "1770\r\n" <> c <> "\r\n"
    s = connect(port)

    :ok =
      :gen_tcp.send(
        s,
        "POST /size HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n" <>
          chunks <> "0\r\n\r\nGET /next HTTP/1.1\r\nHost: h\r\n\r\n"
      )

    {:ok, r, rest} = read(s)
    hash = :crypto.hash(:sha256, data) |> Base.encode16(case: :lower) |> binary_part(0, 16)
    assert r.body == "60000:" <> hash
    assert {:ok, %{body: "GET /next"}, ""} = read(s, rest)
  end

  test "chunked response", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET /stream HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, r, ""} = read(s)
    assert header(r, "transfer-encoding") == "chunked"
    assert r.body == "one,two,three"
    # keep-alive after a chunked response
    :ok = :gen_tcp.send(s, "GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: "GET /x"}, ""} = read(s)
  end

  test "chunked response to HTTP/1.0 is close-delimited", %{port: port} do
    r = request(port, "GET /stream HTTP/1.0\r\n\r\n")
    refute header(r, "transfer-encoding")
    assert header(r, "connection") == "close"
    assert r.body == "one,two,three"
  end

  test "send_file with a length and with :all", %{port: port} do
    file = File.read!(__ENV__.file)
    r = request(port, "GET /file HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {header(r, "content-length"), r.body} == {"100", binary_part(file, 0, 100)}
    r = request(port, "GET /file_off HTTP/1.1\r\nHost: h\r\n\r\n")
    assert r.body == binary_part(file, 10, byte_size(file) - 10)
  end

  test "HEAD sends the length and no body", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "HEAD /hello HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, r, ""} = read(s, <<>>, head: true)
    assert header(r, "content-length") == "11"
    :ok = :gen_tcp.send(s, "GET /again HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {:ok, %{body: "GET /again"}, ""} = read(s)
  end

  test "204 has no body or length", %{port: port} do
    s = connect(port)

    :ok =
      :gen_tcp.send(
        s,
        "GET /nobody HTTP/1.1\r\nHost: h\r\n\r\nGET /z HTTP/1.1\r\nHost: h\r\n\r\n"
      )

    {:ok, r, rest} = read(s)
    assert r.status == 204
    refute header(r, "content-length")
    assert {:ok, %{body: "GET /z"}, ""} = read(s, rest)
  end

  test "Connection: close from the client closes after the response", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET /a HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
    {:ok, r, ""} = read(s)
    assert header(r, "connection") == "close"
    assert closed?(s)
  end

  test "Connection: close from the plug closes after the response", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET /close HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, %{body: "bye"}, ""} = read(s)
    assert closed?(s)
  end

  test "HTTP/1.0 closes after the response", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET /old HTTP/1.0\r\n\r\n")
    {:ok, r, ""} = read(s)
    assert r.body == "GET /old"
    assert closed?(s)
  end

  test "a 4xx from the plug closes, as the book decides", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET /teapot HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, r, ""} = read(s)
    assert {r.status, header(r, "connection")} == {418, "close"}
    assert closed?(s)
  end

  test "a raising plug gets the book's 500 and a close", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET /raise HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, r, ""} = ExUnit.CaptureLog.with_log(fn -> read(s) end) |> elem(0)
    assert r.status == 500
    assert closed?(s)
  end

  test "a plug that sends nothing gets the book's 500", %{port: port} do
    {r, _log} =
      ExUnit.CaptureLog.with_log(fn ->
        request(port, "GET /unsent HTTP/1.1\r\nHost: h\r\n\r\n")
      end)

    assert r.status == 500
  end

  test "a response that is set but not sent is sent", %{port: port} do
    r = request(port, "GET /set HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {r.status, r.body} == {201, "set only"}
  end

  test "inform/3 writes a 103 before the response", %{port: port} do
    s = connect(port)
    :ok = :gen_tcp.send(s, "GET /hints HTTP/1.1\r\nHost: h\r\n\r\n")
    {:ok, info, rest} = read(s)
    assert info.status == 103
    assert header(info, "link") == "</a.css>; rel=preload"
    assert {:ok, %{status: 200, body: "after hints"}, ""} = read(s, rest)
  end

  test "upgrade is not supported yet", %{port: port} do
    assert request(port, "GET /upgrade HTTP/1.1\r\nHost: h\r\n\r\n").body ==
             "{:error, :not_supported}"
  end

  test "peer data, protocol, host and port", %{port: port} do
    assert request(port, "GET /peer HTTP/1.1\r\nHost: example.com:8080\r\n\r\n").body ==
             "127.0.0.1 HTTP/1.1 example.com:8080"

    assert request(port, "GET /peer HTTP/1.0\r\n\r\n").body =~
             ~r/^127\.0\.0\.1 HTTP\/1\.0 127\.0\.0\.1:\d+$/
  end

  test "absolute-form targets", %{port: port} do
    assert request(port, "GET http://h/abs/path?q=1 HTTP/1.1\r\nHost: h\r\n\r\n").body ==
             "GET /abs/path"
  end

  test "idle connections close after idle_timeout" do
    port = serve(App, idle_timeout: 100)
    s = connect(port)
    assert closed?(s, 2_000)
  end

  describe "review fixes" do
    # A chunk of 20000 bytes puts the chunk tail past 16384, so the book
    # hands the body to the host to stream (status ok, reply span).
    @big 20_000
    @big_hex Integer.to_string(@big, 16)

    defp streamed_chunked(port, after_first) do
      s = connect(port)

      :ok =
        :gen_tcp.send(
          s,
          "POST /size HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n" <>
            @big_hex <> "\r\n" <> String.duplicate("a", @big)
        )

      Process.sleep(50)
      :ok = :gen_tcp.send(s, after_first)
      s
    end

    test "a streamed chunked body with bad framing is the book's 400, then close", %{port: port} do
      s = streamed_chunked(port, "\r\nzz\r\nxx\r\n0\r\n\r\n")
      {r, log} = ExUnit.CaptureLog.with_log(fn -> read(s) end)
      assert {:ok, %{status: 400} = r, ""} = r
      assert header(r, "connection") == "close"
      assert closed?(s)
      refute log =~ "[error]"
    end

    test "a streamed chunked body with a bare LF is 400", %{port: port} do
      s = streamed_chunked(port, "\n0\r\n\r\n")
      {{:ok, r, ""}, _} = ExUnit.CaptureLog.with_log(fn -> read(s) end)
      assert r.status == 400
      assert closed?(s)
    end

    test "a streamed chunked total past max_body is the book's 413, then close" do
      port = serve(App, max_body: 30_000)
      s = streamed_chunked(port, "\r\n" <> @big_hex <> "\r\n" <> String.duplicate("b", @big))
      {{:ok, r, ""}, log} = ExUnit.CaptureLog.with_log(fn -> read(s) end)
      assert r.status == 413
      assert header(r, "connection") == "close"
      assert closed?(s)
      refute log =~ "[error]"
    end

    test "a buffered chunk with a bare LF is the book's 400", %{port: port} do
      req =
        "POST /echo HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n3\nabc\r\n0\r\n\r\n"

      assert request(port, req).status == 400
    end

    test "a Plug connection: keep-alive is replaced by close when the book closes", %{port: port} do
      s = connect(port)
      :ok = :gen_tcp.send(s, "GET /teapot_keep HTTP/1.1\r\nHost: h\r\n\r\n")
      {:ok, r, ""} = read(s)
      assert r.status == 418
      assert for({"connection", v} <- r.headers, do: v) == ["close"]
      assert closed?(s)
    end

    test "an unread streamed body forces Connection: close", %{port: port} do
      s = connect(port)

      :ok =
        :gen_tcp.send(
          s,
          "POST /ignore HTTP/1.1\r\nHost: h\r\nContent-Length: 100000\r\n\r\n" <>
            String.duplicate("x", 20_000)
        )

      {:ok, r, ""} = read(s)
      assert {r.status, r.body} == {200, "did not read"}
      assert header(r, "connection") == "close"
      assert closed?(s)
    end

    test "send_file on a broken socket does not crash and the handler closes" do
      test = self()
      s = connect(serve({__MODULE__.Tagged, {App, test}}))

      {_, log} =
        ExUnit.CaptureLog.with_log(fn ->
          :ok = :gen_tcp.send(s, "GET /file_broken HTTP/1.1\r\nHost: h\r\n\r\n")
          assert_receive {:file_broken, {:failed, _out}}, 2_000
          assert closed?(s)
        end)

      refute log =~ "MatchError"
      refute log =~ "[error]"
    end

    test "send_file with an offset past EOF sends an empty body and keeps the connection",
         %{port: port} do
      s = connect(port)
      :ok = :gen_tcp.send(s, "GET /file_past HTTP/1.1\r\nHost: h\r\n\r\n")
      assert {:ok, %{status: 200, body: ""} = r, ""} = read(s)
      assert header(r, "content-length") == "0"
      :ok = :gen_tcp.send(s, "GET /next HTTP/1.1\r\nHost: h\r\n\r\n")
      assert {:ok, %{body: "GET /next"}, ""} = read(s)
    end

    test "an HTTP/1.0 request gets an HTTP/1.0 status line", %{port: port} do
      r = request(port, "GET /old HTTP/1.0\r\n\r\n")
      assert r.version == "HTTP/1.0"
      assert r.raw_head =~ ~r/\AHTTP\/1\.0 200 OK\r\n/
    end
  end

  defmodule Tagged do
    # Runs App with the test pid in conn.private.
    def init({app, test}), do: {app.init([]), app, test}

    def call(conn, {opts, app, test}),
      do: conn |> Plug.Conn.put_private(:test, test) |> app.call(opts)
  end
end
