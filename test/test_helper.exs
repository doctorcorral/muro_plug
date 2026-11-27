Logger.configure(level: :warning)
ExUnit.start()

defmodule MuroPlug.TestClient do
  @moduledoc false
  # A raw HTTP/1.1 client over :gen_tcp, so tests see the exact wire.
  # A TLS socket from `connect_tls/2` is `{:tls, sslsocket}`; `read/3`,
  # `send_bytes/2`, and `closed?/2` take either.

  def serve(plug, opts \\ []) do
    pid =
      ExUnit.Callbacks.start_supervised!(
        {MuroPlug, Keyword.merge([plug: plug, port: 0, id: make_ref()], opts)}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(pid)
    port
  end

  def connect(port) do
    {:ok, s} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    s
  end

  def connect_tls(port, opts \\ []) do
    opts =
      Keyword.merge([active: false, packet: :raw] ++ MuroPlug.TestCert.client_options(), opts)

    # The servers listen on IPv4; the certificate's SAN has IP:127.0.0.1.
    {:ok, s} = :ssl.connect({127, 0, 0, 1}, port, [:binary | opts], 5_000)
    {:tls, s}
  end

  def request(port, data, opts \\ []) do
    s = connect(port)
    :ok = :gen_tcp.send(s, data)
    {:ok, resp, _rest} = read(s, <<>>, opts)
    :gen_tcp.close(s)
    resp
  end

  def request_tls(port, data, opts \\ []) do
    s = connect_tls(port)
    :ok = send_bytes(s, data)
    {:ok, resp, _rest} = read(s, <<>>, opts)
    close(s)
    resp
  end

  def send_bytes({:tls, s}, data), do: :ssl.send(s, data)
  def send_bytes(s, data), do: :gen_tcp.send(s, data)

  def recv({:tls, s}, n, timeout), do: :ssl.recv(s, n, timeout)
  def recv(s, n, timeout), do: :gen_tcp.recv(s, n, timeout)

  def close({:tls, s}), do: :ssl.close(s)
  def close(s), do: :gen_tcp.close(s)

  # One response from `s`. `buf` is bytes already read. Returns the
  # response and the bytes past it.
  def read(s, buf \\ <<>>, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2_000)

    with {:ok, head, rest} <- head(s, buf, timeout) do
      [status_line | lines] = :binary.split(head, "\r\n", [:global])
      [version, code | _] = String.split(status_line, " ", parts: 3)
      true = version in ["HTTP/1.1", "HTTP/1.0"]

      headers =
        for l <- lines, l != "" do
          [k, v] = :binary.split(l, ": ")
          {k, v}
        end

      status = String.to_integer(code)
      resp = %{status: status, version: version, headers: headers, body: <<>>, raw_head: head}
      no_body = Keyword.get(opts, :head, false) or status in 100..199 or status in [204, 304]

      cond do
        no_body ->
          {:ok, resp, rest}

        te = header(resp, "transfer-encoding") ->
          "chunked" = te
          {body, rest} = chunks(s, rest, [], timeout)
          {:ok, %{resp | body: body}, rest}

        cl = header(resp, "content-length") ->
          {body, rest} = exactly(s, rest, String.to_integer(cl), timeout)
          {:ok, %{resp | body: body}, rest}

        true ->
          {:ok, %{resp | body: until_close(s, rest, timeout)}, <<>>}
      end
    end
  end

  def header(%{headers: hs}, name) do
    case List.keyfind(hs, name, 0) do
      {_, v} -> v
      nil -> nil
    end
  end

  def closed?(s, timeout \\ 1_000), do: recv(s, 0, timeout) == {:error, :closed}

  defp head(s, buf, timeout) do
    case :binary.split(buf, "\r\n\r\n") do
      [head, rest] ->
        {:ok, head, rest}

      [_] ->
        case recv(s, 0, timeout) do
          {:ok, more} -> head(s, buf <> more, timeout)
          {:error, reason} -> {:error, reason, buf}
        end
    end
  end

  defp exactly(_s, buf, n, _timeout) when byte_size(buf) >= n do
    <<body::binary-size(^n), rest::binary>> = buf
    {body, rest}
  end

  defp exactly(s, buf, n, timeout) do
    {:ok, more} = recv(s, 0, timeout)
    exactly(s, buf <> more, n, timeout)
  end

  defp chunks(s, buf, acc, timeout) do
    case :binary.split(buf, "\r\n") do
      [size_line, rest] ->
        case String.to_integer(size_line, 16) do
          0 ->
            {"\r\n", rest} = exactly(s, rest, 2, timeout)
            {IO.iodata_to_binary(Enum.reverse(acc)), rest}

          n ->
            {data, rest} = exactly(s, rest, n + 2, timeout)
            chunks(s, rest, [binary_part(data, 0, n) | acc], timeout)
        end

      [_] ->
        {:ok, more} = recv(s, 0, timeout)
        chunks(s, buf <> more, acc, timeout)
    end
  end

  defp until_close(s, buf, timeout) do
    case recv(s, 0, timeout) do
      {:ok, more} -> until_close(s, buf <> more, timeout)
      {:error, :closed} -> buf
    end
  end
end

defmodule MuroPlug.TestCert do
  @moduledoc false
  # A test CA and a localhost / 127.0.0.1 certificate it signs, made
  # once per test run with `openssl` in a temp dir that is removed after
  # the suite. No key is committed. (`:ssl` refuses a self-signed leaf
  # as its own trust anchor, so the leaf is signed by a separate CA.)

  def setup! do
    dir = Path.join(System.tmp_dir!(), "muro_plug_tls_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    ExUnit.after_suite(fn _ -> File.rm_rf!(dir) end)
    f = &Path.join(dir, &1)

    openssl!(
      ~w(req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=muro_plug-test-ca) ++
        ["-keyout", f.("ca_key.pem"), "-out", f.("ca.pem")]
    )

    openssl!(
      ~w(req -newkey rsa:2048 -nodes -subj /CN=localhost) ++
        ["-keyout", f.("key.pem"), "-out", f.("req.pem")]
    )

    File.write!(f.("ext.cnf"), "subjectAltName=DNS:localhost,IP:127.0.0.1\n")

    openssl!(
      ~w(x509 -req -days 2 -CAcreateserial) ++
        ["-in", f.("req.pem"), "-CA", f.("ca.pem"), "-CAkey", f.("ca_key.pem")] ++
        ["-extfile", f.("ext.cnf"), "-out", f.("cert.pem")]
    )

    :persistent_term.put(__MODULE__, %{
      dir: dir,
      cacertfile: f.("ca.pem"),
      keyfile: f.("key.pem"),
      certfile: f.("cert.pem")
    })
  end

  defp openssl!(args) do
    {out, status} = System.cmd("openssl", args, stderr_to_stdout: true)
    if status != 0, do: raise("openssl #{Enum.join(args, " ")} failed:\n#{out}")
  end

  def dir, do: :persistent_term.get(__MODULE__).dir
  def cacertfile, do: :persistent_term.get(__MODULE__).cacertfile
  def keyfile, do: :persistent_term.get(__MODULE__).keyfile
  def certfile, do: :persistent_term.get(__MODULE__).certfile

  # The server's files, as MuroPlug options.
  def server_options, do: [scheme: :https, certfile: certfile(), keyfile: keyfile()]

  # The same certificate and key as DER terms (`:cert` / `:key`).
  def der do
    [{:Certificate, cert, _}] = :public_key.pem_decode(File.read!(certfile()))
    [{type, key, _}] = :public_key.pem_decode(File.read!(keyfile()))
    %{cert: cert, key: {type, key}}
  end

  # A client that verifies the server against the test CA.
  def client_options,
    do: [verify: :verify_peer, cacertfile: String.to_charlist(cacertfile())]
end

defmodule MuroPlug.WSClient do
  @moduledoc false
  # A raw RFC 6455 client on `MuroPlug.TestClient` sockets (TCP or
  # `{:tls, s}`), so tests write exact frames, masked or not, and read
  # the server's frames off the wire.

  alias MuroPlug.TestClient

  @ops %{continuation: 0, text: 1, binary: 2, close: 8, ping: 9, pong: 10}

  def key, do: Base.encode64(:crypto.strong_rand_bytes(16))

  def upgrade_request(path, opts \\ []) do
    key = Keyword.get(opts, :key, key())
    method = Keyword.get(opts, :method, "GET")
    version = Keyword.get(opts, :version, "HTTP/1.1")

    base = [
      {"Host", "localhost"},
      {"Upgrade", "websocket"},
      {"Connection", "Upgrade"},
      {"Sec-WebSocket-Key", key},
      {"Sec-WebSocket-Version", "13"}
    ]

    drop = Keyword.get(opts, :drop, [])
    headers = Enum.reject(base, fn {k, _} -> k in drop end)
    headers = Enum.map(headers, fn {k, v} -> {k, Keyword.get(opts, header_opt(k), v)} end)
    extra = Keyword.get(opts, :headers, [])

    {key,
     [
       method,
       " ",
       path,
       " ",
       version,
       "\r\n",
       Enum.map(headers ++ extra, fn {k, v} -> [k, ": ", v, "\r\n"] end),
       "\r\n"
     ]
     |> IO.iodata_to_binary()}
  end

  defp header_opt("Upgrade"), do: :upgrade
  defp header_opt("Connection"), do: :connection
  defp header_opt("Sec-WebSocket-Version"), do: :ws_version
  defp header_opt(_), do: :unused

  # Connect, upgrade, and check the 101. Returns {socket, response, rest}.
  def connect(port, scheme, path \\ "/ws", opts \\ []) do
    s = open(port, scheme)
    {key, req} = upgrade_request(path, opts)
    :ok = TestClient.send_bytes(s, [req | Keyword.get(opts, :pipelined, [])])
    {:ok, resp, rest} = TestClient.read(s)
    101 = resp.status
    true = TestClient.header(resp, "sec-websocket-accept") == accept(key)
    {s, resp, rest}
  end

  def open(port, :ws), do: TestClient.connect(port)
  def open(port, :wss), do: TestClient.connect_tls(port)

  def accept(key),
    do: Base.encode64(:crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))

  # One client frame. Options: fin, rsv, mask (default true), and a raw
  # integer opcode.
  def frame(op, payload, opts \\ []) do
    payload = IO.iodata_to_binary(payload)
    fin = if Keyword.get(opts, :fin, true), do: 1, else: 0
    rsv = Keyword.get(opts, :rsv, 0)
    code = if is_integer(op), do: op, else: Map.fetch!(@ops, op)
    len = byte_size(payload)

    {len7, ext} =
      cond do
        len < 126 -> {len, <<>>}
        len < 65_536 -> {126, <<len::16>>}
        true -> {127, <<len::64>>}
      end

    if Keyword.get(opts, :mask, true) do
      key = :crypto.strong_rand_bytes(4)

      <<fin::1, rsv::3, code::4, 1::1, len7::7, ext::binary, key::binary,
        MuroPlug.WebSocket.Frame.unmask(payload, key)::binary>>
    else
      <<fin::1, rsv::3, code::4, 0::1, len7::7, ext::binary, payload::binary>>
    end
  end

  def send_frame(s, op, payload, opts \\ []),
    do: TestClient.send_bytes(s, frame(op, payload, opts))

  # One server frame: {:ok, {fin, op, payload, length_code, rsv}, rest}.
  # The server must not mask. `rsv` is the 3-bit RSV field (4 = RSV1).
  def recv(s, buf \\ <<>>, timeout \\ 2_000) do
    case parse(buf) do
      {:ok, frame, rest} ->
        {:ok, frame, rest}

      :more ->
        case TestClient.recv(s, 0, timeout) do
          {:ok, more} -> recv(s, buf <> more, timeout)
          {:error, reason} -> {:error, reason, buf}
        end
    end
  end

  defp parse(<<fin::1, rsv::3, code::4, 0::1, len7::7, rest::binary>>) do
    {len, rest} =
      case {len7, rest} do
        {126, <<n::16, r::binary>>} -> {n, r}
        {127, <<n::64, r::binary>>} -> {n, r}
        {n, r} when n < 126 -> {n, r}
        _ -> {nil, rest}
      end

    case rest do
      <<payload::binary-size(^len), tail::binary>> when is_integer(len) ->
        op = Enum.find_value(@ops, fn {k, v} -> if v == code, do: k end)
        {:ok, {fin == 1, op, payload, len7, rsv}, tail}

      _ ->
        :more
    end
  end

  defp parse(<<_fin::1, _rsv::3, _code::4, 1::1, _::bitstring>>),
    do: raise("server frame is masked")

  defp parse(_), do: :more

  # The next uncompressed frame's {op, payload}.
  def next(s, buf \\ <<>>) do
    {:ok, {true, op, payload, _, 0}, rest} = recv(s, buf)
    {op, payload, rest}
  end

  # The next data frame, inflating RSV1 payloads with `z`.
  def next_inflated(s, z, buf \\ <<>>, opts \\ []) do
    {:ok, {true, op, payload, _, rsv}, rest} = recv(s, buf)
    data = if rsv == 4, do: inflate(z, payload, opts), else: payload
    {op, data, rest}
  end

  # Read the server's close frame: {code | nil, reason}.
  def close_frame(s, buf \\ <<>>) do
    case recv(s, buf) do
      {:ok, {true, :close, <<code::16, reason::binary>>, _, _}, _} ->
        {code, reason}

      {:ok, {true, :close, <<>>, _, _}, _} ->
        {nil, <<>>}

      {:ok, {_, op, _, _, _}, rest} when op in [:ping, :pong, :text, :binary] ->
        close_frame(s, rest)
    end
  end

  # Client zlib helpers for RFC 7692 tests.
  def inflate_stream(window_bits \\ 15) do
    z = :zlib.open()
    :ok = :zlib.inflateInit(z, MuroPlug.WebSocket.PerMessageDeflate.window(-window_bits))
    z
  end

  def deflate_stream(window_bits \\ 15) do
    z = :zlib.open()

    :ok =
      :zlib.deflateInit(
        z,
        :default,
        :deflated,
        MuroPlug.WebSocket.PerMessageDeflate.window(-window_bits),
        8,
        :default
      )

    z
  end

  def inflate(z, payload, opts \\ []) do
    data = do_safe_inflate(z, :zlib.safeInflate(z, payload <> <<0x00, 0x00, 0xFF, 0xFF>>), [])
    if Keyword.get(opts, :reset, false), do: :ok = :zlib.inflateReset(z)
    data
  end

  def deflate(z, payload, opts \\ []) do
    out = z |> :zlib.deflate(IO.iodata_to_binary(payload), :sync) |> IO.iodata_to_binary()
    n = byte_size(out)

    out =
      if n >= 4 and binary_part(out, n - 4, 4) == <<0x00, 0x00, 0xFF, 0xFF>>,
        do: binary_part(out, 0, n - 4),
        else: out

    if Keyword.get(opts, :reset, false), do: :ok = :zlib.deflateReset(z)
    out
  end

  def close_z(z) do
    :zlib.close(z)
  rescue
    _ -> :ok
  end

  def send_compressed(s, op, payload, z, opts \\ []) do
    {frame_opts, inflate_opts} = Keyword.split(opts, [:fin, :rsv, :mask])
    data = deflate(z, payload, inflate_opts)
    send_frame(s, op, data, Keyword.put_new(frame_opts, :rsv, 4))
  end

  defp do_safe_inflate(z, {:continue, chunk}, acc),
    do: do_safe_inflate(z, :zlib.safeInflate(z, <<>>), [acc, chunk])

  defp do_safe_inflate(_z, {:finished, chunk}, acc), do: IO.iodata_to_binary([acc, chunk])
end

{:ok, _} = Application.ensure_all_started(:ssl)
MuroPlug.TestCert.setup!()
