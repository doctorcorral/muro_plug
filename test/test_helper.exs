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

    {:ok, s} = :ssl.connect(~c"localhost", port, [:binary | opts], 5_000)
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

{:ok, _} = Application.ensure_all_started(:ssl)
MuroPlug.TestCert.setup!()
