Logger.configure(level: :warning)
ExUnit.start()

defmodule MuroPlug.TestClient do
  @moduledoc false
  # A raw HTTP/1.1 client over :gen_tcp, so tests see the exact wire.

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

  def request(port, data, opts \\ []) do
    s = connect(port)
    :ok = :gen_tcp.send(s, data)
    {:ok, resp, _rest} = read(s, <<>>, opts)
    :gen_tcp.close(s)
    resp
  end

  # One response from `s`. `buf` is bytes already read. Returns the
  # response and the bytes past it.
  def read(s, buf \\ <<>>, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2_000)

    with {:ok, head, rest} <- head(s, buf, timeout) do
      [status_line | lines] = :binary.split(head, "\r\n", [:global])
      ["HTTP/1.1", code | _] = String.split(status_line, " ", parts: 3)

      headers =
        for l <- lines, l != "" do
          [k, v] = :binary.split(l, ": ")
          {k, v}
        end

      status = String.to_integer(code)
      resp = %{status: status, headers: headers, body: <<>>, raw_head: head}
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

  def closed?(s, timeout \\ 1_000), do: :gen_tcp.recv(s, 0, timeout) == {:error, :closed}

  defp head(s, buf, timeout) do
    case :binary.split(buf, "\r\n\r\n") do
      [head, rest] ->
        {:ok, head, rest}

      [_] ->
        case :gen_tcp.recv(s, 0, timeout) do
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
    {:ok, more} = :gen_tcp.recv(s, 0, timeout)
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
        {:ok, more} = :gen_tcp.recv(s, 0, timeout)
        chunks(s, buf <> more, acc, timeout)
    end
  end

  defp until_close(s, buf, timeout) do
    case :gen_tcp.recv(s, 0, timeout) do
      {:ok, more} -> until_close(s, buf <> more, timeout)
      {:error, :closed} -> buf
    end
  end
end
