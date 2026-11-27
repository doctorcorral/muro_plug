defmodule MuroPlug.Adapter do
  @moduledoc """
  The `Plug.Conn.Adapter` for `MuroPlug`.

  The payload is one request on one Thousand Island socket. Response
  framing is not decided here: every `send_*` call first asks the
  book (`hand` in step.muro, through `MuroPlug.Book.hand/2`) which span
  to write (2 buffered, 3 content-length pull, 4 chunked, 5
  close-delimited) and whether the connection is kept. This module
  then renders exactly that.

  Request and response bodies are binaries or iodata of binaries. A
  body is never a list of octets.
  """

  @behaviour Plug.Conn.Adapter

  alias MuroPlug.{Book, Chunked, HTTP}
  alias ThousandIsland.Socket

  defstruct [
    :socket,
    :cfg,
    :req,
    :peer,
    :sock,
    http11: true,
    head: false,
    body: :done,
    leftover: <<>>,
    resp: :unsent
  ]

  @type t :: %__MODULE__{}

  @sent_key {__MODULE__, :sent}

  @doc false
  def sent_key, do: @sent_key

  ## Responses

  @impl true
  def send_resp(%__MODULE__{} = p, status, headers, body) do
    out = hand(p, status, headers, 0)
    bodyless = bodyless?(status)
    headers = drop(headers, ["content-length", "transfer-encoding"])

    length =
      if bodyless, do: [], else: [{"content-length", Integer.to_string(IO.iodata_length(body))}]

    head = HTTP.head(status, headers, length ++ framing(p, out, headers))
    data = if bodyless or p.head, do: head, else: [head, body]
    write(p, out, data, :done)
  end

  @impl true
  def send_file(%__MODULE__{} = p, status, headers, path, offset, length) do
    length =
      case length do
        :all -> File.stat!(path).size - offset
        n when is_integer(n) -> n
      end

    out = hand(p, status, headers, 1)
    headers = drop(headers, ["content-length", "transfer-encoding"])
    extra = [{"content-length", Integer.to_string(length)} | framing(p, out, headers)]
    head = HTTP.head(status, headers, extra)

    with {:ok, nil, p} <- write(p, out, head, :done) do
      if p.head or bodyless?(status) or length == 0 do
        {:ok, nil, p}
      else
        case Socket.sendfile(p.socket, path, offset, length) do
          {:ok, _} ->
            {:done, ^out, n} = p.resp
            {:ok, nil, %{p | resp: {:done, out, n + length}}}

          {:error, _} ->
            {:ok, nil, %{p | resp: {:failed, out}}}
        end
      end
    end
  end

  @impl true
  def send_chunked(%__MODULE__{} = p, status, headers) do
    out = hand(p, status, headers, 2)
    headers = drop(headers, ["content-length", "transfer-encoding"])
    te = if out.span == 4, do: [{"transfer-encoding", "chunked"}], else: []
    write(p, out, HTTP.head(status, headers, te ++ framing(p, out, headers)), :chunked)
  end

  @impl true
  def chunk(%__MODULE__{resp: {:chunked, out, n}} = p, body) do
    size = IO.iodata_length(body)

    cond do
      size == 0 or p.head ->
        {:ok, nil, p}

      true ->
        data =
          if out.span == 4,
            do: [Integer.to_string(size, 16), "\r\n", body, "\r\n"],
            else: body

        case Socket.send(p.socket, data) do
          :ok -> {:ok, nil, %{p | resp: {:chunked, out, n + IO.iodata_length(data)}}}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  def chunk(%__MODULE__{}, _body), do: {:error, :not_chunked}

  @impl true
  def inform(%__MODULE__{http11: true} = p, status, headers) do
    case Socket.send(p.socket, HTTP.head(status, normalize(headers), [])) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def inform(%__MODULE__{}, _status, _headers), do: {:error, :not_supported}

  @impl true
  def upgrade(_payload, _protocol, _opts), do: {:error, :not_supported}

  @impl true
  def push(_payload, _path, _headers), do: {:error, :not_supported}

  ## Request body

  @impl true
  def read_req_body(%__MODULE__{body: :done} = p, _opts), do: {:ok, <<>>, p}

  def read_req_body(%__MODULE__{body: {:buffered, b}} = p, opts) do
    len = Keyword.get(opts, :length, 8_000_000)

    if byte_size(b) <= len do
      {:ok, b, %{p | body: :done}}
    else
      <<h::binary-size(^len), t::binary>> = b
      {:more, h, %{p | body: {:buffered, t}}}
    end
  end

  def read_req_body(%__MODULE__{body: {:cl, prefix, rem}} = p, opts) do
    len = Keyword.get(opts, :length, 8_000_000)
    step = Keyword.get(opts, :read_length, 1_000_000)
    timeout = Keyword.get(opts, :read_timeout, 15_000)
    read_cl(p, [prefix], byte_size(prefix), rem, len, step, timeout)
  end

  def read_req_body(%__MODULE__{body: {:chunked, dec, pending}} = p, opts) do
    len = Keyword.get(opts, :length, 8_000_000)
    timeout = Keyword.get(opts, :read_timeout, 15_000)
    read_chunked(p, Chunked.feed(dec, pending), [], 0, len, timeout)
  end

  defp read_cl(p, acc, _got, 0, _len, _step, _timeout),
    do: {:ok, IO.iodata_to_binary(acc), %{p | body: :done}}

  defp read_cl(p, acc, got, rem, len, _step, _timeout) when got >= len,
    do: {:more, IO.iodata_to_binary(acc), %{p | body: {:cl, <<>>, rem}}}

  defp read_cl(p, acc, got, rem, len, step, timeout) do
    case Socket.recv(p.socket, min(rem, step), timeout) do
      {:ok, data} ->
        k = byte_size(data)
        read_cl(p, [acc | data], got + k, rem - k, len, step, timeout)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_chunked(p, {:done, data, rest}, acc, _got, _len, _timeout),
    do: {:ok, IO.iodata_to_binary([acc | data]), %{p | body: :done, leftover: rest}}

  defp read_chunked(_p, {:error, reason}, _acc, _got, _len, _timeout), do: {:error, reason}

  defp read_chunked(p, {:more, data, dec}, acc, got, len, timeout) do
    acc = [acc | data]
    got = got + IO.iodata_length(data)

    if got >= len do
      {:more, IO.iodata_to_binary(acc), %{p | body: {:chunked, dec, <<>>}}}
    else
      case Socket.recv(p.socket, 0, timeout) do
        {:ok, bytes} -> read_chunked(p, Chunked.feed(dec, bytes), acc, got, len, timeout)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  ## Connection data

  @impl true
  def get_peer_data(%__MODULE__{peer: peer}), do: peer

  @impl true
  def get_sock_data(%__MODULE__{sock: sock}), do: sock

  @impl true
  def get_ssl_data(%__MODULE__{}), do: nil

  @impl true
  def get_http_protocol(%__MODULE__{http11: true}), do: :"HTTP/1.1"
  def get_http_protocol(%__MODULE__{}), do: :"HTTP/1.0"

  ## Helpers

  @doc false
  # True while request body bytes are still on the wire.
  def body_open?(%__MODULE__{body: {:cl, _, rem}}), do: rem > 0
  def body_open?(%__MODULE__{body: {:chunked, _, _}}), do: true
  def body_open?(%__MODULE__{}), do: false

  # Ask the book how to frame this response.
  defp hand(p, status, headers, pull) do
    Book.hand(p.req,
      close: close_header?(headers) or body_open?(p),
      bad: status >= 400 and status <= 599,
      owe: true,
      refused: false,
      pull: pull,
      http11: p.http11,
      head: p.head,
      fit: true
    )
  end

  defp write(p, out, data, kind) do
    Process.put(@sent_key, true)

    case Socket.send(p.socket, data) do
      :ok -> {:ok, nil, %{p | resp: {kind, out, IO.iodata_length(data)}}}
      {:error, _} -> {:ok, nil, %{p | resp: {:failed, out}}}
    end
  end

  defp framing(_p, out, headers) do
    date = if List.keymember?(headers, "date", 0), do: [], else: [{"date", HTTP.date()}]

    conn =
      if out.keep == 1 and not List.keymember?(headers, "connection", 0),
        do: [{"connection", "close"}],
        else: []

    date ++ conn
  end

  defp close_header?(headers) do
    Enum.any?(headers, fn {k, v} ->
      k == "connection" and v |> String.downcase() |> String.contains?("close")
    end)
  end

  defp bodyless?(status), do: status in 100..199 or status in [204, 304]

  defp drop(headers, names), do: Enum.reject(headers, fn {k, _} -> k in names end)

  defp normalize(headers), do: Enum.map(headers, fn {k, v} -> {to_string(k), to_string(v)} end)
end
