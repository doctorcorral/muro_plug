defmodule MuroPlug.Handler do
  @moduledoc """
  The Thousand Island handler that drives the emitted `step.muro`.

  Each socket event is one book fact: accept, bytes, wrote, now. The
  book's `Out` says what to do next and the handler does only that:

    * read more (book phases 0, 4, 7),
    * write an owed protocol status and close (phase 1: 400, 413, 414,
      417, 431, 501, or a `status_now` close),
    * write the `100 Continue` interim (span 6) and feed the wrote back,
    * serve a finished request through the Plug (phase 3, span 2),
      then feed the wrote so the book either closes, idles, or
      classifies a pipelined tail in the same step,
    * close (phases 2 and 5, or `cover`).

  The receive buffer is one binary. Request views, framing ends and
  header-block ends all come from the book.
  """

  use ThousandIsland.Handler

  require Logger

  alias MuroPlug.{Adapter, Book, Chunked, HTTP}
  alias ThousandIsland.Socket

  @impl ThousandIsland.Handler
  def handle_connection(socket, opts) do
    cfg = %{max_body: opts.max_body, idle: opts.idle_timeout, deadline: 0}
    out = Book.step(Book.read_st(), :accepted, <<>>, cfg)

    state = %{
      plug: opts.plug,
      scheme: opts.scheme,
      cfg: cfg,
      st: Book.take(out),
      buf: <<>>,
      peer: peer_data(socket),
      sock: sock_data(socket)
    }

    {:continue, state}
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    buf = <<state.buf::binary, data::binary>>
    state = %{state | buf: buf}
    act(Book.step(state.st, :bytes, buf, state.cfg), socket, state)
  end

  @impl ThousandIsland.Handler
  def handle_timeout(_socket, state) do
    # NOW while reading or idle closes in the book. Thousand Island
    # closes the socket after this callback.
    _ = Book.step(state.st, :now, state.buf, state.cfg)
    :ok
  end

  ## The book's verdicts

  defp act(%{cover: 1}, _socket, state), do: {:close, state}

  defp act(%{phase: phase} = out, socket, state) when phase in [2, 5] do
    if out.status_now == 1 and out.code > 0, do: Socket.send(socket, HTTP.status_close(out.code))
    {:close, state}
  end

  defp act(%{read_more: 1} = out, _socket, state),
    do: {:continue, %{state | st: Book.take(out)}}

  defp act(%{span: 6} = out, socket, state),
    do: write_then(out, HTTP.continue(), socket, state)

  defp act(%{phase: 1, span: 2} = out, socket, state),
    do: write_then(out, HTTP.status_close(out.code), socket, state)

  defp act(%{phase: 3, span: 2} = out, socket, state), do: serve(out, socket, state)

  defp act(_out, _socket, state), do: {:close, state}

  defp write_then(out, data, socket, state) do
    case Socket.send(socket, data) do
      :ok ->
        n = IO.iodata_length(data)
        act(Book.step(Book.take(out), :wrote, state.buf, state.cfg, n, n), socket, state)

      {:error, _} ->
        {:close, state}
    end
  end

  ## One finished request

  defp serve(req, socket, state) do
    buf = state.buf
    size = byte_size(buf)
    hend = req.hend
    views = Book.views(buf)

    case body_of(req, buf, size, state.cfg) do
      {:error, _} ->
        Socket.send(socket, HTTP.status_close(400))
        {:close, state}

      {body, tail} ->
        payload = %Adapter{
          socket: socket,
          cfg: state.cfg,
          req: req,
          peer: state.peer,
          sock: state.sock,
          http11: views.version == "HTTP/1.1",
          head: views.method == "HEAD",
          body: body
        }

        headers = HTTP.headers(binary_part(buf, 0, hend))
        conn = build_conn(payload, views, headers, state)
        Process.delete(Adapter.sent_key())

        result = call_plug(conn, state.plug)
        reply = finish(result, req, tail, socket, state)
        flush_sent()
        reply
    end
  end

  # The body view, and the tail past this request. Streamed bodies end
  # at the buffer end in the book (framedEnd), so their tail is empty
  # and anything read past the body is the adapter's leftover.
  defp body_of(%{te: 1} = req, buf, size, cfg) do
    hend = req.hend

    if Book.chunk_done?(req, buf) do
      e = Book.framed_end(req, buf)

      case Chunked.decode_all(binary_part(buf, hend, e - hend)) do
        {:ok, data, <<>>} -> {{:buffered, data}, binary_part(buf, e, size - e)}
        _ -> {:error, :bad_chunk}
      end
    else
      {{:chunked, Chunked.new(cfg.max_body), binary_part(buf, hend, size - hend)}, <<>>}
    end
  end

  defp body_of(%{saw_cl: 1, hend: hend, cl: cl}, buf, size, _cfg) when hend + cl > size,
    do: {{:cl, binary_part(buf, hend, size - hend), hend + cl - size}, <<>>}

  defp body_of(req, buf, size, _cfg) do
    e = Book.framed_end(req, buf)
    body = if req.saw_cl == 1, do: {:buffered, binary_part(buf, req.hend, req.cl)}, else: :done
    {body, binary_part(buf, e, size - e)}
  end

  defp call_plug(conn, {mod, opts}) do
    {:ok, mod.call(conn, opts)}
  catch
    kind, reason -> {:raised, kind, reason, __STACKTRACE__}
  end

  defp finish({:ok, %Plug.Conn{state: :set} = conn}, req, tail, socket, state),
    do: finish({:ok, Plug.Conn.send_resp(conn)}, req, tail, socket, state)

  defp finish({:ok, %Plug.Conn{adapter: {Adapter, p}}}, req, tail, socket, state) do
    case p.resp do
      {:done, out, n} ->
        wrote(out, n, p, tail, socket, state)

      {:chunked, out, n} ->
        last = if out.span == 4 and not p.head, do: "0\r\n\r\n", else: <<>>

        case Socket.send(socket, last) do
          :ok -> wrote(out, n + byte_size(last), p, tail, socket, state)
          {:error, _} -> {:close, state}
        end

      {:failed, _} ->
        {:close, state}

      :unsent ->
        Logger.error("MuroPlug: #{inspect(elem(state.plug, 0))} returned without a response")
        refuse(req, socket, state)
    end
  end

  defp finish({:raised, kind, reason, stack}, req, _tail, socket, state) do
    if status_of(kind, reason) >= 500,
      do: Logger.error(Exception.format(kind, reason, stack), crash_reason: {reason, stack})

    if Process.get(Adapter.sent_key()), do: {:close, state}, else: refuse(req, socket, state)
  end

  # A handler that did not answer: the book's `hand` with href 1 and
  # owe_status 1 shuts with code 500 (book phase 5). The host renders
  # that code as a closing status so the client sees an answer.
  defp refuse(req, socket, state) do
    out = Book.hand(req, refused: true, owe: true)
    if out.code > 0, do: Socket.send(socket, HTTP.status_close(out.code))
    {:close, state}
  end

  defp wrote(out, n, %Adapter{} = p, tail, socket, state) do
    buf = state.buf
    next = Book.step(Book.take(out), :wrote, buf, state.cfg, n, n)

    cond do
      next.phase in [2, 5] ->
        {:close, state}

      Adapter.body_open?(p) ->
        {:close, state}

      p.leftover != <<>> ->
        # A streamed body ended inside this read: the book is idle and
        # the leftover is the next request's first bytes.
        state = %{state | buf: p.leftover, st: Book.take(next)}
        act(Book.step(state.st, :bytes, state.buf, state.cfg), socket, state)

      true ->
        act(next, socket, %{state | buf: tail})
    end
  end

  defp build_conn(payload, views, headers, state) do
    {host, port} = host_port(headers, state)
    {path, query} = path_query(views)

    uri = %URI{scheme: state.scheme, host: host, port: port, path: path, query: query}

    {Adapter, payload}
    |> Plug.Conn.Adapter.conn(views.method, uri, state.peer.address, headers)
    |> Map.put(:owner, self())
  end

  defp path_query(%{target: "http://" <> _ = t}), do: absolute(t)
  defp path_query(%{target: "https://" <> _ = t}), do: absolute(t)
  defp path_query(%{path: path, query: query}), do: {path, query}

  defp absolute(t) do
    uri = URI.parse(t)
    {uri.path || "/", uri.query || ""}
  end

  defp host_port(headers, state) do
    default = state.sock.port

    case List.keyfind(headers, "host", 0) do
      {_, "[" <> _ = v} ->
        case :binary.split(v, "]:") do
          [h, p] -> {h <> "]", port(p, default)}
          [h] -> {h, default}
        end

      {_, v} ->
        case :binary.split(v, ":") do
          [h, p] -> {h, port(p, default)}
          [h] -> {h, default}
        end

      nil ->
        {state.sock.address |> :inet.ntoa() |> to_string(), default}
    end
  end

  defp port(p, default) do
    case Integer.parse(p) do
      {n, ""} -> n
      _ -> default
    end
  end

  defp status_of(:error, reason), do: Plug.Exception.status(reason)
  defp status_of(_, _), do: 500

  defp flush_sent do
    receive do
      {:plug_conn, :sent} -> flush_sent()
    after
      0 -> :ok
    end
  end

  defp peer_data(socket) do
    case Socket.peername(socket) do
      {:ok, {address, port}} -> %{address: address, port: port, ssl_cert: nil}
      _ -> %{address: {0, 0, 0, 0}, port: 0, ssl_cert: nil}
    end
  end

  defp sock_data(socket) do
    case Socket.sockname(socket) do
      {:ok, {address, port}} -> %{address: address, port: port}
      _ -> %{address: {0, 0, 0, 0}, port: 0}
    end
  end
end
