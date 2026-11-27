defmodule MuroPlug.WebSocket.Connection do
  @moduledoc false
  # One upgraded connection: RFC 6455 frames in and out of the
  # ThousandIsland.Socket, and the WebSock callbacks. MuroPlug.Handler
  # delegates every socket event and process message here once the 101
  # is written. Each function returns
  #
  #   {:continue, conn}  keep reading
  #   {:linger, conn}    the server sent a close: read until the client's
  #                      close or `close_timeout`, then drop the socket
  #   {:close, conn}     drop the socket now
  #
  # `terminate/2` runs at most once.

  require Logger

  alias MuroPlug.WebSocket.Frame
  alias ThousandIsland.Socket

  defstruct [
    :handler,
    :hstate,
    :max_frame,
    :max_message,
    :timeout,
    :close_timeout,
    utf8: true,
    control?: false,
    terminate?: false,
    buf: <<>>,
    frag: nil,
    phase: :open
  ]

  @type t :: %__MODULE__{}
  @type result :: {:continue, t()} | {:linger, t()} | {:close, t()}

  @close_timer {__MODULE__, :close_timeout}
  @coalesce 1024

  @doc """
  Check the upgrade's options before the 101 is written. Unknown keys
  (Phoenix passes its whole socket config) are ignored; a known key
  with a bad value raises `ArgumentError`.
  """
  @spec validate_opts!(keyword()) :: :ok
  def validate_opts!(opts) when is_list(opts) do
    Enum.each(opts, fn
      {key, value} when key in [:timeout, :max_frame_size, :max_message_size] ->
        unless value == :infinity or (is_integer(value) and value >= 0),
          do: bad_opt!(key, value, "a non-negative integer or :infinity")

      {:close_timeout, value} ->
        unless is_integer(value) and value >= 0,
          do: bad_opt!(:close_timeout, value, "a non-negative integer (milliseconds)")

      {key, value} when key in [:validate_utf8, :compress] ->
        unless is_boolean(value), do: bad_opt!(key, value, "a boolean")

      {key, nil} when key in [:fullsweep_after, :max_heap_size] ->
        :ok

      {:fullsweep_after, value} ->
        unless is_integer(value) and value >= 0,
          do: bad_opt!(:fullsweep_after, value, "a non-negative integer")

      {:max_heap_size, value} ->
        unless (is_integer(value) and value >= 0) or is_map(value),
          do: bad_opt!(:max_heap_size, value, "a non-negative integer or a map")

      _other ->
        :ok
    end)
  end

  def validate_opts!(opts),
    do:
      raise(
        ArgumentError,
        "MuroPlug: WebSocket options must be a keyword list, got: #{inspect(opts)}"
      )

  defp bad_opt!(key, value, expected) do
    raise ArgumentError,
          "MuroPlug: WebSocket option #{inspect(key)} must be #{expected}, got: #{inspect(value)}"
  end

  @doc "The connection for `{handler, state, opts}` before `init/1` runs."
  @spec new(module(), keyword()) :: t()
  def new(handler, opts) do
    Code.ensure_loaded(handler)
    max_frame = Keyword.get(opts, :max_frame_size, 10_000_000)

    for flag <- [:fullsweep_after, :max_heap_size], value = opts[flag], value != nil do
      Process.flag(flag, value)
    end

    %__MODULE__{
      handler: handler,
      max_frame: max_frame,
      max_message: Keyword.get(opts, :max_message_size, max_frame),
      timeout: Keyword.get(opts, :timeout, 60_000),
      close_timeout: Keyword.get(opts, :close_timeout, 5_000),
      utf8: Keyword.get(opts, :validate_utf8, true),
      control?: function_exported?(handler, :handle_control, 2),
      terminate?: function_exported?(handler, :terminate, 2)
    }
  end

  @doc """
  `handler.init(state)`, then any bytes the client sent after the
  upgrade request (a first frame pipelined in the same packet).
  """
  @spec init(t(), term(), binary(), Socket.t()) :: result()
  def init(conn, state, tail, socket) do
    case callback(conn, :init, [state]) |> result(socket, conn) do
      {:continue, conn} when tail != <<>> -> handle_data(tail, socket, conn)
      other -> other
    end
  end

  ## Socket events

  @spec handle_data(binary(), Socket.t(), t()) :: result()
  def handle_data(data, socket, conn), do: frames(socket, %{conn | buf: conn.buf <> data})

  @spec handle_info(term(), Socket.t(), t()) :: result()
  def handle_info(@close_timer, _socket, %__MODULE__{phase: :closing} = conn), do: {:close, conn}

  def handle_info(_msg, _socket, %__MODULE__{phase: phase} = conn) when phase != :open,
    do: {:continue, conn}

  def handle_info(msg, socket, conn),
    do: callback(conn, :handle_info, [msg]) |> result(socket, conn)

  @doc "No client data for `timeout`: close with 1001. The socket closes after."
  def handle_timeout(socket, %__MODULE__{phase: :open} = conn) do
    _ = Socket.send(socket, close_frame(1001))
    terminate(conn, :timeout)
  end

  def handle_timeout(_socket, conn), do: conn

  @doc "The server is shutting down: close with 1001."
  def handle_shutdown(socket, %__MODULE__{phase: :open} = conn) do
    _ = Socket.send(socket, close_frame(1001))
    terminate(conn, :shutdown)
  end

  def handle_shutdown(_socket, conn), do: conn

  @doc "The client dropped the socket (or it errored) without a close handshake."
  def handle_close(conn), do: terminate(conn, {:error, :closed})
  def handle_error(reason, conn), do: terminate(conn, {:error, reason})

  ## Frames

  defp frames(socket, conn) do
    case Frame.parse(conn.buf, conn.max_frame) do
      :more ->
        {:continue, conn}

      {:error, code, reason} ->
        fail(code, reason, socket, conn)

      {:ok, frame, rest} ->
        case frame(frame, socket, %{conn | buf: rest}) do
          {:continue, conn} -> frames(socket, conn)
          other -> other
        end
    end
  end

  # After the server's close only the client's close matters (§5.5.1).
  defp frame({_fin, :close, _}, _socket, %__MODULE__{phase: :closing} = conn), do: {:close, conn}
  defp frame(_frame, _socket, %__MODULE__{phase: :closing} = conn), do: {:continue, conn}

  defp frame({_fin, op, _}, socket, %__MODULE__{frag: {_, _, _, _}} = conn)
       when op in [:text, :binary],
       do: fail(1002, :data_frame_mid_message, socket, conn)

  defp frame({true, op, data}, socket, conn) when op in [:text, :binary],
    do: message(op, data, socket, conn)

  defp frame({false, op, data}, socket, conn) when op in [:text, :binary],
    do: fragment(op, data, socket, conn)

  defp frame({_fin, :continuation, _}, socket, %__MODULE__{frag: nil} = conn),
    do: fail(1002, :unexpected_continuation, socket, conn)

  defp frame({fin, :continuation, data}, socket, %__MODULE__{frag: {op, chunks, n, size}} = conn) do
    size = size + byte_size(data)

    cond do
      too_big?(size, conn.max_message) ->
        fail(1009, :message_too_big, socket, conn)

      fin ->
        message(op, IO.iodata_to_binary(Enum.reverse([data | chunks])), socket, %{
          conn
          | frag: nil
        })

      true ->
        {:continue, %{conn | frag: chunk(op, [data | chunks], n + 1, size)}}
    end
  end

  defp frame({_fin, :ping, data}, socket, conn) do
    case Socket.send(socket, Frame.encode(:pong, data)) do
      :ok -> control(:ping, data, socket, conn)
      {:error, reason} -> {:close, terminate(conn, {:error, reason})}
    end
  end

  defp frame({_fin, :pong, data}, socket, conn), do: control(:pong, data, socket, conn)

  # The client's close: echo its code (§5.5.1), then drop the socket.
  defp frame({_fin, :close, payload}, socket, conn) do
    case Frame.close_payload(payload, conn.utf8) do
      {:ok, code, _reason} ->
        _ = Socket.send(socket, if(code, do: close_frame(code), else: Frame.encode(:close, <<>>)))
        {:close, terminate(conn, :remote)}

      {:error, code, reason} ->
        fail(code, reason, socket, conn)
    end
  end

  defp fragment(op, data, socket, conn) do
    if too_big?(byte_size(data), conn.max_message),
      do: fail(1009, :message_too_big, socket, conn),
      else: {:continue, %{conn | frag: {op, [data], 1, byte_size(data)}}}
  end

  # A fragmented message is {opcode, chunks newest first, chunk count,
  # octets}. Every @coalesce chunks fold into one binary, so many tiny
  # fragments cost binaries, not a cons cell (and a refc binary) each.
  defp chunk(op, chunks, n, size) when n >= @coalesce,
    do: {op, [IO.iodata_to_binary(Enum.reverse(chunks))], 1, size}

  defp chunk(op, chunks, n, size), do: {op, chunks, n, size}

  defp message(op, data, socket, conn) do
    cond do
      too_big?(byte_size(data), conn.max_message) ->
        fail(1009, :message_too_big, socket, conn)

      op == :text and conn.utf8 and not String.valid?(data) ->
        fail(1007, :invalid_utf8, socket, conn)

      true ->
        callback(conn, :handle_in, [{data, opcode: op}]) |> result(socket, conn)
    end
  end

  defp control(op, data, socket, %__MODULE__{control?: true} = conn),
    do: callback(conn, :handle_control, [{data, opcode: op}]) |> result(socket, conn)

  defp control(_op, _data, _socket, conn), do: {:continue, conn}

  defp too_big?(_size, :infinity), do: false
  defp too_big?(size, max), do: size > max

  # Fail the connection (§7.1.7): a close frame with `code`, then drop it.
  defp fail(code, reason, socket, conn) do
    _ = Socket.send(socket, close_frame(code))
    {:close, terminate(%{conn | phase: :closed}, {:error, reason})}
  end

  ## WebSock results

  # init/1 takes the upgrade state; the others take the current state last.
  defp callback(conn, :init, [state]), do: call(conn.handler, :init, [state])
  defp callback(conn, fun, [arg]), do: call(conn.handler, fun, [arg, conn.hstate])

  defp call(mod, fun, args) do
    {:ok, apply(mod, fun, args)}
  catch
    kind, reason -> {:raised, kind, reason, __STACKTRACE__}
  end

  defp result({:ok, {:ok, state}}, _socket, conn), do: {:continue, %{conn | hstate: state}}

  defp result({:ok, {:push, msgs, state}}, socket, conn),
    do: push(msgs, socket, %{conn | hstate: state})

  defp result({:ok, {:reply, _term, msgs, state}}, socket, conn),
    do: push(msgs, socket, %{conn | hstate: state})

  defp result({:ok, {:stop, reason, state}}, socket, conn),
    do: stop(reason, code_for(reason), [], socket, %{conn | hstate: state})

  defp result({:ok, {:stop, reason, detail, state}}, socket, conn),
    do: stop(reason, detail, [], socket, %{conn | hstate: state})

  defp result({:ok, {:stop, reason, detail, msgs, state}}, socket, conn),
    do: stop(reason, detail, msgs, socket, %{conn | hstate: state})

  defp result({:ok, other}, socket, conn) do
    error = {:bad_return, other}
    Logger.error("MuroPlug: #{inspect(conn.handler)} returned #{inspect(other)}")
    _ = Socket.send(socket, close_frame(1011))
    {:close, terminate(%{conn | phase: :closed}, {:error, error})}
  end

  defp result({:raised, kind, reason, stack}, socket, conn) do
    Logger.error(Exception.format(kind, reason, stack), crash_reason: {reason, stack})
    _ = Socket.send(socket, close_frame(1011))
    {:close, terminate(%{conn | phase: :closed}, {:error, reason})}
  end

  defp push(msgs, socket, conn) do
    case Socket.send(socket, encode_all(msgs)) do
      :ok -> {:continue, conn}
      {:error, reason} -> {:close, terminate(%{conn | phase: :closed}, {:error, reason})}
    end
  end

  # Send any last frames and the close, run terminate/2, then wait for
  # the client's close so the close frame is not lost to a reset.
  defp stop(reason, detail, msgs, socket, conn) do
    conn = terminate(conn, if(reason == :normal, do: :normal, else: {:error, reason}))

    case Socket.send(socket, [encode_all(msgs), close_frame(detail)]) do
      :ok ->
        Process.send_after(self(), @close_timer, conn.close_timeout)
        {:linger, %{conn | phase: :closing}}

      {:error, _} ->
        {:close, %{conn | phase: :closed}}
    end
  end

  defp code_for(:normal), do: 1000
  defp code_for(:shutdown), do: 1000
  defp code_for({:shutdown, :restart}), do: 1012
  defp code_for({:shutdown, _}), do: 1000
  defp code_for(_), do: 1011

  defp encode_all(msgs) when is_list(msgs), do: Enum.map(msgs, &encode_one/1)
  defp encode_all(msg), do: encode_one(msg)

  defp encode_one({op, nil}) when op in [:text, :binary, :ping, :pong], do: Frame.encode(op, <<>>)

  defp encode_one({op, data}) when op in [:text, :binary, :ping, :pong],
    do: Frame.encode(op, data)

  defp close_frame(code) when is_integer(code), do: Frame.encode(:close, <<code::16>>)
  defp close_frame({code, nil}), do: close_frame(code)
  defp close_frame({code, reason}), do: Frame.encode(:close, [<<code::16>>, reason])

  defp terminate(%__MODULE__{terminate?: true, handler: h} = conn, reason) do
    try do
      h.terminate(reason, conn.hstate)
    catch
      kind, error ->
        Logger.error(Exception.format(kind, error, __STACKTRACE__))
    end

    %{conn | terminate?: false, phase: closed_phase(conn)}
  end

  defp terminate(conn, _reason), do: %{conn | phase: closed_phase(conn)}

  defp closed_phase(%__MODULE__{phase: :open}), do: :closed
  defp closed_phase(%__MODULE__{phase: phase}), do: phase
end
