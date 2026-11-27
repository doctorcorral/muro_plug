defmodule MuroPlug.WebSocket do
  @moduledoc """
  WebSocket (RFC 6455) over MuroPlug, through the `WebSock` API.

  A Plug upgrades a request with `WebSockAdapter.upgrade/4` (what
  Phoenix does for sockets and LiveView) or, directly, with

      Plug.Conn.upgrade_adapter(conn, :websocket, {MyWebSock, state, opts})

  The request head was parsed and validated by the book (step.muro) like
  any other request. `MuroPlug.Adapter.upgrade/3` then checks the
  RFC 6455 §4.2.1 opening handshake (`check/3` here) and keeps the
  `WebSock` spec on the payload. When the Plug returns,
  `MuroPlug.Handler` writes `101 Switching Protocols` (with the response
  headers the Plug set, such as `set-cookie` or `sec-websocket-protocol`)
  and runs `MuroPlug.WebSocket.Connection` on the same
  `ThousandIsland.Socket`, so `ws://` and `wss://` are one code path.

  A handshake that fails the checks raises
  `MuroPlug.WebSocket.UpgradeError`: `400 Bad Request`, or
  `426 Upgrade Required` with `sec-websocket-version: 13` for a version
  other than 13.

  Options (the `opts` of the upgrade tuple; the `WebSockAdapter` names):

    * `:timeout` — idle milliseconds with no client data before the
      server closes with 1001 and `terminate(:timeout, state)`.
      Defaults to `60_000`.
    * `:max_frame_size` — the largest frame payload accepted, in octets
      (or `:infinity`); a larger frame closes with 1009. Defaults to
      `10_000_000`.
    * `:max_message_size` — the largest assembled (possibly fragmented)
      message, in octets (or `:infinity`); larger closes with 1009.
      Defaults to the `:max_frame_size` value.
    * `:validate_utf8` — check that text messages and close reasons are
      UTF-8 (1007 when not). Defaults to `true`.
    * `:close_timeout` — after the server sends a close frame, how long
      to wait for the client's close before dropping the socket.
      Defaults to `5_000`.
    * `:fullsweep_after`, `:max_heap_size` — process flags for the
      connection process. A `max_heap_size` map must have a
      non-negative integer `:size` and may have `:kill`,
      `:error_logger`, and `:include_shared_binaries` booleans
      (Erlang's `Process.flag/2`); unknown keys or a negative `:size`
      raise `ArgumentError` before the 101.
    * `:compress` — negotiate RFC 7692 `permessage-deflate` when the
      client offers it. `false` (the default) never advertises
      `Sec-WebSocket-Extensions`. `true`, or a keyword list of
      `:zlib` deflate options (`:level`, `:mem_level`, `:strategy`),
      accepts a valid offer and compresses outgoing data frames
      (RSV1). Control frames are never compressed. `:deflate_options`
      (or `:deflate_opts`) is the same keyword list when `:compress`
      is `true`. Frames with RSV1 set without a negotiated extension,
      or RSV1 on a continuation or control frame, close with 1002.
      A corrupt deflate payload or invalid UTF-8 after inflate closes
      with 1007; inflated bytes past `:max_message_size` close with
      1009.

  Known options with bad values (e.g. `close_timeout: :infinity`)
  raise `ArgumentError` from the upgrade, before the 101; unknown keys
  are ignored.
  """

  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  defmodule UpgradeError do
    @moduledoc """
    Raised by `Plug.Conn.upgrade_adapter/3` (through
    `MuroPlug.Adapter.upgrade/3`) when a request is not a valid
    RFC 6455 opening handshake. `plug_status` is 400, or 426 for an
    unsupported `sec-websocket-version`; `headers` go on that response.
    """
    defexception message: "invalid WebSocket upgrade", plug_status: 400, headers: []
  end

  @doc "`Sec-WebSocket-Accept` for a client's `Sec-WebSocket-Key`."
  @spec accept(binary()) :: binary()
  def accept(key) when is_binary(key), do: Base.encode64(:crypto.hash(:sha, key <> @guid))

  @doc """
  Check an RFC 6455 §4.2.1 opening handshake: `GET`, HTTP/1.1, a Host,
  `Upgrade` with the `websocket` token and `Connection` with the
  `upgrade` token (both case-insensitive, in any header line),
  `Sec-WebSocket-Version: 13`, and one `Sec-WebSocket-Key` that is
  base64 of 16 octets. Returns `{:ok, key}` or `{:error, status,
  message, headers}`.
  """
  @spec check(binary(), boolean(), [{binary(), binary()}]) ::
          {:ok, binary()} | {:error, 400 | 426, binary(), [{binary(), binary()}]}
  def check(method, http11?, headers) do
    with :ok <- method(method),
         :ok <- version(http11?),
         :ok <- nonempty(headers, "host"),
         :ok <- token(headers, "upgrade", "websocket"),
         :ok <- token(headers, "connection", "upgrade"),
         :ok <- ws_version(headers) do
      key(headers)
    end
  end

  defp method("GET"), do: :ok
  defp method(m), do: bad("HTTP method #{m} unsupported for a WebSocket upgrade")

  defp version(true), do: :ok
  defp version(false), do: bad("a WebSocket upgrade needs HTTP/1.1")

  defp nonempty(headers, name) do
    if Enum.any?(values(headers, name), &(&1 != "")),
      do: :ok,
      else: bad("'#{name}' header is absent")
  end

  defp token(headers, name, token) do
    values = values(headers, name)

    found =
      Enum.any?(values, fn v ->
        v |> :binary.split(",", [:global]) |> Enum.any?(&(lower(MuroPlug.HTTP.trim(&1)) == token))
      end)

    if found,
      do: :ok,
      else: bad("'#{name}' header must contain '#{token}', got #{inspect(values)}")
  end

  defp ws_version(headers) do
    case values(headers, "sec-websocket-version") do
      ["13"] ->
        :ok

      other ->
        {:error, 426, "'sec-websocket-version' must be 13, got #{inspect(other)}",
         [{"sec-websocket-version", "13"}]}
    end
  end

  defp key(headers) do
    with [key] <- values(headers, "sec-websocket-key"),
         {:ok, <<_::binary-size(16)>>} <- Base.decode64(key) do
      {:ok, key}
    else
      other when is_list(other) ->
        bad("'sec-websocket-key' must be one header, got #{inspect(other)}")

      _ ->
        bad("'sec-websocket-key' must be base64 of 16 octets")
    end
  end

  defp bad(message), do: {:error, 400, message, []}

  defp values(headers, name), do: for({^name, v} <- headers, do: v)

  defp lower(s), do: String.downcase(s, :ascii)

  @doc false
  # The 101 response: the upgrade headers, then the Plug's response
  # headers without any it must not repeat. A Plug-supplied
  # `sec-websocket-extensions` is dropped; `extension` is the
  # negotiated `permessage-deflate` value, if any.
  @spec handshake(binary(), Plug.Conn.headers(), binary() | nil) :: iodata()
  def handshake(key, resp_headers, extension \\ nil) do
    headers =
      Enum.reject(resp_headers, fn {k, _} ->
        lower(k) in ~w(connection upgrade sec-websocket-accept sec-websocket-extensions
                       content-length transfer-encoding)
      end)

    extra =
      [
        {"upgrade", "websocket"},
        {"connection", "Upgrade"},
        {"sec-websocket-accept", accept(key)}
      ] ++ extension_header(extension)

    MuroPlug.HTTP.head(:"HTTP/1.1", 101, headers, extra)
  end

  defp extension_header(value) when is_binary(value) and value != "",
    do: [{"sec-websocket-extensions", value}]

  defp extension_header(_), do: []
end
