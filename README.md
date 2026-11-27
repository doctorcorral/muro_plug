# muro_plug

An Elixir [Plug](https://hex.pm/packages/plug) and Phoenix adapter that
serves HTTP/1.1 from the [muro-http](https://github.com/doctorcorral/muro-http)
book. The book's `step.muro`, emitted with muro's default Elixir backend,
runs on [Thousand Island](https://hex.pm/packages/thousand_island) and
decides every protocol fact of a connection. Any Plug, or a
`Phoenix.Endpoint`, runs on it instead of Bandit or Cowboy.

The package will live under `doctorcorral` until launch, then move to
`murolang` with the rest.

## Status

HTTP/1.1 over TCP and over TLS (HTTPS), and WebSockets (RFC 6455,
through `WebSock`) over both: `ws://` and `wss://`, including Phoenix
channels and LiveView sockets, with optional RFC 7692
`permessage-deflate`. Not published to Hex. No HTTP/2: over
TLS, ALPN advertises `http/1.1` only.

## Usage

```elixir
children = [
  {MuroPlug, plug: MyApp.Router, port: 4000}
]
```

HTTPS:

```elixir
children = [
  {MuroPlug,
   plug: MyApp.Router,
   scheme: :https,
   port: 4040,
   certfile: "/etc/my_app/cert.pem",
   keyfile: "/etc/my_app/key.pem"}
]
```

Phoenix, with `http:`, `https:`, or both (each is its own server under
the endpoint, child id `{endpoint, scheme}`, and
`Endpoint.server_info/1` answers for either scheme):

```elixir
config :my_app, MyAppWeb.Endpoint,
  adapter: MuroPlug.PhoenixAdapter,
  http: [ip: {127, 0, 0, 1}, port: 4000],
  https: [
    ip: {127, 0, 0, 1},
    port: 4040,
    certfile: "priv/cert/selfsigned.pem",
    keyfile: "priv/cert/selfsigned_key.pem"
  ]
```

Options (`MuroPlug.child_spec/1`, and the endpoint's `:http` and
`:https` lists): `:plug`, `:scheme` (`:http` default, or `:https`),
`:port` (default 4000 for `:http`, 4040 for `:https`), `:ip`,
`:max_body` (the book's `max`, default 8_000_000), `:idle_timeout`
(default 60_000 ms), and `:thousand_island_options`.

With `scheme: :https` the server runs on
`ThousandIsland.Transports.SSL`, and these TLS options go through
`Plug.SSL.configure/1` (the checks Plug.Cowboy and Bandit run) to
`:ssl`: `:keyfile`, `:certfile`, `:cacertfile`, `:dhfile`, `:key`,
`:cert`, `:cacerts`, `:certs_keys`, `:sni_hosts`, `:sni_fun`,
`:password`, `:otp_app` (relative paths resolve against its app dir; a
Phoenix endpoint passes its own), `:cipher_suite` (`:strong` or
`:compatible`), `:ciphers`, `:versions`, `:eccs`, `:verify`,
`:fail_if_no_peer_cert`, and `:alpn_preferred_protocols` (only
`["http/1.1"]`, the default). TLS options inside
`thousand_island_options: [transport_options: ...]` are used too, and
top-level ones override them. `:ssl` is started before the listener. A
bad `:scheme`, a missing key or certificate, a missing file, other
ALPN protocols, or TLS options with `scheme: :http` (top-level or in
`:transport_options`) raise `ArgumentError`.

The handler and adapter are the same on both transports: Thousand
Island finishes the handshake, and every read and write is
`ThousandIsland.Socket`. `conn.scheme` is `:https`, `get_ssl_data/1`
is the `:ssl` connection information, and `get_peer_data/1` carries a
client certificate (DER) when one was sent. `send_file` over TLS is
Thousand Island's SSL `sendfile/4`: `:file.pread` of at most 1 MiB at
a time, written with `:ssl.send` (TCP uses the kernel's
`:file.sendfile`).

## WebSockets

A Plug upgrades with `WebSockAdapter.upgrade/4` (what
`Phoenix.Endpoint` sockets and LiveView do) or directly with
`Plug.Conn.upgrade_adapter(conn, :websocket, {MyWebSock, state, opts})`:

```elixir
defmodule MyApp.Router do
  use Plug.Router
  plug :match
  plug :dispatch

  get "/ws" do
    WebSockAdapter.upgrade(conn, MyApp.EchoSocket, %{}, timeout: 60_000)
  end
end
```

`muro_plug` depends on `websock` only; `websock_adapter` (a Phoenix
dependency already) is what applications call.

* The request head is parsed and validated by the book like any other
  request. `MuroPlug.Adapter.upgrade/3` then checks the RFC 6455
  opening handshake: `GET`, HTTP/1.1, a Host, an `Upgrade` header with
  the `websocket` token and a `Connection` header with `upgrade`
  (case-insensitive, in a list or across lines), `Sec-WebSocket-Version:
  13`, and one `Sec-WebSocket-Key` that is base64 of 16 octets. A
  failing request raises `MuroPlug.WebSocket.UpgradeError` and gets
  `400`, or `426` with `sec-websocket-version: 13` for another version
  (`WebSockAdapter`'s own early check answers `400`, and Phoenix
  renders its own `400`).
* When the Plug returns, `MuroPlug.Handler` writes `101 Switching
  Protocols` with `Sec-WebSocket-Accept` and the response headers the
  Plug set (`set-cookie`, `sec-websocket-protocol`, ...; never a second
  `upgrade`/`connection`/`sec-websocket-accept`, and never a
  Plug-supplied `sec-websocket-extensions`). If `:compress` is enabled
  and the client offered a valid RFC 7692 `permessage-deflate`, the 101
  includes the negotiated `Sec-WebSocket-Extensions` value. Then it
  runs `MuroPlug.WebSocket.Connection` on the same
  `ThousandIsland.Socket`. Bytes the client sent after the upgrade
  request (a pipelined first frame) are the first frame bytes.
* Framing: client frames must be masked; server frames are not. 7-,
  16-, and 64-bit lengths; unmasking is one `:crypto.exor` over the
  payload binary. Text, binary, continuation, ping (answered with a pong
  of the same payload before `handle_control/2`), pong, and close.
  Fragmented messages are reassembled with control frames allowed
  between fragments; every 1024 fragments are folded into one binary,
  so many tiny fragments cannot pile up list cells. Text messages and close reasons must be UTF-8.
* Failures close the connection with a close frame: 1002 for an
  unmasked frame, RSV2/RSV3, RSV1 on a continuation or control frame
  or when `permessage-deflate` was not negotiated, a reserved
  opcode, a fragmented or over-125-byte control frame, an unexpected
  continuation or a data frame inside a fragmented message, a 1-byte
  close or a close code that may not be sent, a 64-bit length with
  the high bit set, or a length not in its minimal encoding (§5.2); 1007 for invalid UTF-8 (including after inflate) or a corrupt deflate payload; 1009 past
  `:max_frame_size` (checked on the frame header) or
  `:max_message_size` (including as inflated octets are produced).
* `WebSock` callbacks: `init/1`, `handle_in/2`, `handle_control/2`
  (optional), `handle_info/2` for every other process message (PubSub,
  channel processes), `terminate/2` (optional, called once: `:normal`,
  `:remote`, `:shutdown`, `:timeout`, or `{:error, reason}`), and every
  return: `{:ok, s}`, `{:push, frames, s}`, `{:reply, _, frames, s}`,
  and the three `{:stop, ...}` forms (`:normal` closes with 1000,
  `{:shutdown, :restart}` with 1012, other reasons with 1011; a raise is
  logged and closes with 1011). A client close is echoed with its code.
  After a server close the connection waits up to `:close_timeout` for
  the client's close.
* Options (the upgrade's `opts`): `:timeout` (idle, default 60_000 ms;
  then close 1001 and `terminate(:timeout, _)`), `:max_frame_size`
  (default 10_000_000), `:max_message_size` (default the frame limit),
  `:validate_utf8` (default `true`), `:close_timeout` (default 5_000
  ms), `:fullsweep_after`, `:max_heap_size` (an integer or a map with
  `:size` and optional `:kill`, `:error_logger`,
  `:include_shared_binaries`). `:compress` (`false` by default; `true`
  or a keyword list of `:zlib` options) negotiates RFC 7692
  `permessage-deflate` when the client offers it, compresses outgoing
  data frames, and inflates incoming RSV1 data messages. A known option
  with a bad value (e.g. `close_timeout: :infinity` or
  `max_heap_size: %{size: -1}`) raises
  `ArgumentError` from the upgrade, before the 101; unknown keys (Phoenix
  passes its whole socket config) are ignored. An upgrade request whose
  body is still unread — a streamed body on the wire, or a fully-buffered
  non-empty body the Plug never read — gets `400` and a close. An empty
  or already-read body still upgrades. A server shutdown
  closes open WebSockets with 1001.

## How it fits together

```
socket event ──fact──▶ MuroPlug.Step.stepFact (emitted step.muro)
                              │ Out
                              ▼
MuroPlug.Handler: read more │ owe 400/413/414/417/431/501 and close
                  │ write 100 Continue │ serve the request │ close
                              │
                              ▼ Plug.Conn on MuroPlug.Adapter
          send_resp / send_file / send_chunked ──▶ Step.hand ──▶ span + keep
                              │
                              ▼ wrote fact
        book closes, idles, or classifies the pipelined tail

upgrade_adapter(:websocket) ──▶ Adapter.upgrade/3 checks the handshake
                              │ Plug returns
                              ▼
        101 Switching Protocols ──▶ MuroPlug.WebSocket.Connection
        (frames + WebSock callbacks on the same ThousandIsland.Socket)
```

* `priv/muro/http/step.muro` and `request.muro` are byte-identical
  copies of muro-http `main` at
  [`1050ea3`](https://github.com/doctorcorral/muro-http/commit/1050ea3)
  (muro-http #31, muro 0.22.34 books, checked here with muro 0.22.38).
* `lib/muro_plug/step.ex` (`MuroPlug.Step`) is generated by
  `mix muro.gen` from `muro.exs`. Do not edit it. `mix muro.gen --check`
  compares it with the book.
* `test/step_book_test.exs` is generated from the same book
  (`kind: :exunit`): the book's closed equations run against the
  emitted `MuroPlug.Step`.
* `MuroPlug.Book` names the fields of the book's `Out` and feeds it
  facts. `MuroPlug.Handler` does what the `Out` says. Request views
  (`methodView`, `targetView`, `pathView`, `queryView`, `versionView`),
  the header-block end, Content-Length, chunked framing, request ends,
  keep-alive, pipelining, `100 Continue`, and every protocol status are
  the book's.
* The host still owns: splitting the validated header block into
  Plug's `req_headers` (checked equal to the book's `headerName` /
  `headerValue` in `test/headers_test.exs`), decoding chunk data (the
  book validates the framing), reading a streamed body (Content-Length
  past 16384, or a chunk tail past 16384) from the socket, and rendering
  status lines and headers.
* WebSocket framing after a `101` (`MuroPlug.WebSocket`,
  `MuroPlug.WebSocket.Frame`, `MuroPlug.WebSocket.Connection`) is host
  code; muro-http has no WebSocket book yet. The book still decides the
  HTTP/1.1 upgrade request itself.

## Octets stay binaries

muro-http #24 requires that `Bytes` and `Slice` lower to BEAM binaries
and that no HTTP body or buffer is a list of octets.
`test/binary_test.exs` checks this on the emitted module and on the
request and response path:

* `sliceFold` is `<<b, rest::binary>>`; `slice`, `sliceAfter`,
  `sliceUntil`, `sliceGet` are `:binary.part`, `:binary.match`,
  `:binary.at`, and `byte_size`.
* The generated module has no list literal, no cons pattern, no
  `Enum`/`List`, and no `binary_to_list` / `bin_to_list` / charlists.
* Fixtures and views are binaries; stepping a charlist buffer fails.
* The receive buffer is one binary; request bodies, header names and
  values, and paths reach the Plug as binaries; responses are iodata.

## Tests

`mix test` runs the book's own equations against the emitted step,
the binary checks, HTTP/1.1 over real TCP sockets (GET, Content-Length
and chunked bodies, one-byte packets, keep-alive, pipelining,
`100 Continue`, 400/413/414/417/431/501, streamed request bodies,
chunked and close-delimited responses, `send_file`, HEAD, 204,
`Connection: close`, HTTP/1.0, `inform/3`, a raising plug), a
`Plug.Router` app through `:httpc`, and a `Phoenix.Endpoint` with a
router and controller on `MuroPlug.PhoenixAdapter`.

`test/tls_test.exs` runs the same paths over real TLS sockets: a test
CA and a localhost certificate are made with `openssl` in a temp dir
when the suite starts and removed after it (no key is committed).
It covers `conn.scheme == :https`, TLS and peer data, ALPN,
Content-Length, chunked and streamed bodies, keep-alive, pipelining,
`100 Continue`, the book's 400/413/414/417/431/501, `send_file` (with a
2.5 MiB file past the SSL transport's 1 MiB chunk, an offset, past
EOF, HEAD, and a closed socket), `send_chunked`, `inform/3`, client
certificates, DER `:cert`/`:key`, option errors, a `Plug.Router` app
through `:httpc` over HTTPS, and Phoenix endpoints with `https:` alone
and `http:` + `https:` together, with `server_info/2` for each.
`openssl` must be on the `PATH` for `mix test`.

`test/websocket_test.exs` runs every WebSocket test over real TCP
(`ws://`) and real TLS (`wss://`) sockets with a raw frame client:
the handshake and `Sec-WebSocket-Accept` (and RFC 6455's example),
response headers on the 101, rejected upgrades (400/426), text and
binary echo, 16- and 64-bit lengths, fragmentation with pings between
fragments, 2000 one-byte fragments (exact reassembly, bounded state),
one-byte packets, a first frame pipelined with the upgrade request, an
upgrade with an unread streamed or fully-buffered body, option validation (including a `max_heap_size` map), RFC 7692 `permessage-deflate` negotiation and compressed echo (zip bombs, corrupt deflate, RSV1 misuse), a client that
never answers the server's close (`close_timeout`), each 1002/1007/1009 failure above, `handle_info/2` pushes,
`handle_control/2`, `terminate/2` on client close, server stop,
timeout, a dropped socket and server shutdown, close codes and reasons,
a raising callback, and a `Phoenix.Endpoint` with both `http:` and
`https:` serving a `Phoenix.Socket.Transport` and a `Phoenix.Socket`
channel (join, reply, a 70 KB broadcast, heartbeat) over `ws://` and
`wss://`, including with `compress: true`.

## Performance

Correct first, then fast. The Elixir backend's per-byte slice folds
cost about 140 µs per GET in `stepFact`, so this adapter is several
times slower than Bandit today. That gap is the muro Elixir emitter
(curried closures per byte, repeated scans the emitter does not
hoist), not this host; it is a target for the `Muro.IR` work.

## License

MIT, the same license as muro-http.
