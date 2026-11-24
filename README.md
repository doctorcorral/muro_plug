# muro_plug

An Elixir [Plug](https://hex.pm/packages/plug) and Phoenix adapter for a
[muro-http](https://github.com/doctorcorral/muro-http) server. It will host
an Elixir-emitted `step.muro` on
[Thousand Island](https://hex.pm/packages/thousand_island), so a Phoenix
endpoint can run on the muro-http book instead of Bandit or Cowboy.

The package will live under `doctorcorral` until launch, then move to
`murolang` with the rest.

This is a scaffold: the host and `Plug.Conn.Adapter` compile and a smoke
test passes. They do not yet serve HTTP.

## Status

Scaffold. Not published to Hex.

## Roadmap

1. Finish `step.muro` in muro-http, then emit it here with muro's default
   Elixir backend. The books live at `priv/muro/http/step.muro` and friends
   in [muro-http](https://github.com/doctorcorral/muro-http).
2. A Thousand Island host that drives the emitted step.
3. A `Plug.Conn.Adapter` and a Phoenix endpoint adapter.
4. Keep octets as binaries on the BEAM. muro already lowers `Bytes` and
   `Slice` to binaries in the Elixir backend (`Muro.Bytes`, and generated
   modules match on `<<b, rest::binary>>`). Confirm that still holds when
   `step.muro` is emitted here. There is no plan to feed the adapter a list
   of octets.
5. WebSockets for LiveView later. No HTTP/2.

Once a Plug or Phoenix app can be served, benchmark this adapter against
Bandit and Cowboy.

## Installation

The package is not on Hex yet. After it is published:

```elixir
def deps do
  [
    {:muro_plug, "~> 0.1.0"}
  ]
end
```

## License

MIT, the same license as muro-http.
