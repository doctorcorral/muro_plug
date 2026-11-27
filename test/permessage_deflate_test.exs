defmodule MuroPlug.PerMessageDeflateTest do
  use ExUnit.Case, async: true

  alias MuroPlug.WebSocket.PerMessageDeflate, as: PMD

  defp headers(value) when is_binary(value),
    do: [{"sec-websocket-extensions", value}]

  defp headers(values) when is_list(values),
    do: Enum.map(values, &{"sec-websocket-extensions", &1})

  test "negotiate is off unless :compress is enabled" do
    assert PMD.negotiate(headers("permessage-deflate"), []) == :none
    assert PMD.negotiate(headers("permessage-deflate"), compress: false) == :none
    assert PMD.negotiate([], compress: true) == :none
  end

  test "a bare offer is accepted" do
    assert {:ok, params} = PMD.negotiate(headers("permessage-deflate"), compress: true)
    assert params.server_max_window_bits == 15
    assert params.client_max_window_bits == 15
    assert params.server_no_context_takeover == false
    assert params.client_no_context_takeover == false
    assert PMD.header(params) == "permessage-deflate"
  end

  test "compress as a keyword list enables negotiation" do
    assert {:ok, _} = PMD.negotiate(headers("permessage-deflate"), compress: [level: :best_speed])
  end

  test "parameter offers and unknown extensions" do
    assert {:ok, params} =
             PMD.negotiate(
               headers(
                 "permessage-deflate; client_no_context_takeover; server_no_context_takeover"
               ),
               compress: true
             )

    assert params.client_no_context_takeover
    assert params.server_no_context_takeover

    assert PMD.header(params) ==
             "permessage-deflate; server_no_context_takeover; client_no_context_takeover"

    assert {:ok, params} =
             PMD.negotiate(
               headers(
                 "permessage-deflate; client_max_window_bits=12; server_max_window_bits=10"
               ),
               compress: true
             )

    assert {params.client_max_window_bits, params.server_max_window_bits} == {12, 10}

    assert PMD.header(params) ==
             "permessage-deflate; server_max_window_bits=10; client_max_window_bits=12"

    assert {:ok, params} =
             PMD.negotiate(headers("permessage-deflate; client_max_window_bits"), compress: true)

    assert params.client_max_window_bits == 15
    assert PMD.header(params) == "permessage-deflate; client_max_window_bits=15"

    assert {:ok, params} =
             PMD.negotiate(headers(["mux", "permessage-deflate"]), compress: true)

    assert PMD.header(params) == "permessage-deflate"
  end

  test "invalid offers are skipped; a later valid offer wins; none falls back" do
    assert {:ok, params} =
             PMD.negotiate(
               headers(
                 "permessage-deflate; foo=1, permessage-deflate; client_no_context_takeover"
               ),
               compress: true
             )

    assert params.client_no_context_takeover

    for bad <- [
          "permessage-deflate; server_max_window_bits",
          "permessage-deflate; server_max_window_bits=99",
          "permessage-deflate; server_max_window_bits=08",
          "permessage-deflate; client_max_window_bits=7",
          "permessage-deflate; client_no_context_takeover=true",
          "permessage-deflate; client_no_context_takeover; client_no_context_takeover",
          "permessage-deflate; unknown=1"
        ] do
      assert PMD.negotiate(headers(bad), compress: true) == :none
    end

    assert PMD.negotiate(
             headers("permessage-deflate; server_max_window_bits=99, mux"),
             compress: true
           ) == :none
  end

  test "inflate is bounded by max_message_size as octets are produced" do
    {:ok, params} = PMD.negotiate(headers("permessage-deflate"), compress: true)
    ctx = PMD.open(params)
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, :best_compression, :deflated, -15, 8, :default)
    payload = :binary.copy(<<0>>, 50_000)

    compressed =
      z
      |> :zlib.deflate(payload, :sync)
      |> IO.iodata_to_binary()
      |> then(fn data ->
        n = byte_size(data)

        if n >= 4 and binary_part(data, n - 4, 4) == <<0x00, 0x00, 0xFF, 0xFF>>,
          do: binary_part(data, 0, n - 4),
          else: data
      end)

    :zlib.close(z)

    assert {:error, :message_too_big} = PMD.inflate(compressed, ctx, 1_000)
    assert {:ok, ^payload, _} = PMD.inflate(compressed, PMD.open(params), 50_000)
    PMD.close(ctx)
  end

  test "a corrupt payload is a bad_deflate error" do
    {:ok, params} = PMD.negotiate(headers("permessage-deflate"), compress: true)
    ctx = PMD.open(params)
    assert {:error, :bad_deflate} = PMD.inflate(<<1, 2, 3, 4, 5>>, ctx, 1_000)
    PMD.close(ctx)
  end

  test "close is safe to call twice and on nil" do
    {:ok, params} = PMD.negotiate(headers("permessage-deflate"), compress: true)
    ctx = PMD.open(params)
    assert PMD.close(ctx) == :ok
    assert PMD.close(ctx) == :ok
    assert PMD.close(nil) == :ok
  end
end
