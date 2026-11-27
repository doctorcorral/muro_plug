defmodule MuroPlug.WebSocket.Frame do
  @moduledoc false
  # RFC 6455 §5 framing. Frames, payloads and masks are binaries; the
  # mask is applied with one :crypto.exor over the whole payload.

  @type opcode :: :continuation | :text | :binary | :close | :ping | :pong
  @type frame :: {fin :: boolean(), opcode(), payload :: binary()}
  @type error :: {:error, 1002 | 1009, atom()}

  @doc """
  One client frame from the front of `buf`: `{:ok, frame, rest}` with
  the payload unmasked, `:more` when the frame is not all here yet, or
  `{:error, close_code, reason}` as soon as the header shows a
  violation (before its payload arrives).
  """
  @spec parse(binary(), non_neg_integer() | :infinity) ::
          {:ok, frame(), binary()} | :more | error()
  def parse(<<fin::1, rsv::3, op::4, mask::1, len7::7, rest::binary>>, max) do
    with {:ok, opcode} <- opcode(op),
         :ok <- rsv(rsv),
         :ok <- masked(mask),
         {:ok, len, rest} <- payload_length(len7, rest),
         :ok <- control(opcode, fin, len),
         :ok <- fits(len, max) do
      case rest do
        <<key::binary-size(4), payload::binary-size(^len), tail::binary>> ->
          {:ok, {fin == 1, opcode, unmask(payload, key)}, tail}

        _ ->
          :more
      end
    end
  end

  def parse(_buf, _max), do: :more

  defp opcode(0x0), do: {:ok, :continuation}
  defp opcode(0x1), do: {:ok, :text}
  defp opcode(0x2), do: {:ok, :binary}
  defp opcode(0x8), do: {:ok, :close}
  defp opcode(0x9), do: {:ok, :ping}
  defp opcode(0xA), do: {:ok, :pong}
  defp opcode(_reserved), do: {:error, 1002, :reserved_opcode}

  # No extension is negotiated, so every RSV bit must be 0.
  defp rsv(0), do: :ok
  defp rsv(_), do: {:error, 1002, :reserved_bits}

  # §5.1: a client frame must be masked.
  defp masked(1), do: :ok
  defp masked(0), do: {:error, 1002, :unmasked_frame}

  defp payload_length(126, <<len::16, rest::binary>>), do: {:ok, len, rest}
  defp payload_length(127, <<0::1, len::63, rest::binary>>), do: {:ok, len, rest}
  defp payload_length(127, <<1::1, _::63, _::binary>>), do: {:error, 1002, :bad_length}
  defp payload_length(len, rest) when len < 126, do: {:ok, len, rest}
  defp payload_length(_, _), do: :more

  # §5.5: control frames are not fragmented and carry at most 125 octets.
  defp control(op, fin, len) when op in [:close, :ping, :pong] do
    cond do
      fin != 1 -> {:error, 1002, :fragmented_control}
      len > 125 -> {:error, 1002, :oversized_control}
      true -> :ok
    end
  end

  defp control(_op, _fin, _len), do: :ok

  defp fits(_len, :infinity), do: :ok
  defp fits(len, max) when len <= max, do: :ok
  defp fits(_len, _max), do: {:error, 1009, :frame_too_big}

  @doc "XOR `payload` with the 4-octet `key` repeated (masking is its own inverse)."
  @spec unmask(binary(), <<_::32>>) :: binary()
  def unmask(<<>>, _key), do: <<>>

  def unmask(payload, <<_::32>> = key) do
    n = byte_size(payload)
    :crypto.exor(payload, binary_part(:binary.copy(key, div(n + 3, 4)), 0, n))
  end

  @doc "An unmasked, final server frame."
  @spec encode(opcode(), iodata()) :: iodata()
  def encode(opcode, payload) do
    [<<1::1, 0::3, code(opcode)::4>>, length_bytes(IO.iodata_length(payload)), payload]
  end

  defp code(:continuation), do: 0x0
  defp code(:text), do: 0x1
  defp code(:binary), do: 0x2
  defp code(:close), do: 0x8
  defp code(:ping), do: 0x9
  defp code(:pong), do: 0xA

  defp length_bytes(n) when n < 126, do: <<0::1, n::7>>
  defp length_bytes(n) when n < 65_536, do: <<0::1, 126::7, n::16>>
  defp length_bytes(n), do: <<0::1, 127::7, n::64>>

  @doc """
  A close frame's payload (§5.5.1, §7.4): `{:ok, nil, ""}` for none,
  `{:ok, code, reason}`, or an error for a 1-octet body, a code that
  must not be sent, or (when `utf8?`) a reason that is not UTF-8.
  """
  @spec close_payload(binary(), boolean()) ::
          {:ok, nil | 1000..4999, binary()} | {:error, 1002 | 1007, atom()}
  def close_payload(<<>>, _utf8?), do: {:ok, nil, <<>>}
  def close_payload(<<_>>, _utf8?), do: {:error, 1002, :bad_close_payload}

  def close_payload(<<code::16, reason::binary>>, utf8?) do
    cond do
      not valid_close_code?(code) -> {:error, 1002, :bad_close_code}
      utf8? and not String.valid?(reason) -> {:error, 1007, :bad_close_reason}
      true -> {:ok, code, reason}
    end
  end

  @doc "Close codes a peer may send (§7.4.1 and the IANA registry; 3000-4999 private)."
  @spec valid_close_code?(integer()) :: boolean()
  def valid_close_code?(code),
    do: code in 1000..1003 or code in 1007..1014 or code in 3000..4999
end
