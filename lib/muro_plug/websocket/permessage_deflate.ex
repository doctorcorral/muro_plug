defmodule MuroPlug.WebSocket.PerMessageDeflate do
  @moduledoc false
  # RFC 7692 permessage-deflate. Negotiation is pure; open/2 builds the
  # :zlib streams used to inflate incoming data messages and deflate
  # outgoing ones. A negotiated context is closed with close/1.

  @extension "permessage-deflate"
  @params ~w(server_no_context_takeover client_no_context_takeover
             server_max_window_bits client_max_window_bits)
  @tail <<0x00, 0x00, 0xFF, 0xFF>>

  @valid_levels [:none, :default, :best_speed, :best_compression] ++ Enum.to_list(0..9)
  @valid_strategies [:default, :filtered, :huffman_only, :rle, :fixed]

  defstruct server_no_context_takeover: false,
            client_no_context_takeover: false,
            server_max_window_bits: 15,
            client_max_window_bits: 15,
            advertised: [],
            inflate: nil,
            deflate: nil

  @type t :: %__MODULE__{}

  @type params :: %{
          required(:server_no_context_takeover) => boolean(),
          required(:client_no_context_takeover) => boolean(),
          required(:server_max_window_bits) => 8..15,
          required(:client_max_window_bits) => 8..15,
          required(:advertised) => keyword()
        }

  @doc """
  Pick a `permessage-deflate` offer from the request headers when
  `:compress` is enabled. Invalid, duplicate, or unsupported parameter
  values skip that offer (RFC 7692 §7.1). Returns `:none` when
  compression is off, no offer matches, or `:compress` is absent.
  """
  @spec negotiate([{binary(), binary()}], keyword()) :: :none | {:ok, params()}
  def negotiate(headers, opts) when is_list(headers) and is_list(opts) do
    if enabled?(opts) do
      headers
      |> offers()
      |> Enum.find_value(:none, fn offer ->
        case accept(offer) do
          nil -> false
          params -> {:ok, params}
        end
      end)
    else
      :none
    end
  end

  @doc "The `Sec-WebSocket-Extensions` value for a negotiated offer."
  @spec header(params() | t()) :: binary()
  def header(%{advertised: advertised}) do
    advertised
    |> Enum.flat_map(fn
      {_k, false} -> []
      {k, true} -> [Atom.to_string(k)]
      {k, v} -> ["#{k}=#{v}"]
    end)
    |> then(&Enum.join([@extension | &1], "; "))
  end

  @doc "Open raw :zlib streams for the negotiated parameters."
  @spec open(params(), keyword()) :: t()
  def open(params, opts \\ []) do
    deflate_opts = deflate_opts(opts)
    inflate = :zlib.open()
    :ok = :zlib.inflateInit(inflate, window(-params.client_max_window_bits))
    deflate = :zlib.open()

    :ok =
      :zlib.deflateInit(
        deflate,
        Keyword.get(deflate_opts, :level, :default),
        :deflated,
        window(-params.server_max_window_bits),
        Keyword.get(deflate_opts, :mem_level, 8),
        Keyword.get(deflate_opts, :strategy, :default)
      )

    %__MODULE__{
      server_no_context_takeover: params.server_no_context_takeover,
      client_no_context_takeover: params.client_no_context_takeover,
      server_max_window_bits: params.server_max_window_bits,
      client_max_window_bits: params.client_max_window_bits,
      advertised: params.advertised,
      inflate: inflate,
      deflate: deflate
    }
  end

  @doc """
  Inflate one assembled data message, appending the RFC 7692 §7.2.2
  trailer. `max` is enforced as octets are produced so a zip bomb
  cannot grow past `:max_message_size`.
  """
  @spec inflate(binary(), t(), non_neg_integer() | :infinity) ::
          {:ok, binary(), t()} | {:error, :message_too_big | :bad_deflate}
  def inflate(data, %__MODULE__{} = ctx, max) do
    input = data <> @tail

    case take(ctx.inflate, :zlib.safeInflate(ctx.inflate, input), [], 0, max) do
      {:ok, iodata} ->
        if ctx.client_no_context_takeover, do: :ok = :zlib.inflateReset(ctx.inflate)
        {:ok, IO.iodata_to_binary(iodata), ctx}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    _ -> {:error, :bad_deflate}
  end

  defp take(z, {:continue, chunk}, acc, size, max) do
    size = size + IO.iodata_length(chunk)

    if overflow?(size, max),
      do: {:error, :message_too_big},
      else: take(z, :zlib.safeInflate(z, <<>>), [acc, chunk], size, max)
  end

  defp take(_z, {:finished, chunk}, acc, size, max) do
    size = size + IO.iodata_length(chunk)
    if overflow?(size, max), do: {:error, :message_too_big}, else: {:ok, [acc, chunk]}
  end

  defp overflow?(_size, :infinity), do: false
  defp overflow?(size, max), do: size > max

  @doc "Deflate one outgoing data message and strip the RFC 7692 trailer."
  @spec deflate(iodata(), t()) :: {:ok, binary(), t()} | {:error, :deflate}
  def deflate(data, %__MODULE__{} = ctx) do
    out =
      ctx.deflate
      |> :zlib.deflate(IO.iodata_to_binary(data), :sync)
      |> IO.iodata_to_binary()
      |> strip_tail()

    if ctx.server_no_context_takeover, do: :ok = :zlib.deflateReset(ctx.deflate)
    {:ok, out, ctx}
  rescue
    _ -> {:error, :deflate}
  end

  defp strip_tail(data) when byte_size(data) >= 4 do
    n = byte_size(data) - 4

    case data do
      <<head::binary-size(^n), @tail::binary>> -> head
      other -> other
    end
  end

  defp strip_tail(data), do: data

  @doc "Close both :zlib streams. Safe to call more than once."
  @spec close(t() | nil) :: :ok
  def close(nil), do: :ok

  def close(%__MODULE__{} = ctx) do
    close_z(ctx.inflate)
    close_z(ctx.deflate)
    :ok
  end

  defp close_z(nil), do: :ok

  defp close_z(z) do
    :zlib.close(z)
  rescue
    _ -> :ok
  end

  @doc false
  # :zlib's raw deflate does not accept window_bits = 8; 9 is the floor.
  @spec window(-15..-8) :: -15..-9
  def window(-8), do: -9
  def window(bits), do: bits

  @doc false
  @spec enabled?(keyword()) :: boolean()
  def enabled?(opts) do
    case Keyword.get(opts, :compress, false) do
      true -> true
      false -> false
      list when is_list(list) -> true
      _ -> false
    end
  end

  @doc false
  @spec deflate_opts(keyword()) :: keyword()
  def deflate_opts(opts) do
    case Keyword.get(opts, :deflate_options) || Keyword.get(opts, :deflate_opts) ||
           Keyword.get(opts, :compress) do
      list when is_list(list) -> Keyword.take(list, [:level, :mem_level, :strategy])
      _ -> []
    end
  end

  @doc false
  @spec valid_deflate_opts?(term()) :: boolean()
  def valid_deflate_opts?(opts) when is_list(opts) do
    Keyword.keyword?(opts) and Enum.all?(opts, &valid_deflate_opt?/1)
  end

  def valid_deflate_opts?(_), do: false

  defp valid_deflate_opt?({:level, v}), do: v in @valid_levels
  defp valid_deflate_opt?({:mem_level, v}), do: is_integer(v) and v in 1..9
  defp valid_deflate_opt?({:strategy, v}), do: v in @valid_strategies
  defp valid_deflate_opt?({:window_bits, _}), do: true
  defp valid_deflate_opt?(_), do: false

  ## Negotiation

  defp offers(headers) do
    for {"sec-websocket-extensions", value} <- headers,
        part <- split_offers(value),
        offer <- parse_offer(part),
        do: offer
  end

  # Comma-separated offers; commas inside quoted parameter values stay.
  defp split_offers(value), do: split_offers(value, [], [])

  defp split_offers(<<>>, acc, parts) do
    part = acc |> Enum.reverse() |> IO.iodata_to_binary() |> String.trim()
    parts = if part == "", do: parts, else: [part | parts]
    Enum.reverse(parts)
  end

  defp split_offers(<<?", rest::binary>>, acc, parts) do
    {quoted, rest} = take_quoted(rest, [?"])
    split_offers(rest, [quoted | acc], parts)
  end

  defp split_offers(<<?,, rest::binary>>, acc, parts) do
    part = acc |> Enum.reverse() |> IO.iodata_to_binary() |> String.trim()
    parts = if part == "", do: parts, else: [part | parts]
    split_offers(rest, [], parts)
  end

  defp split_offers(<<c, rest::binary>>, acc, parts),
    do: split_offers(rest, [c | acc], parts)

  defp take_quoted(<<>>, acc), do: {IO.iodata_to_binary(Enum.reverse(acc)), <<>>}

  defp take_quoted(<<?", rest::binary>>, acc),
    do: {IO.iodata_to_binary(Enum.reverse([?" | acc])), rest}

  defp take_quoted(<<?\\, c, rest::binary>>, acc), do: take_quoted(rest, [c, ?\\ | acc])
  defp take_quoted(<<c, rest::binary>>, acc), do: take_quoted(rest, [c | acc])

  defp parse_offer(text) do
    case text |> String.split(";") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == "")) do
      [name | rest] ->
        case parse_params(rest, []) do
          {:ok, params} -> [{String.downcase(name, :ascii), params}]
          :error -> []
        end

      [] ->
        []
    end
  end

  defp parse_params([], acc), do: {:ok, Enum.reverse(acc)}

  defp parse_params([param | rest], acc) do
    case param |> String.split("=", parts: 2) |> Enum.map(&String.trim/1) do
      [key] when key != "" ->
        parse_params(rest, [{String.downcase(key, :ascii), true} | acc])

      [key, value] when key != "" and value != "" ->
        parse_params(rest, [{String.downcase(key, :ascii), unquote_value(value)} | acc])

      _ ->
        :error
    end
  end

  defp unquote_value(<<?", rest::binary>>) do
    case String.split(rest, "\"", parts: 2) do
      [inner, ""] -> inner
      _ -> rest
    end
  end

  defp unquote_value(value), do: value

  defp accept({@extension, params}) do
    with true <- unique_keys?(params),
         {:ok, taken} <- take_params(params) do
      build(taken)
    else
      _ -> nil
    end
  end

  defp accept(_other), do: nil

  defp unique_keys?(params), do: params |> Enum.map(&elem(&1, 0)) |> then(&(&1 == Enum.uniq(&1)))

  defp take_params(params) do
    unknown = Enum.any?(params, fn {k, _} -> k not in @params end)
    if unknown, do: :error, else: collect(params, %{})
  end

  defp collect([], acc), do: {:ok, acc}

  defp collect([{"server_no_context_takeover", true} | rest], acc),
    do: collect(rest, Map.put(acc, :server_no_context_takeover, true))

  defp collect([{"client_no_context_takeover", true} | rest], acc),
    do: collect(rest, Map.put(acc, :client_no_context_takeover, true))

  defp collect([{"server_max_window_bits", true} | _], _acc), do: :error

  defp collect([{"server_max_window_bits", value} | rest], acc) do
    case bits(value) do
      nil -> :error
      n -> collect(rest, Map.put(acc, :server_max_window_bits, n))
    end
  end

  # Valueless: the client supports a server-chosen client window (15).
  defp collect([{"client_max_window_bits", true} | rest], acc),
    do:
      collect(
        rest,
        Map.put(acc, :client_max_window_bits, 15) |> Map.put(:client_bits_offered, true)
      )

  defp collect([{"client_max_window_bits", value} | rest], acc) do
    case bits(value) do
      nil ->
        :error

      n ->
        collect(
          rest,
          Map.put(acc, :client_max_window_bits, n) |> Map.put(:client_bits_offered, true)
        )
    end
  end

  defp collect([_bad | _], _acc), do: :error

  # 8..15, no leading zeroes (RFC 7692 §7.1.2).
  defp bits(n) when is_integer(n) and n in 8..15, do: n

  defp bits(value) when is_binary(value) do
    if value =~ ~r/^[1-9][0-9]*$/,
      do: bits(String.to_integer(value)),
      else: nil
  end

  defp bits(_), do: nil

  defp build(taken) do
    server_nct? = Map.get(taken, :server_no_context_takeover, false)
    client_nct? = Map.get(taken, :client_no_context_takeover, false)
    server_bits = Map.get(taken, :server_max_window_bits)
    client_bits = Map.get(taken, :client_max_window_bits, 15)

    advertised =
      []
      |> put_flag(:server_no_context_takeover, server_nct?)
      |> put_flag(:client_no_context_takeover, client_nct?)
      |> put_bits(:server_max_window_bits, server_bits)
      |> put_client_bits(Map.get(taken, :client_bits_offered, false), client_bits)

    %{
      server_no_context_takeover: server_nct?,
      client_no_context_takeover: client_nct?,
      server_max_window_bits: server_bits || 15,
      client_max_window_bits: client_bits,
      advertised: advertised
    }
  end

  defp put_flag(acc, _k, false), do: acc
  defp put_flag(acc, k, true), do: acc ++ [{k, true}]
  defp put_bits(acc, _k, nil), do: acc
  defp put_bits(acc, k, n), do: acc ++ [{k, n}]
  defp put_client_bits(acc, false, _n), do: acc
  defp put_client_bits(acc, true, n), do: acc ++ [{:client_max_window_bits, n}]
end
