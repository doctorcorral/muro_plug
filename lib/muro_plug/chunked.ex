defmodule MuroPlug.Chunked do
  @moduledoc """
  An incremental `Transfer-Encoding: chunked` request-body decoder.

  The book validates chunk framing for a body it buffers (chunkScan)
  and owes 400 or 413 itself. A body the book hands to the host to
  stream (a chunk tail past 16384 bytes) is decoded here as it is read
  from the socket. Everything is a binary; decoded data is iodata of
  binaries, never a list of octets.
  """

  @max_line 4096

  defstruct state: :size, pending: <<>>, total: 0, max: :infinity

  @type t :: %__MODULE__{}

  @spec new(non_neg_integer() | :infinity) :: t()
  def new(max \\ :infinity), do: %__MODULE__{max: max}

  @doc """
  Feed bytes. Returns `{:more, data, dec}` when the body is not
  finished, `{:done, data, rest}` once the last chunk and trailers are
  read (`rest` is bytes past the message), or `{:error, reason}`.
  """
  @spec feed(t(), binary()) ::
          {:more, iodata(), t()} | {:done, iodata(), binary()} | {:error, atom()}
  def feed(%__MODULE__{pending: p} = dec, bytes) when is_binary(bytes) do
    run(%{dec | pending: <<p::binary, bytes::binary>>}, [])
  end

  @doc "Decode a whole chunked body the book has already framed."
  @spec decode_all(binary()) :: {:ok, binary(), binary()} | {:error, atom()}
  def decode_all(bin) when is_binary(bin) do
    case feed(new(), bin) do
      {:done, data, rest} -> {:ok, IO.iodata_to_binary(data), rest}
      {:more, _, _} -> {:error, :incomplete}
      {:error, _} = e -> e
    end
  end

  defp run(%{state: :size, pending: p} = dec, acc) do
    case line(p) do
      {:ok, l, rest} ->
        case size(l) do
          {:ok, 0} -> run(%{dec | state: :trailers, pending: rest}, acc)
          {:ok, n} -> grow(%{dec | state: {:data, n}, pending: rest}, n, acc)
          :error -> {:error, :bad_chunk}
        end

      :more when byte_size(p) > @max_line ->
        {:error, :bad_chunk}

      :more ->
        {:more, Enum.reverse(acc), dec}
    end
  end

  defp run(%{state: {:data, n}, pending: p} = dec, acc) do
    case p do
      <<data::binary-size(^n), rest::binary>> ->
        run(%{dec | state: :data_end, pending: rest}, [data | acc])

      _ when byte_size(p) > 0 ->
        k = byte_size(p)
        {:more, Enum.reverse([p | acc]), %{dec | state: {:data, n - k}, pending: <<>>}}

      _ ->
        {:more, Enum.reverse(acc), dec}
    end
  end

  defp run(%{state: :data_end, pending: p} = dec, acc) do
    case p do
      <<"\r\n", rest::binary>> -> run(%{dec | state: :size, pending: rest}, acc)
      <<"\n", rest::binary>> -> run(%{dec | state: :size, pending: rest}, acc)
      <<"\r">> -> {:more, Enum.reverse(acc), dec}
      <<>> -> {:more, Enum.reverse(acc), dec}
      _ -> {:error, :bad_chunk}
    end
  end

  defp run(%{state: :trailers, pending: p} = dec, acc) do
    case line(p) do
      {:ok, <<>>, rest} -> {:done, Enum.reverse(acc), rest}
      {:ok, _trailer, rest} -> run(%{dec | pending: rest}, acc)
      :more when byte_size(p) > @max_line -> {:error, :bad_chunk}
      :more -> {:more, Enum.reverse(acc), dec}
    end
  end

  defp grow(%{total: t, max: max} = dec, n, acc) do
    if max != :infinity and t + n > max,
      do: {:error, :too_large},
      else: run(%{dec | total: t + n}, acc)
  end

  defp line(p) do
    case :binary.split(p, "\n") do
      [l, rest] -> {:ok, strip_cr(l), rest}
      [_] -> :more
    end
  end

  defp strip_cr(l) do
    case byte_size(l) do
      0 -> l
      k -> if :binary.last(l) == ?\r, do: binary_part(l, 0, k - 1), else: l
    end
  end

  defp size(l) do
    hex = l |> :binary.split(";") |> hd() |> String.trim_trailing()

    if hex != "" and byte_size(hex) <= 16 and hex?(hex),
      do: {:ok, String.to_integer(hex, 16)},
      else: :error
  end

  defp hex?(<<>>), do: true
  defp hex?(<<c, rest::binary>>) when c in ?0..?9 or c in ?a..?f or c in ?A..?F, do: hex?(rest)
  defp hex?(_), do: false
end
