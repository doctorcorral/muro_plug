defmodule MuroPlug.HTTP do
  @moduledoc false
  # Wire rendering and header splitting for the host. The book has
  # already validated the request head before any of this runs.

  @doc """
  Split a header block the book has validated into Plug's
  `[{lowercase_name, value}]`. `head` is the request line, the header
  lines, and the blank line (`hend` bytes). Values are trimmed of
  optional whitespace, the same views as the book's `headerName` /
  `headerValue` (test/headers_test.exs compares them).
  """
  @spec headers(binary()) :: [{binary(), binary()}]
  def headers(head) when is_binary(head) do
    case :binary.split(head, "\n") do
      [_request_line, rest] -> split_lines(rest, [])
      [_] -> []
    end
  end

  defp split_lines(<<>>, acc), do: Enum.reverse(acc)

  defp split_lines(bin, acc) do
    {line, rest} =
      case :binary.split(bin, "\n") do
        [l, r] -> {l, r}
        [l] -> {l, <<>>}
      end

    case strip_cr(line) do
      <<>> ->
        Enum.reverse(acc)

      l ->
        case :binary.split(l, ":") do
          [name, value] -> split_lines(rest, [{downcase(name), trim(value)} | acc])
          [_] -> split_lines(rest, acc)
        end
    end
  end

  defp strip_cr(l) do
    case byte_size(l) do
      0 -> l
      k -> if :binary.last(l) == ?\r, do: binary_part(l, 0, k - 1), else: l
    end
  end

  defp downcase(name), do: for(<<c <- name>>, into: <<>>, do: <<lower(c)>>)
  defp lower(c) when c in ?A..?Z, do: c + 32
  defp lower(c), do: c

  @doc false
  def trim(v), do: v |> trim_lead() |> trim_trail()

  defp trim_lead(<<c, rest::binary>>) when c in [?\s, ?\t], do: trim_lead(rest)
  defp trim_lead(v), do: v

  defp trim_trail(v) do
    case byte_size(v) do
      0 ->
        v

      k ->
        if :binary.last(v) in [?\s, ?\t], do: trim_trail(binary_part(v, 0, k - 1)), else: v
    end
  end

  @doc "Status line and headers. `extra` are host headers added before Plug's."
  @spec head(Plug.Conn.status(), Plug.Conn.headers(), [{binary(), binary()}]) :: iodata()
  def head(status, headers, extra) do
    [
      "HTTP/1.1 ",
      Integer.to_string(status),
      ?\s,
      reason(status),
      "\r\n",
      Enum.map(extra ++ headers, fn {k, v} -> [k, ": ", v, "\r\n"] end),
      "\r\n"
    ]
  end

  @doc "A protocol status the book owes: no body, and the connection closes."
  @spec status_close(non_neg_integer()) :: iodata()
  def status_close(code) do
    head(code, [], [{"content-length", "0"}, {"connection", "close"}, {"date", date()}])
  end

  @doc "The `100 Continue` interim (span 6)."
  def continue, do: "HTTP/1.1 100 Continue\r\n\r\n"

  defp reason(status) do
    Plug.Conn.Status.reason_phrase(status)
  rescue
    ArgumentError -> "Unknown"
  end

  @doc "RFC 9110 IMF-fixdate, cached per second in the calling process."
  @spec date() :: binary()
  def date do
    now = System.os_time(:second)

    case Process.get(:muro_plug_date) do
      {^now, d} ->
        d

      _ ->
        d = imf(:calendar.system_time_to_universal_time(now, :second))
        Process.put(:muro_plug_date, {now, d})
        d
    end
  end

  @days {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
  @months {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}

  defp imf({{y, mo, d} = date, {h, mi, s}}) do
    dow = elem(@days, :calendar.day_of_the_week(date) - 1)

    IO.iodata_to_binary([
      dow,
      ", ",
      pad(d),
      ?\s,
      elem(@months, mo - 1),
      ?\s,
      Integer.to_string(y),
      ?\s,
      pad(h),
      ?:,
      pad(mi),
      ?:,
      pad(s),
      " GMT"
    ])
  end

  defp pad(n) when n < 10, do: [?0, Integer.to_string(n)]
  defp pad(n), do: Integer.to_string(n)
end
