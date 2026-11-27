defmodule MuroPlug.ChunkedTest do
  use ExUnit.Case, async: true

  alias MuroPlug.Chunked

  test "decodes a whole body with extensions and trailers" do
    assert Chunked.decode_all("4;x=y\r\nWiki\r\n5\r\npedia\r\n0\r\nT: 1\r\n\r\n") ==
             {:ok, "Wikipedia", ""}
  end

  test "decodes one byte at a time and keeps the bytes past the message" do
    msg = "3\r\nabc\r\na\r\n0123456789\r\n0\r\n\r\nNEXT"

    {data, rest} =
      for(<<b <- msg>>, do: b)
      |> Enum.reduce({[], Chunked.new()}, fn
        b, {acc, %Chunked{} = dec} ->
          case Chunked.feed(dec, <<b>>) do
            {:more, d, dec} -> {[acc, d], dec}
            {:done, d, rest} -> {[acc, d], {:done, rest}}
          end

        b, {acc, {:done, rest}} ->
          {acc, {:done, rest <> <<b>>}}
      end)
      |> then(fn {acc, {:done, rest}} -> {IO.iodata_to_binary(acc), rest} end)

    assert {data, rest} == {"abc0123456789", "NEXT"}
  end

  test "line endings are strict CRLF, as in the book's chunkScan" do
    assert {:error, :bad_chunk} = Chunked.feed(Chunked.new(), "3\nabc\r\n")
    assert {:error, :bad_chunk} = Chunked.feed(Chunked.new(), "3\r\nabc\n0\r\n\r\n")
    assert {:error, :bad_chunk} = Chunked.feed(Chunked.new(), "0\r\nT: 1\n\r\n")
    assert {:error, :bad_chunk} = Chunked.feed(Chunked.new(), "\n")
    assert {:more, [], _} = Chunked.feed(Chunked.new(), "3\r")
    assert {:ok, "abc", ""} = Chunked.decode_all("3\r\nabc\r\n0\r\n\r\n")
  end

  test "rejects bad sizes and enforces max" do
    assert {:error, :bad_chunk} = Chunked.feed(Chunked.new(), "zz\r\n")
    assert {:error, :bad_chunk} = Chunked.feed(Chunked.new(), "+3\r\nabc\r\n")
    assert {:error, :bad_chunk} = Chunked.feed(Chunked.new(), "3\r\nabcX")
    assert {:error, :too_large} = Chunked.feed(Chunked.new(5), "6\r\n")
  end
end
