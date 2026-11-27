defmodule MuroPlug.HeadersTest do
  use ExUnit.Case, async: true

  alias MuroPlug.{HTTP, Step}

  @corpus [
    "GET / HTTP/1.1\r\nHost: h\r\n\r\n",
    "GET /a?b HTTP/1.1\r\nHost: example.com\r\nUser-Agent: curl/8.0\r\nAccept: */*\r\n\r\n",
    "POST /p HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nX-Pad:   spaced value \t\r\n\r\nabcd",
    "GET / HTTP/1.1\r\nHOST: h\r\nX-Empty:\r\nX-Colon: a:b:c\r\n\r\n",
    "GET / HTTP/1.0\r\nCookie: a=1; b=2\r\nCookie: c=3\r\n\r\n"
  ]

  test "the host's header split matches the book's headerName / headerValue" do
    for buf <- @corpus do
      hend = Step.headerEnd(buf)
      ours = HTTP.headers(binary_part(buf, 0, hend))
      n = Step.headerCount(buf)

      book =
        for i <- 0..(n - 1)//1,
            do: {String.downcase(Step.headerName(buf, i)), Step.headerValue(buf, i)}

      assert ours == book, "differs on #{inspect(buf)}"
    end
  end

  test "names are lowercased and values trimmed" do
    assert HTTP.headers("GET / HTTP/1.1\r\nX-A:  1 \r\nx-b:2\r\n\r\n") ==
             [{"x-a", "1"}, {"x-b", "2"}]
  end
end
