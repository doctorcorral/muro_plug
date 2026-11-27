defmodule MuroPlug.PlugRouterTest do
  use ExUnit.Case, async: true

  defmodule Router do
    use Plug.Router

    plug(Plug.Head)
    plug(Plug.Parsers, parsers: [:urlencoded, :multipart])
    plug(:match)
    plug(:dispatch)

    get "/hello/:name" do
      send_resp(conn, 200, "hello #{name}")
    end

    post "/form" do
      conn
      |> put_resp_content_type("text/plain")
      |> send_resp(201, "#{conn.body_params["a"]}+#{conn.body_params["b"]}")
    end

    get "/static" do
      conn
      |> put_resp_content_type("text/plain")
      |> send_file(200, Path.expand("../mix.exs", __DIR__))
    end

    match _ do
      send_resp(conn, 404, "nope")
    end
  end

  setup_all do
    {:ok, _} = Application.ensure_all_started(:inets)
    :ok
  end

  setup do
    {:ok, port: MuroPlug.TestClient.serve(Router)}
  end

  defp url(port, path), do: ~c"http://127.0.0.1:#{port}#{path}"

  test "a Plug.Router app served end to end through :httpc", %{port: port} do
    assert {:ok, {{_, 200, _}, _, ~c"hello muro"}} =
             :httpc.request(:get, {url(port, "/hello/muro"), []}, [], [])

    assert {:ok, {{_, 201, _}, headers, ~c"1+two"}} =
             :httpc.request(
               :post,
               {url(port, "/form"), [], ~c"application/x-www-form-urlencoded", ~c"a=1&b=two"},
               [],
               []
             )

    assert {~c"content-type", ~c"text/plain; charset=utf-8"} in headers

    assert {:ok, {{_, 404, _}, _, ~c"nope"}} =
             :httpc.request(:get, {url(port, "/missing"), []}, [], [])

    mix = File.read!(Path.expand("../mix.exs", __DIR__))

    assert {:ok, {{_, 200, _}, _, body}} =
             :httpc.request(:get, {url(port, "/static"), []}, [], body_format: :binary)

    assert body == mix

    assert {:ok, {{_, 200, _}, head_headers, []}} =
             :httpc.request(:head, {url(port, "/hello/x"), []}, [], [])

    assert {~c"content-length", ~c"7"} in head_headers
  end

  test "many sequential requests over one keep-alive connection", %{port: port} do
    s = MuroPlug.TestClient.connect(port)

    for i <- 1..200 do
      :ok = :gen_tcp.send(s, "GET /hello/#{i} HTTP/1.1\r\nHost: h\r\n\r\n")
      assert {:ok, %{status: 200, body: body}, ""} = MuroPlug.TestClient.read(s)
      assert body == "hello #{i}"
    end
  end

  test "concurrent clients" do
    port = MuroPlug.TestClient.serve(Router)

    1..50
    |> Task.async_stream(
      fn i ->
        MuroPlug.TestClient.request(port, "GET /hello/#{i} HTTP/1.1\r\nHost: h\r\n\r\n").body
      end,
      max_concurrency: 50
    )
    |> Enum.each(fn {:ok, body} -> assert body =~ "hello " end)
  end
end
