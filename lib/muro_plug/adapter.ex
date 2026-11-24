defmodule MuroPlug.Adapter do
  @moduledoc """
  A `Plug.Conn.Adapter` for a muro-http server.

  This is a stub. The callbacks compile and return inert values. They
  do not send a response or read a body until the Thousand Island host
  drives an emitted `step.muro`.
  """

  @behaviour Plug.Conn.Adapter

  @impl true
  def get_http_protocol(_payload), do: :"HTTP/1.1"

  @impl true
  def send_resp(payload, _status, _headers, _body), do: {:ok, nil, payload}

  @impl true
  def send_file(payload, _status, _headers, _file, _offset, _length),
    do: {:ok, nil, payload}

  @impl true
  def send_chunked(payload, _status, _headers), do: {:ok, nil, payload}

  @impl true
  def chunk(payload, _body), do: {:ok, nil, payload}

  @impl true
  def read_req_body(payload, _opts), do: {:ok, "", payload}

  @impl true
  def inform(_payload, _status, _headers), do: {:error, :not_implemented}

  @impl true
  def upgrade(_payload, _protocol, _opts), do: {:error, :not_implemented}

  @impl true
  def push(_payload, _path, _headers), do: {:error, :not_implemented}

  @impl true
  def get_peer_data(_payload),
    do: %{address: {127, 0, 0, 1}, port: 0, ssl_cert: nil}

  @impl true
  def get_sock_data(_payload), do: %{address: {127, 0, 0, 1}, port: 0}

  @impl true
  def get_ssl_data(_payload), do: nil
end
