defmodule MuroPlug.NotFound do
  @moduledoc "A plug that answers every request with 404. The host's default."
  @behaviour Plug

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts), do: Plug.Conn.send_resp(conn, 404, "not found")
end
