defmodule MuroPlug.HostTest do
  use ExUnit.Case, async: true

  test "MuroPlug.Host starts a listener that answers 404 by default" do
    pid = start_supervised!({MuroPlug.Host, port: 0})
    {:ok, {_, port}} = ThousandIsland.listener_info(pid)
    r = MuroPlug.TestClient.request(port, "GET / HTTP/1.1\r\nHost: h\r\n\r\n")
    assert {r.status, r.body} == {404, "not found"}
  end

  test "child_spec accepts {plug, opts} and an id" do
    spec = MuroPlug.child_spec(plug: {MuroPlug.NotFound, :x}, port: 0, id: :mine)
    assert spec.id == :mine
  end
end
