defmodule MuroPlugTest do
  use ExUnit.Case, async: true

  test "host child_spec starts and stops a Thousand Island listener" do
    pid = start_supervised!({MuroPlug.Host, port: 0})
    assert Process.alive?(pid)
  end

  test "adapter reports HTTP/1.1 and accepts the Plug behaviour calls" do
    payload = %{}
    assert MuroPlug.Adapter.get_http_protocol(payload) == :"HTTP/1.1"
    assert {:ok, nil, ^payload} = MuroPlug.Adapter.send_resp(payload, 200, [], "ok")
    assert {:ok, "", ^payload} = MuroPlug.Adapter.read_req_body(payload, [])

    assert %{address: {127, 0, 0, 1}, port: 0, ssl_cert: nil} =
             MuroPlug.Adapter.get_peer_data(payload)
  end
end
