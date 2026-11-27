defmodule MuroPlug.Host do
  @moduledoc """
  The Thousand Island host. Kept for the scaffold's name: it is
  `MuroPlug.child_spec/1` with `port: 0` as the default.
  """

  @doc "A child spec for a MuroPlug listener. See `MuroPlug` for options."
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    opts
    |> Keyword.put_new(:port, 0)
    |> Keyword.put_new(:plug, MuroPlug.NotFound)
    |> Keyword.put_new(:id, __MODULE__)
    |> MuroPlug.child_spec()
  end
end
