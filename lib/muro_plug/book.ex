defmodule MuroPlug.Book do
  @moduledoc """
  A thin, total reading of the emitted `step.muro` (`MuroPlug.Step`).

  The book decides every protocol fact of one HTTP/1.1 connection:
  when a request is complete, where it ends, whether it keeps the
  connection, which protocol status (400, 413, 414, 417, 431, 501) is
  owed, when a `100 Continue` interim is owed, and how a handler
  response is framed. This module only names the fields of the
  book's `Out` and `St` products and feeds it facts. It does not make
  protocol decisions of its own.

  The buffer passed to the book is always a binary (`Slice` on the
  BEAM). Nothing here builds a list of octets.
  """

  alias MuroPlug.Step

  # Fact numbers are c/step.h in muro-http.
  @accepted 1
  @bytes 2
  @peer 3
  @wrote 4
  @failed 5
  @now 6

  @typedoc "The book's `St`: `{:st, phase, code, left, keep, hend, te, sawCL, cl}`."
  @type st :: tuple()

  @typedoc "The book's `Out` product, decoded by `out/1`."
  @type out :: %{
          raw: tuple(),
          phase: non_neg_integer(),
          code: non_neg_integer(),
          close: 0 | 1,
          status_now: 0 | 1,
          read_more: 0 | 1,
          write_more: 0 | 1,
          span: non_neg_integer(),
          cover: 0 | 1,
          left: non_neg_integer(),
          wake: non_neg_integer(),
          keep: 0 | 1,
          hend: non_neg_integer(),
          te: 0 | 1,
          saw_cl: 0 | 1,
          cl: non_neg_integer()
        }

  @doc "The book's initial read state."
  @spec read_st() :: st()
  def read_st, do: Step.readSt()

  @doc "Feed one fact to the book. `fact` is an atom; `buf` the unconsumed buffer."
  @spec step(st(), atom(), binary(), map(), non_neg_integer(), non_neg_integer()) :: out()
  def step(st, fact, buf, cfg, n \\ 0, owed \\ 0) when is_binary(buf) do
    st
    |> Step.stepFact(code(fact), buf, cfg.max_body, n, owed, now(), cfg.deadline, cfg.idle)
    |> out()
  end

  @doc """
  Feed the handler's result to the book (`hand` in step.muro).

  Returns the framing the book chose: span 2 is a buffered body,
  3 a content-length pull (`send_file`), 4 chunked, 5 close-delimited;
  `keep` 0 keeps the connection, 1 closes it.
  """
  @spec hand(out(), keyword()) :: out()
  def hand(%{} = req, flags) do
    Step.hand(
      req.keep,
      bit(flags[:close]),
      bit(flags[:bad]),
      bit(Keyword.get(flags, :owe, true)),
      bit(flags[:refused]),
      Keyword.get(flags, :pull, 0),
      bit(flags[:http11]),
      bit(flags[:head]),
      bit(Keyword.get(flags, :fit, true)),
      req.hend,
      req.te,
      req.saw_cl,
      req.cl
    )
    |> out()
  end

  @doc "The book's `St` for an `Out` (`take` in step.muro)."
  @spec take(out()) :: st()
  def take(%{raw: raw}), do: Step.take(raw)

  @doc "First byte past the request this `Out` describes (`framedEndAt`)."
  @spec framed_end(out(), binary()) :: non_neg_integer()
  def framed_end(%{} = o, buf) when is_binary(buf),
    do: Step.framedEndAt(o.hend, o.te, o.saw_cl, o.cl, buf)

  @doc "True when the chunked body after `hend` is finished in `buf` (`ckDone`)."
  @spec chunk_done?(out(), binary()) :: boolean()
  def chunk_done?(%{hend: hend}, buf) when is_binary(buf),
    do: Step.ckDone(Step.chunkScanAt(hend, buf))

  @doc "Request-line views over the buffer."
  @spec views(binary()) :: %{
          method: binary(),
          target: binary(),
          path: binary(),
          query: binary(),
          version: binary()
        }
  def views(buf) when is_binary(buf) do
    %{
      method: Step.methodView(buf),
      target: Step.targetView(buf),
      path: Step.pathView(buf),
      query: Step.queryView(buf),
      version: Step.versionView(buf)
    }
  end

  @doc "Decode the book's `Out` tuple into a map."
  @spec out(tuple()) :: out()
  def out(
        {:out, phase, code, close, status_now, read_more, write_more, span, cover, left, wake,
         keep, hend, te, saw_cl, cl} = raw
      ) do
    %{
      raw: raw,
      phase: phase,
      code: code,
      close: close,
      status_now: status_now,
      read_more: read_more,
      write_more: write_more,
      span: span,
      cover: cover,
      left: left,
      wake: wake,
      keep: keep,
      hend: hend,
      te: te,
      saw_cl: saw_cl,
      cl: cl
    }
  end

  defp code(:accepted), do: @accepted
  defp code(:bytes), do: @bytes
  defp code(:peer), do: @peer
  defp code(:wrote), do: @wrote
  defp code(:failed), do: @failed
  defp code(:now), do: @now

  defp bit(true), do: 1
  defp bit(1), do: 1
  defp bit(_), do: 0

  defp now, do: System.system_time(:millisecond)
end
