defmodule BucketHydraTest do
  use ExUnit.Case
  doctest BucketHydra

  test "" do
    import BucketHydra
    opts = [
      # set for_time so we don't run into boundary issues (unless we want to)
      for_time: System.system_time(:millisecond),
      size: 10
    ]
    for i <- 1..10 do
      assert ApiRateLimit.consume("abc", opts) == true
      wait_for fn ->
        {:ok, bucket(consumed: consumed)} = ApiRateLimit.get_bucket("abc", opts)
        consumed == i
      end
    end
    refute ApiRateLimit.consume("abc", opts)
  end

  def wait_for(fun, retries \\ 50)
  def wait_for(_, 0) do
    raise :waited_too_much
  end
  def wait_for(fun, retries) do
    if fun.() do
      :ok
    else
      Process.sleep(50)
      wait_for(fun, retries - 1)
    end
  end
end
