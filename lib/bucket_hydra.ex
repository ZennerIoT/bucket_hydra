defmodule BucketHydra do
  import Record

  @moduledoc """
  A bucket-based rate limit algorithm with support for clusters.

  ## Usage

  Create a module that acts as the rate limiter:

      defmodule MyApiRateLimit do
        use #{inspect __MODULE__},
          pubsub: MyAppWeb.Endpoint,
          size: 50,
          time_scale: :timer.minutes(5)
      end


  Start your rate limiter in each node of the cluster:

      children = [
        # ...
        MyApiRateLimit,
        # ...
      ]

  Consume space in a bucket with `consume/2`, for instance in a plug that guards
  your API:

      def call(conn, opts) do
        # ...
        if MyApiRateLimit.consume(conn.assigns.api_key) do
          conn
        else
          conn
          |> send_resp(429, "Rate limit exceeded")
          |> halt()
        end
      end

  ## Clustering

  #{inspect __MODULE__} was specifically built to act as a rate limiter in a multi-
  node cluster. It syncronizes the usages across all instances of itself using
  `Phoenix.PubSub` broadcasts.

  If you're not using phoenix, you can still use the `phoenix_pubsub` library
  and simply start a pubsub (a good starting point is `Phoenix.PubSub.PG2`)
  instance in your application supervisor.

  ## Caveats

  When a bucket only has one space left and 2 requests for the same bucket
  arrive at the same time, both might be allowed to happen.
  #{
    # TODO we could start another ets table that's public and global, and
    # use that ets to obtain a lock for each key while we figure out `can_consume/2`
    # in an atomic way
  }

  In cases of a net split, the resulting clusters will work on their own and
  might allow more incoming requests in total. After the net split heals,
  all nodes are synchronized again.
  """

  @typedoc """
  The key for a bucket can be anything but we recommend using primitive IDs such as
  UUIDs, integers or atoms.
  """
  @type key :: term

  @type key_record :: record(:key, key: key, time_scale: pos_integer(), time_bucket: pos_integer())
  @doc false
  defrecord(:key, key: nil, time_scale: nil, time_bucket: nil)

  @type bucket :: record(:bucket, key: key_record, consumed: pos_integer())
  @doc """
  Generated record functions to interact with buckets returned by `get_bucket/2`.
  """
  defrecord(:bucket, key: nil, consumed: 0)

  @type consumption :: record(:consumption, key: key, cost: pos_integer())
  @doc false
  defrecord(:consumption, key: nil, cost: 1)

  @typedoc """
  `:size` a positive integer of how big the bucket is, default 120

  `:time_scale` a positive integer in milliseconds how long a bucket lasts until it is emptied, default 60_000 ms

  `:cost` if consuming from a bucket should cost more than 1 use, specify the increased cost here, default 1

  `:for_time` default `System.system_time(:millisecond)` - a different time (in milliseconds) can be passed so a
  request might fall into an older or newer bucket.
  """
  @type bucket_opt :: {:size, non_neg_integer()} | {:time_scale, non_neg_integer()} | {:cost, non_neg_integer()}

  @type bucket_opts :: [bucket_opt]

  @doc """
  Checks if the bucket for the given key still has enough space, and then consumes
  that space.

  Returns true if there was space left and consumed.
  """
  @callback consume(key, bucket_opts) :: boolean()

  @doc """
  Checks if the bucket for the given key still has enough space without
  consuming that space.
  """
  @callback can_consume?(key, bucket_opts) :: boolean()

  @doc """
  Gets the bucket for the given key, time_scale and time (via bucket_opts).
  """
  @callback get_bucket(key, bucket_opts) :: {:ok, bucket}

  @doc """
  Consumes space in the bucket without checking for capacity
  """
  @callback do_consume(key, bucket_opts) :: :ok

  @doc """
  Calculates the time bucket for the given scale that a request for the given
  time would fall into.
  """
  @callback time_bucket(time_scale :: pos_integer(), for_time :: pos_integer()) :: pos_integer()

  defmacro __using__(opts) do
    quote location: :keep do
      @behaviour unquote(__MODULE__)
      use GenServer
      require Logger
      import unquote(__MODULE__)

      @pubsub_server Keyword.get(unquote(opts), :pubsub)
      @pubsub_topic "#{unquote(__MODULE__)}_#{__MODULE__}_channel"
      @ets :"#{__MODULE__}_table"

      @default_time_scale Keyword.get(unquote(opts), :time_scale, 10_000)
      @default_size Keyword.get(unquote(opts), :size, 120)

      def start_link(opts \\ []) do
        opts = Keyword.put(opts, :name, __MODULE__)
        GenServer.start_link(__MODULE__, opts, opts)
      end

      # PUBLIC CALLBACK IMPLEMENTATIONS

      def consume(key, opts \\ [])
      def consume(key, opts) do
        if can_consume?(key, opts) do
          do_consume(key, opts)
          true
        else
          false
        end
      end

      def can_consume?(key, opts \\ [])
      def can_consume?(key, opts) do
        cost = Keyword.get(opts, :cost, 1)
        size = Keyword.get(opts, :size, @default_size)
        {:ok, bucket(consumed: consumed)} = get_bucket(key, opts)

        consumed + cost <= size
      end

      def get_bucket(key, opts \\ []) do
        time_scale = Keyword.get(opts, :time_scale, @default_time_scale)
        for_time = Keyword.get(opts, :for_time, System.system_time(:millisecond))
        time_bucket = time_bucket(for_time, time_scale)
        key = key(key: key, time_scale: time_scale, time_bucket: time_bucket)

        case :ets.select(@ets, ms(key)) do
          [] ->
            {:ok, bucket(key: key, consumed: 0)}

          [bucket] ->
            {:ok, bucket}
        end
      end

      def do_consume(key, opts \\ []) do
        time_scale = Keyword.get(opts, :time_scale, @default_time_scale)
        for_time = Keyword.get(opts, :for_time, System.system_time(:millisecond))
        cost = Keyword.get(opts, :cost, 1)
        time_bucket = time_bucket(for_time, time_scale)
        key = key(key: key, time_scale: time_scale, time_bucket: time_bucket)
        Phoenix.PubSub.broadcast(@pubsub_server, @pubsub_topic, consumption(key: key, cost: cost))
        :ok
      end

      def time_bucket(for_time \\ System.system_time(:millisecond), time_scale) do
        for_time - Integer.mod(for_time, time_scale)
      end

      # PRIVATE CALLBACK IMPLEMENTATIONS

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          restart: :permanent,
          shutdown: 500,
          type: :worker
        }
      end

      @doc false
      def init(opts) do
        name = Keyword.get(opts, :name, __MODULE__)

        :ets.new(@ets, [
          :set,
          :named_table,
          :protected,
          {:keypos, 2}
        ])

        :timer.send_interval(Keyword.get(opts, :cleanup_every, 60_000), :cleanup)

        Phoenix.PubSub.subscribe(@pubsub_server, @pubsub_topic)
        Phoenix.PubSub.broadcast_from!(@pubsub_server, self(), @pubsub_topic, {:init, self()})

        :net_kernel.monitor_nodes(true)

        {:ok, []}
      end

      def handle_info(consumption() = consumption, state) do
        __consume(consumption, state)
        {:noreply, state}
      end

      def handle_info(:cleanup, state) do
        # ms needs to update every time bucket or key spec changes
        ms = [{
          {:bucket, {:key, :_, :"$2", :"$1"}, :_},
          [],
          [{:<, {:+, :"$1", :"$2"}, System.system_time(:millisecond)}]
        }]
        case :ets.select_delete(@ets, ms) do
          {:error, _} = e ->
            Logger.error(inspect e)

          0 ->
            :ok

          n ->
            :telemetry.execute([__MODULE__, :cleaned_up], %{buckets: n}, %{})
        end
        {:noreply, state}
      end

      def handle_info({:init, from}, state) do
        send(from, {:init_data, :ets.tab2list(@ets)})
        {:noreply, state}
      end

      def handle_info({:init_data, buckets}, state) do
        Enum.each(buckets, fn bucket(key: key, consumed: consumed) = bucket ->
          case :ets.select(@ets, ms(key)) do
            [] -> :ets.insert(@ets, bucket)
            [bucket(consumed: c)] when c >= consumed -> :ok
            [bucket(consumed: c)] when c < consumed -> :ets.insert(@ets, bucket)
          end
        end)
        {:noreply, state}
      end

      def handle_info({:nodeup, node}, state) do
        msg = {:init_data, :ets.tab2list(@ets)}
        Phoenix.PubSub.direct_broadcast(node, @pubsub_server, @pubsub_topic, msg)
        {:noreply, state}
      end

      def handle_info({:nodedown, _node}, state) do
        {:noreply, state}
      end

      # UTIL FUNS
      defp make_bucket(consumption(key: key, cost: c)) do
        bucket(key: key, consumed: c)
      end

      defp ms(key() = key) do
        # needs to be rewritten every time the bucket record changes
        [
          {
            {:bucket, :"$1", :_},
            [
              {:==, :"$1", {:const, key}}
            ],
            [:"$_"]
          }
        ]
      end
      defp ms(consumption(key: key)) do
        ms(key)
      end

      defp __consume(consumption(cost: cost) = consumption, state) do
        case :ets.select(@ets, ms(consumption)) do
          [] ->
            :ets.insert(@ets, make_bucket(consumption))

          [bucket(consumed: consumed) = bucket] ->
            :ets.insert(@ets, bucket(bucket, consumed: consumed + cost))
        end
      end
    end
  end
end
