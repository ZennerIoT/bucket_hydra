defmodule ApiRateLimit do
  use BucketHydra,
    pubsub: BucketHydraTest.PubSub
end
