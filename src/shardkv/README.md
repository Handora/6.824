## Performance challenge: performance limited by replicated log
  All Put operations must go through leader
  Each Put operations requires chatter with followers to replicate
  Idea: shard K/V
    A raft instance per shard
    Operations on different shards can go in parallel
    => increases throughput of K/V service
  Implementation using a master
    Master to coordinate shards
    Stores mapping from keys to shards
    Load balances keys across shards (lab 4)
    Master implemented as its own Raft instance

## Performance challenge for Spinnaker
  Several cohorts share a single disk
  Seeks are slow on magnetic disks
  Solution: share log between different cohorts
  Complications:
    Logical truncation, etc.
