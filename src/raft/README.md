# raft implementation tips

- heartbeats RPCs should also ensure the consensus of raft
- four errors: livelocks, incorrect or incomplete RPC handlers, failure to follow The Rules, and term confusion.


# unread questions
> What exactly do the C_old and C_new variables in section 6 (and figure
> 11) represent? Are they the leader in each configuration?

They are the set of servers in the old/new configuration.


# Not truly understood things
Another issue many had (often immediately after fixing the issue above), was that, upon receiving a heartbeat, they would truncate the follower’s log following prevLogIndex, and then append any entries included in the AppendEntries arguments. This is also not correct. We can once again turn to Figure 2:

` If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it. `

The if here is crucial. If the follower has all the entries the leader sent, the follower MUST NOT truncate its log. Any elements following the entries sent by the leader MUST be kept. This is because we could be receiving an outdated AppendEntries RPC from the leader, and truncating the log would mean “taking back” entries that we may have already told the leader that we have in our log.
