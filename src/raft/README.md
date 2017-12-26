# some awesome way to enhance the performance
# this is awesome
if follower rejects, includes this in reply:
    the follower's term in the conflicting entry
    the index of follower's first entry with that term
  if leader knows about the conflicting term:
    move nextIndex[i] back to leader's last entry for the conflicting term
  else:
    move nextIndex[i] back to follower's first index

# lab3
reject if term is old (not the current leader)
  reject (ignore) if follower already has last included index/term
    it's an old/delayed RPC
  follower empties its log, replaces with fake "prev" entry
  set lastApplied to lastIncludedIndex
  send snapshot in applyCh to service
  service replaces its k/v table with snapshot contents
