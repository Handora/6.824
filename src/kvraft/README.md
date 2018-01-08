## for Put (correct, retry with duplicate detection)
   Client keeps retrying until response from service
     If failure, try other server
     Add seqno to client requests
     When Put() is successful, increase seqno
   Server maintain duplicate detection table
     When server executes op from log:
       if !duplicate[clientid] = seqno:
          res = perform Put on K/V
          duplicate[clientid] = res
       if leader:
          send duplicate[clientid] to client

## for Get
  Simple solution:
    Run Get() operations also through the log, like Put()s
    Lab 3 does this
  Raft paper
    Don't run Get() through log, but make sure leader has last committed operation
    New primary adds a null entry after becoming primary
    Primary pings followers to see if it is still the primary before processing Get()


## Engineering challenge: making snapshots
  K/V lives above Raft
  Log lives in Raft
  Solution:
    K/V makes shapshots
    K/V tells Raft about shapshot, including LSN for snapshot
    Raft tells K/V to install snapshots on applyCh
  Why do communicate snapshots over applyCh?
  Where do they appear on applyCh?
  Must snapshot include duplicate-detection table?

## Engineering challenge: coordination between K/V server and Raft library
  K/V server must have locks to protect its state
  Raft library has locks to protect its state
  Upcalls delivered through applyCh
  Easy to get deadlocks. For example on primary:
    Raft goroutine holds Raft lock
      E.g., received response from follower
    Sends on applyCh while holding lock
      the follower indicated a new log entry has been committed
      holds lock because log entries must be inserted in order in applyCh
    Goroutine blocks because  applyCh is full
    Goroutine in K/V layer calls Raft library for Start(), or Snapshot()
      library already holds lock, so K/V goroutine blocks
    No Goroutine reads from applyCh
  Need to have a plan to manage concurrency

### One possible plan:
  Always acquire locks in specific order
     E.g., K/V lock, then Raft lock
     How to deal with upcalls? (calls from Raft to K/V)
  Raft:
    RPC reply handlers never write to applyCh
    Separate goroutine for writing to applyCh
      release raft lock before writing to applyCh
      order will be ok, because only one goroutine that sends on applyCh
  K/V layer
    One goroutine that reads from applyCh and applies Ops on K/V
    Release K/V lock before reading from applyCh
