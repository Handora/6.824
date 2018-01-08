package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvBase  map[string]string
	chanMap map[Op]chan Dispather

	curIndex int
	curTerm  int
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{"Get", args.Key, ""}
	oldIndex, oldTerm, ok := kv.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.LeaderId = kv.me
	kv.mu.Lock()
	kv.chanMap[op] = make(chan Dispather)
	kv.mu.Unlock()
	d := <-kv.chanMap[op]
	delete(kv.chanMap, op)
	reply.Value = d.value
	if d.index != oldIndex || d.term != oldTerm {
		reply.Err = "out-of-dated leader"
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Op, args.Key, args.Value}
	oldIndex, oldTerm, ok := kv.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.LeaderId = kv.me
	kv.mu.Lock()
	kv.chanMap[op] = make(chan Dispather)
	kv.mu.Unlock()
	d := <-kv.chanMap[op]
	kv.mu.Lock()
	delete(kv.chanMap, op)
	kv.mu.Unlock()
	if d.index != oldIndex || d.term != oldTerm {
		reply.Err = "out-of-dated leader"
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

type Dispather struct {
	key   string
	value string
	index int
	term  int
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvBase = make(map[string]string, 100)
	kv.chanMap = make(map[Op]chan Dispather)

	// You may need initialization code here.
	go func() {
		for v := range kv.applyCh {
			op := (v.Command).(Op)
			d := Dispather{}
			d.index = v.Index
			d.key = op.Key
			d.value = op.Value
			d.term = v.Term

			switch op.Op {
			case "Get":
				value, ok := kv.kvBase[op.Key]
				if ok {
					d.value = value
				}
			case "Put":
				kv.kvBase[op.Key] = op.Value
			case "Append":
				kv.kvBase[op.Key] += op.Value
			}

			go func() {
				kv.chanMap[op] <- d
			}()
		}
	}()

	return kv
}
