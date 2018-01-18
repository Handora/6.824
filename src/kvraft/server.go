package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
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
	Id    string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvBase    map[string]string
	chanMap   map[int]chan Dispather
	actionMap map[string]string

	curIndex int
	curTerm  int
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if v, ok := kv.actionMap[args.Id]; ok {
		reply.WrongLeader = false
		reply.Value = v
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{"Get", args.Key, "", args.Id}
	oldIndex, oldTerm, ok := kv.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if _, ok := kv.chanMap[oldIndex]; !ok {
		kv.chanMap[oldIndex] = make(chan Dispather)
	}

	c := kv.chanMap[oldIndex]
	kv.mu.Unlock()

	var d Dispather
	select {
	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = "Leadership change"
		return
	case d = <-c:
	}

	kv.mu.Lock()
	delete(kv.chanMap, oldIndex)
	kv.mu.Unlock()
	reply.WrongLeader = false
	reply.Value = d.value
	reply.LeaderId = kv.me
	reply.Err = ""
	// TODO
	//  NEED TO BE IMPROVED
	if d.term != oldTerm || d.key != args.Key {
		reply.WrongLeader = true
		reply.Err = "out-of-dated leader"
		return
	}

	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Op, args.Key, args.Value, args.Id}

	kv.mu.Lock()
	if _, ok := kv.actionMap[args.Id]; ok {
		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	oldIndex, oldTerm, ok := kv.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if _, ok := kv.chanMap[oldIndex]; !ok {
		kv.chanMap[oldIndex] = make(chan Dispather)
	}

	c := kv.chanMap[oldIndex]
	kv.mu.Unlock()

	var d Dispather
	select {
	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = "Leadership change"
		return
	case d = <-c:
	}

	kv.mu.Lock()
	delete(kv.chanMap, oldIndex)
	kv.mu.Unlock()
	// TODO
	//   NEED TO BE IMPROVED
	reply.WrongLeader = false
	reply.LeaderId = kv.me
	reply.Err = ""
	if d.term != oldTerm || d.key != args.Key || d.value != args.Value {
		reply.WrongLeader = true
		reply.Err = "out-of-dated leader"
		return
	}

	return
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

	kv := &RaftKV{}
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.mu = sync.Mutex{}
	kv.actionMap = make(map[string]string, 100)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvBase = make(map[string]string, 100)
	kv.chanMap = make(map[int]chan Dispather)

	// You may need initialization code here.
	go func() {
		for v := range kv.applyCh {
			op := (v.Command).(Op)
			d := Dispather{}
			d.index = v.Index
			d.key = op.Key
			d.value = op.Value
			d.term = v.Term

			kv.mu.Lock()
			value, ok := kv.actionMap[op.Id]
			kv.mu.Unlock()

			if ok {
				kv.mu.Lock()
				d.value = value
				kv.mu.Unlock()
			} else {
				switch op.Op {
				case "Get":
					kv.mu.Lock()
					value, ok := kv.kvBase[op.Key]
					if ok {
						d.value = value
					}
					kv.actionMap[op.Id] = value
					kv.mu.Unlock()
				case "Put":
					kv.mu.Lock()
					kv.kvBase[op.Key] = op.Value
					kv.actionMap[op.Id] = op.Value
					kv.mu.Unlock()
				case "Append":
					kv.mu.Lock()
					value, ok := kv.kvBase[op.Key]
					if !ok {
						kv.kvBase[op.Key] = op.Value
						kv.actionMap[op.Id] = op.Value
					} else {
						kv.kvBase[op.Key] = value + op.Value
						kv.actionMap[op.Id] = value + op.Value
					}
					kv.mu.Unlock()
				}
			}

			go func() {
				kv.mu.Lock()
				c, ok := kv.chanMap[d.index]
				kv.mu.Unlock()
				if ok {
					c <- d
				}
			}()
		}
	}()

	return kv
}
