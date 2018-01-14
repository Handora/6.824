package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import crand "math/rand"
import "time"
import "strconv"

var clientNumberIterator = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	rdom         *crand.Rand
	lastLeaderId int
	id           int
	finished     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	s := crand.NewSource(time.Now().UnixNano())
	ck.rdom = crand.New(s)
	ck.lastLeaderId = -1
	ck.id = clientNumberIterator
	clientNumberIterator++
	ck.finished = 0

	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	var i int
	if ck.lastLeaderId < 0 {
		i = ck.rdom.Int() % len(ck.servers)
	} else {
		i = ck.lastLeaderId
	}

	for {
		args := &GetArgs{}
		reply := &GetReply{}
		args.Key = key
		args.Id = strconv.Itoa(ck.id) + ", " + strconv.Itoa(ck.finished)
		ok := ck.servers[i].Call("RaftKV.Get", args, reply)
		if !ok {
			if len(ck.servers) == 1 {
				continue
			}
			tmp := ck.rdom.Int() % len(ck.servers)
			for i == tmp {
				tmp = ck.rdom.Int() % len(ck.servers)
			}
			i = tmp
		} else {
			if reply.WrongLeader {
				i = ck.rdom.Int() % len(ck.servers)
			} else {
				if reply.Err != "" {
					i = ck.rdom.Int() % len(ck.servers)
				} else {
					ck.lastLeaderId = i
					ck.finished++
					return reply.Value
				}
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	var i int
	if ck.lastLeaderId < 0 {
		i = ck.rdom.Int() % len(ck.servers)
	} else {
		i = ck.lastLeaderId
	}

	for {
		args := &PutAppendArgs{}
		reply := &PutAppendReply{}
		args.Key = key
		args.Value = value
		args.Op = op
		args.Id = strconv.Itoa(ck.id) + ", " + strconv.Itoa(ck.finished)
		ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply)
		if !ok {
			if len(ck.servers) == 1 {
				continue
			}
			tmp := ck.rdom.Int() % len(ck.servers)
			for i == tmp {
				tmp = ck.rdom.Int() % len(ck.servers)
			}
			i = tmp
		} else {
			if reply.WrongLeader {
				i = ck.rdom.Int() % len(ck.servers)
			} else {
				if reply.Err != "" {
					i = ck.rdom.Int() % len(ck.servers)
				} else {
					ck.lastLeaderId = i
					ck.finished++
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
