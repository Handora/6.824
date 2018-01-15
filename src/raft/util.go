package raft

import (
	"log"
)

// Debugging
const Debug = 0

var STATE = [3]string{"FOLLOWER", "LEADER", "CANDIDATE"}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
