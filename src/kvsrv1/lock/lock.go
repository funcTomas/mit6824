package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	idx     string
	version rpc.Tversion
	l       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.idx = kvtest.RandValue(8)
	lk.l = l
	lk.version = rpc.Tversion(0)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// if lk.version == 0, all client try to Put first
		// but only one could succeed
		// then all the failed try to Get the newest version of the key
		// keep trying to Put with the version it kept
		ok := lk.ck.Put(lk.l, lk.idx, lk.version)
		if ok == rpc.OK {
			// acquire success
			lk.version++
			break
		} else {
			value, version, _ := lk.ck.Get(lk.l)
			if ok == rpc.ErrVersion {
				if value == "" {
					// another client just released and all other compete the lock
					lk.version = version
				} else {
					// another client already acquried success
				}
			} else if ok == rpc.ErrMaybe {
				if value == lk.idx {
					// the restransmited for the acquired success
					lk.version++
					break
				} else {
					// another client already acquired success
				}
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	// possible rpc.Err are OK, ErrMaybe and ErrVersion
	lk.ck.Put(lk.l, "", rpc.Tversion(lk.version))
}
