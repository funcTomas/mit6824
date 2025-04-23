package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAPPING  = 1
	REDUCING = 2
)

type Task struct {
	TaskType  int
	TaskIndex int
}

func (t Task) String() string {
	if t.TaskType == MAPPING {
		return fmt.Sprintf("map-%d", t.TaskIndex)
	}
	return fmt.Sprintf("reduce-%d", t.TaskIndex)
}

type TaskPool struct {
	sync.RWMutex
	Worker2Map    map[int]int
	Worker2Reduce map[int]int
	MapCnt        int32
	ReduceCnt     int32
	JobStatus     int
}

func (tp *TaskPool) attachJob(taskType, workerId, index int) {
	tp.Lock()
	defer tp.Unlock()
	if taskType != tp.JobStatus {
		return
	}
	if taskType == MAPPING {
		tp.Worker2Map[workerId] = index
	} else if taskType == REDUCING {
		tp.Worker2Reduce[workerId] = index
	}
}

func (tp *TaskPool) detachJob(workerId int) (taskType, taskIndex int, has bool) {
	tp.Lock()
	defer tp.Unlock()
	taskType = tp.JobStatus
	if tp.JobStatus == MAPPING {
		if taskIndex, has = tp.Worker2Map[workerId]; has {
			delete(tp.Worker2Map, workerId)
		}
	} else if tp.JobStatus == REDUCING {
		if taskIndex, has = tp.Worker2Reduce[workerId]; has {
			delete(tp.Worker2Reduce, workerId)
		}
	}
	return
}

func (tp *TaskPool) finishJob(taskType, workerId int) {
	tp.Lock()
	defer tp.Unlock()
	if tp.JobStatus != taskType {
		return
	}
	if tp.JobStatus == MAPPING {
		if _, has := tp.Worker2Map[workerId]; has {
			delete(tp.Worker2Map, workerId)
			tp.MapCnt++
		}
	} else if tp.JobStatus == REDUCING {
		if _, has := tp.Worker2Reduce[workerId]; has {
			delete(tp.Worker2Reduce, workerId)
			tp.ReduceCnt++
		}
	}
}

type Coordinator struct {
	// Your definitions here.
	WorkerIdAlloc int
	WorkerIdMu    sync.Mutex
	HeartBeatMap  map[int]chan struct{}
	HbMu          sync.RWMutex
	taskChan      chan Task
	taskPool      *TaskPool
	FileArr       []string
	NReduce       int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	var err error
	if args.WorkerId == 0 {
		// first come and start heartbeatChan and the deleteChan
		c.WorkerIdMu.Lock()
		c.WorkerIdAlloc++
		reply.WorkerId = c.WorkerIdAlloc
		c.WorkerIdMu.Unlock()
		fmt.Printf("new worker %d\n", reply.WorkerId)

		hbCh := make(chan struct{})
		c.HbMu.Lock()
		c.HeartBeatMap[reply.WorkerId] = hbCh
		c.HbMu.Unlock()
		go func(workerId int, theCh chan struct{}) {
			c.heartBeatCheck(workerId, theCh)
		}(reply.WorkerId, hbCh)
	} else {
		var hbCh chan struct{}
		var has bool
		c.HbMu.RLock()
		hbCh, has = c.HeartBeatMap[args.WorkerId]
		if has {
			hbCh <- struct{}{}
		} else {
			err = errors.New("this worker already timeout")
		}
		c.HbMu.RUnlock()
	}
	return err
}

func (c *Coordinator) PullTask(args *PullTaskArgs, reply *PullTaskReply) error {
	if args.WorkerId == 0 {
		return nil
	}
	select {
	case task := <-c.taskChan:
		reply.TaskType = task.TaskType
		reply.NReduce = c.NReduce
		reply.TaskIndex = task.TaskIndex
		c.taskPool.attachJob(task.TaskType, args.WorkerId, task.TaskIndex)

		if task.TaskType == MAPPING {
			reply.FileName = c.FileArr[task.TaskIndex]
		}
		fmt.Printf("worker %d pull %s\n", args.WorkerId, task)
		go func() {
			c.checkWorkerTimeout(args.WorkerId)
		}()

	default:
		reply.TaskType = 0
		// no task available, worker should idle for a while
	}
	return nil
}

func (c *Coordinator) TaskFinish(args *TaskFinishArgs, reply *TaskFinishReply) error {
	if args.WorkerId == 0 {
		return nil
	}
	c.taskPool.finishJob(args.TaskType, args.WorkerId)
	return nil
}

func (c *Coordinator) heartBeatCheck(workerId int, checkCh chan struct{}) {
	flag := true
	for flag {
		select {
		case <-checkCh:
		case <-time.After(time.Duration(5) * time.Second):
			fmt.Printf("detect offline %d\n", workerId)
			if taskType, taskIndex, has := c.taskPool.detachJob(workerId); has {
				c.taskChan <- Task{TaskType: taskType, TaskIndex: taskIndex}
				fmt.Printf("release worker %d %s\n", workerId, Task{taskType, taskIndex})
			}
			c.HbMu.Lock()
			close(c.HeartBeatMap[workerId])
			delete(c.HeartBeatMap, workerId)
			c.HbMu.Unlock()
			flag = false
		}
	}
}

func (c *Coordinator) checkWorkerTimeout(workerId int) {
	time.Sleep(time.Duration(600) * time.Second)
	if taskType, taskIndex, has := c.taskPool.detachJob(workerId); has {
		c.taskChan <- Task{TaskType: taskType, TaskIndex: taskIndex}
		fmt.Printf("timeout release worker %d %s\n", workerId, Task{taskType, taskIndex})
	}

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	c.taskPool.RLock()
	defer c.taskPool.RUnlock()
	ret = c.taskPool.JobStatus == REDUCING && c.taskPool.ReduceCnt == int32(c.NReduce)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		WorkerIdAlloc: 0,
		WorkerIdMu:    sync.Mutex{},
		HeartBeatMap:  make(map[int]chan struct{}),
		HbMu:          sync.RWMutex{},
		taskChan:      make(chan Task),
		taskPool: &TaskPool{
			Worker2Map:    make(map[int]int),
			Worker2Reduce: make(map[int]int),
			MapCnt:        0,
			ReduceCnt:     0,
			JobStatus:     MAPPING,
		},
		FileArr: files,
		NReduce: nReduce,
	}

	// Your code here.

	go func() {
		for j := range len(files) {
			c.taskChan <- Task{TaskType: MAPPING, TaskIndex: j}
		}
	}()

	c.server()

	go func() {
		flag := true
		ticker := time.NewTicker(time.Duration(3) * time.Second)
		for flag {
			<-ticker.C
			c.taskPool.Lock()
			if c.taskPool.MapCnt == int32(len(c.FileArr)) {
				c.taskPool.JobStatus = REDUCING
				flag = false
			}
			c.taskPool.Unlock()
		}
		for j := range c.NReduce {
			c.taskChan <- Task{TaskType: REDUCING, TaskIndex: j}
		}
	}()

	return &c
}
