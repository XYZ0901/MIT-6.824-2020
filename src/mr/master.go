package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Task struct {
	taskType_ int
	isCompleted_ bool
	isDistributed_ bool
	index_ int
}

type Master struct {
	// Your definitions here.
	files []string

	mapTasks []Task

	reduceTasks []Task

	completedMapCount int64

	completeReduceCount int64

	mutex sync.Mutex

	isFinished_ bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m* Master) waitForTask(index int,taskType int) {
	timer := time.NewTimer(time.Second * 10)
	<-timer.C
	m.mutex.Lock()
	if taskType == MAP {
		if m.mapTasks[index].isCompleted_ == false {
			m.mapTasks[index].isDistributed_ = false
		}
	} else {
		if m.reduceTasks[index].isCompleted_ == false {
			m.reduceTasks[index].isDistributed_ = false
		}
	}
	m.mutex.Unlock()
}

func (m* Master) AskTask (args *AskTaskRequest,reply *AskTaskResponse) error {
	if m.IsFinishedReduce() {
		reply.TaskType_ = NONE
		return nil
	}
	if m.IsFinishedMap() {
		task := m.tryGetTask(REDUCE)
		if task.taskType_ == NONE {
			reply.TaskType_ = NONE
			return nil
		}
		reply.TaskType_ = REDUCE
		reply.Id_ = task.index_
		//fmt.Printf("master send %v reduce \n",reply.Id_)
		//timer
		go m.waitForTask(reply.Id_,REDUCE)
	} else {
		task := m.tryGetTask(MAP)
		if task.taskType_ == NONE {
			reply.TaskType_ = NONE
			return nil
		}
		reply.TaskType_ = MAP
		reply.Id_ = task.index_
		//fmt.Printf("master send %v map \n",reply.Id_)
		reply.FileNames_ = append(reply.FileNames_, m.getFile(reply.Id_))
		//timer
		go m.waitForTask(reply.Id_,MAP)
	}
	return nil
}

func (m* Master) SubmitTask(args *SubmitTaskRequest,reply *SubmitTaskResponse) error{
	m.mutex.Lock()
	if args.TaskType_ == MAP && m.mapTasks[args.Index].isCompleted_ == false {
		m.mapTasks[args.Index].isCompleted_ = true
		if m.completedMapCount == -1{
			m.completedMapCount = 0
		}
		m.completedMapCount++
		//fmt.Printf("compelet map %v \n",args.Index)
	} else if args.TaskType_ == REDUCE && m.reduceTasks[args.Index].isCompleted_ == false {
		m.reduceTasks[args.Index].isCompleted_ = true
		if m.completeReduceCount == -1{
			m.completeReduceCount = 0
		}
		m.completeReduceCount++
		//fmt.Printf("compelet reduce %v \n",args.Index)
		if m.completeReduceCount == int64(len(m.reduceTasks)) {
			m.isFinished_ = true
		}
	}
	m.mutex.Unlock()
	return  nil
}

func (m* Master) getFile(index int) string {
	var file string
	m.mutex.Lock()
	file = m.files[index]
	m.mutex.Unlock()
	return file
}
func (m* Master) IsFinishedMap() bool {
	isFinished := false
	m.mutex.Lock()
	if m.completedMapCount >= int64(len(m.mapTasks)) {
		isFinished = true
	}
	m.mutex.Unlock()
	return  isFinished
}
func (m* Master) IsFinishedReduce() bool {
	isFinished := false
	m.mutex.Lock()
	if m.completeReduceCount >= int64(10) {
		isFinished = true
	}
	m.mutex.Unlock()
	return isFinished
}


func (m* Master) tryGetTask(taskType int) Task{
	task := Task{}
	task.taskType_ = NONE
	m.mutex.Lock()
	if taskType == MAP {
		for i := 0; i < len(m.mapTasks); i++ {
			if m.mapTasks[i].isDistributed_ == false{
				m.mapTasks[i].isDistributed_ = true
				task = m.mapTasks[i]
				break
			}
		}
	} else if taskType == REDUCE {
		for i := 0; i < len(m.reduceTasks); i++ {
			if m.reduceTasks[i].isDistributed_ == false{
				m.reduceTasks[i].isDistributed_ = true
				task = m.reduceTasks[i]
				break
			}
		}
	}
	m.mutex.Unlock()
	return task
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	m.mutex.Lock()
	ret = m.isFinished_
	m.mutex.Unlock()
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	//os.RemoveAll(Dir)
	m := Master{}
	m.files = files
	m.isFinished_ = false
	m.completedMapCount = -1
	m.completeReduceCount = -1
	m.mutex = sync.Mutex{}
	//init mapTask
	if len(files) != 8 {
		os.Exit(-1)
	}
	for index, _ := range files {
		task := Task{MAP,false,false,index}
		m.mapTasks = append(m.mapTasks,task)
	}
	fmt.Printf("init files %v \n",len(files))
	for i := 0; i < nReduce; i++ {
		task := Task{REDUCE,false,false,i}
		m.reduceTasks = append(m.reduceTasks, task)
	}
	//os.Mkdir(Dir,os.ModePerm)
	m.server()
	return &m
}
