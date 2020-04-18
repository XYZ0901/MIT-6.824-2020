package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)

import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker struct {
	isRunning_ bool
	mutex sync.Mutex
	id_ int
}

const Dir = "/Users/zhouchouyi/cpp/6.824/src/main/mr-tmp"

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readMapInput(key string) []byte {
	file, err := os.Open(key)
	if err != nil {
		log.Fatalf("cannot open %v", key)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", key)
	}
	file.Close()
	return  content
}

func (state *worker)setIsRunning(isRunning bool) {
	state.mutex.Lock()
	state.isRunning_ = isRunning
	state.mutex.Unlock()
}
func (state* worker) getId() int {
	index := -1
	state.mutex.Lock()
	index = state.id_
	state.mutex.Unlock()
	return  index
}
func (state* worker) setId (index int){


	state.mutex.Lock()
	state.id_ = index
	state.mutex.Unlock()


}
func FileExist(filename string) bool {
	if _, err := os.Stat(filename); err == nil {
		return  true
	} else {
		return  false
	}
}
func (state *worker) isRunning() bool {
	state.mutex.Lock()
	isRunning := state.isRunning_
	state.mutex.Unlock()
	return isRunning
}


func saveIntermediate(kva *[]KeyValue,index int) {
	i := 0
	var tempFiles []string
	for i := 0; i < 10; i++ {
		oname := fmt.Sprintf("mr-%v-%v",index,i)
		os.Remove(oname)
		file, err := ioutil.TempFile("./", "prefix" + oname)
		if err != nil {
			log.Fatal(err)
		}
		tempFiles = append(tempFiles, file.Name())
		file.Close()
	}
	for i < len(*kva) {
		j := i + 1
		for j < len(*kva) && (*kva)[j].Key == (*kva)[i].Key {
			j++
		}
		reducerIndex := ihash((*kva)[i].Key) % 10
		ofile, err := os.OpenFile(tempFiles[reducerIndex],os.O_APPEND|os.O_RDWR,os.ModePerm)
		if err != nil{
			fmt.Printf("error : %v in openfile when saveing intermediate data ",err)
		}
		enc := json.NewEncoder(ofile)
		for k := i; k < j; k++ {
			err := enc.Encode((*kva)[k])
			if err != nil{
				fmt.Printf("error in encode %v ",err)
				os.Exit(-1)
			}
		}
		ofile.Close()
		i = j
	}
	for reducerIndex, oname := range tempFiles {
		newname := "./" + fmt.Sprintf("mr-%v-%v",index,reducerIndex)
		os.Rename(oname,newname)
	}
}

func loadIntermediate(index int) []KeyValue{
	fs,err:= ioutil.ReadDir(Dir)
	if err != nil{
		fmt.Printf("read dir %v error %v \n",Dir,err)
	}
	var kva []KeyValue
	for _,fileInfo:=range fs{
		filename := "./" + fileInfo.Name()
		reducerIndex := -1
		mapIndex := -1
		matching,err := fmt.Sscanf(fileInfo.Name(),"mr-%v-%v",&mapIndex,&reducerIndex)
		if err == nil && matching == 2 && reducerIndex == index {
			file,err := os.Open(filename)
			if err != nil{
				fmt.Printf("open %v error %v \n",filename,err)
				os.Exit(-1)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv);err != nil{
					break
				}
				kva = append(kva,kv)
			}
		}
	}
	sort.Sort(ByKey(kva))
	return kva
}



func DoMap(mapf func(string, string) []KeyValue,
	fileNames[]string,
	index int) {
	for _, file := range fileNames {
		content := readMapInput(file)
		kva := mapf(file,string(content))
		sort.Sort(ByKey(kva))
		saveIntermediate(&kva,index)
	}
}

func DoReduce(reducef func(string, []string) string,
	index int) {
	kva := loadIntermediate(index)
	sort.Sort(ByKey(kva))
	i := 0
	ofile, err := ioutil.TempFile("./", "prefix")
	if err != nil {
		log.Fatal(err)
	}
	//fmt.Printf("%v begin doing reduce \n",workerSock())
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && (kva)[j].Key == (kva)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (kva)[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	//fmt.Printf("%v end doing reduce \n",workerSock())
	oldname := ofile.Name()
	ofile.Close()
	name := fmt.Sprintf("mr-out-%v",index)
	os.Rename(oldname,name)
}

func doSubmit (taskType int,index int) bool {
	request := SubmitTaskRequest{}
	response := SubmitTaskResponse{}
	request.TaskType_ = taskType
	request.Index = index
	//if taskType == MAP {
	//	fmt.Printf("compelet MAP %v \n",request.Index)
	//} else {
	//	fmt.Printf("compelet REDUCE %v \n",request.Index)
	//}
	success := call("Master.SubmitTask",&request,&response)
	return success
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	// CallExample()
	state := worker{false,sync.Mutex{},-1}
	for  {
		request := AskTaskRequest{}
		response := AskTaskResponse{}
		success := call("Master.AskTask",&request,&response)
		//exit
		if success == false {
			fmt.Printf("aks task failed \n")
			os.Exit(-1)
		}
		state.setId(response.Id_)
		if response.TaskType_ == NONE {
			//fmt.Printf("%v is sleeping \n",workerSock())
			time.Sleep(time.Millisecond * 200)
			continue
		}
		if response.TaskType_ == MAP {
			//map
			//fmt.Printf("%v get map task %v \n",workerSock(),response.Id_)
			DoMap(mapf,response.FileNames_,state.getId())
			//finish map
			doSubmit(MAP,state.getId())
		} else {
			//reduce
			//fmt.Printf("%v get reduce task %v \n",workerSock(),response.Id_)
			DoReduce(reducef,state.getId())
			doSubmit(REDUCE,state.getId())
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
