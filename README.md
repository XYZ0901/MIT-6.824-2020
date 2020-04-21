# MIT 6.824 LAB 记录

寒假之前做了下6.824 2012的yfs，受疫情影响一直没返校，macos上对实验所需要的fuse支持较差，经常莫名其妙的编译错误。于是开始抽空完成一下2020的lab，并记录下所遇到的坑.

## LAB1 MAPREDUCE

第一个实验是完成一个简单的分布式mapreduce框架，整个实验分三步走:

### 1 完成worker

worker的主体是一个for循环，不断的向master 调用**AskTask RPC**。然后执行不同的回调函数:**doMap  doReduce**.

**doMap**负责依次读取master获取的输入文件列表，调用mapf,存取中间结果

**doReduce** 负责读取<u>对应</u>的中间结果文件，调用reducef,存取输出

大致完成后，可以修改mrsequential.go文件，以串行的方式模拟调用worker里的函数，比较下输出是否正确

### 2 完成master

master负责对任务的调度。整个运行过程分两种任务，**MAP** 和 **REDUCRE**

```go
type Task struct {
  taskType_ int
  isCompleted_ bool
  isDistributed_ bool
  index_ int
}
```

分别用两个list来保存。每次收到来自worker的**Asktask** RPC请求，调用 tryGetTask尝试获取一个当前阶段尚未分配的task.在执行不同阶段时，得到的task种类也是不一样的。例如倘若在map阶段，所有的map都已经被分配了,tryGetTask会返回一个类型为NONE的task.worker收到之后，应该暂时sleep几百毫秒之后再次请求.

完成master之后基本可以通过前几个测试.

### 3 完成fault-tolerance

第三部主要是针对最后一个测试。最后一个测试会随机阻塞当前的任务，master超时之后，会重新分配这个任务。但由于前一个worker执行该任务时候，可能只把结果写了一半，所以如果什么都不做的话，最后总的结果大概率会出错。所以这个步骤就是主要解决map和reduce过程中文件的存储问题.

课程已经给出了相应的解决方案，首先创建一个tempfile,然后进行写入。在写入彻底完成后，把这个tempFile rename 成对应的文件名.

当master分配出一个任务之后，应该启动一个定时协程**waitForTask**。worker完成任务之后，应该调用**submitTask** RPC，master将对应的task.isCompleted_设置为true.当**waitForTask**超时之后，会检查对应的tasK的完成情况，没有完成的话应该设置isDistributed__为false,以便重新分配没按时完成的task.



## LAB2 Raft

整个lab2是要求你实现一个精简版Raft，只实现Raft最核心的两个RPC----RequestVote和AppendEntries.对于Raft算法，已经有很多开源了的，例如[braft](https://github.com/baidu/braft) [ectd](https://github.com/etcd-io/etcd) 等。这些都是非常值得学习的算法库，但对初学者却不够友好，因为通常在工业生产中会对Raft做一些细节上的改动，如PreVoted，同时为了性能做了复杂的并发模型并且整个库还包括其他部分如存储系统。所以824的这个lab2是很适合Raft的初学者加深对基础算法的理解。



