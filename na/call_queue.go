package na

import (
	"errors"
	"sync"
	"sync/atomic"
)

type TaskResult struct {
	data interface{}
	err  error
}

type Task struct {
	data    interface{}
	retChan chan TaskResult // send message back
}

// ordered calling sequence
type CallQueue struct {
	queue    *[]Task
	qlock    *sync.Mutex
	flag     *int32
	executor func(interface{}) (interface{}, error)
}

func GetCallQueue(executor func(interface{}) (interface{}, error)) *CallQueue {
	var queue []Task
	var qlock sync.Mutex
	var flag int32 = 0
	return &CallQueue{&queue, &qlock, &flag, executor}
}

// run in order
func (cq *CallQueue) Enqueue(data interface{}) (interface{}, error) {
	var task = Task{data, make(chan TaskResult, 1)}

	cq.push(task)
	go cq.consume()

	taskResult := <-task.retChan
	close(task.retChan)

	return taskResult.data, taskResult.err
}

func (cq *CallQueue) push(task Task) {
	cq.qlock.Lock()
	defer cq.qlock.Unlock()
	*cq.queue = append(*cq.queue, task)
}

func (cq *CallQueue) remove() *Task {
	cq.qlock.Lock()
	defer cq.qlock.Unlock()

	fst := (*cq.queue)[0]
	*cq.queue = (*cq.queue)[1:]
	return &fst
}

func (cq *CallQueue) GetSize() int {
	cq.qlock.Lock()
	defer cq.qlock.Unlock()

	return len(*(cq.queue))
}

func (cq *CallQueue) consume() {
	if cq.GetSize() == 0 {
		return
	}
	// grab flag
	if atomic.CompareAndSwapInt32(cq.flag, 0, 1) {
		fst := cq.remove()
		res, err := cq.executor(fst.data)
		fst.retChan <- TaskResult{res, err}
		// release flag
		atomic.CompareAndSwapInt32(cq.flag, 1, 0)
		cq.consume()
	}
}

type CallQueueMap struct {
	m        *sync.Map
	executor func(interface{}) (interface{}, error)
}

func GetCallQueueMap(executor func(interface{}) (interface{}, error)) *CallQueueMap {
	var m sync.Map
	return &CallQueueMap{&m, executor}
}

func (cqm *CallQueueMap) Enqueue(key string, data interface{}) (interface{}, error) {
	cqi, _ := cqm.m.LoadOrStore(key, GetCallQueue(cqm.executor))
	cq, ok := cqi.(*CallQueue)
	if !ok {
		return nil, errors.New("unexpect type error in CallQueueMap.Enqueue")
	}
	defer func() {
		// remove from map if queue is empty
		if cq.GetSize() == 0 {
			cqm.m.Delete(key)
		}
	}()
	return cq.Enqueue(data)
}
