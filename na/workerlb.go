package na

import (
	"github.com/lock-free/gopcp_rpc"
	"math/rand"
	"sync"
)

type Worker struct {
	Id          string
	PCHandler   *gopcp_rpc.PCPConnectionHandler
	ServiceType string
}

// Load balancer for workers
type WorkerLB struct {
	ActiveWorkerMap sync.Map
}

func GetWorkerLB() *WorkerLB {
	var ActiveWorkerMap sync.Map
	return &WorkerLB{ActiveWorkerMap}
}

func (wlb *WorkerLB) AddWorker(worker Worker) {
	var workerMap sync.Map
	v, _ := wlb.ActiveWorkerMap.LoadOrStore(worker.ServiceType, &workerMap)
	m := v.(*sync.Map)
	// save it to active worker map
	m.Store(worker.Id, &worker)
}

func (wlb *WorkerLB) RemoveWorker(worker Worker) bool {
	// remove worker when connection closed
	if v, ok := wlb.ActiveWorkerMap.Load(worker.ServiceType); ok {
		m := v.(*sync.Map)
		if _, ok := m.Load(worker.Id); ok {
			m.Delete(worker.Id)
			// TODO remove if woker map is empty
			return true
		}
	}
	return false
}

// current use random on picking up strategy
// random algorithm: reservoir sampling, https://en.wikipedia.org/wiki/Reservoir_sampling
// TODO switch to a O(1) algorithm
func (wlb *WorkerLB) PickUpWorker(serviceType string) (*Worker, bool) {
	if v, ok := wlb.ActiveWorkerMap.Load(serviceType); ok {
		var m = v.(*sync.Map)
		var chosen *Worker
		var curIdx = -1

		m.Range(func(key, worker interface{}) bool {
			curIdx++

			if chosen == nil {
				chosen = worker.(*Worker)
			} else {
				j := rand.Intn(curIdx + 1)
				if j < 1 {
					chosen = worker.(*Worker)
				}
			}

			return true
		})

		if chosen == nil {
			return nil, false
		}

		return chosen, true
	} else {
		return nil, false
	}
}

func (wlb *WorkerLB) PickUpWorkerById(workerId string, serviceType string) (*Worker, bool) {
	v, ok := wlb.ActiveWorkerMap.Load(serviceType)
	if !ok {
		return nil, false
	}
	var m = v.(*sync.Map)

	w, ok := m.Load(workerId)
	if !ok {
		return nil, false
	}

	worker := w.(*Worker)
	return worker, true
}
