package na

import (
	"github.com/idata-shopee/gopcp_rpc"
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
	activeWorkerMap sync.Map
}

func GetWorkerLB() *WorkerLB {
	var activeWorkerMap sync.Map
	return &WorkerLB{activeWorkerMap}
}

// get worker information
func (wlb *WorkerLB) GetWorkersInfo() map[string]map[string]WorkerReport {
	workersInfo := map[string]map[string]WorkerReport{}

	wlb.activeWorkerMap.Range(func(stI, wmI interface{}) bool {
		st, _ := stI.(string)
		wm, _ := wmI.(sync.Map)
		if _, ok := workersInfo[st]; !ok {
			workersInfo[st] = map[string]WorkerReport{}
		}

		wm.Range(func(idI, workerI interface{}) bool {
			id, _ := idI.(string)
			workersInfo[st][id] = WorkerReport{
				Id: id,
			}
			return true
		})

		return true
	})

	return workersInfo
}

func (wlb *WorkerLB) AddWorker(worker Worker) {
	var workerMap sync.Map
	workerMapI, _ := wlb.activeWorkerMap.LoadOrStore(worker.Id, workerMap)
	workerMap = workerMapI.(sync.Map)
	// save it to active worker map
	workerMap.Store(worker.Id, worker)
}

func (wlb *WorkerLB) RemoveWorker(worker Worker) bool {
	// remove worker when connection closed
	if workerMapI, ok := wlb.activeWorkerMap.Load(worker.ServiceType); ok {
		workerMap := workerMapI.(sync.Map)
		if _, ok := workerMap.Load(worker.Id); ok {
			workerMap.Delete(worker.Id)
			// TODO remove if woker map is empty
			return true
		}
	}
	return false
}

// current use random on picking up strategy
// random algorithm: reservoir sampling, https://en.wikipedia.org/wiki/Reservoir_sampling
func (wlb *WorkerLB) PickUpWorker(serviceType string) (*Worker, bool) {
	if activeMapI, ok := wlb.activeWorkerMap.Load(serviceType); ok {
		var activeMap = activeMapI.(sync.Map)
		var chosen interface{} = nil
		var curIdx = -1

		activeMap.Range(func(key, worker interface{}) bool {
			curIdx++
			if chosen == nil {
				chosen = worker
			} else {
				j := rand.Intn(curIdx + 1)
				if j < 1 {
					chosen = worker
				}
			}

			return true
		})

		return chosen.(*Worker), true
	} else {
		return nil, false
	}
}
