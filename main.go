package main

import (
	"errors"
	"github.com/idata-shopee/gopcp"
	"github.com/idata-shopee/gopcp_rpc"
	"github.com/idata-shopee/gopcp_service"
	"github.com/idata-shopee/gopcp_stream"
	"github.com/idata-shopee/gopool"
	"github.com/satori/go.uuid"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const GET_SERVICE_TYPE = "getServiceType"

type Worker struct {
	Id                   string
	pcpConnectionHandler *gopcp_rpc.PCPConnectionHandler
	ServiceType          string
}

type DmpClientConfig struct {
	Host          string
	Port          int
	PoolSize      int
	Duration      time.Duration
	RetryDuration time.Duration
}

type WorkerConfig struct {
	Timeout time.Duration
}

type WorkerReport struct {
	ReportType  string
	Id          string
	ServiceType string
}

type DmpClient struct {
	dmpClientPool *gopool.Pool
	workerReports []WorkerReport
	workerMux     *sync.Mutex
	delayDuration time.Duration
	callDuration  time.Duration
}

func GetDmpClient(dmpClientConfig DmpClientConfig) *DmpClient {
	// client pool for dmp service
	dmpClientPool := gopcp_rpc.GetPCPRPCPool(func() (string, int, error) {
		return dmpClientConfig.Host, dmpClientConfig.Port, nil
	}, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{})
	}, dmpClientConfig.PoolSize, dmpClientConfig.Duration, dmpClientConfig.RetryDuration)

	var workerMux sync.Mutex
	return &DmpClient{dmpClientPool, []WorkerReport{}, &workerMux,
		100 * time.Millisecond, 2 * time.Minute}
}

func (dc *DmpClient) report() {
	for {
		dc.workerMux.Lock()
		for _, workerReport := range dc.workerReports {
			if item, err := dc.dmpClientPool.Get(); err != nil {
				if client, ok := item.(*gopcp_rpc.PCPConnectionHandler); ok {
					client.Call(
						client.PcpClient.Call("ReportWorker", workerReport),
						dc.callDuration,
					)
				}
			}
		}
		dc.workerMux.Unlock()
		time.Sleep(dc.delayDuration)
	}
}

func (dc *DmpClient) ReportGetWorker(id string, serviceType string) {
	dc.workerMux.Lock()
	dc.workerReports = append(dc.workerReports, WorkerReport{"Add", id, serviceType})
	defer dc.workerMux.Unlock()
}

func (dc *DmpClient) ReportLossWorker(id string) {
	dc.workerMux.Lock()
	dc.workerReports = append(dc.workerReports, WorkerReport{"Remove", id, ""})
	defer dc.workerMux.Unlock()
}

// Load balancer for workers
type WorkerLB struct {
	activeWorkerMap sync.Map
}

func GetWorkerLB() *WorkerLB {
	var activeWorkerMap sync.Map
	return &WorkerLB{activeWorkerMap}
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

// proxy protocol
// 	 1. proxy stateless pcp call
// 	 2. proxy streaming pcp call
// service type calling
//   na call worker to get type.
//   (getServiceType)
// for worker, need to expose functions: {getServiceType}
//
func StartTcpServer(port int, dmpClientConfig DmpClientConfig, workerConfig WorkerConfig) error {
	// {type: {id: PcpConnectionHandler}}
	var workerLB = GetWorkerLB()
	var dmpClient = GetDmpClient(dmpClientConfig)

	// create server
	return gopcp_service.StartTcpServer(port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			// proxy pcp call
			// (proxy, serviceType, ...args)
			"proxy": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				if len(args) < 2 {
					return nil, errors.New(`proxy method signature "(serviceType String, params [funName String, ...args], timeout)"`)
				} else if serviceType, ok := args[0].(string); !ok {
					return nil, errors.New(`proxy method signature "(serviceType String, params [funName String, ...args], timeout)"`)
				} else if params, ok := args[1].([]interface{}); !ok {
					return nil, errors.New(`proxy method signature "(serviceType String, params [funName String, ...args], timeout)"`)
				} else if timeout, ok := args[2].(int); !ok {
					return nil, errors.New(`proxy method signature "(serviceType String, params [funName String, ...args], timeout)"`)
				} else if funName, ok := params[0].(string); !ok {
					return nil, errors.New(`proxy method signature "(serviceType String, params [funName String, ...args], timeout)"`)
				} else if worker, ok := workerLB.PickUpWorker(serviceType); !ok {
					return nil, errors.New(`proxy method signature "(serviceType String, params [funName String, ...args], timeout)"`)
				} else {
					return worker.pcpConnectionHandler.Call(
						worker.pcpConnectionHandler.PcpClient.Call(funName, params[2:]...),
						time.Duration(timeout)*time.Second,
					)
				}
			}),

			// proxy pcp stream call
			"proxyStream": streamServer.StreamApi(func(
				streamProducer gopcp_stream.StreamProducer,
				args []interface{},
				attachment interface{},
				pcpServer *gopcp.PcpServer,
			) (interface{}, error) {
				seed := args[0].(string)
				streamProducer.SendData(seed+"1", 10*time.Second)
				streamProducer.SendData(seed+"2", 10*time.Second)
				streamProducer.SendData(seed+"3", 10*time.Second)
				streamProducer.SendEnd(10 * time.Second)
				return nil, nil
			}),
		})
	}, func() *gopcp_rpc.ConnectionEvent {
		var worker Worker
		// generate id for this connection
		worker.Id = uuid.NewV4().String()

		return &gopcp_rpc.ConnectionEvent{
			// on close of connection
			func(err error) {
				// remove worker when connection closed
				if workerLB.RemoveWorker(worker) {
					// report connection status to DMP
					dmpClient.ReportLossWorker(worker.Id)
				}
			},

			// new connection
			func(pcpConnectionHandler *gopcp_rpc.PCPConnectionHandler) {
				// new connection, ask for type.
				if serviceTypeI, err := pcpConnectionHandler.Call(pcpConnectionHandler.PcpClient.Call(GET_SERVICE_TYPE), workerConfig.Timeout); err != nil {
					pcpConnectionHandler.Close()
				} else if serviceType, ok := serviceTypeI.(string); !ok || serviceType == "" {
					pcpConnectionHandler.Close()
				} else {
					workerLB.AddWorker(worker)
					// report connection status to DMP
					dmpClient.ReportGetWorker(worker.Id, worker.ServiceType)
				}
			},
		}
	})
}

// read port from env
func main() {
	port := os.Getenv("PORT")
	if port == "" {
		panic("missing env PORT which must exists.")
	} else {
		if port_int, err := strconv.Atoi(port); err != nil {
			panic("Env PORT must be a number.")
		} else {
			if err = StartTcpServer(port_int, DmpClientConfig{
				Host:          "127.0.0.1",
				Port:          3666,
				Duration:      5 * time.Minute,
				RetryDuration: 2 * time.Second,
			}, WorkerConfig{
				Timeout: 120 * time.Second,
			}); err != nil {
				panic(err)
			}
		}
	}
}
