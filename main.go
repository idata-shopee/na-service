package main

import (
	"errors"
	"fmt"
	"github.com/idata-shopee/gopcp"
	"github.com/idata-shopee/gopcp_rpc"
	"github.com/idata-shopee/gopcp_service"
	"github.com/idata-shopee/gopcp_stream"
	"github.com/satori/go.uuid"
	"math/rand"
	"net"
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

type MCClientConfig struct {
	Host          string
	Port          int
	NALocalIP     string
	NAPort        int
	PoolSize      int
	Duration      time.Duration
	RetryDuration time.Duration
}

type WorkerConfig struct {
	Timeout time.Duration
}

type WorkerReport struct {
	Id string `json:"id"`
}

type MCClient struct {
	mcClient      *gopcp_rpc.PCPConnectionHandler
	delayDuration time.Duration
	callDuration  time.Duration
	host          string
	port          int
	NALocalIP     string
	NAPort        int
	workerMux     sync.Mutex
	// {type: {wokerId: WorkerInfo}}
	workersInfo map[string]map[string]WorkerReport
}

func GetMCClient(mcClientConfig MCClientConfig) *MCClient {
	var workerMux sync.Mutex

	mcClient := &MCClient{
		mcClient:      nil,
		delayDuration: 100 * time.Millisecond,
		callDuration:  2 * time.Minute,
		host:          mcClientConfig.Host,
		port:          mcClientConfig.Port,
		NALocalIP:     mcClientConfig.NALocalIP,
		NAPort:        mcClientConfig.NAPort,

		workerMux: workerMux,
	}

	go mcClient.report()

	return mcClient
}

func (dc *MCClient) connect() {
	if mcClient, err := gopcp_rpc.GetPCPRPCClient(dc.host, dc.port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			//
		})
	}, func(e error) {
		// on close
		dc.mcClient = nil
	}); err != nil {
		// retry
	} else {
		dc.mcClient = mcClient
	}
}

func (dc *MCClient) UpdateReport(workersInfo map[string]map[string]WorkerReport) {
	dc.workerMux.Lock()
	defer dc.workerMux.Unlock()
	dc.workersInfo = workersInfo
}

// report worker information to memory centre for sharing
// {type: {wokerId: WorkerInfo}}
func (dc *MCClient) report() {
	for {
		if dc.mcClient != nil && dc.workersInfo != nil {
			dc.workerMux.Lock()
			for st, workerMap := range dc.workersInfo {
				key := st + "." + dc.NALocalIP + ":" + strconv.Itoa(dc.port)
				if _, err := dc.mcClient.Call(
					dc.mcClient.PcpClient.Call("set", key, workerMap),
					dc.callDuration,
				); err != nil {
					fmt.Printf("error happened when sharing worker information: %v", err)
				}
			}
			dc.workerMux.Unlock()
		} else {
			dc.connect()
		}
		time.Sleep(dc.delayDuration)
	}
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

// proxy protocol
// 	 1. proxy stateless pcp call
// 	 2. proxy streaming pcp call
// service type calling
//   na call worker to get type.
//   (getServiceType)
// for worker, need to expose functions: {getServiceType}
func StartTcpServer(port int, mcClientConfig MCClientConfig, workerConfig WorkerConfig) error {
	// {type: {id: PcpConnectionHandler}}
	var workerLB = GetWorkerLB()
	var mcClient = GetMCClient(mcClientConfig)

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
					go mcClient.UpdateReport(workerLB.GetWorkersInfo())
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
					go mcClient.UpdateReport(workerLB.GetWorkersInfo())
				}
			},
		}
	})
}

// read port from env
func main() {
	portStr := os.Getenv("PORT")
	if portStr == "" {
		panic("missing env PORT which must exists.")
	} else {
		ip, _ := GetOutboundIP()
		fmt.Printf("local ip is %v\n", *ip)

		if port, err := strconv.Atoi(portStr); err != nil {
			panic("Env PORT must be a number.")
		} else {
			if err = StartTcpServer(port, MCClientConfig{
				Host:          "127.0.0.1",
				Port:          3666,
				NALocalIP:     ip.String(),
				NAPort:        port,
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

func GetOutboundIP() (*net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return &localAddr.IP, nil
}
