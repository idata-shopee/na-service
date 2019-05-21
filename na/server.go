package na

import (
	"errors"
	"fmt"
	"github.com/idata-shopee/gopcp"
	"github.com/idata-shopee/gopcp_rpc"
	"github.com/idata-shopee/gopcp_service"
	"github.com/idata-shopee/gopcp_stream"
	"github.com/satori/go.uuid"
	"log"
	"net"
	"time"
)

const GET_SERVICE_TYPE = "getServiceType"

type WorkerConfig struct {
	Timeout time.Duration
}

// proxy protocol
// 	 1. proxy stateless pcp call
// 	 2. proxy streaming pcp call
// maintain state of workers
// (1) for each type of worker, have at least one connection. If not call dpm to start a new client.
// (2) if qps is high for a type of worker, can ask dpm to start a new client.
// for worker, need to expose functions: {getServiceType, shutdown}
// for dpm, need to expose function: {startService}
// for mc, stored configuration:
// {
//   wps: [{serviceType}]
// }

func StartTcpServer(port int, mcClientConfig MCClientConfig, workerConfig WorkerConfig) error {
	ip, _ := GetOutboundIP()
	log.Printf("local ip is %v\n", *ip)

	// {type: {id: PcpConnectionHandler}}
	var workerLB = GetWorkerLB()
	var mcClient = GetMCClient(ip.String(), port, mcClientConfig)

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
					// missing worker
					return nil, errors.New("missing worker")
				} else {
					return worker.PCHandler.Call(
						worker.PCHandler.PcpClient.Call(funName, params[2:]...),
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
			func(PCHandler *gopcp_rpc.PCPConnectionHandler) {
				log.Printf("new connection with id %s\n", worker.Id)
				// new connection, ask for type.
				if serviceTypeI, err := PCHandler.Call(PCHandler.PcpClient.Call(GET_SERVICE_TYPE), workerConfig.Timeout); err != nil {
					log.Printf("close connection with id %s\n", worker.Id)
					PCHandler.Close()
				} else if serviceType, ok := serviceTypeI.(string); !ok || serviceType == "" {
					log.Printf("close connection with id %s\n", worker.Id)
					PCHandler.Close()
				} else {
					workerLB.AddWorker(worker)
					// report connection status to DMP
					go mcClient.UpdateReport(workerLB.GetWorkersInfo())
				}
			},
		}
	})
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
