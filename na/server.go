package na

import (
	"errors"
	"github.com/idata-shopee/gopcp"
	"github.com/idata-shopee/gopcp_rpc"
	"github.com/idata-shopee/gopcp_service"
	"github.com/idata-shopee/gopcp_stream"
	"github.com/satori/go.uuid"
	"log"
	"time"
)

const GET_SERVICE_TYPE = "getServiceType"

type WorkerConfig struct {
	Timeout time.Duration
}

func StartTcpServer(port int, workerConfig WorkerConfig) error {
	// {type: {id: PcpConnectionHandler}}
	var workerLB = GetWorkerLB()

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
				workerLB.RemoveWorker(worker)
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
					log.Printf("new worker with id %s and type %s\n", worker.Id, serviceType)
					workerLB.AddWorker(worker)
				}
			},
		}
	})
}
