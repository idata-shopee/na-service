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
	"time"
)

const GET_SERVICE_TYPE = "getServiceType"

type WorkerConfig struct {
	Timeout time.Duration
}

func getProxySignError(args []interface{}) error {
	return fmt.Errorf(`"proxy" method signature "(serviceType String, params, timeout)", args are %v`, args)
}

func StartTcpServer(port int, workerConfig WorkerConfig) error {
	// {type: {id: PcpConnectionHandler}}
	var workerLB = GetWorkerLB()

	// create server
	return gopcp_service.StartTcpServer(port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			// proxy pcp call
			// (proxy, serviceType, ...args)
			"proxy": gopcp.ToLazySandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				log.Println(args)
				if len(args) < 3 {
					return nil, getProxySignError(args)
				} else if serviceType, ok := args[0].(string); !ok {
					return nil, getProxySignError(args)
				} else if code, ok := args[1].(string); !ok {
					return nil, getProxySignError(args)
				} else if timeout, ok := args[2].(float64); !ok {
					return nil, getProxySignError(args)
				} else if worker, ok := workerLB.PickUpWorker(serviceType); !ok {
					// missing worker
					return nil, errors.New("missing worker for service type " + serviceType)
				} else {
					return worker.PCHandler.CallRemote(
						code,
						time.Duration(int(timeout))*time.Second,
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
				// TODO error handling
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
					worker.ServiceType = serviceType
					worker.PCHandler = PCHandler

					workerLB.AddWorker(worker)
				}
			},
		}
	})
}
