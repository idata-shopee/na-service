package na

import (
	"errors"
	"fmt"
	"github.com/lock-free/gopcp"
	"github.com/lock-free/gopcp_rpc"
	"github.com/lock-free/gopcp_service"
	"github.com/lock-free/gopcp_stream"
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
			// (proxy, serviceType, list, timeout)
			"proxy": gopcp.ToLazySandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				if len(args) < 3 {
					return nil, getProxySignError(args)
				}

				serviceType, ok := args[0].(string)

				if !ok {
					return nil, getProxySignError(args)
				}

				list, ok := args[1].([]interface{})

				if !ok || len(list) <= 0 {
					return nil, getProxySignError(args)
				}

				params, ok := list[0].([]interface{})

				if !ok {
					return nil, getProxySignError(args)
				}

				funName, ok := params[0].(string)

				if !ok {
					return nil, getProxySignError(args)
				}

				timeout, ok := args[2].(float64)
				if !ok {
					return nil, getProxySignError(args)
				}

				worker, ok := workerLB.PickUpWorker(serviceType)
				if !ok {
					// missing worker
					return nil, errors.New("missing worker for service type " + serviceType)
				}

				return worker.PCHandler.Call(
					worker.PCHandler.PcpClient.Call(funName, params[1:]...),
					time.Duration(int(timeout))*time.Second,
				)
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
