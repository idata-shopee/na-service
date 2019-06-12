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
	return fmt.Errorf(`"proxy" method signature "(serviceType String, list []Any, timeout Int)" eg: ("user-service", ["getUser", "01234"], 120), args are %v`, args)
}

func getProxyStreamSignError(args []interface{}) error {
	return fmt.Errorf(`"proxyStream" method signature "(serviceType String, list []Any, timeout Int)" eg: ("download-service", ["getRecords", 1000], 120), args are %v`, args)
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
				var (
					serviceType string
					list        []interface{}
					params      []interface{}
					funName     string
					timeout     float64
					ok          bool = true
				)

				if len(args) < 3 {
					ok = false
				}

				if ok {
					serviceType, ok = args[0].(string)
				}

				if ok {
					list, ok = args[1].([]interface{})
				}

				if ok {
					params, ok = list[0].([]interface{})
				}

				if ok {
					funName, ok = params[0].(string)
				}

				if ok {
					timeout, ok = args[2].(float64)
				}

				if !ok {
					return nil, getProxySignError(args)
				}

				// choose worker
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
			// (proxyStream, serviceType, funName, ...params)
			"proxyStream": streamServer.StreamApi(func(
				streamProducer gopcp_stream.StreamProducer,
				args []interface{},
				attachment interface{},
				pcpServer *gopcp.PcpServer,
			) (interface{}, error) {
				var (
					serviceType string
					list        []interface{}
					params      []interface{}
					funName     string
					timeout     float64
					ok          bool = true
				)

				if len(args) < 3 {
					ok = false
				}

				if ok {
					serviceType, ok = args[0].(string)
				}

				if ok {
					list, ok = args[1].([]interface{})
				}

				if ok {
					params, ok = list[0].([]interface{})
				}

				if ok {
					funName, ok = params[0].(string)
				}

				if ok {
					timeout, ok = args[2].(float64)
				}

				if !ok {
					return nil, getProxyStreamSignError(args)
				}

				// choose worker
				worker, ok := workerLB.PickUpWorker(serviceType)
				if !ok {
					// missing worker
					return nil, errors.New("missing worker for service type " + serviceType)
				}
				timeoutDuration := time.Duration(int(timeout)) * time.Second

				// pipe stream
				sexp, err := worker.PCHandler.StreamClient.StreamCall(funName, append(params[1:], func(t int, d interface{}) {
					// write response of stream back to client
					switch t {
					case gopcp_stream.STREAM_DATA:
						streamProducer.SendData(d, timeoutDuration)
					case gopcp_stream.STREAM_END:
						streamProducer.SendEnd(timeoutDuration)
					default:
						errMsg, ok := d.(string)
						if !ok {
							streamProducer.SendError(fmt.Sprintf("errored at stream, and responsed error message is not string. d=%v", d), timeoutDuration)
						} else {
							streamProducer.SendError(errMsg, timeoutDuration)
						}
					}
				})...)

				if err != nil {
					return nil, err
				}

				// send a stream request to service
				return worker.PCHandler.Call(*sexp, timeoutDuration)
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
