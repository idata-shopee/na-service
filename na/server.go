package na

import (
	"errors"
	"fmt"
	"github.com/lock-free/goaio"
	"github.com/lock-free/gopcp"
	"github.com/lock-free/gopcp_rpc"
	"github.com/lock-free/gopcp_stream"
	"github.com/satori/go.uuid"
	"log"
	"strconv"
	"sync"
	"time"
)

const GET_SERVICE_TYPE = "getServiceType"

type WorkerConfig struct {
	Timeout time.Duration
}

func getProxySignError(args []interface{}) error {
	return fmt.Errorf(`"proxy" method signature "(serviceType String, list []Any, timeout Int)" eg: ("user-service", ["'", ["getUser", "01234"]], 120), args are %v`, args)
}

func getProxyStreamSignError(args []interface{}) error {
	return fmt.Errorf(`"proxyStream" method signature "(serviceType String, list []Any, timeout Int)" eg: ("download-service", ["'", ["getRecords", 1000]], 120), args are %v`, args)
}

func LogMid(logPrefix string, fn gopcp.GeneralFun) gopcp.GeneralFun {
	return func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (ret interface{}, err error) {
		t1 := time.Now().Unix()

		log.Printf("[access-%s] args=%v\n", logPrefix, args)
		ret, err = fn(args, attachment, pcpServer)

		t2 := time.Now().Unix()
		log.Printf("[complete-%s] args=%v, time=%d\n", logPrefix, args, t2-t1)
		return
	}
}

// (proxy, serviceType, list, timeout)
func ParseProxyCallExp(args []interface{}) (string, string, []interface{}, time.Duration, error) {
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
		return serviceType, funName, nil, time.Duration(int(0)) * time.Second, getProxySignError(args)
	}

	timeoutDuration := time.Duration(int(timeout)) * time.Second

	return serviceType, funName, params[1:], timeoutDuration, nil
}

func ParseProxyStreamCallExp(args []interface{}) (string, string, []interface{}, time.Duration, error) {
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
		return "", "", nil, time.Duration(int(0)) * time.Second, getProxyStreamSignError(args)
	}

	return serviceType, funName, params[1:], time.Duration(int(timeout)) * time.Second, nil
}

func StartNoneBlockingTcpServer(port int, workerConfig WorkerConfig) (*goaio.TcpServer, error) {
	log.Println("try to start tcp server at " + strconv.Itoa(port))

	// {type: {id: PcpConnectionHandler}}
	var workerLB = GetWorkerLB()

	if server, err := gopcp_rpc.GetPCPRPCServer(port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			// proxy pcp call
			// (proxy, serviceType, list, timeout)
			"proxy": gopcp.ToSandboxFun(LogMid("proxy", func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				serviceType, funName, params, timeoutDuration, err := ParseProxyCallExp(args)

				if err != nil {
					return nil, err
				}

				// choose worker
				worker, ok := workerLB.PickUpWorker(serviceType)
				if !ok {
					// missing worker
					return nil, errors.New("missing worker for service type " + serviceType)
				}

				return worker.PCHandler.Call(
					gopcp.CallResult{append([]interface{}{funName}, params...)},
					timeoutDuration,
				)
			})),

			// proxy pcp stream call
			// (proxyStream, serviceType, funName, ...params)
			"proxyStream": streamServer.StreamApi(func(
				streamProducer gopcp_stream.StreamProducer,
				args []interface{},
				attachment interface{},
				pcpServer *gopcp.PcpServer,
			) (interface{}, error) {
				serviceType, funName, params, timeoutDuration, err := ParseProxyStreamCallExp(args)

				if err != nil {
					return nil, err
				}

				// choose worker
				worker, ok := workerLB.PickUpWorker(serviceType)
				if !ok {
					// missing worker
					return nil, errors.New("missing worker for service type " + serviceType)
				}

				// pipe stream
				sparams, err := worker.PCHandler.StreamClient.ParamsToStreamParams(append(params, func(t int, d interface{}) {
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
				}))

				if err != nil {
					return nil, err
				}

				// send a stream request to service
				return worker.PCHandler.Call(gopcp.CallResult{append([]interface{}{funName}, sparams...)}, timeoutDuration)
			}),
			// TODO get workers information
		})
	}, func() *gopcp_rpc.ConnectionEvent {
		var worker Worker
		// generate id for this connection
		worker.Id = uuid.NewV4().String()

		return &gopcp_rpc.ConnectionEvent{
			// on close of connection
			func(err error) {
				// remove worker when connection closed
				log.Printf("connection broken, worker is %v\n", worker)
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
	}); err != nil {
		return server, err
	} else {
		return server, nil
	}
}

func StartTcpServer(port int, workerConfig WorkerConfig) error {
	server, err := StartNoneBlockingTcpServer(port, workerConfig)
	if err != nil {
		return err
	}

	defer server.Close()

	// blocking forever
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()

	return nil
}
