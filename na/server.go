package na

import (
	"errors"
	"fmt"
	"github.com/lock-free/goaio"
	"github.com/lock-free/goklog"
	"github.com/lock-free/gopcp"
	"github.com/lock-free/gopcp_rpc"
	"github.com/lock-free/gopcp_stream"
	"github.com/lock-free/obrero"
	"github.com/satori/go.uuid"
	"strconv"
	"sync"
	"time"
)

const GET_SERVICE_TYPE = "getServiceType"

var klog = goklog.GetInstance()

type WorkerConfig struct {
	Timeout time.Duration
}

func getParamsError(args []interface{}) error {
	return fmt.Errorf(`unexpect args in calling, args are %v`, args)
}

func LogMid(logPrefix string, fn gopcp.GeneralFun) gopcp.GeneralFun {
	return func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (ret interface{}, err error) {
		t1 := time.Now().UnixNano()

		klog.LogNormal(fmt.Sprintf("%s-access", logPrefix), fmt.Sprintf("args=%v", args))
		ret, err = fn(args, attachment, pcpServer)

		if err != nil {
			klog.LogError(fmt.Sprintf("%s-error", logPrefix), err)
		}

		t2 := time.Now().UnixNano()
		klog.LogNormal(fmt.Sprintf("%s-done", logPrefix), fmt.Sprintf("args=%v, time=%dms", args, (t2-t1)/int64(time.Millisecond)))
		return
	}
}

type ProxyExp struct {
	ServiceType string
	FunName     string
	Params      []interface{}
	Timeout     time.Duration
}

// args = (proxy, serviceType, list, timeout)
// list = [params]
// params = [funName, ...ps]
func ParseProxyCallExp(args []interface{}) (*ProxyExp, error) {
	var (
		serviceType string
		list        []interface{}
		timeout     int
	)

	err := obrero.ParseArgs(args, []interface{}{&serviceType, &list, &timeout}, "wrong signature, expect (proxy, serviceType: string, list: []Any, timeout: int)")

	if err != nil {
		return nil, err
	}

	var proxyExp = ProxyExp{
		ServiceType: serviceType,
		Timeout:     time.Duration(timeout) * time.Second,
	}

	params, ok := list[0].([]interface{})
	if !ok {
		return nil, errors.New("params expected be an array [funName, ...args].")
	}
	if len(params) == 0 {
		return nil, errors.New("params expected be an array [funName, ...args].")
	}
	funName, ok := params[0].(string)
	if !ok {
		return nil, errors.New("funName expected be an string.")
	}

	proxyExp.FunName = funName
	proxyExp.Params = params[1:]

	return &proxyExp, nil
}

func StartNoneBlockingTcpServer(port int, workerConfig WorkerConfig) (*goaio.TcpServer, error) {
	klog.LogNormal("start-service", "try to start tcp server at "+strconv.Itoa(port))

	// {type: {id: PcpConnectionHandler}}
	var workerLB = GetWorkerLB()

	if server, err := gopcp_rpc.GetPCPRPCServer(port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			// proxy pcp call
			// (proxy, serviceType, list, timeout)
			"proxy": gopcp.ToSandboxFun(LogMid("proxy", func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				proxyExp, err := ParseProxyCallExp(args)

				if err != nil {
					return nil, err
				}

				// choose worker
				// TODO add more information to worker, like deployment location which can used to debug live error
				worker, ok := workerLB.PickUpWorker(proxyExp.ServiceType)
				if !ok {
					// missing worker
					return nil, errors.New("missing worker for service type " + proxyExp.ServiceType)
				}

				return worker.PCHandler.Call(
					gopcp.CallResult{append([]interface{}{proxyExp.FunName}, proxyExp.Params...)},
					proxyExp.Timeout,
				)
			})),

			// can specify a worker by workerId
			// (proxy, workerId, serviceType, list, timeout)
			"proxyById": gopcp.ToSandboxFun(LogMid("proxyById", func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				if len(args) < 1 {
					return nil, getParamsError(args)
				}
				workerId, ok := args[0].(string)
				if !ok {
					return nil, getParamsError(args)
				}

				proxyExp, err := ParseProxyCallExp(args[1:])

				if err != nil {
					return nil, err
				}

				// choose worker
				worker, ok := workerLB.PickUpWorkerById(workerId, proxyExp.ServiceType)
				if !ok {
					// missing worker
					return nil, errors.New("missing worker for service type " + proxyExp.ServiceType)
				}

				return worker.PCHandler.Call(
					gopcp.CallResult{append([]interface{}{proxyExp.FunName}, proxyExp.Params...)},
					proxyExp.Timeout,
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
				proxyExp, err := ParseProxyCallExp(args)

				if err != nil {
					return nil, err
				}

				// choose worker
				worker, ok := workerLB.PickUpWorker(proxyExp.ServiceType)
				if !ok {
					// missing worker
					return nil, errors.New("missing worker for service type " + proxyExp.ServiceType)
				}

				// pipe stream
				sparams, err := worker.PCHandler.StreamClient.ParamsToStreamParams(append(proxyExp.Params, func(t int, d interface{}) {
					// write response of stream back to client
					switch t {
					case gopcp_stream.STREAM_DATA:
						streamProducer.SendData(d, proxyExp.Timeout)
					case gopcp_stream.STREAM_END:
						streamProducer.SendEnd(proxyExp.Timeout)
					default:
						errMsg, ok := d.(string)
						if !ok {
							streamProducer.SendError(fmt.Sprintf("errored at stream, and responsed error message is not string. d=%v", d), proxyExp.Timeout)
						} else {
							streamProducer.SendError(errMsg, proxyExp.Timeout)
						}
					}
				}))

				if err != nil {
					return nil, err
				}

				// send a stream request to service
				return worker.PCHandler.Call(gopcp.CallResult{append([]interface{}{proxyExp.FunName}, sparams...)}, proxyExp.Timeout)
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
				klog.LogNormal("connection-broken", fmt.Sprintf("worker is %v", worker))
				workerLB.RemoveWorker(worker)
			},

			// new connection
			func(PCHandler *gopcp_rpc.PCPConnectionHandler) {
				klog.LogNormal("connection-new", fmt.Sprintf("worker is %v", worker))
				// new connection, ask for type.
				if serviceTypeI, err := PCHandler.Call(PCHandler.PcpClient.Call(GET_SERVICE_TYPE), workerConfig.Timeout); err != nil {
					klog.LogNormal("connection-close", fmt.Sprintf("worker is %v", worker))
					PCHandler.Close()
				} else if serviceType, ok := serviceTypeI.(string); !ok || serviceType == "" {
					klog.LogNormal("connection-close", fmt.Sprintf("worker is %v", worker))
					PCHandler.Close()
				} else {
					// TODO if NA is in public network, need to auth connection
					// TODO validate (serviceType, token) pair
					klog.LogNormal("worker-new", fmt.Sprintf("worker is %v", worker))
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
