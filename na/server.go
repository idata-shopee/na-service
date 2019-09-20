package na

import (
	"errors"
	"fmt"
	"github.com/lock-free/goaio"
	"github.com/lock-free/goklog"
	"github.com/lock-free/gopcp"
	"github.com/lock-free/gopcp_rpc"
	"github.com/lock-free/gopcp_stream"
	"github.com/lock-free/obrero/box/cqbox"
	"github.com/lock-free/obrero/mids"
	"github.com/lock-free/obrero/utils"
	"github.com/lock-free/obrero/utils/dlb"
	"github.com/satori/go.uuid"
	"strconv"
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

	err := utils.ParseArgs(args, []interface{}{&serviceType, &list, &timeout}, "wrong signature, expect (proxy, serviceType: string, list: []Any, timeout: int)")

	if err != nil {
		return nil, err
	}

	var proxyExp = ProxyExp{
		ServiceType: serviceType,
		Timeout:     time.Duration(timeout) * time.Second,
	}

	funName, ps, err := ParseParams(list)
	if err != nil {
		return nil, err
	}

	proxyExp.FunName = funName
	proxyExp.Params = ps

	return &proxyExp, nil
}

// (funName, params)
func ParseParams(list []interface{}) (string, []interface{}, error) {
	params, ok := list[0].([]interface{})
	if !ok {
		return "", nil, errors.New("params expected be an array [funName, ...args].")
	}
	if len(params) == 0 {
		return "", nil, errors.New("params expected be an array [funName, ...args].")
	}
	funName, ok := params[0].(string)
	if !ok {
		return "", nil, errors.New("funName expected be an string.")
	}
	return funName, params[1:], nil
}

func GetWorkerHandler(workerLB *dlb.WorkerLB, serviceType string) (*gopcp_rpc.PCPConnectionHandler, error) {
	// choose worker
	worker, ok := workerLB.PickUpWorkerRandom(serviceType)
	if !ok {
		// missing worker
		return nil, errors.New("missing worker for service type " + serviceType)
	}
	handle, ok := worker.Handle.(*gopcp_rpc.PCPConnectionHandler)
	if !ok {
		return nil, errors.New("unexpect type error, expect *gopcp.PCPConnectionHandler for Handle of worker")
	}
	return handle, nil
}

func StartNoneBlockingTcpServer(port int, workerConfig WorkerConfig) (*goaio.TcpServer, error) {
	klog.LogNormal("start-service", "try to start tcp server at "+strconv.Itoa(port))

	// {type: {id: PcpConnectionHandler}}
	var workerLB = dlb.GetWorkerLB()
	var callQueueBox = cqbox.GetCallQueueBox()

	if server, err := gopcp_rpc.GetPCPRPCServer(port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			// proxy pcp call
			// (proxy, serviceType, list, timeout)
			"proxy": gopcp.ToSandboxFun(mids.LogMid("proxy", func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				proxyExp, err := ParseProxyCallExp(args)

				if err != nil {
					return nil, err
				}

				handle, err := GetWorkerHandler(workerLB, proxyExp.ServiceType)
				if err != nil {
					return nil, err
				}
				return handle.Call(
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
				handle, err := GetWorkerHandler(workerLB, proxyExp.ServiceType)
				if err != nil {
					return nil, err
				}

				// pipe stream
				sparams, err := handle.StreamClient.ParamsToStreamParams(append(proxyExp.Params, func(t int, d interface{}) {
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
				return handle.Call(gopcp.CallResult{append([]interface{}{proxyExp.FunName}, sparams...)}, proxyExp.Timeout)
			}),

			// (queue, key, list)
			// eg: (queue, "abc", (+, a, b))
			"queue": gopcp.ToLazySandboxFun(mids.LogMid("proxy", callQueueBox.CallQueueBoxFn)),
		})
	}, func() *gopcp_rpc.ConnectionEvent {
		var worker dlb.Worker
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
					worker.Group = serviceType
					worker.Handle = PCHandler

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
	utils.RunForever()

	return nil
}
