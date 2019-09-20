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
			// (proxy, serviceType, exp, timeout)
			"proxy": gopcp.ToLazySandboxFun(mids.LogMid("proxy", func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				var (
					serviceType string
					exp         interface{}
					timeout     int
				)

				err := utils.ParseArgs(args, []interface{}{&serviceType, &exp, &timeout}, "wrong signature, expect (proxy, serviceType: string, exp, timeout: int)")
				exp = args[1]

				if err != nil {
					return nil, err
				}

				handle, err := GetWorkerHandler(workerLB, serviceType)
				if err != nil {
					return nil, err
				}

				bs, err := gopcp.JSONMarshal(gopcp.ParseAstToJsonObject(exp))
				if err != nil {
					return nil, err
				}

				return handle.CallRemote(string(bs), time.Duration(timeout)*time.Second)
			})),

			// proxy pcp stream call
			// (proxyStream, serviceType, exp, timeout)
			"proxyStream": streamServer.LazyStreamApi(func(streamProducer gopcp_stream.StreamProducer, args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				var (
					serviceType string
					exp         interface{}
					timeout     int
				)

				err := utils.ParseArgs(args, []interface{}{&serviceType, &exp, &timeout}, "wrong signature, expect (proxy, serviceType: string, exp, timeout: int)")
				exp = args[1]

				if err != nil {
					return nil, err
				}

				var timeoutD = time.Duration(timeout) * time.Second

				jsonObj := gopcp.ParseAstToJsonObject(exp)

				switch arr := jsonObj.(type) {
				case []interface{}:
					// choose worker
					handle, err := GetWorkerHandler(workerLB, serviceType)
					if err != nil {
						return nil, err
					}

					// pipe stream
					sparams, err := handle.StreamClient.ParamsToStreamParams(append(arr[1:], func(t int, d interface{}) {
						// write response of stream back to client
						switch t {
						case gopcp_stream.STREAM_DATA:
							streamProducer.SendData(d, timeoutD)
						case gopcp_stream.STREAM_END:
							streamProducer.SendEnd(timeoutD)
						default:
							errMsg, ok := d.(string)
							if !ok {
								streamProducer.SendError(fmt.Sprintf("errored at stream, and responsed error message is not string. d=%v", d), timeoutD)
							} else {
								streamProducer.SendError(errMsg, timeoutD)
							}
						}
					}))

					if err != nil {
						return nil, err
					}

					// send a stream request to service
					return handle.Call(gopcp.CallResult{append([]interface{}{arr[0]}, sparams...)}, timeoutD)
				default:
					return nil, fmt.Errorf("Expect array, but got %v, args=%v", jsonObj, args)
				}
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
