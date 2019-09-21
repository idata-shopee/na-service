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
	var proxyMid = mids.GetProxyMid(func(serviceType string) (*gopcp_rpc.PCPConnectionHandler, error) {
		return GetWorkerHandler(workerLB, serviceType)
	})

	if server, err := gopcp_rpc.GetPCPRPCServer(port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			// proxy pcp call
			// (proxy, serviceType, exp, timeout)
			"proxy": gopcp.ToLazySandboxFun(mids.LogMid("proxy", proxyMid.Proxy)),

			// proxy pcp stream call
			// (proxyStream, serviceType, exp, timeout)
			"proxyStream": streamServer.LazyStreamApi(proxyMid.ProxyStream),

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
