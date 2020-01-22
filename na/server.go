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
	"strconv"
	"time"
)

const (
	GET_SERVICE_TYPE     = "getServiceType"
	GET_SERVICE_STATE_ID = "getServiceStateId"
)

var klog = goklog.GetInstance()

type WorkerConfig struct {
	Timeout time.Duration
}

func GetWorkerHandler(workerLB *dlb.WorkerLB, serviceType string, workerId string) (*gopcp_rpc.PCPConnectionHandler, error) {
	var (
		worker *dlb.Worker
		ok     bool
	)

	if workerId == "" {
		// choose worker
		worker, ok = workerLB.PickUpWorkerRandom(serviceType)
	} else {
		worker, ok = workerLB.PickUpWorkerById(serviceType, workerId)
	}

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
	var proxyMid = mids.GetProxyMid(func(serviceType string, workerId string) (*gopcp_rpc.PCPConnectionHandler, error) {
		return GetWorkerHandler(workerLB, serviceType, workerId)
	}, mids.DefaultGetCommand)

	if server, err := gopcp_rpc.GetPCPRPCServer(port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			// proxy pcp call
			// (proxy, serviceType, exp, timeout)
			"proxy": gopcp.ToLazySandboxFun(mids.LogMid("proxy", proxyMid.Proxy)),

			// proxy pcp call by worker id
			"proxyById": gopcp.ToLazySandboxFun(mids.LogMid("proxyById", proxyMid.ProxyById)),

			// proxy pcp stream call
			// (proxyStream, serviceType, exp, timeout)
			"proxyStream": streamServer.LazyStreamApi(proxyMid.ProxyStream),

			// (queue, key, list)
			// eg: (queue, "abc", (+, a, b))
			"queue": gopcp.ToLazySandboxFun(mids.LogMid("queue", callQueueBox.CallQueueBoxFn)),
		})
	}, func() *gopcp_rpc.ConnectionEvent {
		var worker dlb.Worker

		return &gopcp_rpc.ConnectionEvent{
			// on close of connection
			func(err error) {
				// remove worker when connection closed
				klog.LogNormal("connection-broken", fmt.Sprintf("worker is %v", worker))
				workerLB.RemoveWorker(worker)
			},

			// new connection
			func(PCHandler *gopcp_rpc.PCPConnectionHandler) {
				var (
					err          error
					ok           bool
					sidI         interface{}
					serviceTypeI interface{}
					sid          string
					serviceType  string
				)
				klog.LogNormal("connection-new", fmt.Sprintf("worker is %v, remote address is: %v", worker, PCHandler.ConnHandler.Conn.RemoteAddr()))

				defer func() {
					if err != nil {
						klog.LogNormal("connection-close", fmt.Sprintf("worker is %v", worker))
						PCHandler.Close()
					}
				}()

				// ask for service state id
				sidI, err = PCHandler.Call(PCHandler.PcpClient.Call(GET_SERVICE_STATE_ID), workerConfig.Timeout)
				if err != nil {
					return
				}
				sid, ok = sidI.(string)
				if !ok || sid == "" {
					err = fmt.Errorf("unexpected state id %v", sidI)
					return
				}

				worker.Id = sid

				// ask for service type
				serviceTypeI, err = PCHandler.Call(PCHandler.PcpClient.Call(GET_SERVICE_TYPE), workerConfig.Timeout)
				if err != nil {
					return
				}
				serviceType, ok = serviceTypeI.(string)
				if !ok || serviceType == "" {
					err = fmt.Errorf("unexpected service type %v", serviceTypeI)
					return
				}

				// TODO if NA is in public network, need to auth connection
				// TODO validate (serviceType, token) pair
				klog.LogNormal("worker-new", fmt.Sprintf("worker is %v", worker))
				worker.Group = serviceType
				worker.Handle = PCHandler

				workerLB.AddWorker(worker)
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
