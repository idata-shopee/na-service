package na

import (
	"fmt"
	"github.com/idata-shopee/gopcp"
	"github.com/idata-shopee/gopcp_rpc"
	"github.com/idata-shopee/gopcp_stream"
	"strconv"
	"sync"
	"time"
)

type MCClient struct {
	mcClient      *gopcp_rpc.PCPConnectionHandler
	delayDuration time.Duration
	callDuration  time.Duration
	host          string
	port          int
	NALocalIP     string
	NAPort        int
	workerMux     sync.Mutex
	// {type: {wokerId: WorkerInfo}}
	workersInfo map[string]map[string]WorkerReport
}

type MCClientConfig struct {
	Host          string
	Port          int
	PoolSize      int
	Duration      time.Duration
	RetryDuration time.Duration
}

type WorkerReport struct {
	Id string `json:"id"`
}

func GetMCClient(localIp string, localPort int, mcClientConfig MCClientConfig) *MCClient {
	var workerMux sync.Mutex

	mcClient := &MCClient{
		mcClient:      nil,
		delayDuration: 100 * time.Millisecond,
		callDuration:  2 * time.Minute,
		host:          mcClientConfig.Host,
		port:          mcClientConfig.Port,
		NALocalIP:     localIp,
		NAPort:        localPort,
		workerMux:     workerMux,
	}

	go mcClient.report()

	return mcClient
}

func (dc *MCClient) connect() {
	if mcClient, err := gopcp_rpc.GetPCPRPCClient(dc.host, dc.port, func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			//
		})
	}, func(e error) {
		// on close
		dc.mcClient = nil
	}); err != nil {
		// retry
	} else {
		dc.mcClient = mcClient
	}
}

func (dc *MCClient) UpdateReport(workersInfo map[string]map[string]WorkerReport) {
	dc.workerMux.Lock()
	defer dc.workerMux.Unlock()
	dc.workersInfo = workersInfo
}

// report worker information to memory centre for sharing
// {type: {wokerId: WorkerInfo}}
func (dc *MCClient) report() {
	for {
		if dc.mcClient != nil && dc.workersInfo != nil {
			dc.workerMux.Lock()
			for st, workerMap := range dc.workersInfo {
				key := st + "." + dc.NALocalIP + ":" + strconv.Itoa(dc.port)
				if _, err := dc.mcClient.Call(
					dc.mcClient.PcpClient.Call("set", key, workerMap),
					dc.callDuration,
				); err != nil {
					fmt.Printf("error happened when sharing worker information: %v", err)
				}
			}
			dc.workerMux.Unlock()
		} else {
			dc.connect()
		}
		time.Sleep(dc.delayDuration)
	}
}
