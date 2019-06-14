package na

import (
	"github.com/lock-free/gopcp"
	"github.com/lock-free/gopcp_stream"
	"github.com/lock-free/obrero"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseProxyCallExp(t *testing.T) {
	st, fn, ps, to, err := ParseProxyCallExp([]interface{}{"user-service", []interface{}{[]interface{}{"getUser", "test"}}, 120.0})
	assertEqual(t, st, "user-service", "")
	assertEqual(t, fn, "getUser", "")
	assertEqual(t, len(ps), 1, "")
	assertEqual(t, ps[0], "test", "")
	assertEqual(t, err, nil, "")
	assertEqual(t, to, time.Duration(120)*time.Second, "")
}

func TestParseProxyCallExpError(t *testing.T) {
	_, _, _, _, err := ParseProxyCallExp([]interface{}{123, []interface{}{[]interface{}{"getUser", "test"}}, 120.0})
	assertEqual(t, strings.Index(err.Error(), "proxy") != -1, true, "")
}

func TestParseProxyStreamCallExp(t *testing.T) {
	st, fn, ps, to, err := ParseProxyStreamCallExp([]interface{}{"user-service", []interface{}{[]interface{}{"getUser", "test"}}, 120.0})
	assertEqual(t, st, "user-service", "")
	assertEqual(t, fn, "getUser", "")
	assertEqual(t, len(ps), 1, "")
	assertEqual(t, ps[0], "test", "")
	assertEqual(t, err, nil, "")
	assertEqual(t, to, time.Duration(120)*time.Second, "")
}

func TestServerBase(t *testing.T) {
	pcpClient := gopcp.PcpClient{}
	// start server
	server, err := StartNoneBlockingTcpServer(0, WorkerConfig{time.Duration(120) * time.Second})
	if err != nil {
		t.Fatal(err.Error())
	}
	defer server.Close()

	// start workers
	obrero.StartWorkerWithNAs(func(*gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker", nil
			}),
			"testFun": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "haha", nil
			}),
		})
	}, obrero.WorkerStartConf{
		PoolSize:            2,
		Duration:            20 * time.Second,
		RetryDuration:       20 * time.Second,
		NAGetClientMaxRetry: 3,
	}, []obrero.NA{obrero.NA{"127.0.0.1", server.GetPort()}})

	naPools := obrero.StartWorkerWithNAs(func(*gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker2", nil
			}),
		})
	}, obrero.WorkerStartConf{
		PoolSize:            2,
		Duration:            20 * time.Second,
		RetryDuration:       20 * time.Second,
		NAGetClientMaxRetry: 3,
	}, []obrero.NA{obrero.NA{"127.0.0.1", server.GetPort()}})

	// wait for connection
	time.Sleep(time.Duration(100) * time.Millisecond)

	for i := 0; i < 100; i++ {
		v, err := naPools.CallProxy("testWorker", pcpClient.Call("testFun"), time.Duration(120)*time.Second)
		if err != nil {
			t.Fatal(err.Error())
		}

		assertEqual(t, v, "haha", "")
	}
}

func TestServerStream(t *testing.T) {
	pcpClient := gopcp.PcpClient{}
	// start server
	server, err := StartNoneBlockingTcpServer(0, WorkerConfig{time.Duration(120) * time.Second})
	if err != nil {
		t.Fatal(err.Error())
	}
	defer server.Close()

	// start workers
	obrero.StartWorkerWithNAs(func(streamServer *gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker", nil
			}),
			"testStream": streamServer.StreamApi(func(
				streamProducer gopcp_stream.StreamProducer,
				args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				_, err = streamProducer.SendData("a", 10*time.Second)
				if err != nil {
					panic(err)
				}
				_, err = streamProducer.SendData("b", 10*time.Second)
				if err != nil {
					panic(err)
				}

				_, err = streamProducer.SendData("c", 10*time.Second)
				if err != nil {
					panic(err)
				}

				_, err = streamProducer.SendEnd(10 * time.Second)
				if err != nil {
					panic(err)
				}

				return nil, nil
			}),
		})
	}, obrero.WorkerStartConf{
		PoolSize:            2,
		Duration:            20 * time.Second,
		RetryDuration:       20 * time.Second,
		NAGetClientMaxRetry: 3,
	}, []obrero.NA{obrero.NA{"127.0.0.1", server.GetPort()}})

	naPools := obrero.StartWorkerWithNAs(func(*gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker2", nil
			}),
		})
	}, obrero.WorkerStartConf{
		PoolSize:            2,
		Duration:            20 * time.Second,
		RetryDuration:       20 * time.Second,
		NAGetClientMaxRetry: 3,
	}, []obrero.NA{obrero.NA{"127.0.0.1", server.GetPort()}})

	// wait for connection
	time.Sleep(time.Duration(100) * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)

	txt := ""
	_, err = naPools.CallProxyStream("testWorker", pcpClient.Call("testStream"), func(t int, d interface{}) {
		if t == gopcp_stream.STREAM_DATA {
			txt += d.(string)
		} else if t == gopcp_stream.STREAM_END {
			wg.Done()
		}
	}, time.Duration(120)*time.Second)

	if err != nil {
		t.Fatal(err.Error())
	}

	wg.Wait()
}
