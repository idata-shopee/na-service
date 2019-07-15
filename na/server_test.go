package na

import (
	"errors"
	"github.com/lock-free/gopcp"
	"github.com/lock-free/gopcp_rpc"
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
	assertEqual(t, strings.Index(err.Error(), "unexpect args in calling") != -1, true, "")
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

func TestParseProxyStreamCallExpError(t *testing.T) {
	_, _, _, _, err := ParseProxyStreamCallExp([]interface{}{123, []interface{}{[]interface{}{"getUser", "test"}}, 120.0})
	assertEqual(t, strings.Index(err.Error(), "unexpect args in calling") != -1, true, "")
}

func testBaseCall(t *testing.T, generateSandbox gopcp_rpc.GenerateSandbox, callResult gopcp.CallResult, expect interface{}) {
	// start server
	server, err := StartNoneBlockingTcpServer(0, WorkerConfig{time.Duration(120) * time.Second})
	if err != nil {
		t.Fatal(err.Error())
	}
	defer server.Close()

	// start worker1
	obrero.StartWorkerWithNAs(generateSandbox, obrero.WorkerStartConf{
		PoolSize:            2,
		Duration:            20 * time.Second,
		RetryDuration:       20 * time.Second,
		NAGetClientMaxRetry: 3,
	}, []obrero.NA{obrero.NA{"127.0.0.1", server.GetPort()}})

	// start worker2
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
		v, err := naPools.CallProxy("testWorker", callResult, time.Duration(120)*time.Second)
		if err != nil {
			t.Fatal(err.Error())
		}

		assertEqual(t, expect, v, "")
	}
}

func TestServerBase(t *testing.T) {
	pcpClient := gopcp.PcpClient{}
	testBaseCall(t, func(*gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker", nil
			}),
			"testFun": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "haha", nil
			}),
		})
	}, pcpClient.Call("testFun"), "haha")
}

func TestFunHandleSlice(t *testing.T) {
	pcpClient := gopcp.PcpClient{}
	testBaseCall(t, func(*gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker", nil
			}),
			"countSlice": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				slice, ok := args[0].([]interface{})
				if !ok {
					return nil, errors.New("expect slice.")
				}
				return len(slice), nil
			}),
		})
	}, pcpClient.Call("countSlice", []int{1, 2, 3}), float64(3))
}

func TestFunHandleMap(t *testing.T) {
	pcpClient := gopcp.PcpClient{}
	testBaseCall(t, func(*gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker", nil
			}),
		})
	}, pcpClient.Call("prop", pcpClient.Call("Map", "a", 1, "b", 2), "a"), float64(1))
}

func TestFunHandleSlice2(t *testing.T) {
	pcpClient := gopcp.PcpClient{}
	testBaseCall(t, func(*gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker", nil
			}),
			"getThat": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				slice, ok := args[0].([]interface{})
				if !ok {
					return nil, errors.New("expect slice.")
				}
				slice2, ok := slice[0].([]interface{})
				if !ok {
					return nil, errors.New("expect slice.")
				}
				return slice2[0], nil
			}),
		})
	}, pcpClient.Call("getThat", [][]int{[]int{1}}), float64(1))
}

func TestFunHandleSlice3(t *testing.T) {
	pcpClient := gopcp.PcpClient{}
	testBaseCall(t, func(*gopcp_stream.StreamServer) *gopcp.Sandbox {
		return gopcp.GetSandbox(map[string]*gopcp.BoxFunc{
			"getServiceType": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return "testWorker", nil
			}),

			"getThis": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				slice, ok := args[0].([]interface{})
				if !ok {
					return nil, errors.New("expect slice.")
				}
				return slice[0], nil
			}),

			"getThat": gopcp.ToSandboxFun(func(args []interface{}, attachment interface{}, pcpServer *gopcp.PcpServer) (interface{}, error) {
				return args[0], nil
			}),
		})
	}, pcpClient.Call("getThat", pcpClient.Call("getThis", []int{1, 2, 3})), float64(1))
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
	_, err = naPools.CallProxyStream("testWorker", pcpClient.Call("testStream"), func(ty int, d interface{}) {
		if ty == gopcp_stream.STREAM_DATA {
			txt += d.(string)
		} else if ty == gopcp_stream.STREAM_END {
			assertEqual(t, txt, "abc", "")
			wg.Done()
		}
	}, time.Duration(120)*time.Second)

	if err != nil {
		t.Fatal(err.Error())
	}

	wg.Wait()
}

func TestServerStreamSlice(t *testing.T) {
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
				p, ok := args[0].([]interface{})
				if !ok {
					panic(errors.New("expect slice"))
				}
				if p[0] != float64(1) {
					panic(errors.New("expect 1"))
				}

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
	_, err = naPools.CallProxyStream("testWorker", pcpClient.Call("testStream", []int{1, 2, 3}), func(ty int, d interface{}) {
		if ty == gopcp_stream.STREAM_DATA {
			txt += d.(string)
		} else if ty == gopcp_stream.STREAM_END {
			assertEqual(t, txt, "abc", "")
			wg.Done()
		}
	}, time.Duration(120)*time.Second)

	if err != nil {
		t.Fatal(err.Error())
	}

	wg.Wait()
}
