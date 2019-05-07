package main

import (
	"github.com/idata-shopee/na_service/na"
	"os"
	"strconv"
	"time"
)

func MustEnvOption(envName string) string {
	if v := os.Getenv(envName); v == "" {
		panic("missing env " + envName + " which must exists.")
	} else {
		return v
	}
}

// read port from env
func main() {
	portStr := MustEnvOption("PORT")

	if port, err := strconv.Atoi(portStr); err != nil {
		panic("Env PORT must be a number.")
	} else {
		if err = na.StartTcpServer(port, na.MCClientConfig{
			Host:          "127.0.0.1",
			Port:          3666,
			Duration:      5 * time.Minute,
			RetryDuration: 2 * time.Second,
		}, na.WorkerConfig{
			Timeout: 120 * time.Second,
		}); err != nil {
			panic(err)
		}
	}
}
