package main

import (
	"github.com/lock-free/na_service/na"
	"github.com/lock-free/obrero/utils"
	"time"
)

// read port from env
func main() {
	port := utils.MustEnvIntOption("PORT")
	err := na.StartTcpServer(port, na.WorkerConfig{
		Timeout: 120 * time.Second,
	})
	if err != nil {
		panic(err)
	}
}
