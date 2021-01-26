package main

import (
	"fmt"

	"github.com/baoqger/distributed-sensor-monitor-system/coordinator"
)

func main() {
	ql := coordinator.NewQueueListener()

	go ql.ListenForNewSource()

	var a string
	fmt.Scanln(&a)
}
