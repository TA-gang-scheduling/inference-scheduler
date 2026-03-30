package main

import (
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/reyhanwiyasa/inference-scheduler/internal/plugin"
)

func main() {
	command := app.NewSchedulerCommand(
		app.WithPlugin(plugin.Name, plugin.New),
	)

	code := cli.Run(command)
	if code != 0 {
		klog.ErrorS(nil, "scheduler exited with non-zero code", "code", code)
		os.Exit(code)
	}
}
