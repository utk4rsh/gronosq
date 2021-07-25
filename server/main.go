package main

import (
	"go.uber.org/zap"
	"gronosq/config"
	"gronosq/server/pb"
	"gronosq/server/utils"
	"log"
)

const serviceName = "gronosq-server"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	select {}
}

func run() error {
	keyValueYARPCServer := pb.NewSchedulerServerInstance(config.Get())
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	procedures := pb.BuildSchedulerYARPCProcedures(keyValueYARPCServer)
	return utils.WithClientInfo(serviceName, procedures, utils.TransportTypeGRPC, logger)
}
