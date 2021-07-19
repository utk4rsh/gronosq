package main

import (
	"flag"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/zap"
	"gronosq/server/pb"
	"gronosq/server/utils"
	"log"
	"os"
)

var (
	flagSet = flag.NewFlagSet("protobuf", flag.ExitOnError)
)

func main() {
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	if err := do(); err != nil {
		log.Fatal(err)
	}
	select {}
}

func do() error {
	return run()
}

func run() error {
	keyValueYARPCServer := pb.NewSchedulerServerInstance()
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	return WithClients(
		keyValueYARPCServer,
		logger,
	)
}

func WithClients(
	keyValueYARPCServer pb.SchedulerYARPCServer,
	logger *zap.Logger,
) error {
	var procedures []transport.Procedure
	if keyValueYARPCServer != nil {
		procedures = append(procedures, pb.BuildSchedulerYARPCProcedures(keyValueYARPCServer)...)
	}
	return utils.WithClientInfo("example", procedures, utils.TransportTypeGRPC, logger)
}
