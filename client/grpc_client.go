package main

import (
	"context"
	"fmt"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gronosq/server/pb"
	"gronosq/server/utils"
	"log"
	"time"

	"google.golang.org/grpc"
)

const (
	serviceName = "gronosq-server"
)

func main() {
	logger := zap.NewNop()
	dispatcherConfig, err := utils.NewDispatcherConfig(serviceName)
	clientDispatcher, err := utils.NewClientDispatcher(utils.TransportTypeGRPC, dispatcherConfig, logger)
	if err := clientDispatcher.Start(); err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func() { err = multierr.Append(err, clientDispatcher.Stop()) }()
	grpcPort, err := dispatcherConfig.GetPort(utils.TransportTypeGRPC)
	grpcClientConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithInsecure())

	clientConfig := utils.ClientInfo{ClientConfig: clientDispatcher.ClientConfig(serviceName), GRPCClientConn: grpcClientConn}
	yarpcClient := pb.NewSchedulerYARPCClient(clientConfig.ClientConfig)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := yarpcClient.Add(ctx, &pb.SchedulerEntryRequest{Key: "key", Payload: "value"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.GetMessage())
}
