package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
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
	dispatcherConfig, err := utils.NewDispatcherConfig(serviceName)

	logger := zap.NewNop()
	clientDispatcher, err := utils.NewClientDispatcher(utils.TransportTypeGRPC, dispatcherConfig, logger)

	if err != nil && clientDispatcher != nil {
		if err := clientDispatcher.Start(); err != nil {
			log.Fatalf("did not connect: %v", err)
		}
	}
	defer func() { err = multierr.Append(err, clientDispatcher.Stop()) }()

	grpcPort, err := dispatcherConfig.GetPort(utils.TransportTypeGRPC)
	grpcClientConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithInsecure())

	clientConfig := utils.ClientInfo{ClientConfig: clientDispatcher.ClientConfig(serviceName), GRPCClientConn: grpcClientConn}
	yarpcClient := pb.NewSchedulerYARPCClient(clientConfig.ClientConfig)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	u, _ := uuid.NewUUID()
	scheduledTime := getScheduledTime()
	payload := getPayload(u, scheduledTime)
	_, err = yarpcClient.Add(ctx, &pb.SchedulerEntryRequest{Key: u.String(), Payload: payload, ScheduledTimeEpoch: scheduledTime})
	if err != nil {
		log.Fatalf("Could not Add scheduler Entry: %v", err)
	}
}

func getScheduledTime() int64 {
	future := int64(20 * 1000)
	millis := time.Now().UnixNano()/int64(time.Millisecond) + future
	return millis
}

func getPayload(u uuid.UUID, millis int64) string {
	t := time.Unix(0, millis*int64(time.Millisecond))
	return "Payload for " + u.String() + "scheduled at " + t.String()
}
