package main

import (
	"context"
	"google.golang.org/grpc"
	"gronosq/server/pb"
	"log"
	"net"
)

const (
	port = ":50051"
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedSchedulerServer
}

func (s *server) Add(ctx context.Context, in *pb.SchedulerEntryRequest) (*pb.SchedulerResponse, error) {
	log.Printf("Received: %v", in.GetKey())
	return &pb.SchedulerResponse{Message: "Hello " + in.GetKey()}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterSchedulerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
