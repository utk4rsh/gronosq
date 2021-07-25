package utils

import (
	"fmt"
	"net"
	"strconv"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/yarpc/transport/http"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
	ggrpc "google.golang.org/grpc"
)

const (
	// TransportTypeHTTP represents using HTTP.
	TransportTypeHTTP TransportType = iota
	// TransportTypeTChannel represents using TChannel.
	TransportTypeTChannel
	// TransportTypeGRPC represents using GRPC.
	TransportTypeGRPC
)

var (
	TransportTypeToPortMap = map[TransportType]uint16{
		TransportTypeTChannel: 63002,
		TransportTypeHTTP:     63004,
		TransportTypeGRPC:     63006,
	}
)

// TransportType is a transport type.
type TransportType int

// String returns a string representation of t.
func (t TransportType) String() string {
	switch t {
	case TransportTypeHTTP:
		return "http"
	case TransportTypeTChannel:
		return "tchannel"
	case TransportTypeGRPC:
		return "grpc"
	default:
		return strconv.Itoa(int(t))
	}
}

type ClientInfo struct {
	ClientConfig   transport.ClientConfig
	GRPCClientConn *ggrpc.ClientConn
}

// WithClientInfo wraps a function by setting up a client and server dispatcher and giving
// the function the client configuration to use in tests for the given TransportType.
//
// The server dispatcher will be brought up using all TransportTypes and with the serviceName.
// The client dispatcher will be brought up using the given TransportType for Unary, HTTP for
// Oneway, and the serviceName with a "-client" suffix.
func WithClientInfo(serviceName string, procedures []transport.Procedure, transportType TransportType, logger *zap.Logger) (err error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	dispatcherConfig, err := NewDispatcherConfig(serviceName)
	if err != nil {
		return err
	}
	serverDispatcher, err := NewServerDispatcher(procedures, dispatcherConfig, logger)
	if err != nil {
		return err
	}
	if err := serverDispatcher.Start(); err != nil {
		return err
	}
	return nil
}

// NewClientDispatcher returns a new client Dispatcher.
//
// HTTP always will be configured as an outbound for Oneway.
// gRPC always will be configured as an outbound for Stream.

// NewServerDispatcher returns a new server Dispatcher.
func NewServerDispatcher(procedures []transport.Procedure, config *DispatcherConfig, logger *zap.Logger) (*yarpc.Dispatcher, error) {
	tchannelPort, err := config.GetPort(TransportTypeTChannel)
	if err != nil {
		return nil, err
	}
	httpPort, err := config.GetPort(TransportTypeHTTP)
	if err != nil {
		return nil, err
	}
	grpcPort, err := config.GetPort(TransportTypeGRPC)
	if err != nil {
		return nil, err
	}
	tChannelAddress := fmt.Sprintf("127.0.0.1:%d", tchannelPort)
	tchannelTransport, err := tchannel.NewChannelTransport(tchannel.ServiceName(config.GetServiceName()), tchannel.ListenAddr(tChannelAddress), tchannel.Logger(logger))
	if err != nil {
		return nil, err
	}
	grpcListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", grpcPort))
	if err != nil {
		return nil, err
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: config.GetServiceName(),
		Inbounds: yarpc.Inbounds{
			tchannelTransport.NewInbound(),
			http.NewTransport(http.Logger(logger)).NewInbound(fmt.Sprintf("127.0.0.1:%d", httpPort)),
			grpc.NewTransport(grpc.Logger(logger)).NewInbound(grpcListener),
		},
	},
	)
	dispatcher.Register(procedures)
	return dispatcher, nil
}

// DispatcherConfig is the configuration for a Dispatcher.
type DispatcherConfig struct {
	serviceName         string
	transportTypeToPort map[TransportType]uint16
}

// NewDispatcherConfig returns a new DispatcherConfig with assigned ports.
func NewDispatcherConfig(serviceName string) (*DispatcherConfig, error) {
	transportTypeToPort := TransportTypeToPortMap
	return &DispatcherConfig{serviceName, transportTypeToPort}, nil
}

// GetServiceName gets the service name.
func (d *DispatcherConfig) GetServiceName() string {
	return d.serviceName
}

// GetPort gets the port for the TransportType.
func (d *DispatcherConfig) GetPort(transportType TransportType) (uint16, error) {
	port, ok := d.transportTypeToPort[transportType]
	if !ok {
		return 0, fmt.Errorf("no port for TransportType %v", transportType)
	}
	return port, nil
}

func NewClientDispatcher(transportType TransportType, config *DispatcherConfig, logger *zap.Logger) (*yarpc.Dispatcher, error) {
	tChannelPort := TransportTypeToPortMap[TransportTypeTChannel]
	httpPort := TransportTypeToPortMap[TransportTypeHTTP]
	grpcPort := TransportTypeToPortMap[TransportTypeGRPC]
	onewayOutbound := http.NewTransport(http.Logger(logger)).NewSingleOutbound(fmt.Sprintf("http://127.0.0.1:%d", httpPort))
	streamOutbound := grpc.NewTransport(grpc.Logger(logger)).NewSingleOutbound(fmt.Sprintf("127.0.0.1:%d", grpcPort))
	var unaryOutbound transport.UnaryOutbound
	switch transportType {
	case TransportTypeTChannel:
		tchannelTransport, err := tchannel.NewChannelTransport(tchannel.ServiceName(config.GetServiceName()), tchannel.Logger(logger))
		if err != nil {
			return nil, err
		}
		unaryOutbound = tchannelTransport.NewSingleOutbound(fmt.Sprintf("127.0.0.1:%d", tChannelPort))
	case TransportTypeHTTP:
		unaryOutbound = onewayOutbound
	case TransportTypeGRPC:
		unaryOutbound = streamOutbound
	default:
		return nil, fmt.Errorf("invalid TransportType: %v", transportType)
	}
	return yarpc.NewDispatcher(
		yarpc.Config{
			Name: fmt.Sprintf("%s-client", config.GetServiceName()),
			Outbounds: yarpc.Outbounds{
				config.GetServiceName(): {
					Oneway: onewayOutbound,
					Unary:  unaryOutbound,
					Stream: streamOutbound,
				},
			},
		},
	), nil
}
