package pb

import "context"

type SchedulerServerInstance struct {
}

func NewSchedulerServerInstance() *SchedulerServerInstance {
	return &SchedulerServerInstance{}
}

func (s SchedulerServerInstance) Add(ctx context.Context, request *SchedulerEntryRequest) (*SchedulerResponse, error) {
	return &SchedulerResponse{request.Key}, nil
}

func (s SchedulerServerInstance) Remove(ctx context.Context, request *SchedulerEntryRequest) (*SchedulerResponse, error) {
	return &SchedulerResponse{request.Key}, nil
}
