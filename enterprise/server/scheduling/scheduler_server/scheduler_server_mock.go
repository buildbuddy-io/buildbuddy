package scheduler_server

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

type SchedulerServerMock struct{}

func NewMockSchedulerServer() *SchedulerServerMock {
	return &SchedulerServerMock{}
}

func (s *SchedulerServerMock) CancelTask(ctx context.Context, taskID string) (bool, error) {
	return true, nil
}

func (s *SchedulerServerMock) RegisterAndStreamWork(stream scpb.Scheduler_RegisterAndStreamWorkServer) error {
	return status.UnimplementedError("not implemented")
}

func (s *SchedulerServerMock) LeaseTask(stream scpb.Scheduler_LeaseTaskServer) error {
	return status.UnimplementedError("not implemented")
}

func (s *SchedulerServerMock) ScheduleTask(ctx context.Context, req *scpb.ScheduleTaskRequest) (*scpb.ScheduleTaskResponse, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (s *SchedulerServerMock) ExistsTask(ctx context.Context, taskID string) (bool, error) {
	return false, status.UnimplementedError("not implemented")
}

func (s *SchedulerServerMock) EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (s *SchedulerServerMock) ReEnqueueTask(ctx context.Context, req *scpb.ReEnqueueTaskRequest) (*scpb.ReEnqueueTaskResponse, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (s *SchedulerServerMock) GetExecutionNodes(ctx context.Context, req *scpb.GetExecutionNodesRequest) (*scpb.GetExecutionNodesResponse, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (s *SchedulerServerMock) GetGroupIDAndDefaultPoolForUser(ctx context.Context, os string, useSelfHosted bool) (string, string, error) {
	return "", "", status.UnimplementedError("not implemented")
}
