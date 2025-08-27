package executor_config

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	expb "github.com/buildbuddy-io/buildbuddy/proto/executor"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
)

func Register(env *real_environment.RealEnv) {
	if env.GetWorkflowService() == nil {
		log.Fatal("WorkflowService is required for ExecutorConfigurationService")
	}
	env.SetExecutorConfigurationService(
		&ExecutorConfigurationService{workflowService: env.GetWorkflowService()})
}

// TODO(iain): read supported versions from experiments(?)
// TODO(iain): support Apply()
type ExecutorConfigurationService struct {
	workflowService interfaces.WorkflowService
}

func (e ExecutorConfigurationService) GetExecutorConfigurations(ctx context.Context, req *expb.GetConfigurationsRequest) (*expb.GetConfigurationsResponse, error) {
	return &expb.GetConfigurationsResponse{
		OperatingSystem: []*expb.OperatingSystemVersion{
			&expb.OperatingSystemVersion{
				Family:      "darwin",
				Version:     "15.6.1",
				DisplayName: "macOS Sequoia 15.6.1",
			},
			&expb.OperatingSystemVersion{
				Family:      "darwin",
				Version:     "14.7.7",
				DisplayName: "macOS Sonoma 14.7.7",
			},
			&expb.OperatingSystemVersion{
				Family:      "darwin",
				Version:     "13.7.8",
				DisplayName: "macOS Ventura 13.7.8",
			},
		},
		Xcode: []*expb.XcodeVersion{
			&expb.XcodeVersion{
				Version:     "16.0",
				DisplayName: "Xcode 16.0",
			},
			&expb.XcodeVersion{
				Version:     "15.4",
				DisplayName: "Xcode 15.4",
			},
			&expb.XcodeVersion{
				Version:     "15.3",
				DisplayName: "Xcode 15.3",
			},
			&expb.XcodeVersion{
				Version:     "15.2",
				DisplayName: "Xcode 15.2",
			},
			&expb.XcodeVersion{
				Version:     "15.1",
				DisplayName: "Xcode 15.1",
			},
			&expb.XcodeVersion{
				Version:     "15.0",
				DisplayName: "Xcode 15.0",
			},
		},
		XcodeSdk: []*expb.XcodeSDK{
			&expb.XcodeSDK{
				Family:      "iOS",
				Version:     "18.5",
				DisplayName: "iOS 18.5",
			},
			&expb.XcodeSDK{
				Family:      "iOS",
				Version:     "18.4",
				DisplayName: "iOS 18.4",
			},
			&expb.XcodeSDK{
				Family:      "iOS",
				Version:     "18.3",
				DisplayName: "iOS 18.3",
			},
			&expb.XcodeSDK{
				Family:      "tvOS",
				Version:     "18.0",
				DisplayName: "tvOS 18.0",
			},
			&expb.XcodeSDK{
				Family:      "watchOS",
				Version:     "11.0",
				DisplayName: "watchOS 11.0",
			},
			&expb.XcodeSDK{
				Family:      "visionOS",
				Version:     "2.0",
				DisplayName: "visionOS 2.0",
			},
		},
	}, nil
}

func (e ExecutorConfigurationService) ApplyExecutorConfiguration(ctx context.Context, req *expb.ApplyConfigurationRequest) (*expb.ApplyConfigurationResponse, error) {
	log.Infof("ApplyExecutorConfiguration({%s})", req)
	if err := check(req); err != nil {
		return nil, err
	}

	coolCtx, cancel := background.ExtendContextForFinalization(ctx, time.Hour)
	defer cancel()
	go func() {
		_, err := e.workflowService.ExecuteAdHocWorkflow(coolCtx,
			&wfpb.ExecuteAdHocWorkflowRequest{
				Command: "ls",
			})
		if err != nil {
			log.Warningf("Error dispatching ad-hoc workflow: %v", err)
		}
	}()
	return &expb.ApplyConfigurationResponse{}, nil
}

func check(req *expb.ApplyConfigurationRequest) error {
	if req.GetOperatingSystem().GetVersion() != "" && req.GetOperatingSystem().GetVersion() != "15.6.1" {
		return status.InvalidArgumentErrorf("Cannot downgrade MacOS version (current version: 15.6.1, requested version: %s). Contact BuildBuddy support for help.",
			req.GetOperatingSystem().GetDisplayName())
	}
	return nil
}
