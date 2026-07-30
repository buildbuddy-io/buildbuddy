package llmproxy

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	aspb "github.com/buildbuddy-io/buildbuddy/proto/agent_security"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
)

const (
	eventReportQueueSize = 1024
	eventReportTimeout   = 5 * time.Second
)

type grpcEventReporter struct {
	client bbspb.BuildBuddyServiceClient

	mu     sync.Mutex
	closed bool
	queue  chan *RedactionReport
	done   chan struct{}
}

func NewGRPCEventReporter(client bbspb.BuildBuddyServiceClient) EventReporter {
	r := &grpcEventReporter{
		client: client,
		queue:  make(chan *RedactionReport, eventReportQueueSize),
		done:   make(chan struct{}),
	}
	go r.run()
	return r
}

func (r *grpcEventReporter) Report(report *RedactionReport) {
	if report == nil || report.JWT == "" || report.InvocationID == "" || report.AgentSessionID == "" || len(report.Events) == 0 {
		return
	}
	cloned := *report
	cloned.Events = append([]RedactionEvent(nil), report.Events...)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	select {
	case r.queue <- &cloned:
	default:
		log.Warning("Dropping agent security event because the reporting queue is full")
	}
}

func (r *grpcEventReporter) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.queue)
	}
	r.mu.Unlock()

	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *grpcEventReporter) run() {
	defer close(r.done)
	for report := range r.queue {
		if err := r.send(report); err != nil {
			log.Warningf("Could not record agent security event: %s", err)
		}
	}
}

func (r *grpcEventReporter) send(report *RedactionReport) error {
	events := make([]*aspb.SecretRedactionEvent, 0, len(report.Events))
	for _, event := range report.Events {
		events = append(events, &aspb.SecretRedactionEvent{
			SecretName:      event.SecretName,
			ProtectionLayer: protectionLayerProto(event.ProtectionLayer),
			Provider:        providerProto(event.Provider),
			Surface:         surfaceProto(event.Surface),
			EventTime:       timestamppb.New(event.EventTime),
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), eventReportTimeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.ContextTokenStringKey, report.JWT)
	_, err := r.client.RecordAgentSecurityEvents(ctx, &aspb.RecordAgentSecurityEventsRequest{
		InvocationId:   report.InvocationID,
		AgentSessionId: report.AgentSessionID,
		Events:         events,
	})
	return err
}

func protectionLayerProto(layer ProtectionLayer) aspb.ProtectionLayer {
	switch layer {
	case AgentContextHook:
		return aspb.ProtectionLayer_AGENT_CONTEXT_HOOK
	case ModelRequestProxy:
		return aspb.ProtectionLayer_MODEL_REQUEST_PROXY
	default:
		return aspb.ProtectionLayer_PROTECTION_LAYER_UNKNOWN
	}
}

func providerProto(provider Provider) aspb.AgentProvider {
	switch provider {
	case Claude:
		return aspb.AgentProvider_CLAUDE
	case Codex:
		return aspb.AgentProvider_CODEX
	default:
		return aspb.AgentProvider_AGENT_PROVIDER_UNKNOWN
	}
}

func surfaceProto(surface Surface) aspb.RedactionSurface {
	switch surface {
	case ToolOutput:
		return aspb.RedactionSurface_TOOL_OUTPUT
	case RequestBody:
		return aspb.RedactionSurface_REQUEST_BODY
	case RequestHeader:
		return aspb.RedactionSurface_REQUEST_HEADER
	case RequestQuery:
		return aspb.RedactionSurface_REQUEST_QUERY
	default:
		return aspb.RedactionSurface_REDACTION_SURFACE_UNKNOWN
	}
}
