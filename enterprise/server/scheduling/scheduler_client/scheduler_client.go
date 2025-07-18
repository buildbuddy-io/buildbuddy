package scheduler_client

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor_auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/prototext"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	pool = flag.String("executor.pool", "", "Executor pool name. Only one of this config option or the MY_POOL environment variable should be specified.")
)

const (
	schedulerCheckInInterval         = 5 * time.Second
	registrationFailureRetryInterval = 1 * time.Second

	// idleExecutorMoreWorkTimeout is how long the executor will wait, when
	// idle, before requesting more work from the scheduler. The scheduler
	// itself controls how long the executor will backoff after requesting
	// more work, so this timeout is only used on the initial call.
	idleExecutorMoreWorkTimeout = 5 * time.Second
)

// Options provide overrides for executor registration properties.
type Options struct {
	// TESTING ONLY: overrides the hostname reported when registering executor
	HostnameOverride string
	// TESTING ONLY: overrides the API key sent by the client
	APIKeyOverride string
}

func makeExecutionNode(pool, executorID, executorHostID string, options *Options) (*scpb.ExecutionNode, error) {
	hostname := options.HostnameOverride
	if hostname == "" {
		resHostname, err := resources.GetMyHostname()
		if err != nil {
			return nil, status.InternalErrorf("could not determine local hostname: %s", err)
		}
		hostname = resHostname
	}
	return &scpb.ExecutionNode{
		Host: hostname,
		// TODO: stop setting port once the scheduler no longer requires it.
		Port:                      1,
		AssignableMemoryBytes:     resources.GetAllocatedRAMBytes(),
		AssignableMilliCpu:        resources.GetAllocatedCPUMillis(),
		AssignableCustomResources: resources.GetAllocatedCustomResources(),
		Os:                        resources.GetOS(),
		Arch:                      resources.GetArch(),
		Pool:                      strings.ToLower(pool),
		Version:                   version.Tag(),
		ExecutorId:                executorID,
		ExecutorHostId:            executorHostID,
	}, nil
}

func sleepWithContext(ctx context.Context, delay time.Duration) (done bool) {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(delay):
		return false
	}
}

type Registration struct {
	schedulerClient scpb.SchedulerClient
	taskScheduler   *priority_task_scheduler.PriorityTaskScheduler
	node            *scpb.ExecutionNode
	apiKey          string
	shutdownSignal  chan struct{}

	mu             sync.Mutex
	connected      bool
	idleSeconds    atomic.Int64
	paused         atomic.Bool
	updateRunState chan bool
}

func (r *Registration) getConnected() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connected
}

func (r *Registration) setConnected(connected bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connected = connected
}

func (r *Registration) Check(ctx context.Context) error {
	paused := r.paused.Load()
	if paused || r.getConnected() {
		return nil
	}
	return errors.New("not registered to scheduler yet")
}

const templateContent = `
<div>
  <input type="checkbox" id="paused" {{if .Paused}}checked{{end}}>
  <label for="paused">Pause scheduling (stop accepting new work)</label>
  <script>
     const checkbox = document.getElementById("paused");
     checkbox.addEventListener('change', (event) => {
       fetch("/statusz/scheduler_client", {
           method: "POST",
	   headers:{
	       "Content-Type": "application/x-www-form-urlencoded",
	   },
	   body: new URLSearchParams({"pause": event.currentTarget.checked ? "true" : "false" }),
       })
      .then(response => { window.alert("Changes applied"); console.log(response); })
      .catch(e => window.alert("Fetch failed: " + String(e)));
    });
  </script>
</div>`

var statusTemplate = template.Must(template.New("scheduler_client").Parse(templateContent))

func (r *Registration) Statusz(ctx context.Context) string {
	data := struct {
		Paused bool
	}{
		Paused: r.paused.Load(),
	}
	buf := &bytes.Buffer{}
	if err := statusTemplate.Execute(buf, data); err != nil {
		return fmt.Sprintf("Failed to execute template: %s", err)
	}
	return buf.String()
}

func (r *Registration) ServeStatusz(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := req.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	paused := req.FormValue("pause") == "true"
	r.updateRunState <- !paused
	r.paused.Store(paused)
	w.WriteHeader(http.StatusOK)
}

func (r *Registration) processWorkStream(ctx context.Context, stream scpb.Scheduler_RegisterAndStreamWorkClient, schedulerMsgs chan *scpb.RegisterAndStreamWorkResponse, schedulerErr chan error, registrationTicker, requestMoreWorkTicker *time.Ticker) (bool, error) {
	registrationMsg := &scpb.RegisterAndStreamWorkRequest{
		RegisterExecutorRequest: &scpb.RegisterExecutorRequest{Node: r.node},
	}

	select {
	case <-ctx.Done():
		log.Debugf("Context cancelled, cancelling node registration.")
		return true, nil
	case <-r.shutdownSignal:
		log.Info("Executor shutting down, cancelling node registration.")
		taskReservations := r.taskScheduler.GetQueuedTaskReservations()
		var taskIDs []string
		for _, r := range taskReservations {
			taskIDs = append(taskIDs, r.GetTaskId())
		}
		rsp := &scpb.RegisterAndStreamWorkRequest{
			ShuttingDownRequest: &scpb.ShuttingDownRequest{
				TaskId: taskIDs,
			},
		}
		if err := stream.Send(rsp); err != nil {
			return false, status.UnavailableErrorf("could not send shutdown notification: %s", err)
		}
		return true, nil
	case msg := <-schedulerMsgs:
		if moreWorkResponse := msg.GetAskForMoreWorkResponse(); moreWorkResponse != nil {
			requestMoreWorkTicker.Reset(moreWorkResponse.GetDelay().AsDuration())
			return false, nil
		}
		if msg.EnqueueTaskReservationRequest == nil {
			out, _ := prototext.Marshal(msg)
			return false, status.FailedPreconditionErrorf("message from scheduler did not contain a task reservation request:\n%s", string(out))
		}
		requestMoreWorkTicker.Reset(idleExecutorMoreWorkTimeout)
		rsp, err := r.taskScheduler.EnqueueTaskReservation(ctx, msg.GetEnqueueTaskReservationRequest())
		if err != nil {
			log.Warningf("Task reservation enqueue failed: %s", err)
			return false, status.UnavailableErrorf("could not enqueue task reservation: %s", err)
		}
		rsp.TaskId = msg.GetEnqueueTaskReservationRequest().GetTaskId()
		rspMsg := &scpb.RegisterAndStreamWorkRequest{EnqueueTaskReservationResponse: rsp}
		if err := stream.Send(rspMsg); err != nil {
			return false, status.UnavailableErrorf("could not send task reservation response: %s", err)
		}
	case err := <-schedulerErr:
		return false, status.WrapError(err, "failed to receive message from scheduler")
	case <-registrationTicker.C:
		if err := stream.Send(registrationMsg); err != nil {
			return false, status.UnavailableErrorf("could not send registration message: %s", err)
		}
	case <-requestMoreWorkTicker.C:
		if idleSeconds := r.idleSeconds.Load(); idleSeconds < 5 {
			requestMoreWorkTicker.Reset(idleExecutorMoreWorkTimeout)
			return false, nil // skip
		}
		requestMoreWorkMsg := &scpb.RegisterAndStreamWorkRequest{
			AskForMoreWorkRequest: &scpb.AskForMoreWorkRequest{},
		}
		if err := stream.Send(requestMoreWorkMsg); err != nil {
			return false, status.UnavailableErrorf("could not send registration message: %s", err)
		}
	}
	return false, nil
}

func (r *Registration) monitorExcessCapacity(ctx context.Context) {
	go func() {
		excessCapacityWatch := time.NewTicker(time.Second)
		defer excessCapacityWatch.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-excessCapacityWatch.C:
				if r.taskScheduler.HasExcessCapacity() {
					r.idleSeconds.Add(1)
				} else {
					r.idleSeconds.Store(0)
				}
			}
		}
	}()
}

// maintainRegistrationAndStreamWork maintains registration with a scheduler server using the newer
// RegisterAndStreamWork API which supports both registration and task reservations.
func (r *Registration) maintainRegistrationAndStreamWork(ctx context.Context) {
	registrationMsg := &scpb.RegisterAndStreamWorkRequest{
		RegisterExecutorRequest: &scpb.RegisterExecutorRequest{Node: r.node},
	}

	defer r.setConnected(false)

	registrationTicker := time.NewTicker(schedulerCheckInInterval)
	defer registrationTicker.Stop()

	requestMoreWorkTicker := time.NewTicker(idleExecutorMoreWorkTimeout)
	defer requestMoreWorkTicker.Stop()

	for {
		stream, err := r.schedulerClient.RegisterAndStreamWork(ctx)
		if err != nil {
			if done := sleepWithContext(ctx, registrationFailureRetryInterval); done {
				log.Debugf("Context cancelled, cancelling node registration.")
				return
			}
			continue
		}
		if err := stream.Send(registrationMsg); err != nil {
			log.Errorf("error registering node with scheduler: %s, will retry...", err)
			continue
		}

		r.setConnected(true)

		r.monitorExcessCapacity(stream.Context())
		schedulerMsgs := make(chan *scpb.RegisterAndStreamWorkResponse)
		schedulerErr := make(chan error, 1)
		go func() {
			for {
				msg, err := stream.Recv()
				if err != nil {
					schedulerErr <- err
					break
				}
				select {
				case schedulerMsgs <- msg:
				case <-stream.Context().Done():
					return
				}
			}
		}()

		for {
			done, err := r.processWorkStream(ctx, stream, schedulerMsgs, schedulerErr, registrationTicker, requestMoreWorkTicker)
			if err != nil {
				_ = stream.CloseSend()
				log.Warningf("Error maintaining registration with scheduler, will retry: %s", err)
				break
			}
			if done {
				_ = stream.CloseSend()
				return
			}
		}
		r.setConnected(false)
		if done := sleepWithContext(ctx, registrationFailureRetryInterval); done {
			log.Debugf("Context cancelled, cancelling node registration.")
			return
		}
	}
}

// Start registers the executor with the scheduler and maintains that registration until the context is cancelled.
func (r *Registration) Start(ctx context.Context) {
	if r.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, r.apiKey)
	}

	go func() {
		r.watchRunState(ctx)
	}()

	r.updateRunState <- true
}

func (r *Registration) watchRunState(rootContext context.Context) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(rootContext)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			cancel()
			return
		case running := <-r.updateRunState:
			// Always cancel first
			cancel()
			wg.Wait()
			ctx, cancel = context.WithCancel(rootContext)

			// Restart if it should be running
			if running {
				wg.Add(1)
				go func() {
					defer wg.Done()
					r.maintainRegistrationAndStreamWork(ctx)
				}()
			}
		}
	}
}

// NewRegistration creates a handle to maintain registration with a scheduler server.
// The registration is not initiated until Start is called on the returned handle.
func NewRegistration(env environment.Env, taskScheduler *priority_task_scheduler.PriorityTaskScheduler, executorID, executorHostID string, options *Options) (*Registration, error) {
	poolName := *pool
	if poolName == "" {
		poolName = resources.GetPoolName()
	} else if resources.GetPoolName() != "" {
		log.Fatal("Only one of the `MY_POOL` environment variable and `executor.pool` config option may be set")
	}
	node, err := makeExecutionNode(poolName, executorID, executorHostID, options)
	if err != nil {
		return nil, status.InternalErrorf("Error determining node properties: %s", err)
	}
	apiKey := executor_auth.APIKey()
	if options.APIKeyOverride != "" {
		apiKey = options.APIKeyOverride
	}

	shutdownSignal := make(chan struct{})
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		close(shutdownSignal)
		return nil
	})

	registration := &Registration{
		schedulerClient: env.GetSchedulerClient(),
		taskScheduler:   taskScheduler,
		node:            node,
		apiKey:          apiKey,
		shutdownSignal:  shutdownSignal,
		updateRunState:  make(chan bool),
	}
	env.GetHealthChecker().AddHealthCheck("registered_to_scheduler", registration)
	statusz.AddSection("scheduler_client", "Remote execution scheduler client", registration)
	return registration, nil
}
