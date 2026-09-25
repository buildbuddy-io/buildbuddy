package usage_service

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/types/known/timestamppb"

	usage_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage/config"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

var (
	usageStartDate      = flag.String("app.usage_start_date", "", "If set, usage data will only be viewable on or after this timestamp. Specified in RFC3339 format, like 2021-10-01T00:00:00Z")
	alertsEnabled       = flag.Bool("app.usage_alerts_enabled", false, "If set, usage alerts will be enabled in the UI.")
	billEnabled         = flag.Bool("app.usage_bill_enabled", false, "If set, the current bill from Metronome can be viewed on the usage page.")
	readUsageFromOLAPDB = flag.Bool("app.read_usage_from_olap_db", false, "If enabled, read Usage page data from OLAP DB.")
)

const (
	// MaxUsageAlertingRulesPerGroup is the maximum number of usage alerting
	// rules a group can create.
	MaxUsageAlertingRulesPerGroup = 100

	billCacheTTL = 15 * time.Minute
)

// UsageField defines a Usage proto field returned by GetUsage.
type UsageField struct {
	// Name is the Usage proto field name. It is also used as the SELECT alias.
	Name string
	// PrimaryDBExpression is the SQL aggregation expression for the primary DB.
	PrimaryDBExpression string
	// OLAPExpression is the ClickHouse RawUsage aggregation expression.
	OLAPExpression string
	// AlertingMetric is the usage alerting enum corresponding to this field.
	AlertingMetric usagepb.UsageAlertingMetric_Value
}

// UsageFields defines each Usage metric returned by GetUsage.
var UsageFields = []UsageField{
	{
		Name:                "invocations",
		PrimaryDBExpression: "SUM(invocations)",
		OLAPExpression:      rawUsageSum(sku.BuildEventsBESCount),
		AlertingMetric:      usagepb.UsageAlertingMetric_INVOCATIONS,
	},
	{
		Name:                "action_cache_hits",
		PrimaryDBExpression: "SUM(action_cache_hits)",
		OLAPExpression:      rawUsageSum(sku.RemoteCacheACHits),
		AlertingMetric:      usagepb.UsageAlertingMetric_ACTION_CACHE_HITS,
	},
	{
		Name:                "total_cached_action_exec_usec",
		PrimaryDBExpression: "SUM(total_cached_action_exec_usec)",
		OLAPExpression:      rawUsageSumUsec(sku.RemoteCacheACCachedExecDurationNanos),
		AlertingMetric:      usagepb.UsageAlertingMetric_TOTAL_CACHED_ACTION_EXEC_USEC,
	},
	{
		Name:                "cas_cache_hits",
		PrimaryDBExpression: "SUM(cas_cache_hits)",
		OLAPExpression:      rawUsageSum(sku.RemoteCacheCASHits),
		AlertingMetric:      usagepb.UsageAlertingMetric_CAS_CACHE_HITS,
	},
	{
		Name:                "total_download_size_bytes",
		PrimaryDBExpression: "SUM(total_download_size_bytes)",
		OLAPExpression:      rawUsageSum(sku.RemoteCacheCASDownloadedBytes),
		AlertingMetric:      usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_external_download_size_bytes",
		PrimaryDBExpression: "SUM(CASE WHEN origin <> 'internal' THEN total_download_size_bytes ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteCacheCASDownloadedBytes,
			rawUsageLabelNotEquals(sku.Origin, sku.OriginInternal),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_TOTAL_EXTERNAL_DOWNLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_internal_download_size_bytes",
		PrimaryDBExpression: "SUM(CASE WHEN (origin = 'internal' AND NOT (client = 'executor-workflows' OR client = 'bazel')) THEN total_download_size_bytes ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteCacheCASDownloadedBytes,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelNotIn(sku.Client, sku.ClientExecutorWorkflows, sku.ClientBazel),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_TOTAL_INTERNAL_DOWNLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_workflow_download_size_bytes",
		PrimaryDBExpression: "SUM(CASE WHEN (origin = 'internal' AND (client = 'executor-workflows' OR client = 'bazel')) THEN total_download_size_bytes ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteCacheCASDownloadedBytes,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelIn(sku.Client, sku.ClientExecutorWorkflows, sku.ClientBazel),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_TOTAL_WORKFLOW_DOWNLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_upload_size_bytes",
		PrimaryDBExpression: "SUM(total_upload_size_bytes)",
		OLAPExpression:      rawUsageSum(sku.RemoteCacheCASUploadedBytes),
		AlertingMetric:      usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_external_upload_size_bytes",
		PrimaryDBExpression: "SUM(CASE WHEN origin <> 'internal' THEN total_upload_size_bytes ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteCacheCASUploadedBytes,
			rawUsageLabelNotEquals(sku.Origin, sku.OriginInternal),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_TOTAL_EXTERNAL_UPLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_internal_upload_size_bytes",
		PrimaryDBExpression: "SUM(CASE WHEN (origin = 'internal' AND NOT (client = 'executor-workflows' OR client = 'bazel')) THEN total_upload_size_bytes ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteCacheCASUploadedBytes,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelNotIn(sku.Client, sku.ClientExecutorWorkflows, sku.ClientBazel),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_TOTAL_INTERNAL_UPLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_workflow_upload_size_bytes",
		PrimaryDBExpression: "SUM(CASE WHEN (origin = 'internal' AND (client = 'executor-workflows' OR client = 'bazel')) THEN total_upload_size_bytes ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteCacheCASUploadedBytes,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelIn(sku.Client, sku.ClientExecutorWorkflows, sku.ClientBazel),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_TOTAL_WORKFLOW_UPLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_customer_proxy_download_size_bytes",
		PrimaryDBExpression: "SUM(CASE WHEN proxy = 'customer' THEN total_download_size_bytes ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteCacheCASDownloadedBytes,
			rawUsageLabelEquals(sku.Proxy, sku.ProxyCustomer),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_TOTAL_CUSTOMER_PROXY_DOWNLOAD_SIZE_BYTES,
	},
	{
		Name:                "total_customer_proxy_upload_size_bytes",
		PrimaryDBExpression: "SUM(CASE WHEN proxy = 'customer' THEN total_upload_size_bytes ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteCacheCASUploadedBytes,
			rawUsageLabelEquals(sku.Proxy, sku.ProxyCustomer),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_TOTAL_CUSTOMER_PROXY_UPLOAD_SIZE_BYTES,
	},
	{
		Name:                "linux_execution_duration_usec",
		PrimaryDBExpression: "SUM(linux_execution_duration_usec)",
		OLAPExpression: rawUsageSumUsec(
			sku.RemoteExecutionExecuteWorkerDurationNanos,
			rawUsageLabelEquals(sku.OS, sku.OSLinux),
			rawUsageLabelEquals(sku.SelfHosted, sku.SelfHostedFalse),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_LINUX_EXECUTION_DURATION_USEC,
	},
	{
		Name:                "cloud_rbe_linux_execution_duration_usec",
		PrimaryDBExpression: "SUM(CASE WHEN (origin = 'internal' AND NOT (client = 'executor-workflows' OR client = 'bazel')) THEN linux_execution_duration_usec ELSE 0 END)",
		OLAPExpression: rawUsageSumUsec(
			sku.RemoteExecutionExecuteWorkerDurationNanos,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelNotIn(sku.Client, sku.ClientExecutorWorkflows, sku.ClientBazel),
			rawUsageLabelEquals(sku.OS, sku.OSLinux),
			rawUsageLabelEquals(sku.SelfHosted, sku.SelfHostedFalse),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_CLOUD_RBE_LINUX_EXECUTION_DURATION_USEC,
	},
	{
		Name:                "cloud_workflow_linux_execution_duration_usec",
		PrimaryDBExpression: "SUM(CASE WHEN (origin = 'internal' AND (client = 'executor-workflows' OR client = 'bazel')) THEN linux_execution_duration_usec ELSE 0 END)",
		OLAPExpression: rawUsageSumUsec(
			sku.RemoteExecutionExecuteWorkerDurationNanos,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelIn(sku.Client, sku.ClientExecutorWorkflows, sku.ClientBazel),
			rawUsageLabelEquals(sku.OS, sku.OSLinux),
			rawUsageLabelEquals(sku.SelfHosted, sku.SelfHostedFalse),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_CLOUD_WORKFLOW_LINUX_EXECUTION_DURATION_USEC,
	},
	{
		Name:                "cloud_cpu_nanos",
		PrimaryDBExpression: "SUM(CASE WHEN origin = 'internal' THEN cpu_nanos ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteExecutionExecuteWorkerCPUNanos,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelEquals(sku.OS, sku.OSLinux),
			rawUsageLabelEquals(sku.SelfHosted, sku.SelfHostedFalse),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_CLOUD_CPU_NANOS,
	},
	{
		Name:                "cloud_rbe_cpu_nanos",
		PrimaryDBExpression: "SUM(CASE WHEN (origin = 'internal' AND NOT (client = 'executor-workflows' OR client = 'bazel')) THEN cpu_nanos ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteExecutionExecuteWorkerCPUNanos,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelNotIn(sku.Client, sku.ClientExecutorWorkflows, sku.ClientBazel),
			rawUsageLabelEquals(sku.OS, sku.OSLinux),
			rawUsageLabelEquals(sku.SelfHosted, sku.SelfHostedFalse),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_CLOUD_RBE_CPU_NANOS,
	},
	{
		Name:                "cloud_workflow_cpu_nanos",
		PrimaryDBExpression: "SUM(CASE WHEN (origin = 'internal' AND (client = 'executor-workflows' OR client = 'bazel')) THEN cpu_nanos ELSE 0 END)",
		OLAPExpression: rawUsageSum(
			sku.RemoteExecutionExecuteWorkerCPUNanos,
			rawUsageLabelEquals(sku.Origin, sku.OriginInternal),
			rawUsageLabelIn(sku.Client, sku.ClientExecutorWorkflows, sku.ClientBazel),
			rawUsageLabelEquals(sku.OS, sku.OSLinux),
			rawUsageLabelEquals(sku.SelfHosted, sku.SelfHostedFalse),
		),
		AlertingMetric: usagepb.UsageAlertingMetric_CLOUD_WORKFLOW_CPU_NANOS,
	},
}

// olapOnlyUsageSKUs are the SKUs returned as per-dimension breakdowns in
// Usage. These metrics are only recorded in the OLAP DB, so they have no
// UsageFields entry: UsageFields
// requires a primary DB expression and a UsageAlertingMetric for each field.
// Nothing prevents alerting on them (usage alerts are evaluated against the
// OLAP DB); it just needs an alerting metric and a UsageFields entry whose
// primary DB expression is 0. Each SKU is returned per combination of
// execution dimensions rather than as a single aggregate, so the Usage page
// can break it down as it likes.
var olapOnlyUsageSKUs = slices.Concat(computeUsageSKUs, snapshotUsageSKUs)

// computeUsageSKUs are recorded in compute-unit-nanoseconds but returned in
// compute-unit-microseconds, divided per row before summing: a few thousand
// compute units running for a whole month overflow an Int64 of nanoseconds.
var computeUsageSKUs = []sku.SKU{
	sku.RemoteExecutionExecuteFixedComputeNanos,
	sku.RemoteExecutionExecuteFlexibleComputeNanos,
}

var snapshotUsageSKUs = []sku.SKU{
	sku.RemoteExecutionExecuteRemoteSnapshotSavedBytes,
	sku.RemoteExecutionExecuteLocalSnapshotSavedBytes,
}

// UsageFieldForAlertingMetric returns the Usage field mapped to an alerting metric.
func UsageFieldForAlertingMetric(metric usagepb.UsageAlertingMetric_Value) (*UsageField, bool) {
	for i := range UsageFields {
		if UsageFields[i].AlertingMetric == metric {
			return &UsageFields[i], true
		}
	}
	return nil, false
}

func rawUsageSum(usageSKU sku.SKU, conditions ...string) string {
	return rawUsageSumOf("count", usageSKU, conditions...)
}

// rawUsageSumOf sums the given expression over the rows matching the SKU and
// conditions.
func rawUsageSumOf(expression string, usageSKU sku.SKU, conditions ...string) string {
	sum := "SUM(CASE WHEN sku = '" + string(usageSKU) + "'"
	if len(conditions) > 0 {
		sum += " AND " + strings.Join(conditions, " AND ")
	}
	return sum + " THEN " + expression + " ELSE 0 END)"
}

// rawUsageSumUsec sums nanosecond counts and returns microseconds. Each row is
// divided before summing so that large nanosecond totals can't overflow the
// sum; the truncation costs at most a microsecond per row.
func rawUsageSumUsec(usageSKU sku.SKU, conditions ...string) string {
	return rawUsageSumOf("intDiv(count, 1000)", usageSKU, conditions...)
}

func rawUsageLabel(name sku.LabelName) string {
	return "labels['" + string(name) + "']"
}

func rawUsageLabelEquals(name sku.LabelName, value sku.LabelValue) string {
	return rawUsageLabel(name) + " = '" + string(value) + "'"
}

func rawUsageLabelNotEquals(name sku.LabelName, value sku.LabelValue) string {
	return rawUsageLabel(name) + " != '" + string(value) + "'"
}

func rawUsageLabelIn(name sku.LabelName, values ...sku.LabelValue) string {
	return rawUsageLabel(name) + " IN (" + quotedRawUsageLabelValues(values...) + ")"
}

func rawUsageLabelNotIn(name sku.LabelName, values ...sku.LabelValue) string {
	return rawUsageLabel(name) + " NOT IN (" + quotedRawUsageLabelValues(values...) + ")"
}

func quotedRawUsageLabelValues(values ...sku.LabelValue) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, "'"+string(value)+"'")
	}
	return strings.Join(quoted, ", ")
}

type usageService struct {
	env            environment.Env
	clock          clockwork.Clock
	olapdbh        interfaces.OLAPDBHandle
	readFromOLAPDB bool

	// start is the earliest moment in time at which we're able to return usage
	// data to the user. We return usage data from the start of the month
	// corresponding to this date.
	start time.Time

	// metronome is nil if the read-only key is not configured.
	metronome *metronome.Client
	bills     lru.LRU[*usagepb.Bill]
}

// Register registers the usage service if usage tracking is enabled.
func Register(env *real_environment.RealEnv) error {
	if usage_config.UsageTrackingEnabled() {
		service, err := New(env, env.GetClock())
		if err != nil {
			return err
		}
		env.SetUsageService(service)
	}
	return nil
}

// New returns a usage service configured with the provided env and clock.
func New(env environment.Env, clock clockwork.Clock) (*usageService, error) {
	olapdbh := env.GetOLAPDBHandle()
	readFromOLAPDB := *readUsageFromOLAPDB
	if readFromOLAPDB && olapdbh == nil {
		return nil, status.FailedPreconditionError("OLAP DB handle must be configured when app.read_usage_from_olap_db is true")
	}
	s := &usageService{
		env:            env,
		clock:          clock,
		olapdbh:        olapdbh,
		readFromOLAPDB: readFromOLAPDB,
		start:          configuredUsageStartDate(),
	}
	if metronome.ReadOnlyConfigured() {
		client, err := metronome.NewReadOnlyClient(nil, nil)
		if err != nil {
			return nil, err
		}
		s.metronome = client
		s.bills, err = lru.New(&lru.Config[*usagepb.Bill]{
			Clock:      clock,
			TTL:        billCacheTTL,
			SizeFn:     func(*usagepb.Bill) int64 { return 1 },
			MaxSize:    10_000,
			ThreadSafe: true,
		})
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// GetAlertsEnabled returns whether usage alerting should be exposed to the frontend.
func (s *usageService) GetAlertsEnabled() bool {
	return *alertsEnabled
}

func (s *usageService) GetBillEnabled() bool {
	return *billEnabled && s.metronome != nil
}

// Just a little function to make testing less miserable.
func (s *usageService) GetUsageInternal(ctx context.Context, g *tables.Group, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error) {
	earliestAvailableUsagePeriod := max(g.CreatedAtUsec, configuredUsageStartDate().UnixMicro())
	now := s.clock.Now().UTC()
	endOfLatestUsagePeriod := addCalendarMonths(getUsagePeriod(now).Start(), 1)

	var availableUsagePeriods []string
	for usagePeriodStart := time.UnixMicro(earliestAvailableUsagePeriod); usagePeriodStart.Before(endOfLatestUsagePeriod); usagePeriodStart = addCalendarMonths(usagePeriodStart, 1) {
		p := getUsagePeriod(usagePeriodStart)
		availableUsagePeriods = append([]string{p.String()}, availableUsagePeriods...)
	}

	var start, end time.Time
	if req.GetUsagePeriod() != "" {
		p, err := parseUsagePeriod(req.GetUsagePeriod())
		if err != nil {
			return nil, err
		}
		start = p.Start()
		end = addCalendarMonths(start, 1)
	} else {
		start = getUsagePeriod(now).Start()
		end = addCalendarMonths(start, 1)
	}

	useOLAP := s.readFromOLAPDB || req.GetUseOlap()
	if useOLAP && s.olapdbh == nil {
		return nil, status.FailedPreconditionError("OLAP DB handle must be configured for usage OLAP reads")
	}

	usages, err := s.scanUsages(ctx, g.GroupID, start, end, useOLAP)
	if err != nil {
		return nil, err
	}

	rsp := &usagepb.GetUsageResponse{
		AvailableUsagePeriods: availableUsagePeriods,
	}
	period := getUsagePeriod(start).String()

	aggregateUsage := &usagepb.Usage{
		Period: period,
	}

	for _, u := range usages {
		aggregateUsage.Invocations += u.GetInvocations()
		aggregateUsage.ActionCacheHits += u.GetActionCacheHits()
		aggregateUsage.CasCacheHits += u.GetCasCacheHits()
		aggregateUsage.TotalDownloadSizeBytes += u.GetTotalDownloadSizeBytes()
		aggregateUsage.TotalExternalDownloadSizeBytes += u.GetTotalExternalDownloadSizeBytes()
		aggregateUsage.TotalInternalDownloadSizeBytes += u.GetTotalInternalDownloadSizeBytes()
		aggregateUsage.TotalWorkflowDownloadSizeBytes += u.GetTotalWorkflowDownloadSizeBytes()
		aggregateUsage.TotalUploadSizeBytes += u.GetTotalUploadSizeBytes()
		aggregateUsage.TotalExternalUploadSizeBytes += u.GetTotalExternalUploadSizeBytes()
		aggregateUsage.TotalInternalUploadSizeBytes += u.GetTotalInternalUploadSizeBytes()
		aggregateUsage.TotalWorkflowUploadSizeBytes += u.GetTotalWorkflowUploadSizeBytes()
		aggregateUsage.TotalCustomerProxyDownloadSizeBytes += u.GetTotalCustomerProxyDownloadSizeBytes()
		aggregateUsage.TotalCustomerProxyUploadSizeBytes += u.GetTotalCustomerProxyUploadSizeBytes()
		aggregateUsage.LinuxExecutionDurationUsec += u.GetLinuxExecutionDurationUsec()
		aggregateUsage.TotalCachedActionExecUsec += u.GetTotalCachedActionExecUsec()
		aggregateUsage.CloudRbeLinuxExecutionDurationUsec += u.GetCloudRbeLinuxExecutionDurationUsec()
		aggregateUsage.CloudWorkflowLinuxExecutionDurationUsec += u.GetCloudWorkflowLinuxExecutionDurationUsec()
		aggregateUsage.CloudCpuNanos += u.GetCloudCpuNanos()
		aggregateUsage.CloudRbeCpuNanos += u.GetCloudRbeCpuNanos()
		aggregateUsage.CloudWorkflowCpuNanos += u.GetCloudWorkflowCpuNanos()
	}

	if useOLAP {
		if err := s.addOLAPOnlyUsage(ctx, g.GroupID, start, end, aggregateUsage); err != nil {
			return nil, err
		}
	}

	rsp.Usage = aggregateUsage
	rsp.DailyUsage = usages
	return rsp, nil
}

func (s *usageService) GetUsage(ctx context.Context, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()

	g, err := s.env.GetUserDB().GetGroupByID(ctx, groupID)
	if err != nil {
		return nil, err
	}

	return s.GetUsageInternal(ctx, g, req)
}

func (s *usageService) GetCurrentBill(ctx context.Context, req *usagepb.GetCurrentBillRequest) (*usagepb.GetCurrentBillResponse, error) {
	if !s.GetBillEnabled() {
		return nil, status.UnimplementedError("viewing the current bill is not enabled")
	}
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()
	if bill, ok := s.bills.Get(groupID); ok {
		return &usagepb.GetCurrentBillResponse{Bill: bill}, nil
	}
	g, err := s.env.GetUserDB().GetGroupByID(ctx, groupID)
	if err != nil {
		return nil, err
	}
	var bill *usagepb.Bill
	if g.Status == grpb.Group_USAGE_BASED_GROUP_STATUS {
		bill, err = s.fetchBill(ctx, groupID)
		if err != nil {
			return nil, err
		}
	}
	s.bills.Add(groupID, bill)
	return &usagepb.GetCurrentBillResponse{Bill: bill}, nil
}

// fetchBill returns nil if the group has no Metronome customer or no invoice
// for the current period.
func (s *usageService) fetchBill(ctx context.Context, groupID string) (*usagepb.Bill, error) {
	customerID, err := s.metronome.FindCustomerID(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if customerID == "" {
		return nil, nil
	}
	now := s.clock.Now()
	invoice, err := s.metronome.GetCurrentInvoice(ctx, customerID, now)
	if err != nil {
		return nil, err
	}
	if invoice == nil {
		return nil, nil
	}
	bill := &usagepb.Bill{
		PeriodStart: timestamppb.New(invoice.StartTimestamp),
		PeriodEnd:   timestamppb.New(invoice.EndTimestamp),
		TotalCents:  invoice.Total,
		FetchedAt:   timestamppb.New(now),
	}
	for _, item := range invoice.UsageLineItems() {
		bill.LineItems = append(bill.LineItems, &usagepb.BillLineItem{
			Name:           item.Name,
			Quantity:       item.Quantity,
			UnitPriceCents: item.UnitPrice,
			TotalCents:     item.Total,
			PeriodStart:    timestamppb.New(item.StartingAt),
			PeriodEnd:      timestamppb.New(item.EndingBefore),
		})
	}
	credit, err := s.metronome.GetCredit(ctx, customerID, now)
	if err != nil {
		return nil, err
	}
	if credit != nil {
		bill.CreditGrantedCents = credit.Granted
		bill.CreditUsedCents = credit.Granted - credit.Remaining
	}
	return bill, nil
}

func (s *usageService) GetUsageAlertingRules(ctx context.Context, req *usagepb.GetUsageAlertingRulesRequest) (*usagepb.GetUsageAlertingRulesResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()
	if groupID == "" {
		return nil, status.PermissionDeniedError("group ID is required")
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		return nil, err
	}

	rq := s.env.GetDBHandle().NewQuery(ctx, "usage_service_get_alerting_rules").Raw(`
		SELECT *
		FROM "UsageAlertingRules"
		WHERE group_id = ?
		ORDER BY created_at_usec ASC, usage_alerting_rule_id ASC
	`, groupID)
	rows, err := db.ScanAll(rq, &tables.UsageAlertingRule{})
	if err != nil {
		return nil, err
	}

	rsp := &usagepb.GetUsageAlertingRulesResponse{}
	for _, row := range rows {
		rule, err := s.usageAlertingRuleToProto(ctx, row)
		if err != nil {
			return nil, err
		}
		rsp.UsageAlertingRule = append(rsp.UsageAlertingRule, rule)
	}
	return rsp, nil
}

func (s *usageService) CreateUsageAlertingRule(ctx context.Context, req *usagepb.CreateUsageAlertingRuleRequest) (*usagepb.CreateUsageAlertingRuleResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()
	if groupID == "" {
		return nil, status.PermissionDeniedError("group ID is required")
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		return nil, err
	}

	config := req.GetConfiguration()
	if err := validateUsageAlertingRuleConfiguration(config); err != nil {
		return nil, err
	}
	id, err := tables.PrimaryKeyForTable((&tables.UsageAlertingRule{}).TableName())
	if err != nil {
		return nil, err
	}
	row := &tables.UsageAlertingRule{
		UsageAlertingRuleID: id,
		GroupID:             groupID,
		UserID:              u.GetUserID(),
		UsageAlertingMetric: config.GetMetric(),
		AbsoluteThreshold:   config.GetAbsoluteThreshold(),
		Window:              config.GetWindow(),
	}
	err = s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		count, err := s.countUsageAlertingRules(ctx, tx, groupID)
		if err != nil {
			return err
		}
		if count >= MaxUsageAlertingRulesPerGroup {
			return status.ResourceExhaustedErrorf("usage alerting rule limit exceeded (%d)", MaxUsageAlertingRulesPerGroup)
		}
		return tx.NewQuery(ctx, "usage_service_create_alerting_rule").Create(row)
	})
	if err != nil {
		if s.env.GetDBHandle().IsDuplicateKeyError(err) {
			return nil, status.AlreadyExistsError("usage alerting rule already exists")
		}
		return nil, err
	}

	rule, err := s.usageAlertingRuleToProto(ctx, row)
	if err != nil {
		return nil, err
	}
	return &usagepb.CreateUsageAlertingRuleResponse{
		UsageAlertingRule: rule,
	}, nil
}

func (s *usageService) DeleteUsageAlertingRule(ctx context.Context, req *usagepb.DeleteUsageAlertingRuleRequest) (*usagepb.DeleteUsageAlertingRuleResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()
	if groupID == "" {
		return nil, status.PermissionDeniedError("group ID is required")
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		return nil, err
	}
	ruleID := req.GetUsageAlertingRuleId()
	if ruleID == "" {
		return nil, status.InvalidArgumentError("usage alerting rule ID is required")
	}

	result := s.env.GetDBHandle().NewQuery(ctx, "usage_service_delete_alerting_rule").Raw(`
		DELETE FROM "UsageAlertingRules"
		WHERE usage_alerting_rule_id = ? AND group_id = ?
	`, ruleID, groupID).Exec()
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, status.NotFoundError("usage alerting rule not found")
	}
	return &usagepb.DeleteUsageAlertingRuleResponse{}, nil
}

func (s *usageService) countUsageAlertingRules(ctx context.Context, dbh interfaces.DB, groupID string) (int64, error) {
	row := &struct{ Count int64 }{}
	if err := dbh.NewQuery(ctx, "usage_service_count_alerting_rules").Raw(`
		SELECT COUNT(*) AS count
		FROM "UsageAlertingRules"
		WHERE group_id = ?
	`, groupID).Take(row); err != nil {
		return 0, err
	}
	return row.Count, nil
}

func (s *usageService) scanUsages(ctx context.Context, groupID string, start, end time.Time, useOLAP bool) ([]*usagepb.Usage, error) {
	if useOLAP {
		return s.scanOLAPUsages(ctx, groupID, start, end)
	}
	return s.scanPrimaryDBUsages(ctx, groupID, start, end)
}

func (s *usageService) scanPrimaryDBUsages(ctx context.Context, groupID string, start, end time.Time) ([]*usagepb.Usage, error) {
	dbh := s.env.GetDBHandle()
	selectExpressions := []string{dbh.DateFromUsecTimestamp("period_start_usec", 0) + ` AS period`}
	for _, field := range UsageFields {
		selectExpressions = append(selectExpressions, fmt.Sprintf("%s AS %s", field.PrimaryDBExpression, field.Name))
	}
	rq := dbh.NewQuery(ctx, "usage_service_scan").Raw(`
		SELECT `+strings.Join(selectExpressions, ",\n\t\t")+`
		FROM "Usages"
		WHERE period_start_usec >= ? AND period_start_usec < ?
		AND group_id = ?
		GROUP BY period
		ORDER BY period ASC
	`, start.UnixMicro(), end.UnixMicro(), groupID)
	return scanUsageRows(rq)
}

func (s *usageService) scanOLAPUsages(ctx context.Context, groupID string, start, end time.Time) ([]*usagepb.Usage, error) {
	dbh := s.olapdbh
	selectExpressions := []string{"formatDateTime(period_start, '%F') AS period"}
	for _, field := range UsageFields {
		selectExpressions = append(selectExpressions, fmt.Sprintf("%s AS %s", field.OLAPExpression, field.Name))
	}
	rq := dbh.NewQuery(ctx, "usage_service_scan_olap").Raw(`
		SELECT `+strings.Join(selectExpressions, ",\n\t\t")+`
		FROM Usage
		WHERE period_start >= ? AND period_start < ?
		AND group_id = ?
		GROUP BY period
		ORDER BY period ASC
	`, start, end, groupID)
	return scanUsageRows(rq)
}

// usageRow is the scan target for the per-period usage queries: the Usage
// proto without its repeated breakdown fields, which gorm can't scan into
// since it mistakes them for relations. Field names must match the
// UsageFields names, converted to CamelCase.
type usageRow struct {
	Period                                  string
	Invocations                             int64
	ActionCacheHits                         int64
	TotalCachedActionExecUsec               int64
	CasCacheHits                            int64
	TotalDownloadSizeBytes                  int64
	TotalExternalDownloadSizeBytes          int64
	TotalInternalDownloadSizeBytes          int64
	TotalWorkflowDownloadSizeBytes          int64
	TotalUploadSizeBytes                    int64
	TotalExternalUploadSizeBytes            int64
	TotalInternalUploadSizeBytes            int64
	TotalWorkflowUploadSizeBytes            int64
	TotalCustomerProxyDownloadSizeBytes     int64
	TotalCustomerProxyUploadSizeBytes       int64
	LinuxExecutionDurationUsec              int64
	CloudRbeLinuxExecutionDurationUsec      int64
	CloudWorkflowLinuxExecutionDurationUsec int64
	CloudCpuNanos                           int64
	CloudRbeCpuNanos                        int64
	CloudWorkflowCpuNanos                   int64
}

func (r *usageRow) toProto() *usagepb.Usage {
	return &usagepb.Usage{
		Period:                                  r.Period,
		Invocations:                             r.Invocations,
		ActionCacheHits:                         r.ActionCacheHits,
		TotalCachedActionExecUsec:               r.TotalCachedActionExecUsec,
		CasCacheHits:                            r.CasCacheHits,
		TotalDownloadSizeBytes:                  r.TotalDownloadSizeBytes,
		TotalExternalDownloadSizeBytes:          r.TotalExternalDownloadSizeBytes,
		TotalInternalDownloadSizeBytes:          r.TotalInternalDownloadSizeBytes,
		TotalWorkflowDownloadSizeBytes:          r.TotalWorkflowDownloadSizeBytes,
		TotalUploadSizeBytes:                    r.TotalUploadSizeBytes,
		TotalExternalUploadSizeBytes:            r.TotalExternalUploadSizeBytes,
		TotalInternalUploadSizeBytes:            r.TotalInternalUploadSizeBytes,
		TotalWorkflowUploadSizeBytes:            r.TotalWorkflowUploadSizeBytes,
		TotalCustomerProxyDownloadSizeBytes:     r.TotalCustomerProxyDownloadSizeBytes,
		TotalCustomerProxyUploadSizeBytes:       r.TotalCustomerProxyUploadSizeBytes,
		LinuxExecutionDurationUsec:              r.LinuxExecutionDurationUsec,
		CloudRbeLinuxExecutionDurationUsec:      r.CloudRbeLinuxExecutionDurationUsec,
		CloudWorkflowLinuxExecutionDurationUsec: r.CloudWorkflowLinuxExecutionDurationUsec,
		CloudCpuNanos:                           r.CloudCpuNanos,
		CloudRbeCpuNanos:                        r.CloudRbeCpuNanos,
		CloudWorkflowCpuNanos:                   r.CloudWorkflowCpuNanos,
	}
}

func scanUsageRows(rq interfaces.DBRawQuery) ([]*usagepb.Usage, error) {
	rows, err := db.ScanAll(rq, &usageRow{})
	if err != nil {
		return nil, err
	}
	usages := make([]*usagepb.Usage, 0, len(rows))
	for _, row := range rows {
		usages = append(usages, row.toProto())
	}
	return usages, nil
}

// addOLAPOnlyUsage adds to agg the usage metrics that are only recorded in the
// OLAP DB, aggregated over [start, end) per combination of the execution
// dimensions returned to the Usage page. Labels that aren't returned (client,
// origin, server) are summed over.
func (s *usageService) addOLAPOnlyUsage(ctx context.Context, groupID string, start, end time.Time, agg *usagepb.Usage) error {
	type executionUsageRow struct {
		SKU           sku.SKU
		SelfHosted    bool
		Workflow      bool
		IsolationType string
		OS            string
		Arch          string
		TotalCount    int64
	}
	rows, err := db.ScanAll(s.olapdbh.NewQuery(ctx, "usage_service_scan_olap_only").Raw(`
		SELECT
			sku,
			`+rawUsageLabelEquals(sku.SelfHosted, sku.SelfHostedTrue)+` AS self_hosted,
			`+rawUsageLabelEquals(sku.Client, sku.ClientExecutorWorkflows)+` AS workflow,
			`+rawUsageLabel(sku.IsolationType)+` AS isolation_type,
			`+rawUsageLabel(sku.OS)+` AS os,
			`+rawUsageLabel(sku.Arch)+` AS arch,
			SUM(intDiv(count, if(sku IN ?, 1000, 1))) AS total_count
		FROM Usage
		WHERE period_start >= ? AND period_start < ?
		AND group_id = ?
		AND sku IN ?
		GROUP BY sku, self_hosted, workflow, isolation_type, os, arch
		HAVING total_count > 0
		ORDER BY sku, self_hosted, workflow, isolation_type, os, arch
	`, computeUsageSKUs, start, end, groupID, olapOnlyUsageSKUs), &executionUsageRow{})
	if err != nil {
		return err
	}
	for _, row := range rows {
		u := &usagepb.ExecutionUsage{
			SelfHosted:    row.SelfHosted,
			Workflow:      row.Workflow,
			IsolationType: row.IsolationType,
			Os:            row.OS,
			Arch:          row.Arch,
			Count:         row.TotalCount,
		}
		switch row.SKU {
		case sku.RemoteExecutionExecuteFixedComputeNanos:
			agg.FixedComputeUsec = append(agg.FixedComputeUsec, u)
		case sku.RemoteExecutionExecuteFlexibleComputeNanos:
			agg.FlexibleComputeUsec = append(agg.FlexibleComputeUsec, u)
		case sku.RemoteExecutionExecuteRemoteSnapshotSavedBytes:
			agg.RemoteSnapshotSavedBytes = append(agg.RemoteSnapshotSavedBytes, u)
		case sku.RemoteExecutionExecuteLocalSnapshotSavedBytes:
			agg.LocalSnapshotSavedBytes = append(agg.LocalSnapshotSavedBytes, u)
		default:
			return status.InternalErrorf("unexpected OLAP-only usage SKU %q", row.SKU)
		}
	}
	return nil
}

func validateUsageAlertingRuleConfiguration(config *usagepb.UsageAlertingRuleConfiguration) error {
	if config == nil {
		return status.InvalidArgumentError("configuration is required")
	}
	if !isValidUsageAlertingMetric(config.GetMetric()) {
		return status.InvalidArgumentError("usage alerting metric is required")
	}
	if config.GetAbsoluteThreshold() <= 0 {
		return status.InvalidArgumentError("absolute threshold must be positive")
	}
	if !isValidUsageAlertingWindow(config.GetWindow()) {
		return status.InvalidArgumentError("usage alerting window is required")
	}
	return nil
}

func isValidUsageAlertingMetric(metric usagepb.UsageAlertingMetric_Value) bool {
	_, ok := usagepb.UsageAlertingMetric_Value_name[int32(metric)]
	return metric != usagepb.UsageAlertingMetric_UNKNOWN && ok
}

func isValidUsageAlertingWindow(window usagepb.UsageAlertingWindow_Value) bool {
	_, ok := usagepb.UsageAlertingWindow_Value_name[int32(window)]
	return window != usagepb.UsageAlertingWindow_UNKNOWN && ok
}

func (s *usageService) usageAlertingRuleToProto(ctx context.Context, row *tables.UsageAlertingRule) (*usagepb.UsageAlertingRule, error) {
	return &usagepb.UsageAlertingRule{
		Metadata: &usagepb.UsageAlertingRuleMetadata{
			UsageAlertingRuleId: row.UsageAlertingRuleID,
			CreatedByUser:       s.displayUser(ctx, row.UserID),
			CreatedTimestamp:    timestampFromUsec(row.CreatedAtUsec),
		},
		Configuration: &usagepb.UsageAlertingRuleConfiguration{
			Metric:            row.UsageAlertingMetric,
			AbsoluteThreshold: row.AbsoluteThreshold,
			Window:            row.Window,
		},
		Status: &usagepb.UsageAlertingRuleStatus{
			LastFiredTimestamp: timestampFromUsec(row.LastFiredUsec),
		},
	}, nil
}

func (s *usageService) displayUser(ctx context.Context, userID string) *uidpb.DisplayUser {
	if userID == "" {
		return nil
	}
	if udb := s.env.GetUserDB(); udb != nil {
		// We only need user profile fields, so only fetch direct memberships.
		u, err := udb.GetUserByIDWithoutAuthCheck(ctx, userID, &interfaces.GetUserOpts{DirectMembershipsOnly: true})
		if err == nil {
			return u.ToProto()
		}
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Could not load user %q for usage alerting rule: %s", userID, err)
		}
	}
	return &uidpb.DisplayUser{
		UserId: &uidpb.UserId{Id: userID},
	}
}

func timestampFromUsec(usec int64) *timestamppb.Timestamp {
	if usec == 0 {
		return nil
	}
	return timestamppb.New(time.UnixMicro(usec).UTC())
}

type usagePeriod struct {
	year  int
	month time.Month
}

func parseUsagePeriod(s string) (*usagePeriod, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return nil, status.InvalidArgumentError("invalid usage period")
	}
	y, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, status.InvalidArgumentError("invalid usage period")
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 1 || m > 12 {
		return nil, status.InvalidArgumentError("invalid usage period")
	}
	return &usagePeriod{year: y, month: time.Month(m)}, nil
}

func getUsagePeriod(t time.Time) *usagePeriod {
	t = t.UTC()
	return &usagePeriod{year: t.Year(), month: t.Month()}
}

func (u *usagePeriod) String() string {
	return fmt.Sprintf("%d-%02d", u.year, u.month)
}

func (u *usagePeriod) Start() time.Time {
	return time.Date(u.year, u.month, 1, 0, 0, 0, 0, time.UTC)
}

func addCalendarMonths(t time.Time, months int) time.Time {
	// Note: the month arithmetic works because Go allows passing month values
	// outside their usual range. For example, a month value of 0 corresponds to
	// December of the preceding year.
	return time.Date(
		t.Year(), time.Month(int(t.Month())+months), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		t.Location())
}

func configuredUsageStartDate() time.Time {
	if *usageStartDate == "" {
		log.Warningf("Usage start date is not configured; usage page may show some months with missing usage data.")
		return time.Unix(0, 0).UTC()
	}
	start, err := time.Parse(time.RFC3339, *usageStartDate)
	if err != nil {
		log.Errorf("Failed to parse app.usage_start_date from string %q: %s", *usageStartDate, err)
		return time.Unix(0, 0).UTC()
	}
	if start.After(time.Now()) {
		log.Warningf("Configured usage start date %q is in the future; usage page may show empty usage data.", *usageStartDate)
	}
	return start.UTC()
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
