// mdcli is an operator/debug CLI for browsing and querying the metadata
// server (the raft store). It talks to the raft Api service, which routes
// scans and reads through the server-side Sender — so the CLI itself carries
// no range/routing state.
//
// It is a privileged, cross-tenant tool: the raft Api enforces no per-key or
// per-group ACL, so mdcli can read any key (including txn/session/system state
// and every group's cache metadata). Point it at a dev cluster or your own
// data; be deliberate before using it against prod.
//
// Examples:
//
//	mdcli --target=grpc://localhost:4772 ranges
//	mdcli range 3
//	mdcli leases
//	mdcli which hex:0350543120...
//	mdcli scan --meta
//	mdcli scan --partition PT1 --limit 50
//	mdcli partitions
//	mdcli get hex:0450543120...
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"google.golang.org/protobuf/encoding/prototext"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	target  = flag.String("target", "grpc://localhost:4772", "Raft Api target to connect to.")
	timeout = flag.Duration("timeout", 30*time.Second, "Per-command RPC timeout.")
	limit   = flag.Int64("limit", 100, "Max KVs to fetch per scan page.")
	maxKVs  = flag.Int64("max", 1000, "Max KVs to print for a scan (0 = unlimited).")
	hexKeys = flag.Bool("hex", false, "Always print keys as hex.")

	// Named span shortcuts for scan.
	spanMeta   = flag.Bool("meta", false, "scan: the meta-range span (\\x02, range descriptors).")
	spanSystem = flag.Bool("system", false, "scan: the system span (\\x03, txn/session/partition).")
	partition  = flag.String("partition", "", "scan: cache-data keys for this partition id (e.g. PT1).")
	rangeID    = flag.Uint64("range", 0, "scan: keys within this range id.")
	scanStart  = flag.String("start", "", "scan: explicit start key (key spec); scans to --end or the end of the keyspace.")
	scanEnd    = flag.String("end", "", "scan: explicit end key (key spec), exclusive.")
)

func main() {
	flag.Parse()
	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config: %s", err)
	}
	args := flag.CommandLine.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}
	// Flags may appear before or after the subcommand. flag.Parse() stops at
	// the subcommand (the first positional), so re-parse the args that follow
	// it; otherwise `scan --meta --limit 50` and similar are ignored.
	cmd := args[0]
	// Parse flags that appear after the subcommand, allowing them to be
	// interspersed with positionals (stdlib flag stops at the first positional,
	// so loop, pulling positionals out one at a time). This makes both
	// `scan --max 1 txn` and `scan txn --max 1` work.
	var rest []string
	remaining := args[1:]
	for len(remaining) > 0 {
		if err := flag.CommandLine.Parse(remaining); err != nil {
			os.Exit(2)
		}
		remaining = flag.CommandLine.Args()
		if len(remaining) == 0 {
			break
		}
		rest = append(rest, remaining[0])
		remaining = remaining[1:]
	}

	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("mdcli"))
	if err := clientidentity.Register(env); err != nil {
		log.Fatalf("Error registering client identity: %s", err)
	}
	conn, err := grpc_client.DialInternal(env, *target)
	if err != nil {
		log.Fatalf("Error dialing %q: %s", *target, err)
	}
	client := rfspb.NewApiClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// NHID->pod names are only shown by the topology commands.
	switch cmd {
	case "ranges", "meta", "range", "lease", "leases", "which":
		loadRegistry(ctx, client)
	}

	if err := run(ctx, client, cmd, rest); err != nil {
		log.Fatalf("%s: %s", cmd, err)
	}
}

func run(ctx context.Context, client rfspb.ApiClient, cmd string, args []string) error {
	switch cmd {
	case "get":
		if len(args) != 1 {
			return fmt.Errorf("usage: get <key>")
		}
		return runGet(ctx, client, args[0])
	case "scan":
		return runScan(ctx, client, args)
	case "ranges", "meta":
		return runRanges(ctx, client)
	case "range":
		if len(args) != 1 {
			return fmt.Errorf("usage: range <id>")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid range id %q: %s", args[0], err)
		}
		return runRange(ctx, client, id)
	case "lease":
		if len(args) != 1 {
			return fmt.Errorf("usage: lease <id>")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid range id %q: %s", args[0], err)
		}
		return runLease(ctx, client, id)
	case "leases":
		return runLeases(ctx, client)
	case "which":
		if len(args) != 1 {
			return fmt.Errorf("usage: which <key>")
		}
		return runWhich(ctx, client, args[0])
	case "partitions":
		return runPartitions(ctx, client)
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func runGet(ctx context.Context, client rfspb.ApiClient, keySpec string) error {
	key, err := parseKey(keySpec)
	if err != nil {
		return err
	}
	rsp, err := client.DebugGet(ctx, &rfpb.DebugGetRequest{Key: key})
	if err != nil {
		if status.IsNotFoundError(err) {
			fmt.Printf("%s = (not found)\n", renderKey(key))
			return nil
		}
		return err
	}
	fmt.Printf("%s = %s\n", renderKey(key), renderValue(key, rsp.GetKv().GetValue()))
	return nil
}

// errStop is a sentinel returned from a scanAll callback to stop early.
var errStop = errors.New("stop")

// scanAll pages through DebugScan over [start, end), invoking fn for each KV
// and following the next_start cursor until the span is exhausted. Return
// errStop from fn to stop early.
func scanAll(ctx context.Context, client rfspb.ApiClient, start, end []byte, fn func(*rfpb.KV) error) error {
	cur := start
	for {
		rsp, err := client.DebugScan(ctx, &rfpb.DebugScanRequest{Start: cur, End: end, Limit: *limit})
		if err != nil {
			return err
		}
		for _, kv := range rsp.GetKvs() {
			if err := fn(kv); err != nil {
				return err
			}
		}
		next := rsp.GetNextStart()
		if len(next) == 0 || (len(end) > 0 && bytes.Compare(next, end) >= 0) {
			return nil
		}
		cur = next
	}
}

func runScan(ctx context.Context, client rfspb.ApiClient, args []string) error {
	start, end, err := scanSpan(ctx, client, args)
	if err != nil {
		return err
	}
	var total int64
	err = scanAll(ctx, client, start, end, func(kv *rfpb.KV) error {
		fmt.Printf("%s = %s\n", renderKey(kv.GetKey()), renderValue(kv.GetKey(), kv.GetValue()))
		total++
		if *maxKVs > 0 && total >= *maxKVs {
			return errStop
		}
		return nil
	})
	if errors.Is(err, errStop) {
		fmt.Printf("... stopped at --max=%d\n", *maxKVs)
		return nil
	}
	if err != nil {
		return err
	}
	fmt.Printf("(%d keys)\n", total)
	return nil
}

// scanSpan resolves the [start, end) span for a scan from the named-span flags
// or a positional key prefix.
func scanSpan(ctx context.Context, client rfspb.ApiClient, args []string) (start, end []byte, err error) {
	switch {
	case *scanStart != "" || *scanEnd != "":
		if *scanStart != "" {
			if start, err = parseKey(*scanStart); err != nil {
				return nil, nil, err
			}
		}
		if *scanEnd != "" {
			if end, err = parseKey(*scanEnd); err != nil {
				return nil, nil, err
			}
		}
		return start, end, nil
	case *spanMeta:
		return constants.MetaRangePrefix, constants.SystemPrefix, nil
	case *spanSystem:
		return constants.SystemPrefix, []byte{constants.UnsplittableMaxByte}, nil
	case *partition != "":
		partID := strings.TrimPrefix(*partition, filestore.PartitionDirectoryPrefix)
		prefix := []byte(filestore.PartitionDirectoryPrefix + partID + "/")
		return prefix, prefixEnd(prefix), nil
	case *rangeID != 0:
		rd, err := lookupRange(ctx, client, *rangeID)
		if err != nil {
			return nil, nil, err
		}
		return rd.GetStart(), rd.GetEnd(), nil
	case len(args) == 1:
		prefix, err := parseKey(args[0])
		if err != nil {
			return nil, nil, err
		}
		return prefix, prefixEnd(prefix), nil
	default:
		return nil, nil, fmt.Errorf("scan needs a span: a key prefix, or one of --start/--end/--meta/--system/--partition/--range")
	}
}

func runRanges(ctx context.Context, client rfspb.ApiClient) error {
	rds, err := fetchRanges(ctx, client)
	if err != nil {
		return err
	}
	fmt.Printf("%-8s %-6s %-24s %-24s %s\n", "RANGE", "GEN", "START", "END", "REPLICAS")
	for _, rd := range rds {
		fmt.Printf("%-8d %-6d %-24s %-24s %s\n",
			rd.GetRangeId(), rd.GetGeneration(),
			renderKey(rd.GetStart()), renderKey(rd.GetEnd()), replicaNHIDs(rd))
	}
	fmt.Printf("(%d ranges)\n", len(rds))
	return nil
}

func runRange(ctx context.Context, client rfspb.ApiClient, id uint64) error {
	rsp, err := client.GetRangeDebugInfo(ctx, &rfpb.GetRangeDebugInfoRequest{RangeId: id})
	if err != nil {
		return err
	}
	rd := rsp.GetRangeDescriptor()
	fmt.Printf("range:      %d (generation %d)\n", id, rd.GetGeneration())
	fmt.Printf("span:       %s .. %s\n", renderKey(rd.GetStart()), renderKey(rd.GetEnd()))
	leaderID := rsp.GetLeader().GetLeaderId()
	fmt.Printf("responder NHID: %s (has_lease=%t)\n", renderNHID(rsp.GetNhid()), rsp.GetHasLease())
	fmt.Printf("leader:     replica %d, NHID %s (term %d, valid=%t)\n",
		leaderID, orUnknown(renderNHID(replicaNHID(rsp, leaderID))),
		rsp.GetLeader().GetTerm(), rsp.GetLeader().GetValid())
	fmt.Printf("replicas (descriptor): %s\n", replicaNHIDs(rd))
	if m := rsp.GetMembership(); m != nil {
		fmt.Printf("membership:\n")
		fmt.Printf("  voters:     %s\n", fmtReplicas(m.GetVoters()))
		fmt.Printf("  non-voters: %s\n", fmtReplicas(m.GetNonVoters()))
		fmt.Printf("  witnesses:  %s\n", fmtReplicas(m.GetWitnesses()))
		fmt.Printf("  removed:    %s\n", fmtReplicaIDs(m.GetRemoved()))
	}
	return nil
}

func runLease(ctx context.Context, client rfspb.ApiClient, id uint64) error {
	rsp, err := client.GetRangeDebugInfo(ctx, &rfpb.GetRangeDebugInfoRequest{RangeId: id})
	if err != nil {
		return err
	}
	rd := rsp.GetRangeDescriptor()
	leaderID := rsp.GetLeader().GetLeaderId()
	fmt.Printf("range %d [%s .. %s]\n", id, renderKey(rd.GetStart()), renderKey(rd.GetEnd()))
	fmt.Printf("  leader:         replica %d, NHID %s (term %d, valid=%t)\n",
		leaderID, orUnknown(renderNHID(replicaNHID(rsp, leaderID))),
		rsp.GetLeader().GetTerm(), rsp.GetLeader().GetValid())
	fmt.Printf("  responder NHID: %s (has_lease=%t)\n", renderNHID(rsp.GetNhid()), rsp.GetHasLease())
	return nil
}

func runLeases(ctx context.Context, client rfspb.ApiClient) error {
	rds, err := fetchRanges(ctx, client)
	if err != nil {
		return err
	}
	fmt.Printf("%-8s %-8s %-20s %s\n", "RANGE", "LEADER", "LEADER-POD", "SPAN")
	for _, rd := range rds {
		rsp, err := client.GetRangeDebugInfo(ctx, &rfpb.GetRangeDebugInfoRequest{RangeId: rd.GetRangeId()})
		if err != nil {
			fmt.Printf("%-8d %-8s %-20s %s\n", rd.GetRangeId(), "ERR", "", err.Error())
			continue
		}
		leaderID := rsp.GetLeader().GetLeaderId()
		pod := orUnknown(nodePods[replicaNHID(rsp, leaderID)])
		fmt.Printf("%-8d r%-7d %-20s %s .. %s\n",
			rd.GetRangeId(), leaderID, pod,
			renderKey(rd.GetStart()), renderKey(rd.GetEnd()))
	}
	return nil
}

func runWhich(ctx context.Context, client rfspb.ApiClient, keySpec string) error {
	key, err := parseKey(keySpec)
	if err != nil {
		return err
	}
	rds, err := fetchRanges(ctx, client)
	if err != nil {
		return err
	}
	for _, rd := range rds {
		if inRange(key, rd) {
			fmt.Printf("%s is in range %d [%s .. %s], replicas %s\n",
				renderKey(key), rd.GetRangeId(),
				renderKey(rd.GetStart()), renderKey(rd.GetEnd()), replicaNHIDs(rd))
			return nil
		}
	}
	return fmt.Errorf("no range found containing %s", renderKey(key))
}

func runPartitions(ctx context.Context, client rfspb.ApiClient) error {
	start := []byte(constants.PartitionPrefix)
	end := prefixEnd(start)
	return scanAll(ctx, client, start, end, func(kv *rfpb.KV) error {
		pd := &rfpb.PartitionDescriptor{}
		if err := proto.Unmarshal(kv.GetValue(), pd); err != nil {
			fmt.Printf("%s = (unparsable) %s\n", renderKey(kv.GetKey()), hexPreview(kv.GetValue()))
			return nil
		}
		fmt.Printf("partition %-12s first_range=%d state=%s generation=%d\n",
			pd.GetId(), pd.GetFirstRangeId(), pd.GetState(), pd.GetGeneration())
		return nil
	})
}

// fetchRanges reads every range descriptor by scanning the meta range span.
func fetchRanges(ctx context.Context, client rfspb.ApiClient) ([]*rfpb.RangeDescriptor, error) {
	var rds []*rfpb.RangeDescriptor
	var parseErr error
	err := scanAll(ctx, client, []byte(constants.MetaRangePrefix), []byte(constants.SystemPrefix), func(kv *rfpb.KV) error {
		rd := &rfpb.RangeDescriptor{}
		if err := proto.Unmarshal(kv.GetValue(), rd); err != nil {
			parseErr = fmt.Errorf("unparsable range descriptor at %s: %w", renderKey(kv.GetKey()), err)
			return errStop
		}
		rds = append(rds, rd)
		return nil
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if err != nil {
		return nil, err
	}
	return rds, nil
}

func lookupRange(ctx context.Context, client rfspb.ApiClient, id uint64) (*rfpb.RangeDescriptor, error) {
	rsp, err := client.GetRangeDebugInfo(ctx, &rfpb.GetRangeDebugInfoRequest{RangeId: id})
	if err != nil {
		return nil, err
	}
	if rsp.GetRangeDescriptor() == nil {
		return nil, fmt.Errorf("range %d not found", id)
	}
	return rsp.GetRangeDescriptor(), nil
}

// parseKey turns a key spec into raw bytes. Supported forms: named prefixes
// (local/meta/system/txn/session/partition), "hex:<hex>", and otherwise the
// literal bytes of the string.
func parseKey(spec string) ([]byte, error) {
	switch spec {
	case "meta":
		return constants.MetaRangePrefix, nil
	case "system":
		return constants.SystemPrefix, nil
	case "txn":
		return constants.TxnRecordPrefix, nil
	case "session":
		return constants.SessionPrefix, nil
	case "partition":
		return constants.PartitionPrefix, nil
	}
	if h, ok := strings.CutPrefix(spec, "hex:"); ok {
		b, err := hex.DecodeString(h)
		if err != nil {
			return nil, fmt.Errorf("invalid hex key %q: %s", h, err)
		}
		return b, nil
	}
	return []byte(spec), nil
}

// renderKey renders a key for display: a prefix label plus the remainder as a
// printable string when safe, otherwise hex.
func renderKey(k []byte) string {
	if len(k) == 0 {
		return `""`
	}
	if *hexKeys {
		return "0x" + hex.EncodeToString(k)
	}
	label, rest := "", k
	switch k[0] {
	case 0x01:
		label, rest = "local/", k[1:]
	case 0x02:
		label, rest = "meta/", k[1:]
	case 0x03:
		label, rest = "system/", k[1:]
	}
	if isPrintable(rest) {
		return label + string(rest)
	}
	return label + "0x" + hex.EncodeToString(rest)
}

// renderValue decodes a value based on the key's keyspace: RangeDescriptor in
// the meta range, FileMetadata in the splittable data ranges; otherwise a hex
// preview. Falls back to hex when the expected decode fails, so it won't render
// an unrelated proto as a bogus FileMetadata.
func renderValue(key, v []byte) string {
	if len(v) == 0 {
		return "(empty)"
	}
	if len(key) > 0 {
		switch {
		case key[0] == constants.MetaRangePrefix[0]:
			rd := &rfpb.RangeDescriptor{}
			if err := proto.Unmarshal(v, rd); err == nil {
				return "RangeDescriptor{ " + prototext.MarshalOptions{}.Format(rd) + "}"
			}
		case key[0] >= constants.UnsplittableMaxByte:
			fm := &sgpb.FileMetadata{}
			if err := proto.Unmarshal(v, fm); err == nil && fm.GetFileRecord() != nil {
				return "FileMetadata{ " + prototext.MarshalOptions{}.Format(fm) + "}"
			}
		}
	}
	return hexPreview(v)
}

func hexPreview(v []byte) string {
	const max = 32
	if len(v) <= max {
		return fmt.Sprintf("0x%s (%d bytes)", hex.EncodeToString(v), len(v))
	}
	return fmt.Sprintf("0x%s… (%d bytes)", hex.EncodeToString(v[:max]), len(v))
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func replicaNHIDs(rd *rfpb.RangeDescriptor) string {
	return fmtReplicas(rd.GetReplicas())
}

// fmtReplicas renders replica descriptors as "r<id>@<nhid>, ..." or "(none)".
func fmtReplicas(rs []*rfpb.ReplicaDescriptor) string {
	if len(rs) == 0 {
		return "(none)"
	}
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, fmt.Sprintf("r%d@%s", r.GetReplicaId(), renderNHID(r.GetNhid())))
	}
	return strings.Join(out, ", ")
}

// fmtReplicaIDs renders replica ids as "r<id>, ..." or "(none)".
func fmtReplicaIDs(ids []uint64) string {
	if len(ids) == 0 {
		return "(none)"
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, fmt.Sprintf("r%d", id))
	}
	return strings.Join(out, ", ")
}

// replicaNHID resolves a replica id to its NHID using the response's range
// descriptor, falling back to live membership. Returns "" if not found.
func replicaNHID(rsp *rfpb.GetRangeDebugInfoResponse, replicaID uint64) string {
	if replicaID == 0 {
		return ""
	}
	for _, r := range rsp.GetRangeDescriptor().GetReplicas() {
		if r.GetReplicaId() == replicaID {
			return r.GetNhid()
		}
	}
	if m := rsp.GetMembership(); m != nil {
		for _, r := range append(m.GetVoters(), m.GetNonVoters()...) {
			if r.GetReplicaId() == replicaID {
				return r.GetNhid()
			}
		}
	}
	return ""
}

func orUnknown(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

// nodePods maps NHID -> pod name (e.g. "metadata-server-2"), populated once
// from the registry. Best-effort: empty if the registry is unavailable.
var nodePods = map[string]string{}

// loadRegistry populates nodePods from GetRegistry, deriving each node's pod
// name from its advertised address.
func loadRegistry(ctx context.Context, client rfspb.ApiClient) {
	rsp, err := client.GetRegistry(ctx, &rfpb.GetRegistryRequest{})
	if err != nil {
		return
	}
	for _, c := range rsp.GetConnections() {
		if p := podName(c.GetGrpcAddress()); p != "" {
			nodePods[c.GetNhid()] = p
		} else if p := podName(c.GetRaftAddress()); p != "" {
			nodePods[c.GetNhid()] = p
		}
	}
}

// podName extracts a pod name from an advertised address: the first DNS label
// of the host (metadata-server-2.headless...:4772 -> metadata-server-2).
// Returns "" for IP addresses (e.g. local dev), which have no pod name.
func podName(addr string) string {
	host := addr
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	if host == "" || net.ParseIP(host) != nil {
		return ""
	}
	label, _, _ := strings.Cut(host, ".")
	return label
}

// renderNHID annotates an NHID with its pod name when known.
func renderNHID(nhid string) string {
	if nhid == "" {
		return ""
	}
	if p := nodePods[nhid]; p != "" {
		return fmt.Sprintf("%s (%s)", nhid, p)
	}
	return nhid
}

func inRange(key []byte, rd *rfpb.RangeDescriptor) bool {
	if bytes.Compare(key, rd.GetStart()) < 0 {
		return false
	}
	return len(rd.GetEnd()) == 0 || bytes.Compare(key, rd.GetEnd()) < 0
}

// prefixEnd returns the smallest key greater than every key with the given
// prefix, i.e. the exclusive upper bound for a prefix scan.
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	// All 0xff: no upper bound.
	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `mdcli — browse/query the metadata server (raft store).

Usage:
  mdcli [flags] <command> [args]

Commands:
  get <key>        Read one key (raw KV). Value decoded by keyspace when possible.
  scan <span>      Scan a key span. Span: a key prefix, --start/--end <keyspec>,
                   or --meta/--system/--partition PT1/--range <id>.
  ranges | meta    List all range descriptors (decodes the meta range).
  range <id>       Show one range: descriptor, lease holder, leader, membership.
  lease <id>       Show the lease/leader for one range (terse).
  leases           List every range with its current leader.
  which <key>      Show which range owns a key.
  partitions       List partition descriptors.

Key specs: meta | system | txn | session | partition | hex:<hex> | <literal>

Flags:
`)
	flag.CommandLine.PrintDefaults()
}
