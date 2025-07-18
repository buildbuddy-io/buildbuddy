package rbuilder

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/prototext"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	gstatus "google.golang.org/grpc/status"
)

type BatchBuilder struct {
	cmd *rfpb.BatchCmdRequest
	err error
}

func NewBatchBuilder() *BatchBuilder {
	return &BatchBuilder{
		cmd: &rfpb.BatchCmdRequest{},
		err: nil,
	}
}

func (bb *BatchBuilder) setErr(err error) {
	if bb.err != nil {
		return
	}
	bb.err = err
}

func (bb *BatchBuilder) Merge(bb2 *BatchBuilder) *BatchBuilder {
	proto.Merge(bb.cmd, bb2.cmd)
	return bb
}

func (bb *BatchBuilder) Add(m proto.Message) *BatchBuilder {
	if bb.cmd == nil {
		bb.cmd = &rfpb.BatchCmdRequest{}
	}

	req := &rfpb.RequestUnion{}
	switch value := m.(type) {
	case *rfpb.DirectReadRequest:
		req.Value = &rfpb.RequestUnion_DirectRead{
			DirectRead: value,
		}
	case *rfpb.DirectWriteRequest:
		req.Value = &rfpb.RequestUnion_DirectWrite{
			DirectWrite: value,
		}
	case *rfpb.DirectDeleteRequest:
		req.Value = &rfpb.RequestUnion_DirectDelete{
			DirectDelete: value,
		}
	case *rfpb.IncrementRequest:
		req.Value = &rfpb.RequestUnion_Increment{
			Increment: value,
		}
	case *rfpb.ScanRequest:
		req.Value = &rfpb.RequestUnion_Scan{
			Scan: value,
		}
	case *rfpb.CASRequest:
		req.Value = &rfpb.RequestUnion_Cas{
			Cas: value,
		}
	case *rfpb.FindSplitPointRequest:
		req.Value = &rfpb.RequestUnion_FindSplitPoint{
			FindSplitPoint: value,
		}
	case *rfpb.GetRequest:
		req.Value = &rfpb.RequestUnion_Get{
			Get: value,
		}
	case *rfpb.SetRequest:
		req.Value = &rfpb.RequestUnion_Set{
			Set: value,
		}
	case *rfpb.DeleteRequest:
		req.Value = &rfpb.RequestUnion_Delete{
			Delete: value,
		}
	case *rfpb.FindRequest:
		req.Value = &rfpb.RequestUnion_Find{
			Find: value,
		}
	case *rfpb.UpdateAtimeRequest:
		req.Value = &rfpb.RequestUnion_UpdateAtime{
			UpdateAtime: value,
		}
	case *rfpb.DeleteSessionsRequest:
		req.Value = &rfpb.RequestUnion_DeleteSessions{
			DeleteSessions: value,
		}
	case *rfpb.FetchRangesRequest:
		req.Value = &rfpb.RequestUnion_FetchRanges{
			FetchRanges: value,
		}
	default:
		bb.setErr(status.FailedPreconditionErrorf("BatchBuilder.Add handling for %+v not implemented.", m))
		return bb
	}

	bb.cmd.Union = append(bb.cmd.Union, req)
	return bb
}

func (bb *BatchBuilder) WithRequests(union []*rfpb.RequestUnion) *BatchBuilder {
	bb.cmd.Union = union
	return bb
}

func (bb *BatchBuilder) Requests() []*rfpb.RequestUnion {
	return bb.cmd.Union
}

func (bb *BatchBuilder) SetTransactionID(txid []byte) *BatchBuilder {
	bb.cmd.TransactionId = txid
	return bb
}

func (bb *BatchBuilder) SetFinalizeOperation(op rfpb.FinalizeOperation) *BatchBuilder {
	bb.cmd.FinalizeOperation = &op
	return bb
}

func (bb *BatchBuilder) SetHeader(h *rfpb.Header) *BatchBuilder {
	bb.cmd.Header = h
	return bb
}

func (bb *BatchBuilder) SetSession(session *rfpb.Session) *BatchBuilder {
	bb.cmd.Session = session
	return bb
}

func (bb *BatchBuilder) SetLockMappedRange(lock bool) *BatchBuilder {
	bb.cmd.LockMappedRange = lock
	return bb
}

func (bb *BatchBuilder) AddPostCommitHook(m proto.Message) *BatchBuilder {
	switch value := m.(type) {
	case *rfpb.SnapshotClusterHook:
		bb.cmd.PostCommitHooks = append(bb.cmd.PostCommitHooks, &rfpb.PostCommitHook{
			SnapshotCluster: value,
		})
	case *rfpb.StartShardHook:
		bb.cmd.PostCommitHooks = append(bb.cmd.PostCommitHooks, &rfpb.PostCommitHook{
			StartShard: value,
		})
	}
	return bb
}

func (bb *BatchBuilder) ToProto() (*rfpb.BatchCmdRequest, error) {
	if bb.err != nil {
		return nil, bb.err
	}
	return bb.cmd, nil
}

func (bb *BatchBuilder) ToBuf() ([]byte, error) {
	if bb.err != nil {
		return nil, bb.err
	}
	return proto.Marshal(bb.cmd)
}

func (bb *BatchBuilder) Size() int {
	if bb.cmd == nil {
		return 0
	}
	return len(bb.cmd.Union)
}

func (bb *BatchBuilder) String() string {
	builder := fmt.Sprintf("Builder(err: %s)", bb.err)
	for i, v := range bb.cmd.Union {
		out, _ := (&prototext.MarshalOptions{Multiline: false}).Marshal(v)
		builder += fmt.Sprintf(" [%d]: %+v", i, string(out))
	}
	return builder
}

type BatchResponse struct {
	cmd *rfpb.BatchCmdResponse
	err error
}

func (br *BatchResponse) setErr(err error) {
	if br.err != nil {
		return
	}
	br.err = err
}

func NewBatchResponse(val interface{}) *BatchResponse {
	br := &BatchResponse{
		cmd: &rfpb.BatchCmdResponse{},
	}

	buf, ok := val.([]byte)
	if !ok {
		br.setErr(status.FailedPreconditionError("Could not coerce value to []byte."))
	}
	if err := proto.Unmarshal(buf, br.cmd); err != nil {
		br.setErr(err)
	}
	if err := gstatus.FromProto(br.cmd.GetStatus()).Err(); err != nil {
		br.setErr(err)
	}
	return br
}

func NewBatchResponseFromProto(c *rfpb.BatchCmdResponse) *BatchResponse {
	br := &BatchResponse{
		cmd: c,
	}
	if err := gstatus.FromProto(br.cmd.GetStatus()).Err(); err != nil {
		br.setErr(err)
	}
	return br
}

func (br *BatchResponse) checkIndex(n int) {
	if n >= len(br.cmd.GetUnion()) {
		br.setErr(status.FailedPreconditionErrorf("batch %+v did not contain element %d", br.cmd, n))
	}
}

func (br *BatchResponse) unionError(u *rfpb.ResponseUnion) error {
	s := gstatus.FromProto(u.GetStatus())
	return s.Err()
}

func (br *BatchResponse) AnyError() error {
	if br.err != nil {
		return br.err
	}
	for _, u := range br.cmd.GetUnion() {
		if err := br.unionError(u); err != nil {
			return err
		}
	}
	return nil
}

func (br *BatchResponse) DirectReadResponse(n int) (*rfpb.DirectReadResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetDirectRead(), br.unionError(u)
}

func (br *BatchResponse) IncrementResponse(n int) (*rfpb.IncrementResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetIncrement(), br.unionError(u)
}

func (br *BatchResponse) ScanResponse(n int) (*rfpb.ScanResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetScan(), br.unionError(u)
}

func (br *BatchResponse) CASResponse(n int) (*rfpb.CASResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetCas(), br.unionError(u)
}

func (br *BatchResponse) FindSplitPointResponse(n int) (*rfpb.FindSplitPointResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetFindSplitPoint(), br.unionError(u)
}

func (br *BatchResponse) GetResponse(n int) (*rfpb.GetResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetGet(), br.unionError(u)
}
func (br *BatchResponse) SetResponse(n int) (*rfpb.SetResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetSet(), br.unionError(u)
}
func (br *BatchResponse) DeleteResponse(n int) (*rfpb.DeleteResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetDelete(), br.unionError(u)
}
func (br *BatchResponse) FindResponse(n int) (*rfpb.FindResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetFind(), br.unionError(u)
}
func (br *BatchResponse) UpdateAtimeResponse(n int) (*rfpb.UpdateAtimeResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetUpdateAtime(), br.unionError(u)
}
func (br *BatchResponse) DeleteSessionsResponse(n int) (*rfpb.DeleteSessionsResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetDeleteSessions(), br.unionError(u)
}

func (br *BatchResponse) FetchRangesResponse(n int) (*rfpb.FetchRangesResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	u := br.cmd.GetUnion()[n]
	return u.GetFetchRanges(), br.unionError(u)
}

type TxnBuilder struct {
	statements []*TxnStatementBuilder
}

func NewTxn() *TxnBuilder {
	return &TxnBuilder{
		statements: make([]*TxnStatementBuilder, 0),
	}
}

type TxnStatementBuilder struct {
	rangeDescriptor         *rfpb.RangeDescriptor
	rawBatch                *BatchBuilder
	hooks                   []*rfpb.TransactionHook
	rangeValidationRequired bool
}

func (sb *TxnStatementBuilder) SetRangeDescriptor(rd *rfpb.RangeDescriptor) *TxnStatementBuilder {
	sb.rangeDescriptor = rd
	return sb
}

func (sb *TxnStatementBuilder) SetBatch(batch *BatchBuilder) *TxnStatementBuilder {
	sb.rawBatch = batch
	return sb
}

func (sb *TxnStatementBuilder) SetRangeValidationRequired(required bool) *TxnStatementBuilder {
	sb.rangeValidationRequired = required
	return sb
}

func (sb *TxnStatementBuilder) AddPostCommitHook(phase rfpb.TransactionHook_Phase, m proto.Message) *TxnStatementBuilder {
	switch value := m.(type) {
	case *rfpb.SnapshotClusterHook:
		sb.hooks = append(sb.hooks, &rfpb.TransactionHook{
			Phase: phase,
			Hook: &rfpb.PostCommitHook{
				SnapshotCluster: value,
			}})
	case *rfpb.StartShardHook:
		sb.hooks = append(sb.hooks, &rfpb.TransactionHook{
			Phase: phase,
			Hook: &rfpb.PostCommitHook{
				StartShard: value,
			}})
	}
	return sb
}

func (tb *TxnBuilder) AddStatement() *TxnStatementBuilder {
	sb := &TxnStatementBuilder{
		hooks: make([]*rfpb.TransactionHook, 0),
	}
	tb.statements = append(tb.statements, sb)
	return sb
}

func (tb *TxnBuilder) ToProto() (*rfpb.TxnRequest, error) {
	guid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	txid := []byte(guid.String())

	req := &rfpb.TxnRequest{
		TransactionId: txid,
	}
	for _, statement := range tb.statements {
		batchProto, err := statement.rawBatch.ToProto()
		if err != nil {
			return nil, err
		}
		req.Statements = append(req.Statements, &rfpb.TxnRequest_Statement{
			Range:                   statement.rangeDescriptor,
			RawBatch:                batchProto,
			Hooks:                   statement.hooks,
			RangeValidationRequired: statement.rangeValidationRequired,
		})
	}
	return req, nil
}
