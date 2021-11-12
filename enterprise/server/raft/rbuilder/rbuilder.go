package rbuilder

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
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

func (bb *BatchBuilder) Add(m proto.Message) *BatchBuilder {
	if bb.cmd == nil {
		bb.cmd = &rfpb.BatchCmdRequest{}
	}

	req := &rfpb.RequestUnion{}
	switch value := m.(type) {
	case *rfpb.FileWriteRequest:
		req.Value = &rfpb.RequestUnion_FileWrite{
			FileWrite: value,
		}
	case *rfpb.DirectReadRequest:
		req.Value = &rfpb.RequestUnion_DirectRead{
			DirectRead: value,
		}
	case *rfpb.DirectWriteRequest:
		req.Value = &rfpb.RequestUnion_DirectWrite{
			DirectWrite: value,
		}
	case *rfpb.IncrementRequest:
		req.Value = &rfpb.RequestUnion_Increment{
			Increment: value,
		}
	case *rfpb.ScanRequest:
		req.Value = &rfpb.RequestUnion_Scan{
			Scan: value,
		}
	default:
		bb.setErr(status.FailedPreconditionErrorf("BatchBuilder.Add handling for %+v not implemented.", m))
		return bb
	}

	bb.cmd.Union = append(bb.cmd.Union, req)
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

	return br
}

func NewBatchResponseFromProto(c *rfpb.BatchCmdResponse) *BatchResponse {
	return &BatchResponse{
		cmd: c,
	}
}

func (br *BatchResponse) checkIndex(n int) {
	if n >= len(br.cmd.GetUnion()) {
		br.setErr(status.FailedPreconditionErrorf("batch did not contain %d elements", n))
	}
}

func (br *BatchResponse) DirectReadResponse(n int) (*rfpb.DirectReadResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	return br.cmd.GetUnion()[n].GetDirectRead(), nil
}

func (br *BatchResponse) IncrementResponse(n int) (*rfpb.IncrementResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	return br.cmd.GetUnion()[n].GetIncrement(), nil
}

func (br *BatchResponse) ScanResponse(n int) (*rfpb.ScanResponse, error) {
	br.checkIndex(n)
	if br.err != nil {
		return nil, br.err
	}
	return br.cmd.GetUnion()[n].GetScan(), nil
}

func ResponseUnion(result dbsm.Result, err error) (*rfpb.ResponseUnion, error) {
	if err != nil {
		return nil, err
	}
	rsp := &rfpb.ResponseUnion{}
	if err := proto.Unmarshal(result.Data, rsp); err != nil {
		return nil, err
	}
	s := gstatus.FromProto(rsp.GetStatus())
	if s.Err() != nil {
		return nil, s.Err()
	}
	return rsp, nil
}
