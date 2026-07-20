package proto_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	dspb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	numSamples = 30
)

var (
	benchmarkMarshalResult []byte

	testProtoTypes = map[string]testProtoType{
		"FileMetadata": testProtoType{
			providerFn: func() protoMessage {
				return &sgpb.FileMetadata{}
			},
		},
		"ScoreCard": testProtoType{
			providerFn: func() protoMessage {
				return &capb.ScoreCard{}
			},
		},
		"ScoreCardResult": testProtoType{
			providerFn: func() protoMessage {
				return &capb.ScoreCard_Result{}
			},
		},
		"TreeCache": testProtoType{
			providerFn: func() protoMessage {
				return &capb.TreeCache{}
			},
		},
		"ReadResponse": testProtoType{
			providerFn: func() protoMessage {
				return &dspb.ReadResponse{}
			},
		},
	}
)

type protoMessage interface {
	SizeVT() int
	proto.Message
}

type protoMessageWithPoolEnabled interface {
	protoMessage
	ReturnToVTPool()
}

type providerFunc func() protoMessage
type providerWithPoolFunc func() protoMessageWithPoolEnabled

type testProtoType struct {
	providerFn         providerFunc
	providerFnWithPool providerWithPoolFunc
}

func generateProtos(t testing.TB, providerFn providerFunc) []protoMessage {
	// Reset Faker for each proto type so benchmark fixtures do not depend on
	// randomized map iteration order.
	faker.SetRandomSource(rand.NewSource(0))
	res := make([]protoMessage, 0, numSamples)
	for i := 0; i < numSamples; i++ {
		pb := providerFn()
		err := faker.FakeData(pb)
		require.NoError(t, err, "unable to fake data")
		res = append(res, pb)
	}
	return res
}

func generateBytes(t testing.TB, protos []protoMessage) [][]byte {
	res := make([][]byte, 0, len(protos))
	for _, pb := range protos {
		buf, err := proto.MarshalOld(pb)
		require.NoError(t, err, "unable to marshal")
		res = append(res, buf)
	}
	return res
}

type marshalFunc func(v protoMessage) ([]byte, error)
type unmarshalFunc func([]byte, protoMessage) error
type cloneFunc func(v proto.Message) proto.Message

func TestMarshal(t *testing.T) {
	md := &sgpb.FileMetadata{}
	err := faker.FakeData(md)
	require.NoError(t, err, "unable to fake data")

	actual, err := proto.Marshal(md)
	require.NoError(t, err)
	expected, err := proto.MarshalOld(md)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestMarshalWithExternalVTMessage(t *testing.T) {
	detail := &anypb.Any{}
	// Field 1 is type_url; field 100 is an unknown varint field.
	require.NoError(t, gproto.Unmarshal([]byte{0x0a, 0x01, 'x', 0xa0, 0x06, 0x01}, detail))

	status := &statuspb.Status{
		Code:    13,
		Message: "internal error",
		Details: []*anypb.Any{detail},
	}
	scoreCard := &capb.ScoreCard{
		Results: []*capb.ScoreCard_Result{{
			ActionMnemonic: "CppCompile",
			Status:         status,
		}},
	}
	_, ok := any(scoreCard.Results[0].Status).(interface {
		MarshalToSizedBufferVT([]byte) (int, error)
	})
	require.True(t, ok)

	expected, err := proto.MarshalOld(scoreCard)
	require.NoError(t, err)
	actual, err := proto.Marshal(scoreCard)
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	cloned := status.CloneVT()
	require.True(t, gproto.Equal(status, cloned))
	statusBytes, err := gproto.Marshal(status)
	require.NoError(t, err)
	decoded := &statuspb.Status{}
	require.NoError(t, decoded.UnmarshalVT(statusBytes))
	require.True(t, gproto.Equal(status, decoded))
}

func TestUnmarshal(t *testing.T) {
	md := &sgpb.FileMetadata{}
	err := faker.FakeData(md)
	require.NoError(t, err, "unable to fake data")

	data, err := proto.MarshalOld(md)
	require.NoError(t, err)

	actual := &sgpb.FileMetadata{}
	err = proto.Unmarshal(data, actual)
	require.NoError(t, err)

	require.True(t, proto.Equal(md, actual))
}

func TestClone(t *testing.T) {
	md := &sgpb.FileMetadata{}
	err := faker.FakeData(md)
	require.NoError(t, err, "unable to fake data")

	actual := proto.Clone(md)
	require.NoError(t, err)
	require.True(t, proto.Equal(md, actual))
}

func benchmarkMarshal(b *testing.B, marshalFn marshalFunc, data []protoMessage) {
	b.ReportAllocs()
	var totalSize int
	for _, pb := range data {
		totalSize += pb.SizeVT()
	}
	b.SetBytes(int64(totalSize / len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pb := data[i%len(data)]
		buf, err := marshalFn(pb)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMarshalResult = buf
	}

}

func benchmarkUnmarshal(b *testing.B, unmarshalFn unmarshalFunc, providerFn providerFunc, data [][]byte) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := data[rand.Intn(len(data))]
		b.SetBytes(int64(len(buf)))
		v := providerFn()
		err := unmarshalFn(buf, v)
		if err != nil {
			b.Fatal(err)
		}
	}

}

func BenchmarkMarshal(b *testing.B) {
	marshalFns := map[string]marshalFunc{
		"Old": func(v protoMessage) ([]byte, error) {
			return proto.MarshalOld(v)
		},
		"New": func(v protoMessage) ([]byte, error) {
			return proto.Marshal(v)
		},
	}

	for pbName, pbType := range testProtoTypes {
		protos := generateProtos(b, pbType.providerFn)
		for name, fn := range marshalFns {
			b.Run(fmt.Sprintf("name=%s/pbName=%s", name, pbName), func(b *testing.B) {
				benchmarkMarshal(b, fn, protos)
			})
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	unmarshalFns := map[string]unmarshalFunc{
		"New": func(buf []byte, v protoMessage) error {
			return proto.Unmarshal(buf, v)
		},
		"Old": func(buf []byte, v protoMessage) error {
			return gproto.Unmarshal(buf, v)
		},
	}

	for pbName, pbType := range testProtoTypes {
		protos := generateProtos(b, pbType.providerFn)
		data := generateBytes(b, protos)
		for name, fn := range unmarshalFns {
			b.Run(fmt.Sprintf("name=%s/pbName=%s", name, pbName), func(b *testing.B) {
				benchmarkUnmarshal(b, fn, pbType.providerFn, data)
			})

		}

		if pbType.providerFnWithPool != nil {
			b.Run(fmt.Sprintf("name=WithPool/pbName=%s", pbName), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					buf := data[rand.Intn(len(data))]
					b.SetBytes(int64(len(buf)))
					v := pbType.providerFnWithPool()
					err := proto.Unmarshal(buf, v)
					if err != nil {
						b.Fatal(err)
					}
					v.ReturnToVTPool()
				}
			})
		}
	}
}

func benchmarkClone(b *testing.B, cloneFn cloneFunc, data []protoMessage) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pb := data[rand.Intn(len(data))]
		b.SetBytes(int64(pb.SizeVT()))
		_ = cloneFn(pb)
	}

}

func BenchmarkClone(b *testing.B) {
	cloneFns := map[string]cloneFunc{
		"New": func(v proto.Message) proto.Message {
			return proto.Clone(v)
		},
		"Old": func(v proto.Message) proto.Message {
			return gproto.Clone(v)
		},
	}

	for pbName, pbType := range testProtoTypes {
		protos := generateProtos(b, pbType.providerFn)
		for name, fn := range cloneFns {
			b.Run(fmt.Sprintf("name=%s/pbName=%s", name, pbName), func(b *testing.B) {
				benchmarkClone(b, fn, protos)
			})
		}
	}
}
