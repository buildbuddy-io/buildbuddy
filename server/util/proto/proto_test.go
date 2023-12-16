package proto_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	dspb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
)

const (
	numSamples = 30
)

var (
	testProtoTypes = map[string]testProtoType{
		"FileMetadata": testProtoType{
			providerFn: func() protoMessage {
				return &rfpb.FileMetadata{}
			},
		},
		"ScoreCard": testProtoType{
			providerFn: func() protoMessage {
				return &capb.ScoreCard{}
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
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
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

func benchmarkMarshal(b *testing.B, marshalFn marshalFunc, data []protoMessage) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pb := data[rand.Intn(len(data))]
		b.SetBytes(int64(pb.SizeVT()))
		_, err := marshalFn(pb)
		if err != nil {
			b.Fatal(err)
		}
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
			return proto.UnmarshalOld(buf, v)
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
