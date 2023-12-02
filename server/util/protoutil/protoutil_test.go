package protoutil_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	"github.com/buildbuddy-io/buildbuddy/server/util/protoutil"
)

type protoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
	SizeVT() int
	proto.Message
}

func generateFileMetadata(t testing.TB) []protoMessage {
	res := make([]protoMessage, 0, 100)
	for i := 0; i < 100; i++ {
		md := &rfpb.FileMetadata{}
		err := faker.FakeData(md)
		require.NoError(t, err, "unable to fake data for fileMetadata")
		res = append(res, md)
	}
	return res
}

func generateScoreCard(t testing.TB) []protoMessage {
	res := make([]protoMessage, 0, 100)
	for i := 0; i < 100; i++ {
		sc := &capb.ScoreCard{}
		err := faker.FakeData(sc)
		require.NoError(t, err, "unable to fake data for scoreCard")
		//for j := 0; j < 1; j++ {
		//   result := &capb.ScoreCard_Result{}
		//   err := faker.FakeData(result)
		//   //sc.Results = append(sc.Results, result)
		//   res = append(res, result)
		//}
		res = append(res, sc)
	}
	return res
}

func generateBytes(t testing.TB, protos []protoMessage) [][]byte {
	res := make([][]byte, 0, len(protos))
	for _, pb := range protos {
		buf, err := proto.Marshal(pb)
		require.NoError(t, err, "unable to marshal")
		res = append(res, buf)
	}
	return res
}

type marshalFunc func(v protoMessage) ([]byte, error)
type unmarshalFunc func([]byte) error

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

func benchmarkUnmarshal(b *testing.B, unmarshalFn unmarshalFunc, data [][]byte) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := data[rand.Intn(len(data))]
		b.SetBytes(int64(len(buf)))
		err := unmarshalFn(buf)
		if err != nil {
			b.Fatal(err)
		}
	}

}

func BenchmarkMarshal(b *testing.B) {
	data := map[string][]protoMessage{
		"fileMetadata": generateFileMetadata(b),
		"scoreCard":    generateScoreCard(b),
	}

	marshalFns := map[string]marshalFunc{
		"protoutil": func(v protoMessage) ([]byte, error) {
			return v.MarshalVT()
		},
		"proto": func(v protoMessage) ([]byte, error) {
			return proto.Marshal(v)
		},
	}

	for name, fn := range marshalFns {
		for pbName, msgs := range data {
			b.Run(fmt.Sprintf("name=%s/pbName=%s", name, pbName), func(b *testing.B) {
				benchmarkMarshal(b, fn, msgs)
			})
		}
	}
}

type unmarshalFnAndData struct {
	data [][]byte
	fns  map[string]unmarshalFunc
}

func BenchmarkUnmarshal(b *testing.B) {
	mds := generateFileMetadata(b)
	scoreCards := generateScoreCard(b)
	tests := map[string]unmarshalFnAndData{
		"fileMetadata": unmarshalFnAndData{
			data: generateBytes(b, mds),
			fns: map[string]unmarshalFunc{
				"protoutil": func(buf []byte) error {
					v := &rfpb.FileMetadata{}
					return protoutil.Unmarshal(buf, v)
				},
				"proto": func(buf []byte) error {
					v := &rfpb.FileMetadata{}
					return proto.Unmarshal(buf, v)
				},
			},
		},
		"scoreCard": unmarshalFnAndData{
			data: generateBytes(b, scoreCards),
			fns: map[string]unmarshalFunc{
				"protoutil": func(buf []byte) error {
					v := &capb.ScoreCard{}
					return protoutil.Unmarshal(buf, v)
				},
				"proto": func(buf []byte) error {
					v := &capb.ScoreCard{}
					return proto.Unmarshal(buf, v)
				},
			},
		},
	}

	for pbName, tc := range tests {
		for name, fn := range tc.fns {
			b.Run(fmt.Sprintf("name=%s/pbName=%s", name, pbName), func(b *testing.B) {
				benchmarkUnmarshal(b, fn, tc.data)
			})
		}
	}
}
