package protoutil_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	"github.com/buildbuddy-io/buildbuddy/server/util/protoutil"
)

const (
	numSamples = 30
)

var (
	testProtoTypes = map[string]providerFunc{
		//"FileMetadata": func() protoMessage {
		//		return &rfpb.FileMetadata{}
		//	},
		//	"ScoreCard": func() protoMessage {
		//		return &capb.ScoreCard{}
		//	},
		"TreeCache": func() protoMessage {
			return &capb.TreeCache{}
		},
	}
)

type protoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
	SizeVT() int
	proto.Message
}

func generateProtos(t testing.TB, providerFn providerFunc) []protoMessage {
	//fmt.Println("started generate protos")
	res := make([]protoMessage, 0, numSamples)
	for i := 0; i < numSamples; i++ {
		pb := providerFn()
		err := faker.FakeData(pb)
		//fmt.Printf("%d: len=%d/n", i, len(pb.(*capb.TreeCache).GetChildren()))
		require.NoError(t, err, "unable to fake data")
		res = append(res, pb)
	}
	//fmt.Println("finished generate protos")
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
type unmarshalFunc func([]byte, protoMessage) error
type providerFunc func() protoMessage

func benchmarkMarshal(b *testing.B, marshalFn marshalFunc, data []protoMessage) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pb := data[rand.Intn(len(data))]
		//b.SetBytes(int64(pb.SizeVT()))
		//fmt.Println("started marshal")
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
		//b.SetBytes(int64(len(buf)))
		v := providerFn()
		//fmt.Println("started unmarshal")
		err := unmarshalFn(buf, v)
		if err != nil {
			b.Fatal(err)
		}
	}

}

func BenchmarkMarshal(b *testing.B) {
	marshalFns := map[string]marshalFunc{
		"proto": func(v protoMessage) ([]byte, error) {
			return proto.Marshal(v)
		},
		"protoutil": func(v protoMessage) ([]byte, error) {
			return v.MarshalVT()
		},
	}

	for pbName, providerFn := range testProtoTypes {
		protos := generateProtos(b, providerFn)
		//fmt.Println("data generated")
		for name, fn := range marshalFns {
			b.Run(fmt.Sprintf("name=%s/pbName=%s", name, pbName), func(b *testing.B) {
				benchmarkMarshal(b, fn, protos)
			})
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	unmarshalFns := map[string]unmarshalFunc{
		"proto": func(buf []byte, v protoMessage) error {
			return protoutil.Unmarshal(buf, v)
		},
		"protoutil": func(buf []byte, v protoMessage) error {
			return proto.Unmarshal(buf, v)
		},
	}

	for pbName, providerFn := range testProtoTypes {
		protos := generateProtos(b, providerFn)
		data := generateBytes(b, protos)
		//fmt.Println("data generated")
		for name, fn := range unmarshalFns {
			b.Run(fmt.Sprintf("name=%s/pbName=%s", name, pbName), func(b *testing.B) {
				benchmarkUnmarshal(b, fn, providerFn, data)
			})
		}
	}
}
