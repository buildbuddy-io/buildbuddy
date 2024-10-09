package proxy_peers

import (
	"fmt"
	"math/big"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	peersFlag = flag.Slice("cache_proxy.peers.addresses", []string{}, "The addresses of the set of cache-proxy peers.")
	localFlag = flag.String("cache_proxy.peers.local_address", "", "The address of this cache-proxy server. Should be one of the peers in --cache_proxy.peers.addresses.")
)

const (
	minHash = "0000000000000000000000000000000000000000000000000000000000000000"
	maxHash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
)

type Peer struct {
	Local     bool
	BSClient  bspb.ByteStreamClient
	CASClient repb.ContentAddressableStorageClient
}

// TODO(iain): bad abstraction, simplify to something that divides hash ranges
// and then put the grpc stuff elsewhere.
type Peers struct {
	rangeMap *rangemap.RangeMap
}

func New(env environment.Env) (Peers, error) {
	peers := Peers{
		rangeMap: rangemap.New(),
	}

	// If no peers are specified, handle all traffic locally.
	if len(*peersFlag) == 0 {
		peers.rangeMap.Add([]byte(minHash), []byte(maxHash), Peer{Local: true})
	}

	// Divide the hash key space into ranges and assign one range to each peer.
	// TODO(iain): this assume keys are 64-byte hex strings.
	max := new(big.Int).SetBytes([]byte(maxHash))
	numPeers := big.NewInt(int64(len(*peersFlag)))
	inc := new(big.Int).Div(max, numPeers)
	cur := new(big.Int).SetBytes([]byte(minHash))
	next := new(big.Int).Add(cur, inc)
	for _, peerAddr := range *peersFlag {
		start := fmt.Sprintf("%x", cur)
		end := fmt.Sprintf("%x", next)
		log.Debugf("Assigning byte range [%s, %s) to peer %s", start, end, peerAddr)
		peer := Peer{Local: true}
		if peerAddr != *localFlag {
			conn, err := grpc_client.DialInternal(env, peerAddr)
			if err != nil {
				return Peers{}, status.InternalErrorf("Error dialing peer %s: %s", peerAddr, err.Error())
			}
			peer = Peer{
				Local:     false,
				BSClient:  bspb.NewByteStreamClient(conn),
				CASClient: repb.NewContentAddressableStorageClient(conn),
			}
		}
		peers.rangeMap.Add([]byte(start), []byte(end), peer)
		cur = next
		next = new(big.Int).Add(cur, inc)
	}

	return peers, nil
}

func (p Peers) Get(key digest.Key) (Peer, error) {
	raw := p.rangeMap.Lookup([]byte(key.Hash))
	if raw == nil {
		return Peer{}, status.NotFoundErrorf("Peer responsible for hash %s not found", key.Hash)
	}
	return raw.(Peer), nil
}
