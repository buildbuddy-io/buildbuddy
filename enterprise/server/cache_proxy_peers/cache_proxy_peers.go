package cache_proxy_peers

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
	peersFlag = flag.Slice("cache_proxy.peers.addresses", []string{}, "The gRPC addresses of the set of cache-proxy peers.")
	localFlag = flag.String("cache_proxy.peers.local_address", "", "The gRPC address of this cache-proxy server. Should be one of the peers in --cache_proxy.peers.addresses.")
)

const (
	minHash        = "0000000000000000000000000000000000000000000000000000000000000000"
	maxHash        = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	maxHashPlusOne = "g"
)

type Peer struct {
	Local     bool
	BSClient  bspb.ByteStreamClient
	CASClient repb.ContentAddressableStorageClient
}

type Peers struct {
	rangeMap *rangemap.RangeMap // values are Peer{}s (above)
}

func New(env environment.Env) (Peers, error) {
	peers := Peers{rangeMap: rangemap.New()}

	// If no peers are specified, handle all traffic locally.
	if len(*peersFlag) == 0 {
		log.Infof("No peers specified, assigning hash key range ['%s', '%s') to local server.", minHash, maxHashPlusOne)
		peers.rangeMap.Add([]byte(minHash), []byte(maxHashPlusOne), Peer{Local: true})
		return peers, nil
	}

	// Divide the hash key space into ranges and assign one range to each peer.
	// TODO(iain): this assume keys are 64-byte hex strings.
	min, ok := new(big.Int).SetString(minHash, 16)
	if !ok {
		return Peers{}, status.InternalErrorf("Error initializing CacheProxy peers: invalid min hash (%s)", minHash)
	}
	max, ok := new(big.Int).SetString(maxHash, 16)
	if !ok {
		return Peers{}, status.InternalErrorf("Error initializing CacheProxy peers: invalid max hash (%s)", maxHash)
	}
	numPeers := big.NewInt(int64(len(*peersFlag)))
	one := big.NewInt(int64(1))

	cur := min
	inc := new(big.Int).Add(new(big.Int).Div(max, numPeers), one)
	next := inc
	for _, peerAddr := range *peersFlag {
		start := fmt.Sprintf("%x", cur)
		end := fmt.Sprintf("%x", next)

		// If 'next' maxes or overflows the 64-byte hash key space, set end to
		// "g" so it covers the entire tail end of the hash key space. Note
		// rangemap tail bounds are exclusive, so use 'g' to include 'ffff...'.
		if next.Cmp(max) >= 0 {
			end = maxHashPlusOne
		}

		log.Infof("Assigning hash key range ['%s', '%s') to peer %s (I am %s)", start, end, peerAddr, *localFlag)
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
