package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
)

func TestRequests(t *testing.T) {
	transport := http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network string, addr string) (conn net.Conn, err error) {
			port := 50000 + rand.Int31n(500)
			localAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(":%d", port))
			if err != nil {
				return nil, err
			}
			log.Infof("local addr: %+v", localAddr)
			d := &net.Dialer{
				LocalAddr: localAddr,
			}
			return d.DialContext(ctx, network, addr)
		},
		TLSHandshakeTimeout: 10 * time.Second,
	}
	c := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &transport,
	}
	for range *n {
		start := time.Now()
		log.Infof("request...")
		resp, err := c.Get(*target)
		length := int64(0)
		if resp != nil {
			length = resp.ContentLength
		}
		log.Infof("got response from %s in %s: %d", *target, time.Since(start), length)
		if err != nil && strings.Contains(err.Error(), "already in use") {
			continue
		}
		require.NoErrorf(t, err, "failed to fetch url %s", *target)
		c.CloseIdleConnections()
		time.Sleep(25 * time.Millisecond)
	}
}
