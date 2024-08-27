package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	target = flag.String("target", "http://23.176.168.2:9000", "target URL")
	n      = flag.Int("n", 1000, "Number of tries")
)

func main() {
	flag.Parse()

	transport := http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network string, addr string) (conn net.Conn, err error) {
			port := 50000 + rand.Int31n(15000)
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
		if err != nil {
			log.Fatalf("failed to fetch url %s: %s", *target, err)
		}
		c.CloseIdleConnections()
		time.Sleep(25 * time.Millisecond)
	}
}
