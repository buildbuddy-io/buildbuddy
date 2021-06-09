package main

import (
	"flag"
	"io"
	"net"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	listen      = flag.String("listen", "", "host:port on which the proxy should listen")
	destination = flag.String("destination", "", "host:port where traffic should be forwarded")
)

func handleConn(srcConn *net.TCPConn) {
	log.Info("Handling new incoming connection.")

	defer srcConn.Close()
	dstAddr, err := net.ResolveTCPAddr("tcp", *destination)

	dstConn, err := net.DialTCP("tcp", nil, dstAddr)
	if err != nil {
		log.Warningf("could not dial destination %q: %s", *destination, err)
		return
	}
	defer dstConn.Close()
	log.Infof("Created new connection to %q", *destination)

	var wg sync.WaitGroup
	wg.Add(2)

	copyData := func(dstConn *net.TCPConn, srcConn *net.TCPConn, description string) {
		defer wg.Done()
		_, err = io.Copy(dstConn, srcConn)
		if err != nil {
			log.Warningf("could not copy %s: %s", description, err)
			_ = dstConn.Close()
			_ = srcConn.Close()
			return
		}
		_ = dstConn.CloseWrite()
	}

	go copyData(dstConn, srcConn, "local->remote")
	go copyData(srcConn, dstConn, "remote->local")

	wg.Wait()
}

func main() {
	flag.Parse()

	if *listen == "" {
		log.Fatalf("--listen is required")
	}
	if *destination == "" {
		log.Fatalf("--destination is required")
	}

	localAddr, err := net.ResolveTCPAddr("tcp", *listen)
	listener, err := net.ListenTCP("tcp", localAddr)
	if err != nil {
		log.Fatalf("could not listen on %q: %s", *listen, err)
	}

	log.Info("Proxy ready for connections.")

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Warningf("could not accept connection: %s", err)
			continue
		}
		go handleConn(conn)
	}
}
