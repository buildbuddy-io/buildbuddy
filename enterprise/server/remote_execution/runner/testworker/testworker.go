// This binary implements a remote persistent worker for use in runner_test.go

package main

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	protocol       = flag.String("protocol", "proto", "Serialization protocol: 'json' or 'proto'.")
	responseBase64 = flag.String("response_base64", "", "Base64-encoded response to return for every request. Includes varint length prefix (for proto responses).")
)

func main() {
	flag.Parse()

	resBytes, err := base64.StdEncoding.DecodeString(*responseBase64)
	if err != nil {
		panic(err)
	}

	var br io.ByteReader
	var dec *json.Decoder
	if *protocol == "json" {
		dec = json.NewDecoder(os.Stdin)
	} else {
		br = bufio.NewReader(os.Stdin)
	}

	for {
		// Note: Logging goes to stderr, so it doesn't mess with the persistent
		// worker's output.
		log.Info("[worker] Waiting for request...")
		if *protocol == "json" {
			if err := readJSONRequest(dec); err != nil {
				panic(err)
			}
		} else {
			if err := readProtoRequest(br); err != nil {
				panic(err)
			}
		}
		log.Info("[worker] Got request! Sending response...")

		_, err = os.Stdout.Write(resBytes)
		if err != nil {
			panic(err)
		}

		log.Info("[worker] Sent response!")
	}
}

func readProtoRequest(r io.ByteReader) error {
	reqSizeBytes, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	reqBytes := make([]byte, reqSizeBytes)
	for i := 0; i < int(reqSizeBytes); i++ {
		reqBytes[i], err = r.ReadByte()
		if err != nil {
			return err
		}
	}
	return nil
}

func readJSONRequest(decoder *json.Decoder) error {
	raw := json.RawMessage{}
	return decoder.Decode(&raw)
}
