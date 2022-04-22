package main

import (
	"flag"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cavaliergopher/cpio"
)

// This tool allows creating cpio archives in SVR4 (newc) format.
// Example:
//   cpio -out initrd.cpio -- goinit vmvfs

var (
	outPath = flag.String("out", "", "Path to the output cpio file.")
)

func check(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {
	flag.Parse()
	inputPaths := flag.Args()
	if *outPath == "" {
		log.Fatalf("Missing -out path")
	}
	out, err := os.Create(*outPath)
	check(err)
	defer out.Close()

	w := cpio.NewWriter(out)
	defer func() {
		err := w.Close()
		check(err)

		s, err := os.Stat(*outPath)
		check(err)
		log.Infof("Wrote %s (%.2f MB)", *outPath, float64(s.Size())/1e6)
	}()

	for _, path := range inputPaths {
		s, err := os.Stat(path)
		check(err)
		hdr := &cpio.Header{Name: path, Mode: cpio.FileMode(s.Mode()), Size: s.Size()}
		err = w.WriteHeader(hdr)
		check(err)
		f, err := os.Open(path)
		check(err)
		defer f.Close()
		_, err = io.Copy(w, f)
		check(err)
	}
}
