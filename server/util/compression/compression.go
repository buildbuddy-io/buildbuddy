package compression

import (
	"bytes"
	"compress/flate"
	"io/ioutil"
)

func CompressFlate(data []byte, level int) ([]byte, error) {
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func DecompressFlate(data []byte) ([]byte, error) {
	dataReader := bytes.NewReader(data)
	rc := flate.NewReader(dataReader)

	buf, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
