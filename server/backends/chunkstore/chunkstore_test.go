package chunkstore

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockstore"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
)

func TestChunkName(t *testing.T) {
	e := "test_0000"
	n := ChunkName("test", 0)
	if n != e {
		t.Fatalf("Chunk name was not equal to expectation: %v should be %v", n, e)
	}

	e = "longertest_1a59"
	n = ChunkName("longertest", 6745)
	if n != e {
		t.Fatalf("Chunk name was not equal to expectation: %v should be %v", n, e)
	}

}

func TestChunkNameWithBucketPerChunk(t *testing.T) {
	flags.Set(t, "storage.bucket_per_chunk", true)
	e := "test_0000/chunk_data"
	n := ChunkName("test", 0)
	if n != e {
		t.Fatalf("Chunk name was not equal to expectation: %v should be %v", n, e)
	}

	e = "longertest_1a59/chunk_data"
	n = ChunkName("longertest", 6745)
	if n != e {
		t.Fatalf("Chunk name was not equal to expectation: %v should be %v", n, e)
	}

}

func TestBlobExists(t *testing.T) {
	m := mockstore.New()
	c := New(m, &ChunkstoreOptions{})
	mtx := &mockstore.Context{}

	if exists, err := c.BlobExists(mtx, "foo"); err != nil {
		t.Fatalf("Encountered error calling BlobExists: %v", err)
	} else if exists {
		t.Fatalf("Blob foo exists before addition")
	}
	m.Set("foo_0000", []byte{})

	if exists, err := c.BlobExists(mtx, "foo"); err != nil {
		t.Fatalf("Encountered error calling BlobExists: %v", err)
	} else if !exists {
		t.Fatalf("Blob foo does not exist after addition")
	}
}

func TestDeleteBlob(t *testing.T) {
	m := mockstore.New()
	c := New(m, &ChunkstoreOptions{})
	mtx := &mockstore.Context{}

	if err := c.DeleteBlob(mtx, "foo"); err != nil {
		t.Errorf("Delete Blob returned error for non-existent blob")
	}

	test_string := []byte("asdfjkl;")

	test_map := make(map[string][]byte)
	test_map["bar_0000"] = []byte("bar contents")

	m.Set("bar_0000", []byte("bar contents"))
	m.Set("foobar_0000", []byte(test_string[:4]))
	m.Set("foobar_0001", []byte(test_string[4:6]))
	m.Set("foobar_0002", []byte(test_string[6:]))

	if err := c.DeleteBlob(mtx, "foobar"); err != nil {
		t.Errorf("Delete Blob returned error for existing blob")
	}

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for delete blob:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

}

func TestReadBlob(t *testing.T) {
	m := mockstore.New()
	c := New(m, &ChunkstoreOptions{})
	mtx := &mockstore.Context{}

	if _, err := c.ReadBlob(mtx, "foo"); !status.IsNotFoundError(err) {
		t.Fatalf("Read did not return status.NotFoundErr for a non-existent blob")
	}

	m.Set("foo_0000", []byte{})

	if data, err := c.ReadBlob(mtx, "foo"); err != nil {
		t.Fatalf("Encountered error calling ReadBlob on empty blob: %v", err)
	} else if data == nil {
		t.Fatalf("Got nil value when reading from empty blob.")
	} else if len(data) != 0 {
		t.Fatalf("Got non-zero length data reading from empty blob: %v", data)
	}

	test_string := []byte("asdfjkl;")
	m.Set("bar_0000", test_string)

	if data, err := c.ReadBlob(mtx, "bar"); err != nil {
		t.Fatalf("Encountered error calling ReadBlob on single chunk blob: %v", err)
	} else if data == nil {
		t.Fatalf("Got nil value when reading from single chunk blob.")
	} else if !bytes.Equal(data, test_string) {
		t.Fatalf("Got wrong data from single chunk blob: %v should be %v", data, test_string)
	}

	m.Set("foobar_0000", []byte(test_string[:4]))
	m.Set("foobar_0001", []byte(test_string[4:6]))
	m.Set("foobar_0002", []byte(test_string[6:]))

	if data, err := c.ReadBlob(mtx, "foobar"); err != nil {
		t.Fatalf("Encountered error calling ReadBlob on multi-chunk blob: %v", err)
	} else if data == nil {
		t.Fatalf("Got nil value when reading from multi-chunk blob.")
	} else if !bytes.Equal(data, test_string) {
		t.Fatalf("Got wrong data from multi-chunk blob: %v should be %v", data, test_string)
	}

}

func TestWriteBlob(t *testing.T) {
	m := mockstore.New()
	c := New(m, &ChunkstoreOptions{})
	mtx := &mockstore.Context{}

	test_map := make(map[string][]byte)
	test_map["foo_0000"] = []byte{}

	if bytesWritten, err := c.WriteBlob(mtx, "foo", []byte{}); err != nil {
		t.Fatalf("Encountered error writing empty file: %v", err)
	} else if bytesWritten != 0 {
		t.Fatalf("WriteBlob wrote wrong number of bytes for empty file: %d should be 0", bytesWritten)
	}

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for empty file:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	test_string := []byte("asdfjkl;")
	test_map["bar_0000"] = test_string

	if bytesWritten, err := c.WriteBlob(mtx, "bar", test_string); err != nil {
		t.Fatalf("Encountered error writing single-chunk file: %v", err)
	} else if bytesWritten != len(test_string) {
		t.Fatalf("WriteBlob wrote wrong number of bytes for single-chunk file: %d should be %d", bytesWritten, len(test_string))
	}

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for single-chunk file:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	test_string = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789`-=[]\\;',./~!@#$%^&*()_+{}|:\"<>?")
	test_map["foobar_0000"] = test_string[:5]
	test_map["foobar_0001"] = test_string[5:10]
	test_map["foobar_0002"] = test_string[10:15]
	test_map["foobar_0003"] = test_string[15:20]
	test_map["foobar_0004"] = test_string[20:25]
	test_map["foobar_0005"] = test_string[25:30]
	test_map["foobar_0006"] = test_string[30:35]
	test_map["foobar_0007"] = test_string[35:40]
	test_map["foobar_0008"] = test_string[40:45]
	test_map["foobar_0009"] = test_string[45:50]
	test_map["foobar_000a"] = test_string[50:55]
	test_map["foobar_000b"] = test_string[55:60]
	test_map["foobar_000c"] = test_string[60:65]
	test_map["foobar_000d"] = test_string[65:70]
	test_map["foobar_000e"] = test_string[70:75]
	test_map["foobar_000f"] = test_string[75:80]
	test_map["foobar_0010"] = test_string[80:85]
	test_map["foobar_0011"] = test_string[85:90]
	test_map["foobar_0012"] = test_string[90:]

	c = New(m, &ChunkstoreOptions{WriteBlockSize: 5})
	if bytesWritten, err := c.WriteBlob(mtx, "foobar", test_string); err != nil {
		t.Fatalf("Encountered error writing multi-chunk file: %v", err)
	} else if bytesWritten != len(test_string) {
		t.Fatalf("WriteBlob wrote wrong number of bytes for multi-chunk file: %d should be %d", bytesWritten, len(test_string))
	}

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for multi-chunk file:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	test_string = []byte("2745904518281828")
	test_map["foobar_0000"] = test_string[:6]
	test_map["foobar_0001"] = test_string[6:12]
	test_map["foobar_0002"] = test_string[12:]
	delete(test_map, "foobar_0003")
	delete(test_map, "foobar_0004")
	delete(test_map, "foobar_0005")
	delete(test_map, "foobar_0006")
	delete(test_map, "foobar_0006")
	delete(test_map, "foobar_0007")
	delete(test_map, "foobar_0008")
	delete(test_map, "foobar_0009")
	delete(test_map, "foobar_000a")
	delete(test_map, "foobar_000b")
	delete(test_map, "foobar_000c")
	delete(test_map, "foobar_000d")
	delete(test_map, "foobar_000e")
	delete(test_map, "foobar_000f")
	delete(test_map, "foobar_0010")
	delete(test_map, "foobar_0011")
	delete(test_map, "foobar_0012")

	c = New(m, &ChunkstoreOptions{WriteBlockSize: 6})
	if bytesWritten, err := c.WriteBlob(mtx, "foobar", test_string); err != nil {
		t.Fatalf("Encountered error overwriting multi-chunk file: %v", err)
	} else if bytesWritten != len(test_string) {
		t.Fatalf("WriteBlob wrote wrong number of bytes for overwriting multi-chunk file: %d should be %d", bytesWritten, len(test_string))
	}

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for overwriting multi-chunk file:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

}

func TestReaders(t *testing.T) {
	m := mockstore.New()
	c := New(m, &ChunkstoreOptions{})
	mtx := &mockstore.Context{}

	r := c.Reader(mtx, "foo")
	rr, err := c.ReverseReader(mtx, "foo")
	if err != nil {
		t.Fatalf("ReverseReader returned an error for non-existent blob: %v", err)
	}

	if _, err := io.ReadAll(r); !status.IsNotFoundError(err) {
		t.Fatalf("Reading from Reader did not return status.NotFound for a non-existent blob: %v", err)
	}

	if _, err := io.ReadAll(rr); !status.IsNotFoundError(err) {
		t.Fatalf("Reading from ReverseReader did not return status.NotFound for a non-existent blob: %v", err)
	}

	m.Set("foo_0000", []byte{})

	r = c.Reader(mtx, "foo")
	rr, err = c.ReverseReader(mtx, "foo")
	if err != nil {
		t.Fatalf("ReverseReader returned an error for empty blob: %v", err)
	}

	if data, err := io.ReadAll(r); err != nil {
		t.Fatalf("Reading from Reader returned error for empty blob: %v", err)
	} else if !bytes.Equal(data, []byte{}) {
		t.Fatalf("Reading from Reader did not return empty slice for empty blob: %v", data)
	}

	if data, err := io.ReadAll(rr); err != nil {
		t.Fatalf("Reading from ReverseReader returned error for empty blob: %v", err)
	} else if !bytes.Equal(data, []byte{}) {
		t.Fatalf("Reading from ReverseReader did not return empty slice for empty blob: %v", data)
	}

	test_string := []byte("asdfjkl;")
	m.Set("bar_0000", test_string)

	r = c.Reader(mtx, "bar")
	rr, err = c.ReverseReader(mtx, "bar")
	if err != nil {
		t.Fatalf("ReverseReader returned an error for single-chunk blob: %v", err)
	}

	data := make([]byte, 6)
	test_data := test_string[:len(data)]
	if bytes_read, err := io.ReadFull(r, data); err != nil {
		t.Fatalf("Reading from Reader returned error for single-chunk blob: %v", err)
	} else if !bytes.Equal(data, test_data) {
		t.Fatalf("Reading from Reader returned data that does not match expectation for single-chunk blob: %v should be %v", data, test_data)
	} else if bytes_read != len(data) {
		t.Fatalf("Reading from Reader did not read correct number of bytes: %v should be %v", bytes_read, len(data))
	}

	overreadData := make([]byte, 6)
	testOverreadData := append(test_string[len(data):], make([]byte, len(overreadData)+len(data)-len(test_string))...)
	if bytes_read, err := io.ReadFull(r, overreadData); err != io.ErrUnexpectedEOF {
		t.Fatalf("Over-reading from Reader did not return io.ErrUnexpectedEOF: %v", err)
	} else if !bytes.Equal(overreadData, testOverreadData) {
		t.Fatalf("Reading from Reader returned data that does not match expectation for single-chunk blob: %v should be %v", overreadData, testOverreadData)
	} else if bytes_read != len(test_string)-len(overreadData) {
		t.Fatalf("Over-reading from Reader did not read enough bytes: %v should be %v", bytes_read, len(overreadData))
	}

	reverseData := make([]byte, 6)
	testReverseData := test_string[(len(test_string) - len(reverseData)):]
	if bytes_read, err := io.ReadFull(rr, reverseData); err != nil {
		t.Fatalf("Reading from ReverseReader returned error for single-chunk blob: %v", err)
	} else if !bytes.Equal(reverseData, testReverseData) {
		t.Fatalf("Reading from ReverseReader returned data that does not match expectation for single-chunk blob: %v should be %v", reverseData, testReverseData)
	} else if bytes_read != len(reverseData) {
		t.Fatalf("Reading from ReverseReader did not read correct number of bytes: %v should be %v", bytes_read, len(reverseData))
	}

	overreadReverseData := make([]byte, 6)
	testOverreadReverseData := append(test_string[:(len(test_string)-len(reverseData))], make([]byte, len(overreadReverseData)+len(reverseData)-len(test_string))...)
	if bytes_read, err := io.ReadFull(rr, overreadReverseData); err != io.ErrUnexpectedEOF {
		t.Fatalf("Over-reading from ReverseReader did not return io.ErrUnexpectedEOF: %v", err)
	} else if !bytes.Equal(overreadReverseData, testOverreadReverseData) {
		t.Fatalf("Reading from ReverseReader returned data that does not match expectation for single-chunk blob: %v should be %v", overreadReverseData, testOverreadReverseData)
	} else if bytes_read != len(test_string)-len(overreadReverseData) {
		t.Fatalf("Over-reading from ReverseReader did not read enough bytes: %v should be %v", bytes_read, len(overreadReverseData))
	}

	test_string = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789`-=[]\\;',./~!@#$%^&*()_+{}|:\"<>?")
	m.Set("foobar_0000", test_string[:5])
	m.Set("foobar_0001", test_string[5:10])
	m.Set("foobar_0002", test_string[10:15])
	m.Set("foobar_0003", test_string[15:20])
	m.Set("foobar_0004", test_string[20:25])
	m.Set("foobar_0005", test_string[25:30])
	m.Set("foobar_0006", test_string[30:35])
	m.Set("foobar_0007", test_string[35:40])
	m.Set("foobar_0008", test_string[40:45])
	m.Set("foobar_0009", test_string[45:50])
	m.Set("foobar_000a", test_string[50:55])
	m.Set("foobar_000b", test_string[55:60])
	m.Set("foobar_000c", test_string[60:65])
	m.Set("foobar_000d", test_string[65:70])
	m.Set("foobar_000e", test_string[70:75])
	m.Set("foobar_000f", test_string[75:80])
	m.Set("foobar_0010", test_string[80:85])
	m.Set("foobar_0011", test_string[85:90])
	m.Set("foobar_0012", test_string[90:])

	r = c.Reader(mtx, "foobar")
	rr, err = c.ReverseReader(mtx, "foobar")
	if err != nil {
		t.Fatalf("ReverseReader returned an error for multi-chunk blob: %v", err)
	}

	data = make([]byte, 52)
	test_data = test_string[:len(data)]
	if bytes_read, err := io.ReadFull(r, data); err != nil {
		t.Fatalf("Reading from Reader returned error for multi-chunk blob: %v", err)
	} else if !bytes.Equal(data, test_data) {
		t.Fatalf("Reading from Reader returned data that does not match expectation for multi-chunk blob: %v should be %v", data, test_data)
	} else if bytes_read != len(data) {
		t.Fatalf("Reading from Reader did not read correct number of bytes: %v should be %v", bytes_read, len(data))
	}

	overreadData = make([]byte, 52)
	testOverreadData = append(test_string[len(data):], make([]byte, len(overreadData)+len(data)-len(test_string))...)
	if bytes_read, err := io.ReadFull(r, overreadData); err != io.ErrUnexpectedEOF {
		t.Fatalf("Over-reading from Reader did not return io.ErrUnexpectedEOF: %v", err)
	} else if !bytes.Equal(overreadData, testOverreadData) {
		t.Fatalf("Reading from Reader returned data that does not match expectation for multi-chunk blob: %v should be %v", overreadData, testOverreadData)
	} else if bytes_read != len(test_string)-len(overreadData) {
		t.Fatalf("Over-reading from Reader did not read enough bytes: %v should be %v", bytes_read, len(overreadData))
	}

	reverseData = make([]byte, 52)
	testReverseData = test_string[(len(test_string) - len(reverseData)):]
	if bytes_read, err := io.ReadFull(rr, reverseData); err != nil {
		t.Fatalf("Reading from ReverseReader returned error for multi-chunk blob: %v", err)
	} else if !bytes.Equal(reverseData, testReverseData) {
		t.Fatalf("Reading from ReverseReader returned data that does not match expectation for multi-chunk blob: %v should be %v", reverseData, testReverseData)
	} else if bytes_read != len(reverseData) {
		t.Fatalf("Reading from ReverseReader did not read correct number of bytes: %v should be %v", bytes_read, len(reverseData))
	}

	overreadReverseData = make([]byte, 52)
	testOverreadReverseData = append(test_string[:(len(test_string)-len(reverseData))], make([]byte, len(overreadReverseData)+len(reverseData)-len(test_string))...)
	if bytes_read, err := io.ReadFull(rr, overreadReverseData); err != io.ErrUnexpectedEOF {
		t.Fatalf("Over-reading from ReverseReader did not return io.ErrUnexpectedEOF: %v", err)
	} else if !bytes.Equal(overreadReverseData, testOverreadReverseData) {
		t.Fatalf("Reading from ReverseReader returned data that does not match expectation for multi-chunk blob: %v should be %v", overreadReverseData, testOverreadReverseData)
	} else if bytes_read != len(test_string)-len(overreadReverseData) {
		t.Fatalf("Over-reading from ReverseReader did not read enough bytes: %v should be %v", bytes_read, len(overreadReverseData))
	}

}

func TestWriter(t *testing.T) {
	m := mockstore.New()
	c := New(m, &ChunkstoreOptions{WriteBlockSize: 5})
	mtx := &mockstore.Context{}

	test_map := make(map[string][]byte)

	flushTime := 50 * time.Millisecond
	w := c.Writer(mtx, "foo", &ChunkstoreWriterOptions{WriteTimeoutDuration: flushTime})

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for open empty file:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	time.Sleep(time.Millisecond * 100)

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for open empty file:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	w.Flush(mtx)

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for open empty file:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	w.Close(mtx)
	test_map["foo_0000"] = []byte{}

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for closed empty file:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	test_string := []byte("asdfjkl;")

	w = c.Writer(mtx, "bar", &ChunkstoreWriterOptions{WriteTimeoutDuration: flushTime})
	w.Write(mtx, test_string)
	test_map["bar_0000"] = test_string[0:5]

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for multi-chunk file before wait for flush:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	time.Sleep(100 * time.Millisecond)
	test_map["bar_0001"] = test_string[5:]

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for multi-chunk file after wait for flush:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	testTail := []byte("zxcvbnm")

	w = c.Writer(mtx, "bar", &ChunkstoreWriterOptions{WriteTimeoutDuration: flushTime})
	w.WriteWithTail(mtx, test_string, testTail)
	w.GetLastChunkIndex(mtx) // Test to ensure this does not wipe out volatile tail
	test_map["bar_0000"] = test_string[0:5]

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for multi-chunk file before wait for flush:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	time.Sleep(100 * time.Millisecond)
	test_map["bar_0001"] = test_string[5:]

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for multi-chunk file after wait for flush:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}

	w.Close(mtx)
	test_map["bar_0002"] = testTail[:5]
	test_map["bar_0003"] = testTail[5:]

	if !cmp.Equal(m.GetBlobMap(), test_map) {
		t.Fatalf("Map contents are incorrect for multi-chunk file after close, which should flush the tail:\n\n%v\n\nshould be:\n\n%v", m.GetBlobMap(), test_map)
	}
}
