package resources

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/stretchr/testify/require"
)

func TestOnEC2(t *testing.T) {
	for _, test := range []struct {
		name      string
		sysVendor string // "" means the sysfs file does not exist
		want      bool
	}{
		{name: "nitro instance", sysVendor: "Amazon EC2\n", want: true},
		{name: "gce instance", sysVendor: "Google\n", want: false},
		{name: "no sysfs file", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			orig := ec2SysVendorPath
			t.Cleanup(func() { ec2SysVendorPath = orig })
			ec2SysVendorPath = filepath.Join(t.TempDir(), "sys_vendor")
			if test.sysVendor != "" {
				require.NoError(t, os.WriteFile(ec2SysVendorPath, []byte(test.sysVendor), 0644))
			}
			require.Equal(t, test.want, onEC2())
		})
	}
}

// fakeIMDS serves the subset of the EC2 instance metadata service (IMDSv2)
// that ec2AvailabilityZone uses: a session token endpoint, and the
// availability zone path which requires that token. It returns a client
// pointed at the fake.
func fakeIMDS(t *testing.T, zone string) *imds.Client {
	t.Helper()
	const token = "test-token"
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /latest/api/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Aws-Ec2-Metadata-Token-Ttl-Seconds") == "" {
			http.Error(w, "missing token TTL header", http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
		_, _ = w.Write([]byte(token))
	})
	mux.HandleFunc("GET /latest/meta-data/placement/availability-zone", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Aws-Ec2-Metadata-Token") != token {
			http.Error(w, "missing or invalid token", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(zone))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return imds.New(imds.Options{Endpoint: srv.URL})
}

func TestEC2AvailabilityZone(t *testing.T) {
	zone, err := ec2AvailabilityZone(context.Background(), fakeIMDS(t, "us-west-2a"))
	require.NoError(t, err)
	require.Equal(t, "us-west-2a", zone)
}

func TestEC2AvailabilityZone_Unavailable(t *testing.T) {
	// A metadata service that knows nothing about the zone path, as when the
	// instance's metadata endpoint is disabled.
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	client := imds.New(imds.Options{Endpoint: srv.URL})

	_, err := ec2AvailabilityZone(context.Background(), client)
	require.Error(t, err)
}
