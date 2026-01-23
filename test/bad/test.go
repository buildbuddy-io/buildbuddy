package bad

import (
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchPklPackage(t *testing.T) {
	resp, err := http.Get("https://pkg.pkl-lang.org/pkl-go/pkl.golang@0.6.0")
	require.NoError(t, err)
	defer resp.Body.Close()

	fmt.Println(resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	fmt.Println(string(body))

	require.GreaterOrEqual(t, resp.StatusCode, 200)
	require.Less(t, resp.StatusCode, 300)
}
