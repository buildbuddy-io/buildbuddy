package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/creack/pty"
	"github.com/logrusorgru/aurora"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

var (
	ngrokToken = flag.String("ngrok_token", "", "Ngrok token")
)

// ngrok tunnel API response
type Tunnel struct {
	PublicURL string `json:"public_url"`
}

type TunnelsResponse struct {
	Tunnels []Tunnel `json:"tunnels"`
}

type RequestData struct {
	Run string `json:"run"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s", err)
	}
}

func run() error {
	flag.Parse()
	http.HandleFunc("/", handler)

	// Install ngrok if it's not installed
	_, err := runCommandWithOutput(context.Background(), "ngrok", []string{}, nil /*=env*/, "" /*=dir*/, io.Discard)
	if err != nil {
		installCmd := `
curl -s https://ngrok-agent.s3.amazonaws.com/ngrok.asc | \
  sudo gpg --dearmor -o /etc/apt/keyrings/ngrok.gpg && \
  echo "deb [signed-by=/etc/apt/keyrings/ngrok.gpg] https://ngrok-agent.s3.amazonaws.com buster main" | \
  sudo tee /etc/apt/sources.list.d/ngrok.list && \
  sudo apt update && sudo apt install ngrok
`
		_, err := runCommandWithOutput(context.Background(), "bash", []string{"-eo", "pipefail", "-c", installCmd}, nil /*=env*/, "" /*=dir*/, os.Stderr)
		if err != nil {
			fmt.Printf("Command failed with: %s", err.Err)
			return err.Err
		}
	}

	var eg errgroup.Group
	eg.Go(func() error {
		return http.ListenAndServe(":1234", nil)
	})
	eg.Go(func() error {
		// Wait until the local server starts up
		time.Sleep(3 * time.Second)

		_, err := runCommandWithOutput(context.Background(), "ngrok", []string{"config", "add-authtoken", *ngrokToken}, nil /*=env*/, "" /*=dir*/, os.Stderr)
		if err != nil {
			fmt.Printf("Command failed with: %s", err.Err)
			return err.Err
		}

		// Run ngrok to create a publicly accessible link to the local server
		_, err = runCommandWithOutput(context.Background(), "ngrok", []string{"http", "http://localhost:1234"}, nil /*=env*/, "" /*=dir*/, io.Discard)
		if err != nil {
			fmt.Printf("Command failed with: %s", err.Err)
			return err.Err
		}
		return nil
	})
	eg.Go(func() error {
		// Wait until local server and ngrok have started
		time.Sleep(5 * time.Second)

		// Get ngrok public url to local server
		resp, err := http.Get("http://localhost:4040/api/tunnels/")
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		var result TunnelsResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return err
		}

		if len(result.Tunnels) != 1 {
			return status.InternalErrorf("unexpected number of tunnels: %d", len(result.Tunnels))
		}

		publicURL := result.Tunnels[0].PublicURL
		output := fmt.Sprintf("Public URL is: %s", publicURL)
		fmt.Println(output)
		return err
	})

	return eg.Wait()
}

func handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Parse the JSON body
	var data RequestData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err := runCommand(context.Background(), "bash", []string{"-eo", "pipefail", "-c", data.Run}, nil /*=env*/, "" /*=dir*/, os.Stderr)
	if err != nil {
		fmt.Printf("Error is %s", err)
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{"status": "success"}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

type commandError struct {
	Err    error
	Output string
}

func runCommandWithOutput(ctx context.Context, executable string, args []string, env map[string]string, dir string, outputSink io.Writer) (string, *commandError) {
	var buf bytes.Buffer
	w := io.MultiWriter(outputSink, &buf)

	if err := runCommand(ctx, executable, args, env, dir, w); err != nil {
		return "", &commandError{err, buf.String()}
	}
	output := buf.String()
	return strings.TrimSpace(output), nil
}

func runCommand(ctx context.Context, executable string, args []string, env map[string]string, dir string, outputSink io.Writer) error {
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	if dir != "" {
		cmd.Dir = dir
	}
	size := &pty.Winsize{Rows: uint16(20), Cols: uint16(114)}
	f, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return err
	}
	defer f.Close()
	copyOutputDone := make(chan struct{})
	go func() {
		io.Copy(outputSink, f)
		copyOutputDone <- struct{}{}
	}()
	err = cmd.Wait()
	<-copyOutputDone

	if err != nil {
		_, _ = outputSink.Write([]byte(aurora.Sprintf(aurora.Red("Command failed: %s\n"), err)))
	}

	return err
}
