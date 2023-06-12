package vertexai

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	googleoauth "golang.org/x/oauth2/google"
)

var credentials = flagutil.New("vertexai.credentials", "", "The GCP credentials to use", flagutil.SecretTag)
var projectID = flag.String("vertexai.project", "flame-build", "The GCP project ID to use")
var region = flag.String("vertexai.region", "us-central1", "The GCP region to use")
var modelID = flag.String("vertexai.model", "chat-bison@001", "The model ID to use")

const (
	predictionEndpoint = "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict"
	vertexScope        = "https://www.googleapis.com/auth/cloud-platform"
)

func IsConfigured() bool {
	return *credentials != ""
}

func GetPrediction(ctx context.Context, data *PredictionRequest) (*PredictionResponse, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	postRequest, err := http.NewRequest("POST", fmt.Sprintf(predictionEndpoint, *region, *projectID, *region, *modelID), bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	if *credentials == "" {
		return nil, status.UnavailableError("No credentials provided.")
	}
	cfg, err := googleoauth.JWTConfigFromJSON([]byte(*credentials), vertexScope)
	if err != nil {
		return nil, err
	}

	token, err := cfg.TokenSource(ctx).Token()
	if err != nil {
		return nil, err
	}

	postRequest.Header.Set("Content-Type", "application/json")
	postRequest.Header.Set("Authorization", "Bearer "+token.AccessToken)

	client := &http.Client{}
	postResp, err := client.Do(postRequest)
	if err != nil {
		return nil, err
	}
	defer postResp.Body.Close()

	body, err := io.ReadAll(postResp.Body)
	if err != nil {
		return nil, err
	}

	if postResp.StatusCode != http.StatusOK {
		log.Debugf("error getting predictions from vertexai: %+v body: %+v", postResp.StatusCode, string(body))
		return nil, status.UnavailableError("Unable to contact suggestion provider.")
	}

	var response PredictionResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

type PredictionMessage struct {
	Author  string `json:"author"`
	Content string `json:"content"`
}

type PredictionInstance struct {
	Context  string              `json:"context"`
	Examples []string            `json:"examples"`
	Messages []PredictionMessage `json:"messages"`
}

type PredictionParameters struct {
	Temperature     float64 `json:"temperature"`
	MaxOutputTokens int     `json:"maxOutputTokens"`
	TopP            float64 `json:"topP"`
	TopK            float64 `json:"topK"`
}

type PredictionRequest struct {
	Instances  []PredictionInstance `json:"instances"`
	Parameters PredictionParameters `json:"parameters"`
}

type Candidate struct {
	Author  string `json:"author"`
	Content string `json:"content"`
}

type SafetyAttributes struct {
	Blocked    bool      `json:"blocked"`
	Categories []string  `json:"categories"`
	Scores     []float64 `json:"scores"`
}

type Prediction struct {
	Candidates       []Candidate      `json:"candidates"`
	SafetyAttributes SafetyAttributes `json:"safetyAttributes"`
}

type PredictionResponse struct {
	Predictions []Prediction `json:"predictions"`
}
