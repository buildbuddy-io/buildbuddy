package openai

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var apiKey = flag.String("openai.api_key", "", "OpenAI API key", flag.Secret)
var Model = flag.String("openai.model", "gpt-3.5-turbo", "OpenAI model name to use. Find them here: https://platform.openai.com/docs/models")

const (
	chatCompletionsEndpoint = "https://api.openai.com/v1/chat/completions"
)

func IsConfigured() bool {
	return *apiKey != ""
}

func GetCompletions(data *CompletionRequest) (*CompletionResponse, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	postRequest, err := http.NewRequest("POST", chatCompletionsEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	postRequest.Header.Set("Content-Type", "application/json")
	postRequest.Header.Set("Authorization", "Bearer "+*apiKey)

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
		log.Debugf("error getting completions from openai: %+v body: %+v", postResp.StatusCode, string(body))
		return nil, status.UnavailableError("Unable to contact suggestion provider.") // todo
	}

	var response CompletionResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

type CompletionMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type CompletionRequest struct {
	Model    string              `json:"model"`
	Messages []CompletionMessage `json:"messages"`
}

type CompletionChoice struct {
	Message CompletionMessage `json:"message"`
}

type CompletionResponse struct {
	Choices []CompletionChoice `json:"choices"`
}
