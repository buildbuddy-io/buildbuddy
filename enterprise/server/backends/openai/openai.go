package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var endpoint = flag.String("openai.endpoint", "https://api.openai.com/v1/chat/completions", "OpenAI endpoint")
var responsesEndpoint = flag.String("openai.responses_endpoint", "https://api.openai.com/v1/responses", "OpenAI Responses API endpoint")
var apiKey = flag.String("openai.api_key", "", "OpenAI API key", flag.Secret)
var Model = flag.String("openai.model", "gpt-5", "OpenAI model name to use. Find them here: https://platform.openai.com/docs/models")

func IsConfigured() bool {
	return *apiKey != ""
}

func GetCompletions(ctx context.Context, data *CompletionRequest) (*CompletionResponse, error) {
	response := &CompletionResponse{}
	if err := post(ctx, *endpoint, data, response); err != nil {
		return nil, err
	}
	return response, nil
}

func GetResponse(ctx context.Context, data *ResponseRequest) (*ResponseResponse, error) {
	response := &ResponseResponse{}
	if err := post(ctx, *responsesEndpoint, data, response); err != nil {
		return nil, err
	}
	return response, nil
}

func post(ctx context.Context, endpoint string, input, output any) error {
	jsonData, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(jsonData))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+*apiKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return status.UnavailableErrorf("OpenAI request failed with HTTP status %d", response.StatusCode)
	}
	return json.Unmarshal(body, output)
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

type ResponseInput struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ResponseReasoning struct {
	Effort string `json:"effort"`
}

type ResponseFormat struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Strict bool   `json:"strict"`
	Schema any    `json:"schema"`
}

type ResponseText struct {
	Format    ResponseFormat `json:"format"`
	Verbosity string         `json:"verbosity,omitempty"`
}

type ResponseRequest struct {
	Model           string             `json:"model"`
	Input           []ResponseInput    `json:"input"`
	Store           bool               `json:"store"`
	Reasoning       *ResponseReasoning `json:"reasoning,omitempty"`
	Text            *ResponseText      `json:"text,omitempty"`
	MaxOutputTokens int                `json:"max_output_tokens,omitempty"`
}

type ResponseContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type ResponseOutput struct {
	Type    string            `json:"type"`
	Content []ResponseContent `json:"content"`
}

type ResponseResponse struct {
	Status string           `json:"status"`
	Output []ResponseOutput `json:"output"`
}

func (r *ResponseResponse) OutputText() string {
	for _, output := range r.Output {
		if output.Type != "message" {
			continue
		}
		for _, content := range output.Content {
			if content.Type == "output_text" {
				return content.Text
			}
		}
	}
	return ""
}
