package suggestion

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/openai"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/vertexai"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	supb "github.com/buildbuddy-io/buildbuddy/proto/suggestion"
)

const (
	// The minimum number of build log lines to fetch. Set this high enough to make sure we get the error logs.
	minLines = 1000
	// GPT 3.5 is limited to 4,096 tokens and each token is roughly 4 english characters.
	// We limit our log inputs to roughly half that to avoid going over any input limits.
	maxChars = 8000

	prompt = "How would you fix this error?"

	// Various vertex model parameters.
	// For more information about each one, read: https://cloud.google.com/vertex-ai/docs/generative-ai/learn/models
	vertexTemperature     = 0.2
	vertexMaxOutputTokens = 1024
	vertexTopK            = 0.8
	vertexTopP            = 40
)

type suggestionService struct {
	env environment.Env
}

func Register(env environment.Env) error {
	if openai.IsConfigured() || vertexai.IsConfigured() {
		env.SetSuggestionService(New(env))
	}
	return nil
}

func New(env environment.Env) *suggestionService {
	return &suggestionService{
		env: env,
	}
}

func (s *suggestionService) MultipleProvidersConfigured() bool {
	return openai.IsConfigured() && vertexai.IsConfigured()
}

func (s *suggestionService) GetSuggestion(ctx context.Context, req *supb.GetSuggestionRequest) (*supb.GetSuggestionResponse, error) {
	if req.GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("GetSuggestionRequest must contain a valid invocation_id")
	}

	chunkReq := &elpb.GetEventLogChunkRequest{
		InvocationId: req.GetInvocationId(),
		MinLines:     minLines,
	}

	resp, err := eventlog.GetEventLogChunk(ctx, s.env, chunkReq)
	if err != nil {
		log.Errorf("Encountered error getting event log chunk: %s\nRequest: %s", err, chunkReq)
		return nil, err
	}

	errorMessage := string(resp.GetBuffer())
	components := strings.SplitN(errorMessage, "ERROR:", 2) // Find the first ERROR: line
	if len(components) > 1 {
		errorMessage = components[1]
	}
	if len(errorMessage) > maxChars {
		errorMessage = errorMessage[:maxChars] // Truncate to avoid going over api input limit.
	}

	r := ""
	if openai.IsConfigured() &&
		(!s.MultipleProvidersConfigured() ||
			(s.MultipleProvidersConfigured() && req.GetService() == supb.SuggestionService_OPENAI)) {
		r, err = openaiRequest(errorMessage)
		if err != nil {
			return nil, err
		}
	} else if vertexai.IsConfigured() {
		r, err = vertexaiRequest(ctx, errorMessage)
		if err != nil {
			return nil, err
		}
	}

	res := &supb.GetSuggestionResponse{
		Suggestion: []string{r},
	}

	return res, nil
}

func openaiRequest(input string) (string, error) {
	data := &openai.CompletionRequest{Model: *openai.Model, Messages: []openai.CompletionMessage{
		openai.CompletionMessage{
			Role:    "user",
			Content: prompt + " " + input,
		},
	}}

	completionResponse, err := openai.GetCompletions(data)
	if err != nil {
		return "", err
	}

	if len(completionResponse.Choices) < 1 {
		return "", status.NotFoundError("No suggestions found.")
	}

	return completionResponse.Choices[0].Message.Content, nil
}

func vertexaiRequest(ctx context.Context, input string) (string, error) {
	data := &vertexai.PredictionRequest{Instances: []vertexai.PredictionInstance{
		{
			Examples: []string{},
			Context:  "",
			Messages: []vertexai.PredictionMessage{
				{
					Author:  "user",
					Content: prompt + " " + input,
				},
			},
		},
	}, Parameters: vertexai.PredictionParameters{
		Temperature:     vertexTemperature,
		MaxOutputTokens: vertexMaxOutputTokens,
		TopK:            vertexTopK,
		TopP:            vertexTopP,
	}}

	predictionResponse, err := vertexai.GetPrediction(ctx, data)
	if err != nil {
		return "", err
	}

	if len(predictionResponse.Predictions) < 1 || len(predictionResponse.Predictions[0].Candidates) < 1 {
		log.Debugf("empty response from vertexai: %+v", predictionResponse)
		return "", status.NotFoundError("No suggestions found.")
	}

	return predictionResponse.Predictions[0].Candidates[0].Content, nil
}
