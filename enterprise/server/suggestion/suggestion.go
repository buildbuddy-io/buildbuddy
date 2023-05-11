package suggestion

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/openai"
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
)

type suggestionService struct {
	env environment.Env
}

func Register(env environment.Env) error {
	if openai.IsConfigured() {
		env.SetSuggestionService(New(env))
	}
	return nil
}

func New(env environment.Env) *suggestionService {
	return &suggestionService{
		env: env,
	}
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

	data := &openai.CompletionRequest{Model: *openai.Model, Messages: []openai.CompletionMessage{
		openai.CompletionMessage{
			Role:    "user",
			Content: "How would you fix this error? " + errorMessage,
		},
	}}

	completionResponse, err := openai.GetCompletions(data)
	if err != nil {
		return nil, err
	}

	if len(completionResponse.Choices) < 1 {
		return nil, status.NotFoundError("No suggestions found.")
	}

	res := &supb.GetSuggestionResponse{
		Suggestion: []string{completionResponse.Choices[0].Message.Content},
	}

	return res, nil
}
