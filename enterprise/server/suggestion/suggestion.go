package suggestion

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	supb "github.com/buildbuddy-io/buildbuddy/proto/suggestion"
)

var apiKey = flag.String("openai.api_key", "", "OpenAI API key")
var model = flag.String("openai.model", "gpt-3.5-turbo", "OpenAI model name to use. Find them here: https://platform.openai.com/docs/models")

const (
	// The minimum number of build log lines to fetch. Set this high enough to make sure we get the error logs.
	minLines = 1000
	// GPT 3.5 is limited to 4,096 tokens and each token is roughly 4 english characters.
	// We limit our log inputs to roughly half that to avoid going over any input limits.
	maxChars = 8000
	// The endpoint to hit for completions calls: https://platform.openai.com/docs/guides/chat
	chatCompletionsEndpoint = "https://api.openai.com/v1/chat/completions"
)

type suggestionService struct {
	env environment.Env
}

func Register(env environment.Env) error {
	if *apiKey != "" {
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

	data := &completionRequest{Model: *model, Messages: []completionMessage{
		completionMessage{
			Role:    "user",
			Content: "How would you fix this error? " + errorMessage,
		},
	}}

	completionResponse, err := getCompletions(data)
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

// TODO(siggisim): Pull this into its own backend if we want to use this in other places.
func getCompletions(data *completionRequest) (*completionResponse, error) {
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
		log.Debugf("%+v %+v", postResp.StatusCode, string(body))
		return nil, status.UnavailableError("Unable to contact suggestion provider.") // todo
	}

	var response completionResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

type completionMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type completionRequest struct {
	Model    string              `json:"model"`
	Messages []completionMessage `json:"messages"`
}

type completionChoice struct {
	Message completionMessage `json:"message"`
}

type completionResponse struct {
	Choices []completionChoice `json:"choices"`
}
