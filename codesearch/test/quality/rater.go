package main

import (
	"context"
	_ "embed"
	"flag"
	"math"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	cspb "github.com/buildbuddy-io/buildbuddy/proto/codesearch_service"
	gpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	spb "github.com/buildbuddy-io/buildbuddy/proto/search"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/search_rating"
)

var (
	//go:embed ratings.proto.txt
	ratingsProtoText []byte

	codesearchBackend = flag.String("codesearch_backend", "grpc://localhost:2633", "Codesearch backend address (e.g. localhost:2633, or a remote gRPC server address).")
	skipIndex         = flag.Bool("skip_index", false, "If true, skip indexing the repos")
	apiKey            = flag.String("api_key", "", "API Key")
)

func main() {
	ctx := context.Background()
	flag.Parse()

	ratings := &srpb.Suite{}
	err := protojson.Unmarshal(ratingsProtoText, ratings)
	if err != nil {
		log.Fatalf("Failed to parse ratings proto text: %s", err)
	}
	log.Infof("Loaded ratings: %v", ratings)
	log.Infof("Raw ratings: %s", ratingsProtoText)

	if *apiKey == "" {
		log.Fatalf("API key must be provided with --api_key")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)

	log.Infof("Dialing codesearch backend: %s", *codesearchBackend)
	conn, err := grpc_client.DialSimple(*codesearchBackend)
	if err != nil {
		log.Fatalf("Failed to connect to codesearch backend: %s", err)
	}
	client := cspb.NewCodesearchServiceClient(conn)

	if !*skipIndex {
		err := indexRepos(ctx, client, ratings.GetRepos())
		if err != nil {
			log.Fatalf("Failed to index repos: %s", err)
		}
	}

	totalScore := 0.0
	for _, tc := range ratings.GetCases() {
		totalScore += runCase(ctx, client, tc)
	}
	log.Infof("Total score across all test cases: %.2f", totalScore)
}

func indexRepos(ctx context.Context, client cspb.CodesearchServiceClient, repos []*srpb.Repo) error {
	for _, repo := range repos {
		log.Infof("Indexing repo: %s", repo.GetUrl())
		_, err := client.Index(ctx, &inpb.IndexRequest{
			GitRepo: &gpb.GitRepo{
				RepoUrl: repo.GetUrl(),
			},
			RepoState: &gpb.RepoState{
				CommitSha: repo.GetSha(),
			},
			ReplacementStrategy: inpb.ReplacementStrategy_REPLACE_REPO,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func runCase(ctx context.Context, client cspb.CodesearchServiceClient, tc *srpb.Case) float64 {
	log.Infof("Running test case: %s", tc.GetQuery())
	results, err := client.Search(ctx, &spb.SearchRequest{
		Query: tc.GetQuery(),
	})
	if err != nil {
		log.Fatalf("Failed to search: %s", err)
	}
	log.Infof("Search results: %s", protojson.Format(results))
	s := score(results, tc)
	log.Infof("Score for query '%s': %.2f", tc.GetQuery(), s)
	return s
}

func scoreDoc(result *spb.Result, idealResults []*srpb.IdealResult, penalizeMissing bool) float64 {
	for _, ideal := range idealResults {
		if result.GetOwner() == ideal.GetOwner() &&
			result.GetRepo() == ideal.GetRepo() &&
			result.GetFilename() == ideal.GetFilename() {
			if result.GetMatchCount() == ideal.GetMatchCount() {
				// TODO: check snippet line numbers?
				return float64(ideal.GetRelevance())
			}
			// TODO: better discount for too few / too many matches?
			return 0.5 * float64(ideal.GetRelevance())
		}
	}
	if penalizeMissing {
		return -1.0 // Penalize for missing results
	} else {
		return 0.0
	}
}

func score(results *spb.SearchResponse, tc *srpb.Case) float64 {
	idealResults := tc.GetIdealResults()

	idealRels := make([]float64, len(idealResults))
	for i, ideal := range idealResults {
		idealRels[i] = float64(ideal.GetRelevance())
	}
	log.Infof("1")

	rels := make([]float64, len(results.GetResults()))
	for i, result := range results.GetResults() {
		rels[i] = scoreDoc(result, idealResults, tc.GetPenalizeUnmatched())
		log.Infof("Score for %s: %.2f", result.GetFilename(), rels[i])
	}

	return ndcg(rels, idealRels)
}

func ndcg(rels []float64, idealRels []float64) float64 {
	if len(rels) == 0 || len(idealRels) == 0 {
		return 0.0
	}

	log.Infof("Calculating nDCG for %d results", len(rels))
	// TODO: sort by rel descending, or rely on the order of the input?
	idealDcg := dcg(idealRels)
	if idealDcg == 0 {
		return 0.0
	}

	final := dcg(rels)
	log.Infof("nDCG: %.2f / %.2f = %.2f", final, idealDcg, final/idealDcg)
	return final / idealDcg
}

func dcg(rels []float64) float64 {
	sum := 0.0
	for i, rel := range rels {
		sum += rel / math.Log2(float64(i+2))
		log.Infof("DCG: Adding rel %.2f at position %d, current sum: %.2f, log: %.2f", rel, i+1, sum, math.Log2(float64(i+2)))
	}
	return sum
}
