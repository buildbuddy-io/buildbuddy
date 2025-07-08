// This file contains a rating harness for evaluating the quality of codesearch results.
//
// See README.md in this directory for more background and instructions on how to create test cases.
package main

import (
	"context"
	_ "embed"
	"flag"
	"math"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	cspb "github.com/buildbuddy-io/buildbuddy/proto/codesearch_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	spb "github.com/buildbuddy-io/buildbuddy/proto/search"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/search_rating"
)

var (
	//go:embed ratings.json
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
		log.Infof("Indexing repo: %s@%s", repo.GetRepo().GetRepoUrl(), repo.GetRepoState().GetCommitSha())
		_, err := client.Index(ctx, &inpb.IndexRequest{
			GitRepo:             repo.GetRepo(),
			RepoState:           repo.GetRepoState(),
			ReplacementStrategy: inpb.ReplacementStrategy_REPLACE_REPO,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func runCase(ctx context.Context, client cspb.CodesearchServiceClient, tc *srpb.Case) float64 {
	log.Debugf("Running test case: %s", tc.GetQuery())
	results, err := client.Search(ctx, &spb.SearchRequest{
		Query: tc.GetQuery(),
	})
	if err != nil {
		log.Fatalf("Failed to search: %s", err)
	}
	log.Debugf("Search results: %s", protojson.Format(results))
	s := score(results, tc)
	log.Infof("Normalized score for query '%s': %.2f", tc.GetQuery(), s)
	return s
}

// scoreDoc computes the score for a single search result.
// Note that the score for an individual document is not position-dependent. All we do is find the
// result in the ideal results, and return its relevance score (discounted if the match count
// doesn't match). The position scoring happens during the DCG calculation.
// Depending on the test case, we may also penalize for results not found in the ideal result set.
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

// score computes the nDCG for a set of actual results, based on the provided ideal results.
func score(results *spb.SearchResponse, tc *srpb.Case) float64 {
	idealResults := tc.GetIdealResults()

	idealRels := make([]float64, len(idealResults))
	for i, ideal := range idealResults {
		idealRels[i] = float64(ideal.GetRelevance())
	}

	rels := make([]float64, len(results.GetResults()))
	for i, result := range results.GetResults() {
		rels[i] = scoreDoc(result, idealResults, tc.GetPenalizeUnmatched())
		log.Debugf("Score for %s: %.2f", result.GetFilename(), rels[i])
	}

	return ndcg(rels, idealRels)
}

// ndcg computes the normalized Discounted Cumulative Gain (nDCG) for a list of relevance scores,
// normalized against the ideal relevance scores. In theory, nDCG scores can be compared across
// different queries with different expected result counts.
// https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Normalized_DCG
func ndcg(rels []float64, idealRels []float64) float64 {
	if len(rels) == 0 || len(idealRels) == 0 {
		return 0.0
	}

	sort.Sort(sort.Reverse(sort.Float64Slice(idealRels)))
	idealDcg := dcg(idealRels)
	if idealDcg == 0 {
		return 0.0
	}

	final := dcg(rels)
	log.Debugf("nDCG: %.2f / %.2f = %.2f", final, idealDcg, final/idealDcg)
	return final / idealDcg
}

// dcg computes the Discounted Cumulative Gain for a list of relevance scores.
// https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Discounted_Cumulative_Gain
func dcg(rels []float64) float64 {
	sum := 0.0
	for i, rel := range rels {
		sum += rel / math.Log2(float64(i+2))
	}
	return sum
}
