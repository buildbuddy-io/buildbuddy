package analysis

import (
	"context"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/encoding/protojson"

	anpb "github.com/buildbuddy-io/buildbuddy/proto/analysis"
)

var (
	botCommentsEnabled = flag.Bool("github.bot_comments_enabled", false, "Whether to post comments to GitHub based on workflow output.")
)

const (
	// maximum number of entries we return in a single GetLogs request.
	pageSize = 20
	analaysisFilename = "buildbuddy_analysis.json"
	maxAnalysisFileSize = 1024 * 1024  // Famous last words: 1MB is plenty
)

type Commenter struct {
	env environment.Env
	dbh interfaces.OLAPDBHandle
}

func (c *Commenter) PostComments(ctx context.Context, root string) error {
	analysisFilepath, err := filepath.Rel(root, analaysisFilename)
	if (err != nil) {
		return err
	}
	fileinfo, err := os.Stat(analysisFilepath)
	if (err != nil) {
		return err
	}
	if (fileinfo.IsDir() || fileinfo.Size() > maxAnalysisFileSize) {
		// Weird that someone did this, but whatever, let's just skip it.
		return status.InvalidArgumentError("F A I L U R E")
	}

	a, err := os.ReadFile(analysisFilepath)
	if err != nil {
		return err
	}
	analysis := &anpb.AnalysisResults{}
	protojson.Unmarshal(a, analysis)

	// Okay! Time to post some comments.

	// XXX: Do we have an event to publish to raw logs?
	return nil
}

func (c *Commenter) Wait() error {
	// XXX: 
	return nil
}