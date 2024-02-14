package analysis

import (
	"context"
	"log"
	"os"
	"path/filepath"

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
}

func (c *Commenter) PostComments(ctx context.Context, root string) error {
	log.Printf("jdh Posting comments")
	analysisFilepath, err := filepath.Rel(root, analaysisFilename)
	if (err != nil) {
		log.Printf("jdh Rel failed: %+v", err)
		return err
	}
	fileinfo, err := os.Stat(analysisFilepath)
	if (err != nil) {
		log.Printf("jdh Fileinfo stat failed: %+v", err)
		return err
	}
	if (fileinfo.IsDir() || fileinfo.Size() > maxAnalysisFileSize) {
		// Weird that someone did this, but whatever, let's just skip it.
		// XXX: Log and quit, don't fail.
		return status.InvalidArgumentError("jdh F A I L U R E")
	}

	a, err := os.ReadFile(analysisFilepath)
	if err != nil {
		log.Printf("jdh readfile failed: %+v", err)
		return err
	}
	analysis := &anpb.AnalysisResults{}
	err = protojson.Unmarshal(a, analysis)
	if err != nil {
		log.Printf("jdh Unmarshal failed: %+v", err)
		return err
	}
	log.Printf("jdh Results parsed from output: %+v", analysis)

	// Okay! Time to post some comments.
	// TODO(jdhollen)
	return nil
}

func (c *Commenter) Wait() error {
	// TODO(jdhollen) 
	return nil
}