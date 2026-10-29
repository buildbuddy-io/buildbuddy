package platform_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const image = "docker://gcr.io/example/image:v1"

func props(kv ...string) []*repb.Platform_Property {
	var out []*repb.Platform_Property
	for i := 0; i < len(kv); i += 2 {
		out = append(out, &repb.Platform_Property{Name: kv[i], Value: kv[i+1]})
	}
	return out
}

func commandTask(p ...*repb.Platform_Property) *repb.ExecutionTask {
	return &repb.ExecutionTask{Command: &repb.Command{Platform: &repb.Platform{Properties: p}}}
}

func TestSetContainerImage(t *testing.T) {
	for _, tc := range []struct {
		name string
		task *repb.ExecutionTask
		want *repb.ExecutionTask
	}{
		{
			name: "inserts in sorted position",
			task: commandTask(props("Arch", "amd64", "OSFamily", "linux", "recycle-runner", "true")...),
			want: commandTask(props("Arch", "amd64", "OSFamily", "linux", "container-image", image, "recycle-runner", "true")...),
		},
		{
			name: "creates command platform",
			task: &repb.ExecutionTask{Command: &repb.Command{Arguments: []string{"pwd"}}},
			want: &repb.ExecutionTask{Command: &repb.Command{
				Arguments: []string{"pwd"},
				Platform:  &repb.Platform{Properties: props("container-image", image)},
			}},
		},
		{
			name: "replaces every existing value",
			task: commandTask(props("Container-Image", "none", "container-image", "", "recycle-runner", "true")...),
			want: commandTask(props("Container-Image", image, "container-image", image, "recycle-runner", "true")...),
		},
		{
			name: "edits action platform when set",
			task: &repb.ExecutionTask{
				Action:  &repb.Action{Platform: &repb.Platform{Properties: props("recycle-runner", "true")}},
				Command: &repb.Command{Platform: &repb.Platform{Properties: props("recycle-runner", "true")}},
			},
			want: &repb.ExecutionTask{
				Action:  &repb.Action{Platform: &repb.Platform{Properties: props("container-image", image, "recycle-runner", "true")}},
				Command: &repb.Command{Platform: &repb.Platform{Properties: props("recycle-runner", "true")}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			platform.SetContainerImage(tc.task, image)
			require.Empty(t, cmp.Diff(tc.want, tc.task, protocmp.Transform()))
		})
	}
}
