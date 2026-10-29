package platform_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const defaultImage = "gcr.io/example/default:v1"

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

func TestSetDefaultContainerImage(t *testing.T) {
	for _, tc := range []struct {
		name          string
		task          *repb.ExecutionTask
		isolationType platform.ContainerType
		defaultImage  string
		want          *repb.ExecutionTask
	}{
		{
			name:          "explicit image",
			task:          commandTask(props("container-image", "docker://other", "OSFamily", "linux")...),
			isolationType: platform.OCIContainerType,
			defaultImage:  defaultImage,
			want:          commandTask(props("container-image", "docker://other", "OSFamily", "linux")...),
		},
		{
			name: "explicit image in header override",
			task: &repb.ExecutionTask{
				Command:           &repb.Command{Platform: &repb.Platform{Properties: props("OSFamily", "linux")}},
				PlatformOverrides: &repb.Platform{Properties: props("container-image", "docker://other")},
			},
			isolationType: platform.OCIContainerType,
			defaultImage:  defaultImage,
			want: &repb.ExecutionTask{
				Command:           &repb.Command{Platform: &repb.Platform{Properties: props("OSFamily", "linux")}},
				PlatformOverrides: &repb.Platform{Properties: props("container-image", "docker://other")},
			},
		},
		{
			name:          "bare isolation",
			task:          commandTask(props("OSFamily", "linux")...),
			isolationType: platform.BareContainerType,
			defaultImage:  defaultImage,
			want:          commandTask(props("OSFamily", "linux")...),
		},
		{
			name:          "empty default image",
			task:          commandTask(props("OSFamily", "linux")...),
			isolationType: platform.OCIContainerType,
			defaultImage:  "",
			want:          commandTask(props("OSFamily", "linux")...),
		},
		{
			name:          "inserts in sorted position",
			task:          commandTask(props("Arch", "amd64", "OSFamily", "linux", "recycle-runner", "true")...),
			isolationType: platform.FirecrackerContainerType,
			defaultImage:  defaultImage,
			want:          commandTask(props("Arch", "amd64", "OSFamily", "linux", "container-image", "docker://"+defaultImage, "recycle-runner", "true")...),
		},
		{
			name:          "appends when last",
			task:          commandTask(props("OSFamily", "linux")...),
			isolationType: platform.OCIContainerType,
			defaultImage:  defaultImage,
			want:          commandTask(props("OSFamily", "linux", "container-image", "docker://"+defaultImage)...),
		},
		{
			name:          "replaces none in place",
			task:          commandTask(props("recycle-runner", "true", "Container-Image", "NONE")...),
			isolationType: platform.OCIContainerType,
			defaultImage:  defaultImage,
			want:          commandTask(props("recycle-runner", "true", "Container-Image", "docker://"+defaultImage)...),
		},
		{
			name:          "replaces empty in place",
			task:          commandTask(props("container-image", "", "recycle-runner", "true")...),
			isolationType: platform.OCIContainerType,
			defaultImage:  defaultImage,
			want:          commandTask(props("container-image", "docker://"+defaultImage, "recycle-runner", "true")...),
		},
		{
			name:          "keeps existing docker prefix",
			task:          commandTask(),
			isolationType: platform.OCIContainerType,
			defaultImage:  "docker://" + defaultImage,
			want:          commandTask(props("container-image", "docker://"+defaultImage)...),
		},
		{
			name:          "creates command platform",
			task:          &repb.ExecutionTask{Command: &repb.Command{Arguments: []string{"pwd"}}},
			isolationType: platform.OCIContainerType,
			defaultImage:  defaultImage,
			want: &repb.ExecutionTask{Command: &repb.Command{
				Arguments: []string{"pwd"},
				Platform:  &repb.Platform{Properties: props("container-image", "docker://"+defaultImage)},
			}},
		},
		{
			name: "edits action platform when set",
			task: &repb.ExecutionTask{
				Action:  &repb.Action{Platform: &repb.Platform{Properties: props("recycle-runner", "true")}},
				Command: &repb.Command{Platform: &repb.Platform{Properties: props("recycle-runner", "true")}},
			},
			isolationType: platform.OCIContainerType,
			defaultImage:  defaultImage,
			want: &repb.ExecutionTask{
				Action:  &repb.Action{Platform: &repb.Platform{Properties: props("container-image", "docker://"+defaultImage, "recycle-runner", "true")}},
				Command: &repb.Command{Platform: &repb.Platform{Properties: props("recycle-runner", "true")}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			platform.SetDefaultContainerImage(tc.task, tc.isolationType, tc.defaultImage)
			require.Empty(t, cmp.Diff(tc.want, tc.task, protocmp.Transform()))
		})
	}
}

func TestSetDefaultContainerImage_MatchesExplicitDefault(t *testing.T) {
	implicit := commandTask(props("OSFamily", "linux", "recycle-runner", "true")...)
	explicit := commandTask(props("OSFamily", "linux", "container-image", "docker://"+defaultImage, "recycle-runner", "true")...)

	platform.SetDefaultContainerImage(implicit, platform.FirecrackerContainerType, defaultImage)

	require.Empty(t, cmp.Diff(
		platform.GetProto(explicit.GetAction(), explicit.GetCommand()),
		platform.GetProto(implicit.GetAction(), implicit.GetCommand()),
		protocmp.Transform()))
}
