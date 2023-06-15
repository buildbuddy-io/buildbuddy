package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

const (
	getconfBinary     = "/usr/bin/getconf"
	sandboxExecBinary = "/usr/bin/sandbox-exec"
)

var (
	supportedOnce       sync.Once
	sandboxingSupported bool

	alwaysWritableDirsOnce sync.Once
	alwaysWritableDirs     []sbxPath
)

// This functionality was inspired by DarwinSandboxedSpawnRunner.java (in bazel) and
// Sandbox.swift (in apple/swift-package-manager).

type sbxPathType int

const (
	// Three different modes are supported:
	// 1) regular expression
	// 2) literal
	// 3) subpath
	regexPath sbxPathType = iota
	literalPath
	subPath
)

// sbxPath represents a path and the associated sandbox path type. Calling
// String() on a sbxPath will return a tinyScheme sandbox statement that can
// be used in a sandbox config.
type sbxPath struct {
	expression string
	pathType   sbxPathType
}

func (s *sbxPath) typeString() string {
	switch s.pathType {
	case regexPath:
		return "regex"
	case literalPath:
		return "literal"
	case subPath:
		return "subpath"
	default:
		return "unknown-sbx-path-type"
	}
}

func (s *sbxPath) String() string {
	return fmt.Sprintf(`(%s "%s")`, s.typeString(), s.expression)
}

func NewSubPath(expr string) sbxPath {
	return sbxPath{expression: expr, pathType: subPath}
}

func NewRegexPath(expr string) sbxPath {
	return sbxPath{expression: expr, pathType: regexPath}
}

func runSimpleCommand(ctx context.Context, command []string) *interfaces.CommandResult {
	return commandutil.Run(ctx, &repb.Command{Arguments: command}, "" /*=workDir*/, nil /*=statsListener*/, &container.Stdio{})
}

func computeSandboxingSupported(ctx context.Context) bool {
	if runtime.GOOS != "darwin" {
		return false
	}
	command := []string{
		sandboxExecBinary,
		"-p",
		"(version 1) (allow default)",
		"/usr/bin/true",
	}
	result := runSimpleCommand(ctx, command)
	return result.ExitCode == 0 && result.Error == nil
}

func getConfString(ctx context.Context, confVar string) (string, error) {
	command := []string{getconfBinary, confVar}
	result := runSimpleCommand(ctx, command)
	if result.Error != nil {
		return "", result.Error
	}
	return strings.TrimSpace(string(result.Stdout)), nil
}

func resolveSymlink(path string, fileInfo os.FileInfo) (string, error) {
	if fileInfo.Mode()&os.ModeSymlink != 0 {
		linkPath, err := os.Readlink(path)
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(linkPath) {
			linkPath = filepath.Join(filepath.Dir(path), linkPath)
		}
		return linkPath, nil
	}
	return path, nil
}

func resolveAlwaysWritableDirs(ctx context.Context) ([]sbxPath, error) {
	dirSet := make(map[string]struct{}, 0)
	dirs := []string{
		"/dev",
		"/tmp",
		"/private/tmp",
		"/private/var/tmp",
	}

	// On macOS, processes may write to not only $TMPDIR but also to two
	// other temporary directories. We have to get their location by calling
	// "getconf".
	darwinUserTmp, err := getConfString(ctx, "DARWIN_USER_TEMP_DIR")
	if err != nil {
		return nil, err
	}
	darwinUserCache, err := getConfString(ctx, "DARWIN_USER_CACHE_DIR")
	if err != nil {
		return nil, err
	}
	// These directories seem identical to their "/private" prefixed
	// counterparts, but sandboxed commands will fail to write if they do
	// not begin with "/private".
	const privatePrefix = "/private"
	if !strings.HasPrefix(darwinUserTmp, privatePrefix) {
		darwinUserTmp = privatePrefix + darwinUserTmp
	}
	if !strings.HasPrefix(darwinUserCache, privatePrefix) {
		darwinUserCache = privatePrefix + darwinUserCache
	}
	dirs = append(dirs, darwinUserTmp)
	dirs = append(dirs, darwinUserCache)

	// Add some user specific dirs as well.
	homeDir := "/"
	if h, err := os.UserHomeDir(); err == nil {
		homeDir = h
	}
	dirs = append(dirs, filepath.Join(homeDir, "Library/Caches"))
	dirs = append(dirs, filepath.Join(homeDir, "Library/Logs"))
	dirs = append(dirs, filepath.Join(homeDir, "Library/Developer"))

	for _, path := range dirs {
		if strings.HasSuffix(path, "/") {
			path = path[:len(path)-1]
		}
		// If the path exists...
		if fileInfo, err := os.Lstat(path); err == nil {
			absPath, err := resolveSymlink(path, fileInfo)
			if err != nil {
				return nil, err
			}
			dirSet[absPath] = struct{}{}
		}
	}

	alwaysWritable := make([]sbxPath, 0, len(dirSet))
	for path := range dirSet {
		alwaysWritable = append(alwaysWritable, NewSubPath(path))
	}
	sort.Slice(alwaysWritable, func(i, j int) bool {
		return alwaysWritable[i].String() < alwaysWritable[j].String()
	})
	return alwaysWritable, nil
}

// See https://reverse.put.as/wp-content/uploads/2011/09/Apple-Sandbox-Guide-v1.0.pdf
// for docs on how sandboxing works.
func makeSandboxConfig(writeable, inaccessible []sbxPath, enableNetworking bool) []byte {
	buf := &bytes.Buffer{}
	buf.WriteString("(version 1)\n")
	buf.WriteString("(debug deny)\n")
	buf.WriteString("(allow default)\n")
	buf.WriteString("(allow process-exec (with no-sandbox) (literal \"/bin/ps\"))\n")

	if !enableNetworking {
		buf.WriteString("(deny network*)\n")
		buf.WriteString("(allow network-inbound (local ip \"localhost:*\"))\n")
		buf.WriteString("(allow network* (remote ip \"localhost:*\"))\n")
		buf.WriteString("(allow network* (remote unix-socket))\n")
	}

	// Disallow reads from inaccessible paths.
	buf.WriteString("(deny file-read*\n")
	for _, path := range inaccessible {
		buf.WriteString("    " + path.String() + "\n")
	}
	buf.WriteString(")\n")

	// But allow reads for any of those paths found in writeable
	buf.WriteString("(allow file-read*\n")
	for _, denied := range inaccessible {
		for _, path := range writeable {
			if !strings.HasPrefix(path.expression, denied.expression) {
				continue
			}
			buf.WriteString("    " + path.String() + "\n")
		}
	}
	buf.WriteString(")\n")

	// By default, deny all writes.
	buf.WriteString("(deny file-write*)\n")

	// But allow writes to our writeablePaths.
	buf.WriteString("(allow file-write*\n")
	for _, path := range writeable {
		buf.WriteString("    " + path.String() + "\n")
	}
	buf.WriteString(")\n")

	return buf.Bytes()
}

func sandboxPrereqs(ctx context.Context) error {
	supportedOnce.Do(func() {
		sandboxingSupported = computeSandboxingSupported(ctx)
	})
	if !sandboxingSupported {
		return status.FailedPreconditionError("sandboxing is not supported")
	}

	alwaysWritableDirsOnce.Do(func() {
		if dirs, err := resolveAlwaysWritableDirs(ctx); err == nil {
			alwaysWritableDirs = dirs
		} else {
			log.Warningf("Error resolving always writable dirs: %s", err)
		}
	})
	if len(alwaysWritableDirs) == 0 {
		return status.InternalError("error initializing sandbox")
	}
	return nil
}

// Options contains configuration options for the sandbox runner.
type Options struct {
	Network string
}

// sandbox executes commands using /usr/bin/sandbox-exec. This
// functionality is only supported on mac os.
type sandbox struct {
	WorkDir       string
	enableNetwork bool
}

func New(options *Options) container.CommandContainer {
	return &sandbox{
		enableNetwork: strings.ToLower(options.Network) != "off",
	}
}

func (c *sandbox) runCmdInSandbox(ctx context.Context, command *repb.Command, workDir string, stdio *container.Stdio) *interfaces.CommandResult {
	result := &interfaces.CommandResult{
		CommandDebugString: fmt.Sprintf("(sandbox) %s", command.GetArguments()),
		ExitCode:           commandutil.NoExitCode,
	}

	if err := sandboxPrereqs(ctx); err != nil {
		result.Error = err
		return result
	}

	// filepath.Dir will return the parent dir if the arg does not contain a
	// trailing slash, so make sure it does not!
	inaccessiblePaths := []sbxPath{NewSubPath(filepath.Dir(strings.TrimRight(workDir, "/")))}
	writablePaths := append(alwaysWritableDirs, NewSubPath(workDir))

	sandboxConfigPath := filepath.Join(workDir, "sandbox.sb")
	sandboxConfigBuf := makeSandboxConfig(writablePaths, inaccessiblePaths, c.enableNetwork)
	if err := os.WriteFile(sandboxConfigPath, sandboxConfigBuf, 0660); err != nil {
		result.Error = err
		return result
	}

	sandboxCmd := proto.Clone(command).(*repb.Command)
	sandboxCmd.Arguments = append([]string{sandboxExecBinary, "-f", sandboxConfigPath}, command.Arguments...)
	result = commandutil.Run(ctx, sandboxCmd, workDir, nil /*=statsListener*/, stdio)
	return result
}

func (c *sandbox) Run(ctx context.Context, command *repb.Command, workDir string, _ container.PullCredentials) *interfaces.CommandResult {
	return c.runCmdInSandbox(ctx, command, workDir, &container.Stdio{})
}

func (c *sandbox) Create(ctx context.Context, workDir string) error {
	c.WorkDir = workDir
	return nil
}

func (c *sandbox) Exec(ctx context.Context, cmd *repb.Command, stdio *container.Stdio) *interfaces.CommandResult {
	return c.runCmdInSandbox(ctx, cmd, c.WorkDir, stdio)
}

func (c *sandbox) IsImageCached(ctx context.Context) (bool, error)                      { return false, nil }
func (c *sandbox) PullImage(ctx context.Context, creds container.PullCredentials) error { return nil }
func (c *sandbox) Start(ctx context.Context) error                                      { return nil }
func (c *sandbox) Remove(ctx context.Context) error                                     { return nil }
func (c *sandbox) Pause(ctx context.Context) error                                      { return nil }
func (c *sandbox) Unpause(ctx context.Context) error                                    { return nil }
func (c *sandbox) Stats(ctx context.Context) (*repb.UsageStats, error) {
	return nil, nil
}
func (c *sandbox) State(ctx context.Context) (*rnpb.ContainerState, error) {
	return nil, status.UnimplementedError("not implemented")
}
