package workspace

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	wspb "github.com/buildbuddy-io/buildbuddy/proto/workspace"
)

var (
	enabled      = flag.Bool("workspace.enabled", false, "If true, enable workspaces.")
	useBlobstore = flag.Bool("workspace.use_blobstore", true, "If true, use blobstore to store workspaces. Otherwise the cache will be used")
)

type workspaceService struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if *enabled {
		env.SetWorkspaceService(New(env))
	}
	return nil
}

func New(env environment.Env) *workspaceService {
	return &workspaceService{
		env: env,
	}
}

func (s *workspaceService) GetWorkspace(ctx context.Context, req *wspb.GetWorkspaceRequest) (*wspb.GetWorkspaceResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InternalErrorf("authenticated user's group ID is empty")
	}
	ws, err := s.getWorkspace(ctx, u.GetGroupID(), req.GetName())
	if err != nil {
		return nil, err
	}
	return &wspb.GetWorkspaceResponse{
		Workspace: ws,
	}, nil
}

func (s *workspaceService) getWorkspace(ctx context.Context, groupID, name string) (*wspb.Workspace, error) {
	var workspaceBytes []byte
	if *useBlobstore {
		b, err := s.env.GetBlobstore().ReadBlob(ctx, workspaceMetadataPath(groupID, name))
		if err != nil {
			return nil, err
		}
		workspaceBytes = b
	} else {
		b, err := s.env.GetCache().Get(ctx, resourceNameForWorkspace(name))
		if err != nil {
			return nil, err
		}
		workspaceBytes = b
	}
	var ws wspb.Workspace
	err := proto.Unmarshal(workspaceBytes, &ws)
	if err != nil {
		return nil, err
	}
	return &ws, nil
}

func (s *workspaceService) SaveWorkspace(ctx context.Context, req *wspb.SaveWorkspaceRequest) (*wspb.SaveWorkspaceResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InternalErrorf("authenticated user's group ID is empty")
	}

	ws := req.GetWorkspace()

	for _, change := range ws.GetChanges() {
		if change.Content != nil {
			blobPath := workspaceFilePath(u.GetGroupID(), req.GetWorkspace(), change.GetPath())

			change.Sha = calculateBlobSha(change.Content)

			if *useBlobstore {
				// TODO(siggisim): Consider putting a TTL on these (though the volume should be pretty low). Would need
				// to add support for TTLs to the blobstore API.
				_, err = s.env.GetBlobstore().WriteBlob(ctx, blobPath, change.Content)
				if err != nil {
					return nil, err
				}
			} else {
				if err := s.env.GetCache().Set(ctx, resourceNameForNode(req.GetWorkspace().GetName(), change), change.Content); err != nil {
					return nil, err
				}
			}
		}

		change.Content = nil // Clear out file content since we wrote it in a separate blob
	}

	protoBytes, err := proto.Marshal(ws)
	if err != nil {
		return nil, err
	}

	if *useBlobstore {
		if _, err := s.env.GetBlobstore().WriteBlob(ctx, workspaceMetadataPath(u.GetGroupID(), ws.GetName()), protoBytes); err != nil {
			return nil, err
		}
	} else {
		if s.env.GetCache().Set(ctx, resourceNameForWorkspace(ws.GetName()), protoBytes) != nil {
			return nil, err
		}
	}
	return &wspb.SaveWorkspaceResponse{Workspace: req.GetWorkspace().GetName()}, nil
}

func (s *workspaceService) GetWorkspaceFile(ctx context.Context, req *wspb.GetWorkspaceFileRequest) (*wspb.GetWorkspaceFileResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InternalErrorf("authenticated user's group ID is empty")
	}

	if req.GetFile().GetOriginalSha() != "" && req.GetFile().GetSha() == req.GetFile().GetOriginalSha() {
		return s.getFileFromGitHub(ctx, req.GetRepo().GetRepoUrl(), req.GetRepo().GetCommitSha(), req.GetFile())
	}

	ws, err := s.getWorkspace(ctx, u.GetGroupID(), req.GetWorkspace())
	if err == nil {
		for _, change := range ws.GetChanges() {
			if req.GetFile().GetPath() != change.GetPath() {
				continue
			}
			return s.getFile(ctx, u.GetGroupID(), ws, change)
		}
	}

	// TODO(siggisim): Fire off background request to upload github repo to cache (under SHA1) on page load, and check
	// there first for faster performance.

	return s.getFileFromGitHub(ctx, req.GetRepo().GetRepoUrl(), req.GetRepo().GetCommitSha(), req.GetFile())
}

func (s *workspaceService) getFileFromGitHub(ctx context.Context, repo, commit string, file *wspb.Node) (*wspb.GetWorkspaceFileResponse, error) {
	if file.GetSha() != "" {
		return s.getGithubFileFromSha(ctx, repo, file.GetPath(), file.GetSha())
	}

	return s.getGithubFileFromPath(ctx, repo, commit, file.GetPath())
}

func (s *workspaceService) getFile(ctx context.Context, groupID string, workspace *wspb.Workspace, file *wspb.Node) (*wspb.GetWorkspaceFileResponse, error) {
	if *useBlobstore {
		blobPath := workspaceFilePath(groupID, workspace, file.GetPath())
		content, err := s.env.GetBlobstore().ReadBlob(ctx, blobPath)
		if err != nil {
			return &wspb.GetWorkspaceFileResponse{
				File: file,
			}, nil
		}
		file.Content = content
		return &wspb.GetWorkspaceFileResponse{
			File: file,
		}, nil
	}

	content, err := s.env.GetCache().Get(ctx, resourceNameForNode(workspace.GetName(), file))
	if err != nil {
		return &wspb.GetWorkspaceFileResponse{
			File: file,
		}, nil
	}
	file.Content = content
	return &wspb.GetWorkspaceFileResponse{
		File: file,
	}, nil
}

func (s *workspaceService) GetWorkspaceDirectory(ctx context.Context, req *wspb.GetWorkspaceDirectoryRequest) (*wspb.GetWorkspaceDirectoryResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InternalErrorf("authenticated user's group ID is empty")
	}

	directory := req.GetDirectory()

	ref := directory.GetSha()
	if directory.GetPath() == "" && ref == "" {
		ref = req.GetRepo().GetCommitSha()
		if ref == "" {
			ref = req.GetRepo().GetBranch()
		}
	}

	nodes := []*wspb.Node{}
	sha := directory.GetSha()
	if ref != "" {
		nodes, sha, err = s.nodesFromGitHub(ctx, req.GetRepo().GetRepoUrl(), ref)
		if err != nil {
			return nil, err
		}
	}
	directory.Sha = sha

	res := &wspb.GetWorkspaceDirectoryResponse{
		Directory:  directory,
		ChildNodes: nodes,
	}

	ws, err := s.getWorkspace(ctx, u.GetGroupID(), req.GetWorkspace())
	if err != nil {
		return res, nil
	}

	res.ChildNodes = s.addNodesFromWorkspace(res.ChildNodes, ws, directory.GetPath())

	return res, nil
}

func (s *workspaceService) addNodesFromWorkspace(nodes []*wspb.Node, ws *wspb.Workspace, requestedPath string) []*wspb.Node {
	for _, change := range ws.GetChanges() {
		requestedPathParts := strings.Split(requestedPath, "/")
		if requestedPath == "" {
			requestedPathParts = []string{}
		}
		pathParts := strings.Split(change.GetPath(), "/")
		if len(requestedPathParts) > len(pathParts) {
			continue
		}
		match := true
		for i := range requestedPathParts {
			if requestedPathParts[i] != pathParts[i] {
				match = false
			}
		}
		if !match {
			continue
		}

		if len(requestedPathParts) != len(pathParts)-1 {
			if pathExists(pathParts[len(requestedPathParts)], nodes) {
				continue
			}
			nodes = append(nodes, &wspb.Node{
				Path:       pathParts[len(requestedPathParts)],
				NodeType:   wspb.NodeType_DIRECTORY,
				ChangeType: wspb.ChangeType_ADDED,
			})
			continue
		}

		change.NodeType = wspb.NodeType_FILE
		change.Path = pathParts[len(pathParts)-1]

		if change.ChangeType == wspb.ChangeType_ADDED {
			nodes = append(nodes, change)
		}

		if change.ChangeType == wspb.ChangeType_DELETED {
			for i, node := range nodes {
				if node.Path != pathParts[len(requestedPathParts)] {
					continue
				}
				nodes = append(nodes[:i], nodes[i+1:]...)
			}
		}
	}
	return nodes
}

func pathExists(path string, nodes []*wspb.Node) bool {
	for _, node := range nodes {
		if node.Path != path {
			continue
		}
		return true
	}
	return false
}

func (s *workspaceService) nodesFromGitHub(ctx context.Context, githubRepo, ref string) ([]*wspb.Node, string, error) {
	gh := s.env.GetGitHubAppService()
	if gh == nil {
		return nil, "", status.UnimplementedError("No GitHub app configured")
	}
	a, err := gh.GetGitHubApp(ctx)
	if err != nil {
		return nil, "", err
	}

	repo, err := git.ParseGitHubRepoURL(githubRepo)
	if err != nil {
		return nil, "", err
	}

	ghReq := &ghpb.GetGithubTreeRequest{
		Owner: repo.Owner,
		Repo:  repo.Repo,
		Ref:   ref,
	}

	ghRes, err := a.GetGithubTree(ctx, ghReq)
	if err != nil {
		return nil, "", err
	}

	nodes := []*wspb.Node{}
	for _, node := range ghRes.GetNodes() {
		nodeType := wspb.NodeType_FILE
		if node.Type == "tree" {
			nodeType = wspb.NodeType_DIRECTORY
		}
		nodes = append(nodes, &wspb.Node{
			Path:     node.Path,
			Sha:      node.Sha,
			NodeType: nodeType,
		})
	}

	return nodes, ghRes.Sha, nil
}

// Blobstore

func workspaceMetadataPath(groupID, name string) string {
	return filepath.Join("workspace", groupID, filepath.Clean("/"+name), "metadata")
}

func workspaceFilePath(groupID string, workspace *wspb.Workspace, path string) string {
	return filepath.Join("workspace", groupID, filepath.Clean("/"+workspace.GetName()), "file", filepath.Clean("/"+path))
}

// Cache

func resourceNameForWorkspace(workspaceName string) *rspb.ResourceName {
	d := &repb.Digest{
		Hash:      hash.String("workspace"),
		SizeBytes: 1,
	}
	return digest.NewResourceName(d, workspaceName, rspb.CacheType_AC, repb.DigestFunction_SHA1).ToProto()
}

func resourceNameForNode(workspaceName string, node *wspb.Node) *rspb.ResourceName {
	d := &repb.Digest{
		Hash:      node.Sha,
		SizeBytes: 1,
	}
	return digest.NewResourceName(d, workspaceName, rspb.CacheType_CAS, repb.DigestFunction_SHA1).ToProto()
}

// Github

func (s *workspaceService) getGithubFileFromSha(ctx context.Context, githubRepo, path, sha string) (*wspb.GetWorkspaceFileResponse, error) {
	gh := s.env.GetGitHubAppService()
	if gh == nil {
		return nil, status.UnimplementedError("No GitHub app configured")
	}
	a, err := gh.GetGitHubApp(ctx)
	if err != nil {
		return nil, err
	}

	repo, err := git.ParseGitHubRepoURL(githubRepo)
	if err != nil {
		return nil, err
	}

	ghReq := &ghpb.GetGithubBlobRequest{
		Owner: repo.Owner,
		Repo:  repo.Repo,
		Sha:   sha,
	}

	ghRes, err := a.GetGithubBlob(ctx, ghReq)
	if err != nil {
		return nil, err
	}
	return &wspb.GetWorkspaceFileResponse{
		File: &wspb.Node{
			Path:        path,
			NodeType:    wspb.NodeType_FILE,
			ChangeType:  wspb.ChangeType_UNCHANGED,
			Content:     ghRes.Content,
			Sha:         sha,
			OriginalSha: sha,
		},
	}, nil
}

func (s *workspaceService) getGithubFileFromPath(ctx context.Context, githubRepo, ref, path string) (*wspb.GetWorkspaceFileResponse, error) {
	gh := s.env.GetGitHubAppService()
	if gh == nil {
		return nil, status.UnimplementedError("No GitHub app configured")
	}
	a, err := gh.GetGitHubApp(ctx)
	if err != nil {
		return nil, err
	}

	repo, err := git.ParseGitHubRepoURL(githubRepo)
	if err != nil {
		return nil, err
	}

	ghReq := &ghpb.GetGithubContentRequest{
		Owner: repo.Owner,
		Repo:  repo.Repo,
		Path:  path,
		Ref:   ref,
	}

	ghRes, err := a.GetGithubContent(ctx, ghReq)
	if err != nil {
		return nil, err
	}

	sha := calculateBlobSha(ghRes.Content)

	return &wspb.GetWorkspaceFileResponse{
		File: &wspb.Node{
			Path:        path,
			NodeType:    wspb.NodeType_FILE,
			ChangeType:  wspb.ChangeType_UNCHANGED,
			Content:     ghRes.Content,
			Sha:         sha,
			OriginalSha: sha,
		},
	}, nil
}

func calculateBlobSha(content []byte) string {
	h := sha1.New()
	h.Write([]byte(fmt.Sprintf("blob %d\x00", len(content))))
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}
