package repo_downloader

import (
	"context"
	"net/url"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	memory "github.com/go-git/go-git/v5/storage/memory"
)

type gitRepoDownloader struct{}

func NewRepoDownloader() *gitRepoDownloader {
	return &gitRepoDownloader{}
}

func (d *gitRepoDownloader) TestRepoAccess(ctx context.Context, repoURL, accessToken string) error {
	authURL := repoURL

	u, err := url.Parse(repoURL)
	if err == nil {
		if accessToken != "" {
			u.User = url.UserPassword(accessToken, "")
			authURL = u.String()
		}
	}

	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: repoURL,
		URLs: []string{authURL},
	})
	_, err = remote.List(&git.ListOptions{})
	return err
}
