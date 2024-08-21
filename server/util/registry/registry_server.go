package registry

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/docker/go-units"

	regpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

type SizeableConstraint[T any] interface {
	*T
	Sizeable
}

type Sizeable interface {
	Size() uint64
}

type SchemaDiscriminant struct {
	SchemaVersion int    `json:"schemaVersion"`
	MediaType     string `json:"mediaType"`
}

type DockerImageManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	MediaType     string `json:"mediaType"`
	Config        struct {
		MediaType string `json:"mediaType"`
		Size      int    `json:"size"`
		Digest    string `json:"digest"`
	} `json:"config"`
	Layers []struct {
		MediaType string   `json:"mediaType"`
		Size      int      `json:"size"`
		Digest    string   `json:"digest"`
		URLs      []string `json:"urls"`
	} `json:"layers"`
}

func (m *DockerImageManifest) Size() uint64 {
	size := uint64(0)
	for _, l := range m.Layers {
		size += uint64(l.Size)
	}
	return size
}

type DockerPlatformManifest struct {
	MediaType string `json:"mediaType"`
	Size      int    `json:"size"`
	Digest    string `json:"digest"`
	Platform  struct {
		Architecture string   `json:"architecture"`
		OS           string   `json:"os"`
		OSVersion    string   `json:"os.version"`
		OSFeatures   []string `json:"os.features"`
		Variant      string   `json:"variant"`
		Features     string   `json:"features"`
	} `json:"platform"`
}

type DockerImageManifestList struct {
	SchemaVersion int                      `json:"schemaVersion"`
	MediaType     string                   `json:"mediaType"`
	Manifests     []DockerPlatformManifest `json:"manifests"`
}

type OCIDescriptor struct {
	MediaType    string            `json:"mediaType"`
	Digest       string            `json:"digest"`
	Size         int               `json:"size"`
	URLs         []string          `json:"urls"`
	Annotations  map[string]string `json:"annotations"`
	Data         string            `json:"data"`
	ArtifactType string            `json:"artifactType"`
	Platform     struct {
		Architecture string   `json:"architecture"`
		OS           string   `json:"os"`
		OSVersion    string   `json:"os.version"`
		OSFeatures   []string `json:"os.features"`
		Variant      string   `json:"variant"`
		Features     string   `json:"features"`
	} `json:"platform"`
}

type OCIImageManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	MediaType     string `json:"mediaType"`
	ArtifactType  string `json:"artifactType"`
	Config        struct {
		MediaType string `json:"mediaType"`
	} `json:"config"`
	Layers      []OCIDescriptor   `json:"layers"`
	Subject     *OCIDescriptor    `json:"subject"`
	Annotations map[string]string `json:"annotations"`
}

func (m *OCIImageManifest) Size() uint64 {
	size := uint64(0)
	for _, l := range m.Layers {
		size += uint64(l.Size)
	}
	return size
}

type OCIImageIndex struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	ArtifactType  string            `json:"artifactType"`
	Manifests     []OCIDescriptor   `json:"manifests"`
	Subject       *OCIDescriptor    `json:"subject"`
	Annotations   map[string]string `json:"annotations"`
}

type LocalResponseWriter struct {
	*bytes.Buffer
	header      http.Header
	statusCodes []int
}

func NewLocalResponseWriter() *LocalResponseWriter {
	return &LocalResponseWriter{
		Buffer:      bytes.NewBuffer([]byte{}),
		header:      map[string][]string{},
		statusCodes: []int{},
	}
}

func (l *LocalResponseWriter) Header() http.Header {
	return l.header
}

func (l *LocalResponseWriter) WriteHeader(statusCode int) {
	if statusCode > 99 && statusCode < 200 {
		l.header = map[string][]string{}
	}
	l.statusCodes = append(l.statusCodes, statusCode)
}

func GetResponse(handler http.Handler, method string, requestURI string, headers http.Header) ([]byte, http.Header, []int) {
	resp := NewLocalResponseWriter()
	handler.ServeHTTP(
		resp,
		&http.Request{
			URL:        &url.URL{Path: requestURI},
			Method:     method,
			RequestURI: requestURI,
			Header:     headers,
		},
	)

	return resp.Buffer.Bytes(), resp.header, resp.statusCodes
}

func As[T any](b []byte) (*T, error) {
	t := [1]T{}
	if err := json.Unmarshal(b, &t[0]); err != nil {
		return nil, err
	}
	return &t[0], nil
}

type RegistryService struct {
	env   environment.Env
	cache interfaces.Cache
}

func Register(realEnv *real_environment.RealEnv) error {
	realEnv.SetCtrRegistryService(NewRegistryService(realEnv))
	return nil
}

func NewRegistryService(env environment.Env) *RegistryService {
	return &RegistryService{env: env, cache: env.GetCache()}
}

// TODO(iain): a lot of this is in common with stuff in manifest.go, factor out.
func (s *RegistryService) GetCatalog(ctx context.Context, req *regpb.GetCatalogRequest) (*regpb.GetCatalogResponse, error) {
	app := s.env.GetContainerRegistry().GetRegistryHandler().GetApp()
	b, _, _ := GetResponse(app, http.MethodGet, "/v2/_catalog", nil)
	catalog, err := As[Catalog](b)
	if err != nil {
		return nil, err
	}

	resp := regpb.GetCatalogResponse{Repository: []*regpb.Repository{}}
	for _, repo := range catalog.Repositories {
		repoProto := regpb.Repository{Name: repo, Images: []*regpb.Image{}}
		b, _, _ := GetResponse(app, http.MethodGet, "/v2/"+repo+"/tags/list", nil)
		repository, err := As[struct {
			Name string   `json:"name"`
			Tags []string `json:"tags"`
		}](b)
		if err != nil {
			return nil, err
		}
		imageProtos := map[string]*regpb.Image{}
		for _, tag := range repository.Tags {
			imageProto, err := PopulateImageProtoForTag(imageProtos, app, repo, tag)
			if err != nil {
				return nil, err
			}
			if len(imageProto.Tags) == 1 {
				repoProto.Images = append(repoProto.Images, imageProto)
			}
		}
		resp.Repository = append(resp.Repository, &repoProto)
	}
	return &resp, nil
}

func PopulateImageProtoForTag(imageProtos map[string]*regpb.Image, app http.Handler, repo string, tag string) (*regpb.Image, error) {
	b, headers, _ := GetResponse(
		app,
		http.MethodGet,
		"/v2/"+repo+"/manifests/"+tag,
		map[string][]string{
			"Accept": []string{
				"application/vnd.docker.distribution.manifest.v2+json",
				"application/vnd.docker.distribution.manifest.list.v2+json",
				"application/vnd.oci.image.manifest.v1+json",
				"application/vnd.oci.image.index.v1+json",
			},
		},
	)
	d, err := As[SchemaDiscriminant](b)
	if err != nil {
		return nil, err
	}
	log.Infof("Headers:\n%#v", headers)
	switch d.MediaType {
	case "application/vnd.oci.image.index.v1+json":
		manifestList, err := As[OCIImageIndex](b)
		if err != nil {
			return nil, err
		}
		for _, manifest := range manifestList.Manifests {
			if manifest.Platform.Architecture == "amd64" && manifest.Platform.OS == "linux" {
				if imageProto, ok := imageProtos[manifest.Digest]; ok {
					imageProto.Checkpoint = imageProto.Checkpoint || isCheckpoint([]string{tag})
					imageProto.Tags = append(imageProto.Tags, tag)
					return imageProto, nil
				} else {
					b, headers, _ := GetResponse(
						app,
						http.MethodGet,
						"/v2/"+repo+"/manifests/"+manifest.Digest,
						map[string][]string{
							"Accept": []string{
								"application/vnd.oci.image.manifest.v1+json",
							},
						},
					)
					return PopulateFromManifest[OCIImageManifest](imageProtos, repo, tag, b, headers)
				}
			}
		}
		return nil, status.InternalErrorf("No matching platform in manifest list for %s:%s. Manifest list: %#v", repo, tag, manifestList)
	case "application/vnd.oci.image.manifest.v1+json":
		return PopulateFromManifest[OCIImageManifest](imageProtos, repo, tag, b, headers)
	case "application/vnd.docker.distribution.manifest.list.v2+json":
		manifestList, err := As[DockerImageManifestList](b)
		if err != nil {
			return nil, err
		}
		for _, manifest := range manifestList.Manifests {
			if manifest.Platform.Architecture == "amd64" && manifest.Platform.OS == "linux" {
				if imageProto, ok := imageProtos[manifest.Digest]; ok {
					imageProto.Checkpoint = imageProto.Checkpoint || isCheckpoint([]string{tag})
					imageProto.Tags = append(imageProto.Tags, tag)
					return imageProto, nil
				} else {
					b, headers, _ := GetResponse(
						app,
						http.MethodGet,
						"/v2/"+repo+"/manifests/"+manifest.Digest,
						map[string][]string{
							"Accept": []string{
								"application/vnd.docker.distribution.manifest.v2+json",
							},
						},
					)
					return PopulateFromManifest[DockerImageManifest](imageProtos, repo, tag, b, headers)
				}
			}
		}
		return nil, status.InternalErrorf("No matching platform in manifest list for %s:%s. Manifest list: %#v", repo, tag, manifestList)
	case "application/vnd.docker.distribution.manifest.v2+json":
		return PopulateFromManifest[DockerImageManifest](imageProtos, repo, tag, b, headers)
	default:
		return nil, status.InternalErrorf("Response did not match any available manifest type. Manifest type provided: %s", d.MediaType)
	}
}

func PopulateFromManifest[T any, S SizeableConstraint[T]](imageProtos map[string]*regpb.Image, repo, tag string, b []byte, headers http.Header) (*regpb.Image, error) {
	var digest string
	if digests, ok := headers["Docker-Content-Digest"]; ok && len(digests) > 0 {
		digest = digests[0]
	} else {
		return nil, status.InternalErrorf("Container registry did not respond with the digest")
	}

	if imageProto, ok := imageProtos[digest]; ok {
		imageProto.Checkpoint = imageProto.Checkpoint || isCheckpoint([]string{tag})
		imageProto.Tags = append(imageProto.Tags, tag)
		return imageProto, nil
	}

	imageManifest, err := As[T](b)
	if err != nil {
		return nil, err
	}
	imageProtos[digest] = &regpb.Image{
		Repository: repo,
		Digest:     digest,
		Fullname:   hex.EncodeToString([]byte(fmt.Sprintf("%s:%s", repo, digest))),
		Tags:       []string{tag},
		Size:       units.BytesSize(float64(S(imageManifest).Size())),
		Checkpoint: isCheckpoint([]string{tag}),
	}
	return imageProtos[digest], nil
}

func (s *RegistryService) GetImage(ctx context.Context, req *regpb.GetImageRequest) (*regpb.Image, error) {
	decodedFullNameBytes, err := hex.DecodeString(req.Fullname)
	if err != nil {
		return nil, err
	}
	decodedFullName := string(decodedFullNameBytes)
	split := strings.Split(decodedFullName, ":")
	repoName := split[0]
	tag := split[len(split)-1]
	log.Debugf("searching image repository %s for tag %s", repoName, tag)

	var latestImage *Image
	repository := s.getRepo(ctx, repoName)
	for _, image := range repository.Images {

		// This is just a hack to get pulling checkpoints working. See comment
		// in enterprise/app/registry/image.tsx for more context.
		if latestImage == nil {
			latestImage = &image
		} else if !isCheckpoint(image.Tags) && image.UploadedTime.After(latestImage.UploadedTime) {
			latestImage = &image
		}

		if image.Digest == "sha256:"+tag {
			i := regpb.Image{
				Repository:     repoName,
				Digest:         image.Digest,
				Tags:           image.Tags,
				Size:           units.BytesSize(float64(image.SizeBytes)),
				UploadedTime:   image.UploadedTime.Format("2006-01-02 15:04:05"),
				LastAccessTime: image.LastAccessTime.Format("2006-01-02 15:04:05"),
				Accesses:       int64(image.Accesses),
				Checkpoint:     isCheckpoint(image.Tags),
			}
			if image.Digest != latestImage.Digest {
				i.Baseimage = fmt.Sprintf("%s:%s", repoName, latestImage.Tags[0])
			}
			return &i, nil
		}
		for _, imageTag := range image.Tags {
			if imageTag == tag {
				i := regpb.Image{
					Repository:     repoName,
					Digest:         image.Digest,
					Tags:           image.Tags,
					Size:           units.BytesSize(float64(image.SizeBytes)),
					UploadedTime:   image.UploadedTime.Format("2006-01-02 15:04:05"),
					LastAccessTime: image.LastAccessTime.Format("2006-01-02 15:04:05"),
					Accesses:       int64(image.Accesses),
					Checkpoint:     isCheckpoint(image.Tags),
				}
				if image.Digest != latestImage.Digest {
					i.Baseimage = fmt.Sprintf("%s:%s", repoName, latestImage.Tags[0])
				}
				return &i, nil
			}
		}
	}
	return &regpb.Image{}, status.NotFoundError("404 image not found!")
}

func isCheckpoint(tags []string) bool {
	for _, tag := range tags {
		if strings.HasPrefix(tag, "cr/checkpoint") {
			return true
		}
	}
	return false
}

func (s *RegistryService) repoExists(ctx context.Context, repo string) bool {
	exists, _ := s.cache.Contains(ctx, repoResourceName(repo))
	return exists
}

func (s *RegistryService) getRepo(ctx context.Context, repo string) Repository {
	if !s.repoExists(ctx, repo) {
		return Repository{}
	}
	var repository Repository
	raw, _ := s.cache.Get(ctx, repoResourceName(repo))
	if err := json.Unmarshal(raw, &repository); err != nil {
		panic(err)
	}
	return repository
}
