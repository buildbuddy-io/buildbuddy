package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const (
	catalogHash = "652f55016243bf1b9f1bbea46d5749ef892dbe394e46de9d66ab1aacf0b4af57"
)

var (
	catalogResourceName = rspb.ResourceName{
		Digest: &repb.Digest{
			Hash:      catalogHash,
			SizeBytes: 1,
		},
		InstanceName:   "registry",
		Compressor:     repb.Compressor_IDENTITY,
		CacheType:      rspb.CacheType_AC,
		DigestFunction: repb.DigestFunction_SHA256,
	}
)

type Catalog struct {
	Repositories []string `json:"repositories"`
}

type Repository struct {
	Name   string  `json:"name"`
	Images []Image `json:"tags"`
}

type Image struct {
	Digest         string    `json:"tag"`
	Tags           []string  `json:"aliases"`
	SizeBytes      int       `json:"sizebytes"`
	UploadedTime   time.Time `json:"uploaded-time"`
	LastAccessTime time.Time `json:"last-access-usec"`
	Accesses       int       `json:"accesses"`
}

type Manifest struct {
	ContentType string `json:"content-type"`
	Blob        []byte `json:"blob"`
}

type manifests struct {
	env   environment.Env
	cache interfaces.Cache
}

func repoResourceName(repo string) *rspb.ResourceName {
	return &rspb.ResourceName{
		Digest: &repb.Digest{
			Hash:      hash.String(repo),
			SizeBytes: 1,
		},
		InstanceName:   "registry",
		Compressor:     repb.Compressor_IDENTITY,
		CacheType:      rspb.CacheType_AC,
		DigestFunction: repb.DigestFunction_SHA256,
	}
}

func targetResourceName(repo string, target string) *rspb.ResourceName {
	return &rspb.ResourceName{
		Digest: &repb.Digest{
			Hash:      hash.String(fmt.Sprintf("%s:%s", repo, target)),
			SizeBytes: 1,
		},
		InstanceName:   "registry",
		Compressor:     repb.Compressor_IDENTITY,
		CacheType:      rspb.CacheType_AC,
		DigestFunction: repb.DigestFunction_SHA256,
	}
}

// Catalog-level readers and writers
// ================================================================================
func (m *manifests) getCatalog(ctx context.Context) Catalog {
	containsCatalog, _ := m.cache.Contains(ctx, &catalogResourceName)
	if !containsCatalog {
		m.setCatalog(ctx, Catalog{Repositories: []string{}})
	}

	var catalog Catalog
	raw, _ := m.cache.Get(ctx, &catalogResourceName)
	if err := json.Unmarshal(raw, &catalog); err != nil {
		panic(err)
	}
	return catalog
}

func (m *manifests) setCatalog(ctx context.Context, catalog Catalog) {
	raw, err := json.Marshal(catalog)
	if err != nil {
		panic(err)
	}
	m.cache.Set(ctx, &catalogResourceName, raw)
}

// ================================================================================

// Repository-level readers and writers
// ================================================================================
func (m *manifests) repoExists(ctx context.Context, repo string) bool {
	exists, _ := m.cache.Contains(ctx, repoResourceName(repo))
	return exists
}

func (m *manifests) addRepo(ctx context.Context, repo string) {
	catalog := m.getCatalog(ctx)
	for _, existingRepo := range catalog.Repositories {
		if repo == existingRepo {
			return
		}
	}
	catalog.Repositories = append(catalog.Repositories, repo)
	sort.Strings(catalog.Repositories)
	m.setCatalog(ctx, catalog)
	m.setRepo(ctx, repo, Repository{Name: repo, Images: []Image{}})
}

func (m *manifests) getRepo(ctx context.Context, repo string) Repository {
	if !m.repoExists(ctx, repo) {
		return Repository{}
	}
	var tags Repository
	raw, _ := m.cache.Get(ctx, repoResourceName(repo))
	if err := json.Unmarshal(raw, &tags); err != nil {
		panic(err)
	}
	return tags
}

func (m *manifests) setRepo(ctx context.Context, repo string, tags Repository) {
	if !m.repoExists(ctx, repo) {
		m.addRepo(ctx, repo)
	}
	raw, err := json.Marshal(tags)
	if err != nil {
		panic(err)
	}
	m.cache.Set(ctx, repoResourceName(repo), raw)
}

// No deleter, I guess.
// ================================================================================

// Target-level readers and writers
// ================================================================================
func (m *manifests) targetExists(ctx context.Context, repo string, target string) bool {
	exists, _ := m.cache.Contains(ctx, targetResourceName(repo, target))
	return exists
}

func (m *manifests) aliasExists(ctx context.Context, repo string, alias string) bool {
	repository := m.getRepo(ctx, repo)
	for _, tag := range repository.Images {
		for _, a := range tag.Tags {
			if a == alias {
				return true
			}
		}
	}
	return false
}

// This code is not really thread-safe in the sense that it can
// read-modifies-write from multiple servers simultaneously. Oh well!
func (m *manifests) getTarget(ctx context.Context, repo string, target string) Manifest {
	if m.targetExists(ctx, repo, target) {
		var manifest Manifest
		raw, _ := m.cache.Get(ctx, targetResourceName(repo, target))
		if err := json.Unmarshal(raw, &manifest); err != nil {
			panic(err)
		}

		repository := m.getRepo(ctx, repo)
		for i, ri := range repository.Images {
			if ri.Digest == target {
				ri.LastAccessTime = time.Now()
				ri.Accesses = ri.Accesses + 1
				repository.Images[i] = ri
				fmt.Println("rewriting repository")
				fmt.Println(repository)
				m.setRepo(ctx, repo, repository)
				break
			}
		}

		return manifest
	}

	if m.aliasExists(ctx, repo, target) {
		repository := m.getRepo(ctx, repo)
		for _, tag := range repository.Images {
			for _, alias := range tag.Tags {
				if alias == target {
					return m.getTarget(ctx, repo, tag.Digest)
				}
			}
		}
	}

	return Manifest{}
}

func (m *manifests) setTarget(ctx context.Context, repo string, digest string, alias string, manifest Manifest) {
	if !m.repoExists(ctx, repo) {
		m.addRepo(ctx, repo)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		panic(err)
	}
	m.cache.Set(ctx, targetResourceName(repo, digest), raw)

	repository := m.getRepo(ctx, repo)

	// Remove other occurrences of alias.
	for _, tag := range repository.Images {
		found := false
		if found {
			break
		}
		for i, a := range tag.Tags {
			if a == alias {
				tag.Tags = removeString(tag.Tags, i)
				found = true
				break
			}
		}
	}

	// I think this logic is wrong for manifest lists or whatever they're
	// called. Just don't demo one of those.
	sizeBytes := 0
	// It is hack week, my friends.
	fmt.Println(string(manifest.Blob))
	split := strings.Split(string(manifest.Blob), "\"size\":")
	if len(split) > 1 {
		split = split[1:]
		for _, piece := range split {
			num := strings.Split(piece, ",")[0]
			size, err := strconv.Atoi(num)
			if err == nil {
				sizeBytes = sizeBytes + size
			} else {
				// whoops, probably the "size" field was at the end of a json message
				num = strings.Split(piece, "}")[0]
				size, err = strconv.Atoi(num)
				if err == nil {
					sizeBytes = sizeBytes + size
				} else {
					// yeesh
					panic("error parsing size from manifest, dying!")
				}
			}
		}
	}

	image := Image{
		Digest:         digest,
		SizeBytes:      sizeBytes,
		UploadedTime:   time.Now(),
		LastAccessTime: time.Now(),
		Accesses:       0,
	}

	// Check if this image already exists and update it in-place if so.
	for _, otherImage := range repository.Images {
		if image.Digest == otherImage.Digest {
			otherImage.UploadedTime = time.Now()
			otherImage.LastAccessTime = time.Now()
			m.setRepo(ctx, repo, repository)
			return
		}
	}

	image.Tags = uniqueify(append(image.Tags, alias))
	repository.Images = append(repository.Images, image)
	m.setRepo(ctx, repo, repository)
}

func (m *manifests) deleteTarget(ctx context.Context, repo string, target string) {
	if m.targetExists(ctx, repo, target) {
		m.cache.Delete(ctx, targetResourceName(repo, target))
		return
	}

	// This is an alias. Time to hunt it down and exterminate it.
	repository := m.getRepo(ctx, repo)
	for _, tag := range repository.Images {
		for i, alias := range tag.Tags {
			if alias == target {
				tag.Tags = removeString(tag.Tags, i)
				break
			}
		}
	}
	m.setRepo(ctx, repo, repository)
}

func removeString(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func removeTag(slice []Image, s int) []Image {
	return append(slice[:s], slice[s+1:]...)
}

func uniqueify(s []string) []string {
	unique := map[string]struct{}{}
	for _, sss := range s {
		unique[sss] = struct{}{}
	}
	uniqueified := []string{}
	for sss := range unique {
		uniqueified = append(uniqueified, sss)
	}
	sort.Strings(uniqueified)
	return uniqueified
}

// ================================================================================

func isManifest(req *http.Request) bool {
	elems := strings.Split(req.URL.Path, "/")
	elems = elems[1:]
	if len(elems) < 4 {
		return false
	}
	return elems[len(elems)-2] == "manifests"
}

func isTags(req *http.Request) bool {
	elems := strings.Split(req.URL.Path, "/")
	elems = elems[1:]
	if len(elems) < 4 {
		return false
	}
	return elems[len(elems)-2] == "tags"
}

func isCatalog(req *http.Request) bool {
	elems := strings.Split(req.URL.Path, "/")
	elems = elems[1:]
	if len(elems) < 2 {
		return false
	}

	return elems[len(elems)-1] == "_catalog"
}

// Returns whether this url should be handled by the referrers handler
func isReferrers(req *http.Request) bool {
	elems := strings.Split(req.URL.Path, "/")
	elems = elems[1:]
	if len(elems) < 4 {
		return false
	}
	return elems[len(elems)-2] == "referrers"
}

// https://github.com/opencontainers/distribution-spec/blob/master/spec.md#pulling-an-image-manifest
// https://github.com/opencontainers/distribution-spec/blob/master/spec.md#pushing-an-image
func (m *manifests) handle(resp http.ResponseWriter, req *http.Request) *regError {
	elem := strings.Split(req.URL.Path, "/")
	elem = elem[1:]
	target := elem[len(elem)-1]
	repo := strings.Join(elem[1:len(elem)-2], "/")
	ctx, _ := prefix.AttachUserPrefixToContext(req.Context(), m.env)

	switch req.Method {
	case http.MethodGet:
		if !m.repoExists(ctx, repo) {
			return &regError{
				Status:  http.StatusNotFound,
				Code:    "NAME_UNKNOWN",
				Message: "Unknown name1",
			}
		}
		if !m.targetExists(ctx, repo, target) && !m.aliasExists(ctx, repo, target) {
			return &regError{
				Status:  http.StatusNotFound,
				Code:    "MANIFEST_UNKNOWN",
				Message: "Unknown manifest",
			}
		}
		manifest := m.getTarget(ctx, repo, target)
		h, _, _ := v1.SHA256(bytes.NewReader(manifest.Blob))
		resp.Header().Set("Docker-Content-Digest", h.String())
		resp.Header().Set("Content-Type", manifest.ContentType)
		resp.Header().Set("Content-Length", fmt.Sprint(len(manifest.Blob)))
		resp.WriteHeader(http.StatusOK)
		io.Copy(resp, bytes.NewReader(manifest.Blob))
		return nil

	case http.MethodHead:
		exists := m.targetExists(ctx, repo, target) || m.aliasExists(ctx, repo, target)
		if !exists {
			return &regError{
				Status:  http.StatusNotFound,
				Code:    "MANIFEST_UNKNOWN",
				Message: fmt.Sprintf("Couldn't find %s:%s", repo, target),
			}
		}
		manifest := m.getTarget(ctx, repo, target)
		h, _, _ := v1.SHA256(bytes.NewReader(manifest.Blob))
		resp.Header().Set("Docker-Content-Digest", h.String())
		resp.Header().Set("Content-Type", manifest.ContentType)
		resp.Header().Set("Content-Length", fmt.Sprint(len(manifest.Blob)))
		resp.WriteHeader(http.StatusOK)
		return nil

	case http.MethodPut:
		b := &bytes.Buffer{}
		io.Copy(b, req.Body)
		h, _, _ := v1.SHA256(bytes.NewReader(b.Bytes()))
		digest := h.String()
		mf := Manifest{
			Blob:        b.Bytes(),
			ContentType: req.Header.Get("Content-Type"),
		}

		// If the manifest is a manifest list, check that the manifest
		// list's constituent manifests are already uploaded.
		// This isn't strictly required by the registry API, but some
		// registries require this.
		if types.MediaType(mf.ContentType).IsIndex() {
			if err := func() *regError {
				im, err := v1.ParseIndexManifest(b)
				if err != nil {
					return &regError{
						Status:  http.StatusBadRequest,
						Code:    "MANIFEST_INVALID",
						Message: err.Error(),
					}
				}
				for _, desc := range im.Manifests {
					if !desc.MediaType.IsDistributable() {
						continue
					}
					if desc.MediaType.IsIndex() || desc.MediaType.IsImage() {
						exists := m.targetExists(ctx, repo, desc.Digest.String())
						if !exists {
							return &regError{
								Status:  http.StatusNotFound,
								Code:    "MANIFEST_UNKNOWN",
								Message: fmt.Sprintf("Sub-manifest %q not found", desc.Digest),
							}
						}
					} else {
						// TODO: Probably want to do an existence check for blobs.
						fmt.Println("TODO: Check blobs for " + desc.Digest.String())
					}
				}
				return nil
			}(); err != nil {
				return err
			}
		}

		// Allow future references by target (tag) and immutable digest.
		// See https://docs.docker.com/engine/reference/commandline/pull/#pull-an-image-by-digest-immutable-identifier.
		alias := target
		if target == digest {
			alias = ""
		}
		m.setTarget(ctx, repo, digest, alias, mf)
		resp.Header().Set("Docker-Content-Digest", digest)
		resp.WriteHeader(http.StatusCreated)
		return nil

	case http.MethodDelete:
		if !m.targetExists(ctx, repo, target) && !m.aliasExists(ctx, repo, target) {
			return &regError{
				Status:  http.StatusNotFound,
				Code:    "MANIFEST_UNKNOWN",
				Message: "Unknown manifest",
			}
		}
		m.deleteTarget(ctx, repo, target)
		return nil

	default:
		return &regError{
			Status:  http.StatusBadRequest,
			Code:    "METHOD_UNKNOWN",
			Message: "We don't understand your method + url",
		}
	}
}

func (m *manifests) handleTags(resp http.ResponseWriter, req *http.Request) *regError {
	elem := strings.Split(req.URL.Path, "/")
	elem = elem[1:]
	repo := strings.Join(elem[1:len(elem)-2], "/")
	ctx, err := prefix.AttachUserPrefixToContext(req.Context(), m.env)
	if err != nil {
		panic(err)
	}

	if req.Method == "GET" {
		if !m.repoExists(ctx, repo) {
			return &regError{
				Status:  http.StatusNotFound,
				Code:    "NAME_UNKNOWN",
				Message: "Unknown name2",
			}
		}

		rawTags := m.getRepo(ctx, repo)
		strTags := []string{}
		for _, tag := range rawTags.Images {
			if !strings.Contains(tag.Digest, "sha256:") {
				strTags = append(strTags, tag.Digest)
			}
		}
		sort.Strings(strTags)

		// https://github.com/opencontainers/distribution-spec/blob/b505e9cc53ec499edbd9c1be32298388921bb705/detail.md#tags-paginated
		// Offset using last query parameter.
		if last := req.URL.Query().Get("last"); last != "" {
			for i, t := range strTags {
				if t > last {
					strTags = strTags[i:]
					break
				}
			}
		}

		// Limit using n query parameter.
		if ns := req.URL.Query().Get("n"); ns != "" {
			if n, err := strconv.Atoi(ns); err != nil {
				return &regError{
					Status:  http.StatusBadRequest,
					Code:    "BAD_REQUEST",
					Message: fmt.Sprintf("parsing n: %v", err),
				}
			} else if n < len(strTags) {
				strTags = strTags[:n]
			}
		}

		tags := []Image{}
		for _, strTag := range strTags {
			tags = append(tags, Image{Digest: strTag})
		}

		tagsToList := Repository{
			Name:   repo,
			Images: tags,
		}

		msg, _ := json.Marshal(tagsToList)
		resp.Header().Set("Content-Length", fmt.Sprint(len(msg)))
		resp.WriteHeader(http.StatusOK)
		io.Copy(resp, bytes.NewReader([]byte(msg)))
		return nil
	}

	return &regError{
		Status:  http.StatusBadRequest,
		Code:    "METHOD_UNKNOWN",
		Message: "We don't understand your method + url",
	}
}

func (m *manifests) handleCatalog(resp http.ResponseWriter, req *http.Request) *regError {
	ctx, err := prefix.AttachUserPrefixToContext(req.Context(), m.env)
	if err != nil {
		panic(err)
	}

	// TODO: implement pagination
	// query := req.URL.Query()
	// nStr := query.Get("n")
	// n := 10000
	// if nStr != "" {
	// 	n, _ = strconv.Atoi(nStr)
	// }

	if req.Method == "GET" {
		catalog := m.getCatalog(ctx)
		msg, _ := json.Marshal(catalog)
		resp.Header().Set("Content-Length", fmt.Sprint(len(msg)))
		resp.WriteHeader(http.StatusOK)
		io.Copy(resp, bytes.NewReader([]byte(msg)))
		return nil
	}

	return &regError{
		Status:  http.StatusBadRequest,
		Code:    "METHOD_UNKNOWN",
		Message: "We don't understand your method + url",
	}
}

// TODO: implement handling of artifactType querystring
func (m *manifests) handleReferrers(resp http.ResponseWriter, req *http.Request) *regError {
	ctx, err := prefix.AttachUserPrefixToContext(req.Context(), m.env)
	if err != nil {
		panic(err)
	}

	// Ensure this is a GET request
	if req.Method != "GET" {
		return &regError{
			Status:  http.StatusBadRequest,
			Code:    "METHOD_UNKNOWN",
			Message: "We don't understand your method + url",
		}
	}

	elem := strings.Split(req.URL.Path, "/")
	elem = elem[1:]
	target := elem[len(elem)-1]
	repo := strings.Join(elem[1:len(elem)-2], "/")

	// Validate that incoming target is a valid digest
	if _, err := v1.NewHash(target); err != nil {
		return &regError{
			Status:  http.StatusBadRequest,
			Code:    "UNSUPPORTED",
			Message: "Target must be a valid digest",
		}
	}
	if !m.repoExists(ctx, repo) {
		return &regError{
			Status:  http.StatusNotFound,
			Code:    "NAME_UNKNOWN",
			Message: "Unknown name3",
		}
	}
	tags := m.getRepo(ctx, repo)

	im := v1.IndexManifest{
		SchemaVersion: 2,
		MediaType:     types.OCIImageIndex,
		Manifests:     []v1.Descriptor{},
	}
	for _, tag := range tags.Images {
		digest := tag.Digest
		manifest := m.getTarget(ctx, repo, tag.Digest)
		h, err := v1.NewHash(digest)
		if err != nil {
			continue
		}
		var refPointer struct {
			Subject *v1.Descriptor `json:"subject"`
		}
		json.Unmarshal(manifest.Blob, &refPointer)
		if refPointer.Subject == nil {
			continue
		}
		referenceDigest := refPointer.Subject.Digest
		if referenceDigest.String() != target {
			continue
		}
		// At this point, we know the current digest references the target
		var imageAsArtifact struct {
			Config struct {
				MediaType string `json:"mediaType"`
			} `json:"config"`
		}
		json.Unmarshal(manifest.Blob, &imageAsArtifact)
		im.Manifests = append(im.Manifests, v1.Descriptor{
			MediaType:    types.MediaType(manifest.ContentType),
			Size:         int64(len(manifest.Blob)),
			Digest:       h,
			ArtifactType: imageAsArtifact.Config.MediaType,
		})
	}
	msg, _ := json.Marshal(&im)
	resp.Header().Set("Content-Length", fmt.Sprint(len(msg)))
	resp.Header().Set("Content-Type", string(types.OCIImageIndex))
	resp.WriteHeader(http.StatusOK)
	io.Copy(resp, bytes.NewReader([]byte(msg)))
	return nil
}
