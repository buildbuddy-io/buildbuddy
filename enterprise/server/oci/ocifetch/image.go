package ocifetch

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocimanifest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/types"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
)

// Image returns a go-containerregistry Image for ref whose config and layers
// are read through this Fetcher. If ref names an image index, the child
// manifest matching platform is fetched (at most one level deep).
func (f *Fetcher) Image(ctx context.Context, ref ctrname.Reference, platform ctr.Platform, creds *rgpb.Credentials, opts Options) (ctr.Image, error) {
	desc, raw, err := f.FetchManifest(ctx, ref, creds, opts)
	if err != nil {
		return nil, err
	}
	return f.imageFromManifest(ctx, ref.Context(), *desc, raw, platform, creds, opts, false)
}

func (f *Fetcher) imageFromManifest(ctx context.Context, repo ctrname.Repository, desc ctr.Descriptor, raw []byte, platform ctr.Platform, creds *rgpb.Credentials, opts Options, isChild bool) (ctr.Image, error) {
	if desc.MediaType.IsSchema1() {
		return nil, status.UnknownErrorf("unsupported MediaType %q", desc.MediaType)
	}
	if desc.MediaType.IsIndex() {
		if isChild {
			return nil, status.UnknownErrorf("image index %s@%s refers to another index", repo, desc.Digest)
		}
		indexManifest, err := ctr.ParseIndexManifest(bytes.NewReader(raw))
		if err != nil {
			return nil, status.UnknownErrorf("error parsing index manifest: %s", err)
		}
		childDesc, err := ocimanifest.FindFirstImageManifest(*indexManifest, platform)
		if err != nil {
			return nil, status.UnknownErrorf("Could not find child image for platform in index: %s", err)
		}
		childRef := repo.Digest(childDesc.Digest.String())
		d, childRaw, err := f.FetchManifest(ctx, childRef, creds, opts)
		if err != nil {
			return nil, err
		}
		return f.imageFromManifest(ctx, repo, *d, childRaw, platform, creds, opts, true)
	}
	img := &image{
		ctx:         ctx,
		f:           f,
		repo:        repo,
		desc:        desc,
		rawManifest: raw,
		creds:       creds,
		opts:        opts,
	}
	img.rawConfigOnce = sync.OnceValues(func() ([]byte, error) {
		manifest, err := img.Manifest()
		if err != nil {
			return nil, err
		}
		if manifest.Config.Data != nil {
			return manifest.Config.Data, nil
		}
		cfg := manifest.Config
		rc, err := img.layer(cfg.Digest, &cfg).Uncompressed()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	})
	return img, nil
}

// image implements ctr.Image on top of a raw manifest. Its config and layers
// are fetched through the Fetcher on demand.
type image struct {
	ctx context.Context
	f   *Fetcher

	repo        ctrname.Repository
	desc        ctr.Descriptor
	rawManifest []byte
	creds       *rgpb.Credentials
	opts        Options

	rawConfigOnce func() ([]byte, error)
}

var _ ctr.Image = (*image)(nil)

func (i *image) Digest() (ctr.Hash, error)           { return i.desc.Digest, nil }
func (i *image) RawManifest() ([]byte, error)        { return i.rawManifest, nil }
func (i *image) MediaType() (types.MediaType, error) { return i.desc.MediaType, nil }
func (i *image) Size() (int64, error)                { return i.desc.Size, nil }
func (i *image) RawConfigFile() ([]byte, error)      { return i.rawConfigOnce() }
func (i *image) Manifest() (*ctr.Manifest, error) {
	return ctr.ParseManifest(bytes.NewReader(i.rawManifest))
}
func (i *image) ConfigFile() (*ctr.ConfigFile, error) {
	raw, err := i.RawConfigFile()
	if err != nil {
		return nil, err
	}
	return ctr.ParseConfigFile(bytes.NewReader(raw))
}

func (i *image) ConfigName() (ctr.Hash, error) {
	m, err := i.Manifest()
	if err != nil {
		return ctr.Hash{}, err
	}
	return m.Config.Digest, nil
}

func (i *image) Layers() ([]ctr.Layer, error) {
	m, err := i.Manifest()
	if err != nil {
		return nil, err
	}
	layers := make([]ctr.Layer, 0, len(m.Layers))
	for idx := range m.Layers {
		d := m.Layers[idx]
		layers = append(layers, i.layer(d.Digest, &d))
	}
	return layers, nil
}

func (i *image) LayerByDigest(digest ctr.Hash) (ctr.Layer, error) {
	return i.layer(digest, nil), nil
}

func (i *image) LayerByDiffID(diffID ctr.Hash) (ctr.Layer, error) {
	digest, err := partial.DiffIDToBlob(i, diffID)
	if err != nil {
		return nil, err
	}
	return i.layer(digest, nil), nil
}

func (i *image) layer(digest ctr.Hash, desc *ctr.Descriptor) *layer {
	return &layer{image: i, digest: digest, desc: desc}
}

// layer implements ctr.Layer for one blob of an image.
type layer struct {
	image  *image
	digest ctr.Hash
	// desc is the manifest descriptor for the blob when known.
	desc *ctr.Descriptor
}

var _ ctr.Layer = (*layer)(nil)

func (l *layer) Digest() (ctr.Hash, error) { return l.digest, nil }

func (l *layer) DiffID() (ctr.Hash, error) {
	return partial.BlobToDiffID(l.image, l.digest)
}

// MediaType reports the generic Docker layer type, as the remote layer
// implementation in go-containerregistry does.
func (l *layer) MediaType() (types.MediaType, error) { return types.DockerLayer, nil }

func (l *layer) Size() (int64, error) {
	if l.desc != nil {
		return l.desc.Size, nil
	}
	desc, err := l.image.f.FetchBlobMetadata(l.image.ctx, l.ref(), l.image.creds, l.image.opts)
	if err != nil {
		return 0, err
	}
	return desc.Size, nil
}

// Compressed streams the blob through the Fetcher. Reading stops and the
// fetch is abandoned when the returned reader is closed early.
func (l *layer) Compressed() (io.ReadCloser, error) {
	opts := l.image.opts
	if l.desc != nil {
		opts.SizeBytes = l.desc.Size
		opts.MediaType = string(l.desc.MediaType)
	}
	ctx, cancel := context.WithCancel(l.image.ctx)
	pr, pw := io.Pipe()
	go func() {
		_, err := l.image.f.FetchBlob(ctx, pw, l.ref(), l.image.creds, opts)
		pw.CloseWithError(err)
	}()
	return &pipeReadCloser{PipeReader: pr, cancel: cancel}, nil
}

// Uncompressed decompresses Compressed on the fly.
func (l *layer) Uncompressed() (io.ReadCloser, error) {
	cl, err := partial.CompressedToLayer(l)
	if err != nil {
		return nil, err
	}
	return cl.Uncompressed()
}

func (l *layer) ref() ctrname.Digest {
	return l.image.repo.Digest(l.digest.String())
}

type pipeReadCloser struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (p *pipeReadCloser) Close() error {
	p.cancel()
	return p.PipeReader.Close()
}
