package artifactsigner

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"os"
	"os/exec"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ispb "github.com/buildbuddy-io/buildbuddy/proto/imagesigner"
)

// openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -x509 -nodes -days 365 -out cert.pem -keyout key.pem -subj "/CN=example.com"

var (
	keyFile       = flag.String("artifactsigner.key_file", "", "Path to a PEM encoded certificate key used for signing.")
	cosignKeyFile = flag.String("artifactsigner.cosign_key_file", "", "Path to a PEM encoded certificate key used for signing.")
)

type Signer struct {
	env environment.Env

	key *ecdsa.PrivateKey
}

func Register(env *real_environment.RealEnv) error {
	if *keyFile == "" {
		return nil
	}
	s, err := New(env)
	if err != nil {
		return err
	}
	env.SetArtifactSigner(s)
	return nil
}

func parseCertificateKey(data []byte) (*ecdsa.PrivateKey, error) {
	kd, _ := pem.Decode(data)
	if kd == nil {
		return nil, status.InvalidArgumentErrorf("certificate key did not contain valid PEM data")
	}
	key, err := x509.ParsePKCS8PrivateKey(kd.Bytes)
	if err != nil {
		return nil, status.UnknownErrorf("could not parse certificate key: %s", err)
	}
	return key.(*ecdsa.PrivateKey), nil
}

func New(env environment.Env) (*Signer, error) {
	data, err := os.ReadFile(*keyFile)
	if err != nil {
		return nil, err
	}

	key, err := parseCertificateKey(data)
	if err != nil {
		return nil, err
	}
	return &Signer{
		env: env,
		key: key,
	}, nil
}

func (s *Signer) Sign(data []byte) ([]byte, error) {
	hash := sha256.New()
	_, err := hash.Write(data)
	if err != nil {
		return nil, err
	}
	digest := hash.Sum(nil)
	return ecdsa.SignASN1(rand.Reader, s.key, digest)
}

func (s *Signer) Verify(data []byte, signature []byte) error {
	hash := sha256.New()
	_, err := hash.Write(data)
	if err != nil {
		return err
	}
	digest := hash.Sum(nil)
	if !ecdsa.VerifyASN1(&s.key.PublicKey, digest, signature) {
		return status.DataLossErrorf("signature validation failed")
	}
	return nil
}

func (s *Signer) PushSignedImage(ctx context.Context, req *ispb.SignRequest) (*ispb.SignResponse, error) {
	ac := s.env.GetActionCacheClient()
	rsp, err := ac.GetActionResult(ctx, &repb.GetActionResultRequest{
		ActionDigest:   req.GetDigest(),
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	if err != nil {
		return nil, err
	}
	td, err := os.MkdirTemp("", "cosign-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = os.RemoveAll(td)
	}()
	for _, od := range rsp.GetOutputDirectories() {
		log.Infof("fetching directory %s", od.GetPath())
		rn := digest.NewResourceName(od.GetTreeDigest(), "", rspb.CacheType_CAS, repb.DigestFunction_BLAKE3)
		bs, err := s.env.GetCache().Get(ctx, rn.ToProto())
		if err != nil {
			return nil, err
		}
		t := &repb.Tree{}
		if err := proto.Unmarshal(bs, t); err != nil {
			return nil, err
		}
		_, err = dirtools.DownloadTree(ctx, s.env, "", repb.DigestFunction_BLAKE3, t, td, &dirtools.DownloadTreeOpts{})
		if err != nil {
			return nil, err
		}
	}

	cmd := exec.CommandContext(ctx, "crane", "push", td, req.GetDestination())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	cmd = exec.CommandContext(ctx, "cosign", "sign", "--key", *cosignKeyFile, req.GetDestination())
	cmd.Stdin = bytes.NewReader(nil)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	log.Infof("action result: %+v", rsp)
	return &ispb.SignResponse{}, nil
}
