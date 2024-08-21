package endpoints

import (
	"net/http"
	"net/url"
	"regexp"
	"strconv"
)

const (
	pathSeparator = "/"
	v2Path        = pathSeparator + "v2" + pathSeparator
	blobsPath     = "blobs" + pathSeparator
	manifestsPath = "manifests" + pathSeparator
	uploadsPath   = "uploads" + pathSeparator
	tagsPath      = "tags" + pathSeparator
	referrersPath = "referrers" + pathSeparator
	list          = "list"
)

const (
	nameGroup      = "name"
	referenceGroup = "reference"
	tagGroup       = "tag"
	digestGroup    = "digest"
)

var (
	nameRegexSrc = func() string {
		pathComponent := `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*`
		return `(?P<` + nameGroup + `>` + pathComponent + `(?:` + pathSeparator + pathComponent + `)*)`
	}()
	digestRegexSrc = func() string {
		algorithmComponent := `[a-z0-9]+`
		algorithmSeparator := `[+._-]`
		algorithm := algorithmComponent + `(?:` + algorithmSeparator + algorithmComponent + `)*`
		encoded := `[a-zA-Z0-9=_-]+`
		return `(?P<` + digestGroup + `>` + algorithm + `:` + encoded + `)`
	}()
	tagRegexSrc       = `(?P<` + tagGroup + `>[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127})`
	referenceRegexSrc = `(?P<` + referenceGroup + `>` + tagRegexSrc + "|" + digestRegexSrc + `)`
)

var (
	nameRegex = regexp.MustCompile(
		`^` + nameRegexSrc + `$`,
	)
	referenceRegex = regexp.MustCompile(
		`^` + referenceRegexSrc + `$`,
	)
	digestRegex = regexp.MustCompile(
		`^` + digestRegexSrc + `$`,
	)

	catalogRegex = regexp.MustCompile(
		`^` + v2Path + `_catalog` + `$`,
	)

	end1Regex = regexp.MustCompile(
		`^` + v2Path + `$`,
	)
	end2Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + blobsPath + digestRegexSrc + `$`,
	)
	end3Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + manifestsPath + referenceRegexSrc + `$`,
	)
	end4Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + blobsPath + uploadsPath + `$`,
	)
	end5Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + blobsPath + uploadsPath + referenceRegexSrc + `$`,
	)
	end6Regex = end5Regex
	end7Regex = end3Regex
	end8Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + tagsPath + list + `$`,
	)
	end9Regex  = end3Regex
	end10Regex = end2Regex
	end11Regex = end4Regex
	end12Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + referrersPath + digestRegexSrc + `$`,
	)
	end13Regex = end5Regex
)

const (
	digestKey       = "digest"
	lastKey         = "last"
	nKey            = "n"
	mountKey        = "mount"
	fromKey         = "from"
	artifactTypeKey = "artifactType"
)

type Nameable interface {
	Name() string
}

type withName struct {
	name string
}

func (w *withName) Name() string { return w.name }

type withMethod struct {
	method string
}

func (w *withMethod) Method() string { return w.method }

type withReference struct {
	reference string
}

func (w *withReference) Reference() string { return w.reference }

type withDigest struct {
	digest string
}

func (w *withDigest) Digest() string { return w.digest }

type withN struct {
	n int
}

func (w *withN) N() int { return w.n }

type withLast struct {
	last int
}

func (w *withLast) Last() int { return w.last }

type withMount struct {
	mount string
}

func (w *withMount) Mount() string { return w.mount }

type withFrom struct {
	from string
}

func (w *withFrom) From() string { return w.from }

type withArtifactType struct {
	artifactType string
}

func (w *withArtifactType) ArtifactType() string { return w.artifactType }

type Catalog struct {
	withN
}

func (_ *Catalog) Name() string { return "" }

type End1 struct{}

func (_ *End1) Name() string { return "" }

type End2 struct {
	withName
	withMethod
	withDigest
}

type End3 struct {
	withName
	withMethod
	withReference
}

type End4a struct {
	withName
}

type End4b struct {
	withName
	withDigest
}

type End5 struct {
	withName
	withReference
}

type End6 struct {
	withName
	withReference
	withDigest
}

type End7 struct {
	withName
	withReference
}

type End8a struct {
	withName
}

type End8b struct {
	withName
	withN
	withLast
}

type End9 struct {
	withName
	withReference
}

type End10 struct {
	withName
	withDigest
}

type End11 struct {
	withName
	withMount
	withFrom
}

type End12a struct {
	withName
	withDigest
}

type End12b struct {
	withName
	withDigest
	withArtifactType
}

type End13 struct {
	withName
	withReference
}

func Endpoint(method string, uri *url.URL) Nameable {
	if method == "" {
		method = http.MethodGet
	}
	switch method {
	case http.MethodGet:
		if catalogRegex.FindStringSubmatch(uri.Path) != nil {
			if n, err := strconv.Atoi(uri.Query().Get(nKey)); err == nil {
				return &Catalog{
					withN: withN{n: n},
				}
			}
			return &Catalog{}
		}
		if end1Regex.FindStringSubmatch(uri.Path) != nil {
			return &End1{}
		}
		if m := end2Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End2{
				withMethod: withMethod{method: method},
				withName:   withName{name: m[end2Regex.SubexpIndex(nameGroup)]},
				withDigest: withDigest{digest: m[end2Regex.SubexpIndex(digestGroup)]},
			}
		}
		if m := end3Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End3{
				withMethod:    withMethod{method: method},
				withName:      withName{name: m[end3Regex.SubexpIndex(nameGroup)]},
				withReference: withReference{reference: m[end3Regex.SubexpIndex(referenceGroup)]},
			}
		}
		if m := end8Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(nKey) && uri.Query().Has(lastKey) {
				if n, err := strconv.Atoi(uri.Query().Get(nKey)); err == nil {
					if last, err := strconv.Atoi(uri.Query().Get(lastKey)); err == nil {
						return &End8b{
							withName: withName{name: m[end8Regex.SubexpIndex(nameGroup)]},
							withN:    withN{n},
							withLast: withLast{last: last},
						}
					}
				}
			}
			return &End8a{
				withName: withName{name: m[end8Regex.SubexpIndex(nameGroup)]},
			}
		}
		if m := end12Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(artifactTypeKey) {
				return &End12b{
					withName:         withName{name: m[end12Regex.SubexpIndex(nameGroup)]},
					withDigest:       withDigest{digest: m[end12Regex.SubexpIndex(digestGroup)]},
					withArtifactType: withArtifactType{artifactType: uri.Query().Get(artifactTypeKey)},
				}
			}
			return &End12a{
				withName:   withName{name: m[end12Regex.SubexpIndex(nameGroup)]},
				withDigest: withDigest{digest: m[end12Regex.SubexpIndex(digestGroup)]},
			}
		}
		if m := end13Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End13{
				withName:      withName{name: m[end13Regex.SubexpIndex(nameGroup)]},
				withReference: withReference{reference: m[end13Regex.SubexpIndex(referenceGroup)]},
			}
		}
	case http.MethodHead:
		if m := end2Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End2{
				withMethod: withMethod{method: method},
				withName:   withName{name: m[end2Regex.SubexpIndex(nameGroup)]},
				withDigest: withDigest{digest: m[end2Regex.SubexpIndex(digestGroup)]},
			}
		}
		if m := end3Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End3{
				withMethod:    withMethod{method: method},
				withName:      withName{name: m[end3Regex.SubexpIndex(nameGroup)]},
				withReference: withReference{reference: m[end3Regex.SubexpIndex(referenceGroup)]},
			}
		}
	case http.MethodPost:
		if m := end4Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(digestKey) {
				digest := uri.Query().Get(digestKey)
				if digestRegex.MatchString(digest) {
					return &End4b{
						withName:   withName{name: m[end4Regex.SubexpIndex(nameGroup)]},
						withDigest: withDigest{digest: uri.Query().Get(digestKey)},
					}
				}
			}
			return &End4a{
				withName: withName{name: m[end4Regex.SubexpIndex(nameGroup)]},
			}
		}
		if m := end11Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(mountKey) && uri.Query().Has(fromKey) {
				mount := uri.Query().Get(mountKey)
				if digestRegex.MatchString(mount) {
					from := uri.Query().Get(fromKey)
					if nameRegex.MatchString(from) {
						return &End11{
							withName:  withName{name: m[end11Regex.SubexpIndex(nameGroup)]},
							withMount: withMount{mount: mount},
							withFrom:  withFrom{from: from},
						}
					}
				}
			}
		}
	case http.MethodPatch:
		if m := end5Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End5{
				withName:      withName{name: m[end5Regex.SubexpIndex(nameGroup)]},
				withReference: withReference{reference: m[end5Regex.SubexpIndex(referenceGroup)]},
			}
		}
	case http.MethodPut:
		if m := end6Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(digestKey) {
				digest := uri.Query().Get(digestKey)
				if digestRegex.MatchString(digest) {
					return &End6{
						withName:      withName{name: m[end6Regex.SubexpIndex(nameGroup)]},
						withReference: withReference{reference: m[end6Regex.SubexpIndex(referenceGroup)]},
						withDigest:    withDigest{digest: uri.Query().Get(digestKey)},
					}
				}
			}
		}
		if m := end7Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End7{
				withName:      withName{name: m[end7Regex.SubexpIndex(nameGroup)]},
				withReference: withReference{reference: m[end7Regex.SubexpIndex(referenceGroup)]},
			}
		}
	case http.MethodDelete:
		if m := end9Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End9{
				withName:      withName{name: m[end9Regex.SubexpIndex(nameGroup)]},
				withReference: withReference{reference: m[end9Regex.SubexpIndex(referenceGroup)]},
			}
		}
		if m := end10Regex.FindStringSubmatch(uri.Path); m != nil {
			return &End10{
				withName:   withName{name: m[end10Regex.SubexpIndex(nameGroup)]},
				withDigest: withDigest{digest: m[end10Regex.SubexpIndex(digestGroup)]},
			}
		}
	}
	return nil
}
