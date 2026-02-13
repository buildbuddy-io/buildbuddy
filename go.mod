module github.com/buildbuddy-io/buildbuddy

go 1.26.0

// Device Manager
tool gitlab.com/arm-research/smarter/smarter-device-manager

// Gazelle deps for 'bb fix'
tool github.com/pmezard/go-difflib/difflib

// rules_webtesting deps
tool github.com/gorilla/mux

tool (
	// static analyzers
	github.com/nishanths/exhaustive
	golang.org/x/tools/go/analysis
	honnef.co/go/tools/staticcheck
)

// Used by cli/explain/compactgraph/testdata/generate
tool github.com/otiai10/copy

// Used indirectly through a data dependency by //tools/lint on gomimports
tool golang.org/x/telemetry/counter

replace (
	github.com/buildkite/terminal-to-html/v3 => github.com/buildbuddy-io/terminal-to-html/v3 v3.16.8-19
	github.com/lni/dragonboat/v4 => github.com/buildbuddy-io/dragonboat/v4 v4.0.1
	github.com/lni/vfs => github.com/buildbuddy-io/vfs v0.2.3
	github.com/throttled/throttled/v2 => github.com/buildbuddy-io/throttled/v2 v2.9.1-rc2
	kythe.io => github.com/buildbuddy-io/kythe v0.0.72
)

require (
	cloud.google.com/go/compute v1.38.0
	cloud.google.com/go/compute/metadata v0.9.0
	cloud.google.com/go/logging v1.13.0
	cloud.google.com/go/longrunning v0.6.7
	cloud.google.com/go/secretmanager v1.14.7
	cloud.google.com/go/storage v1.57.0
	github.com/AlecAivazis/survey/v2 v2.3.7
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/ClickHouse/clickhouse-go/v2 v2.41.0
	github.com/GoogleCloudPlatform/cloudsql-proxy v1.35.0
	github.com/Masterminds/semver/v3 v3.2.1
	github.com/RoaringBitmap/roaring v1.9.1
	github.com/VictoriaMetrics/metrics v1.33.1
	github.com/armon/circbuf v0.0.0-20190214190532-5111143e8da2
	github.com/aws/aws-sdk-go v1.55.6
	github.com/aws/aws-sdk-go-v2 v1.36.3
	github.com/aws/aws-sdk-go-v2/config v1.29.14
	github.com/aws/aws-sdk-go-v2/credentials v1.17.67
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.5.11
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.69
	github.com/aws/aws-sdk-go-v2/service/s3 v1.78.2
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.19
	github.com/aws/smithy-go v1.22.3
	github.com/bazelbuild/bazel-gazelle v0.46.0
	github.com/bazelbuild/bazelisk v1.25.1-0.20250219134847-cdb99bfb1b7d
	github.com/bazelbuild/buildtools v0.0.0-20250930140053-2eb4fccefb52
	github.com/bazelbuild/rules_go v0.59.0
	github.com/bazelbuild/rules_webtesting v0.2.1-0.20250911195827-e09c04b7d4d1
	github.com/bduffany/godemon v0.0.0-20221115232931-09721d48e30e
	github.com/bits-and-blooms/bloom/v3 v3.7.0
	github.com/bojand/ghz v0.120.0
	github.com/bradfitz/gomemcache v0.0.0-20230611145640-acc696258285
	github.com/buildbuddy-io/fastcdc2020 v0.0.2
	github.com/buildbuddy-io/tensorflow-proto v0.0.0-20220908151343-929b41ab4dc6
	github.com/buildkite/terminal-to-html/v3 v3.0.0-00010101000000-000000000000
	github.com/charmbracelet/bubbles v1.0.0
	github.com/charmbracelet/bubbletea v1.3.10
	github.com/charmbracelet/lipgloss v1.1.0
	github.com/cavaliergopher/cpio v1.0.1
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cockroachdb/pebble v1.1.4
	github.com/coreos/go-oidc/v3 v3.10.0
	github.com/creack/pty v1.1.18
	github.com/crewjam/saml v0.4.14
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f
	github.com/distribution/reference v0.6.0
	github.com/docker/docker v28.5.2+incompatible
	github.com/docker/go-units v0.5.0
	github.com/dop251/goja v0.0.0-20230626124041-ba8a63e79201
	github.com/elastic/gosigar v0.14.2
	github.com/firecracker-microvm/firecracker-go-sdk v0.0.0-20241028184712-f74f43bb036d
	github.com/gabriel-vasile/mimetype v1.4.4
	github.com/go-enry/go-enry/v2 v2.8.7
	github.com/go-faker/faker/v4 v4.0.0-beta.3
	github.com/go-git/go-git/v5 v5.16.5
	github.com/go-redis/redis/extra/redisotel/v8 v8.11.5
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-sql-driver/mysql v1.8.1
	github.com/gobwas/glob v0.2.3
	github.com/golang-jwt/jwt/v4 v4.5.2
	github.com/google/cadvisor v0.53.0
	github.com/google/go-cmp v0.7.0
	github.com/google/go-containerregistry v0.20.3
	github.com/google/go-github/v59 v59.0.0
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.6.0
	github.com/groob/plist v0.0.0-20220217120414-63fa881b19a5
	github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus v1.0.1
	github.com/hanwen/go-fuse/v2 v2.7.2
	github.com/hashicorp/golang-lru v1.0.2
	github.com/hashicorp/memberlist v0.5.1
	github.com/hashicorp/serf v0.10.1
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa
	github.com/jackc/pgx/v5 v5.5.5
	github.com/jhump/protoreflect v1.15.1
	github.com/jonboulle/clockwork v0.4.0
	github.com/jsimonetti/rtnetlink v1.3.3
	github.com/klauspost/compress v1.18.0
	github.com/klauspost/cpuid/v2 v2.2.7
	github.com/lestrrat-go/jwx v1.2.29
	github.com/lni/dragonboat/v4 v4.0.0-20240618143154-6a1623140f27
	github.com/lni/goutils v1.4.0
	github.com/logrusorgru/aurora v2.0.3+incompatible
	github.com/manifoldco/promptui v0.9.0
	github.com/mattn/go-isatty v0.0.20
	github.com/mattn/go-shellwords v1.0.12
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/mdlayher/vsock v1.2.1
	github.com/miekg/dns v1.1.61
	github.com/mitchellh/go-ps v1.0.0
	github.com/mwitkow/grpc-proxy v0.0.0-20230212185441-f345521cb9c9
	github.com/ncruces/go-sqlite3 v0.25.2
	github.com/open-feature/go-sdk v1.17.1
	github.com/open-feature/go-sdk-contrib/providers/flagd v0.3.2
	github.com/opencontainers/runtime-spec v1.2.1
	github.com/pkg/errors v0.9.1
	github.com/planetscale/vtprotobuf v0.6.1-0.20241121165744-79df5c4772f2
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.67.2
	github.com/rantav/go-grpc-channelz v0.0.3
	github.com/rs/zerolog v1.29.1
	github.com/shirou/gopsutil/v3 v3.23.12
	github.com/shurcooL/githubv4 v0.0.0-20231126234147-1cffa1f02456
	github.com/sirupsen/logrus v1.9.3
	github.com/smacker/go-tree-sitter v0.0.0-20240827094217-dd81d9e9be82
	github.com/stretchr/testify v1.11.1
	github.com/tebeka/selenium v0.9.9
	github.com/throttled/throttled/v2 v2.12.0
	github.com/tink-crypto/tink-go-awskms/v2 v2.1.0
	github.com/tink-crypto/tink-go-gcpkms/v2 v2.2.0
	github.com/tink-crypto/tink-go/v2 v2.3.0
	github.com/tklauser/go-sysconf v0.3.12
	github.com/vishvananda/netlink v1.2.1-beta.2
	github.com/xiam/s-expr v0.0.0-20240311144413-9d7e25c9d7d1
	github.com/zeebo/blake3 v0.2.3
	go.opentelemetry.io/contrib/detectors/gcp v1.38.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.64.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.62.0
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.38.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.38.0
	go.opentelemetry.io/otel/exporters/prometheus v0.60.0
	go.opentelemetry.io/otel/metric v1.39.0
	go.opentelemetry.io/otel/sdk v1.39.0
	go.opentelemetry.io/otel/sdk/metric v1.39.0
	go.opentelemetry.io/otel/trace v1.39.0
	go.uber.org/atomic v1.11.0
	golang.org/x/crypto v0.45.0
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546
	golang.org/x/mod v0.29.0
	golang.org/x/net v0.47.0
	golang.org/x/oauth2 v0.32.0
	golang.org/x/sync v0.18.0
	golang.org/x/sys v0.39.0
	golang.org/x/text v0.31.0
	golang.org/x/time v0.12.0
	google.golang.org/api v0.247.0
	google.golang.org/genproto/googleapis/api v0.0.0-20251022142026-3a174f9686a8
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20250804133106-a7a43d27e69b
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217
	google.golang.org/grpc v1.77.0
	google.golang.org/protobuf v1.36.10
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/driver/clickhouse v0.5.1
	gorm.io/driver/mysql v1.5.1
	gorm.io/driver/postgres v1.5.2
	gorm.io/driver/sqlite v1.5.2
	gorm.io/gorm v1.25.2
	kythe.io v0.0.72
)

require (
	bitbucket.org/creachadair/stringset v0.0.11 // indirect
	buf.build/gen/go/open-feature/flagd/connectrpc/go v1.18.1-20250529171031-ebdc14163473.1 // indirect
	buf.build/gen/go/open-feature/flagd/grpc/go v1.5.1-20250529171031-ebdc14163473.2 // indirect
	buf.build/gen/go/open-feature/flagd/protocolbuffers/go v1.36.6-20250529171031-ebdc14163473.1 // indirect
	cel.dev/expr v0.24.0 // indirect
	cloud.google.com/go v0.121.6 // indirect
	cloud.google.com/go/auth v0.16.5 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	connectrpc.com/connect v1.18.1 // indirect
	connectrpc.com/otelconnect v0.7.2 // indirect
	dario.cat/mergo v1.0.2 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	github.com/ClickHouse/ch-go v0.69.0 // indirect
	github.com/DataDog/zstd v1.5.5 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.30.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.53.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.53.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/sprig/v3 v3.2.3 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/ProtonMail/go-crypto v1.1.6 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.7.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.1 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/barkimedes/go-deepcopy v0.0.0-20220514131651-17c30cfc62df // indirect
	github.com/beevik/etree v1.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bgentry/go-netrc v0.0.0-20140422174119-9fd32a8b3d3d // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/bmatcuk/doublestar/v4 v4.9.1 // indirect
	github.com/bufbuild/protocompile v0.5.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/charmbracelet/colorprofile v0.4.1 // indirect
	github.com/charmbracelet/x/ansi v0.11.6 // indirect
	github.com/charmbracelet/x/cellbuf v0.0.15 // indirect
	github.com/charmbracelet/x/term v0.2.2 // indirect
	github.com/chzyer/readline v1.5.1 // indirect
	github.com/clipperhouse/displaywidth v0.9.0 // indirect
	github.com/clipperhouse/stringish v0.1.1 // indirect
	github.com/clipperhouse/uax29/v2 v2.5.0 // indirect
	github.com/cloudflare/circl v1.6.1 // indirect
	github.com/cncf/xds/go v0.0.0-20251022180443-0feb69152e9f // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240616162244-4768e80dfb9a // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/fifo v1.1.0 // indirect
	github.com/containerd/stargz-snapshotter/estargz v0.16.3 // indirect
	github.com/containernetworking/cni v1.2.3 // indirect
	github.com/containernetworking/plugins v1.5.1 // indirect
	github.com/crewjam/httperr v0.2.0 // indirect
	github.com/cyphar/filepath-securejoin v0.4.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/diegoholiveira/jsonlogic/v3 v3.8.4 // indirect
	github.com/dlclark/regexp2 v1.11.0 // indirect
	github.com/docker/cli v27.5.0+incompatible // indirect
	github.com/docker/distribution v2.8.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.8.2 // indirect
	github.com/docker/go-connections v0.6.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.35.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/erikgeiser/coninput v0.0.0-20211004153227-1c3628e74d0f // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/getsentry/sentry-go v0.28.1 // indirect
	github.com/go-chi/chi/v5 v5.2.2 // indirect
	github.com/go-enry/go-oniguruma v1.2.1 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-git/gcfg v1.5.1-0.20230307220236-3a3c6141e376 // indirect
	github.com/go-git/go-billy/v5 v5.6.2 // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.23.0 // indirect
	github.com/go-openapi/errors v0.22.0 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/jsonreference v0.21.0 // indirect
	github.com/go-openapi/loads v0.22.0 // indirect
	github.com/go-openapi/runtime v0.26.0 // indirect
	github.com/go-openapi/spec v0.21.0 // indirect
	github.com/go-openapi/strfmt v0.23.0 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/go-openapi/validate v0.24.0 // indirect
	github.com/go-redis/redis/extra/rediscmd/v8 v8.11.5 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/gofrs/flock v0.12.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/codesearch v1.2.0 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/orderedcode v0.0.1 // indirect
	github.com/google/pprof v0.0.0-20241029153458-d1b30febd7db // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/grafana/regexp v0.0.0-20240518133315-a468a5bfb3bc // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-memdb v1.3.5 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.6 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/huandu/xstrings v1.4.0 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jbenet/go-context v0.0.0-20150711004518-d14ea06fba99 // indirect
	github.com/jinzhu/configor v1.2.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/josharian/native v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/kevinburke/ssh_config v1.2.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.2 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/lni/vfs v0.2.1-0.20231221160030-d43d3c60804c // indirect
	github.com/lucasb-eyer/go-colorful v1.3.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20230326075908-cb1d2100619a // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattermost/xml-roundtrip-validator v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.11 // indirect
	github.com/mattn/go-localereader v0.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.19 // indirect
	github.com/mdlayher/netlink v1.7.2 // indirect
	github.com/mdlayher/socket v0.4.1 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/muesli/ansi v0.0.0-20230316100256-276c6243b2f6 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/muesli/termenv v0.16.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/nishanths/exhaustive v0.7.11 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/onsi/ginkgo/v2 v2.21.0 // indirect
	github.com/onsi/gomega v1.35.1 // indirect
	github.com/open-feature/flagd-schemas v0.2.13 // indirect
	github.com/open-feature/flagd/core v0.13.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/otiai10/copy v1.14.0 // indirect
	github.com/paulmach/orb v0.12.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pjbgf/sha1cd v0.3.2 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/prometheus/otlptranslator v0.0.2 // indirect
	github.com/prometheus/procfs v0.19.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/russellhaering/goxmldsig v1.4.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/sergi/go-diff v1.3.2-0.20230802210424-5b0b94c5c0d3 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/shurcooL/graphql v0.0.0-20230722043721-ed46e5a46466 // indirect
	github.com/skeema/knownhosts v1.3.1 // indirect
	github.com/spf13/cast v1.5.1 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/tetratelabs/wazero v1.9.0 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/vbatts/tar-split v0.11.6 // indirect
	github.com/vishvananda/netns v0.0.4 // indirect
	github.com/xanzy/ssh-agent v0.3.3 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/assert v1.3.0 // indirect
	gitlab.com/arm-research/smarter/smarter-device-manager v1.20.11 // indirect
	go.mongodb.org/mongo-driver v1.14.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.38.0 // indirect
	go.opentelemetry.io/proto/otlp v1.7.1 // indirect
	go.uber.org/mock v0.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/telemetry v0.0.0-20251028164327-d7a2859f34e8 // indirect
	golang.org/x/term v0.37.0 // indirect
	golang.org/x/tools v0.38.0 // indirect
	golang.org/x/tools/go/vcs v0.1.0-deprecated // indirect
	google.golang.org/genproto v0.0.0-20250603155806-513f23925822 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	honnef.co/go/tools v0.5.1 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/kubelet v0.29.1 // indirect
)
