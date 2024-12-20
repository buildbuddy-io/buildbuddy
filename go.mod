module github.com/buildbuddy-io/buildbuddy

go 1.23.4

replace (
	github.com/awslabs/soci-snapshotter => github.com/buildbuddy-io/soci-snapshotter v0.7.0-buildbuddy // keep in sync with buildpatches/com_github_awslabs_soci_snapshotter.patch
	github.com/buildkite/terminal-to-html/v3 => github.com/buildbuddy-io/terminal-to-html/v3 v3.7.0-patched-1
	github.com/jotfs/fastcdc-go v0.2.0 => github.com/buildbuddy-io/fastcdc-go v0.2.0-rc2
	github.com/lni/dragonboat/v4 => github.com/buildbuddy-io/dragonboat/v4 v4.0.1
	github.com/lni/vfs => github.com/buildbuddy-io/vfs v0.2.3
	github.com/throttled/throttled/v2 => github.com/buildbuddy-io/throttled/v2 v2.9.1-rc2
	kythe.io => github.com/buildbuddy-io/kythe v0.0.72
)

require (
	cloud.google.com/go/compute v1.28.1
	cloud.google.com/go/compute/metadata v0.5.0
	cloud.google.com/go/logging v1.11.0
	cloud.google.com/go/longrunning v0.6.0
	cloud.google.com/go/secretmanager v1.14.0
	cloud.google.com/go/storage v1.43.0
	github.com/AlecAivazis/survey/v2 v2.3.7
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/ClickHouse/clickhouse-go/v2 v2.10.1
	github.com/GoogleCloudPlatform/cloudsql-proxy v1.35.0
	github.com/Masterminds/semver/v3 v3.2.1
	github.com/RoaringBitmap/roaring v1.9.1
	github.com/armon/circbuf v0.0.0-20190214190532-5111143e8da2
	github.com/aws/aws-sdk-go v1.44.286
	github.com/aws/aws-sdk-go-v2 v1.18.1
	github.com/aws/aws-sdk-go-v2/config v1.18.27
	github.com/aws/aws-sdk-go-v2/credentials v1.13.26
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.2.12
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.70
	github.com/aws/aws-sdk-go-v2/service/s3 v1.35.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.19.2
	github.com/aws/smithy-go v1.13.5
	github.com/awslabs/soci-snapshotter v0.1.0
	github.com/bazelbuild/bazel-gazelle v0.39.0
	github.com/bazelbuild/bazelisk v1.19.0
	github.com/bazelbuild/buildtools v0.0.0-20240827154017-dd10159baa91
	github.com/bazelbuild/rules_go v0.50.1
	github.com/bazelbuild/rules_webtesting v0.0.0-20210910170740-6b2ef24cfe95
	github.com/bduffany/godemon v0.0.0-20221115232931-09721d48e30e
	github.com/bits-and-blooms/bloom/v3 v3.7.0
	github.com/bojand/ghz v0.120.0
	github.com/bradfitz/gomemcache v0.0.0-20230611145640-acc696258285
	github.com/buildbuddy-io/tensorflow-proto v0.0.0-20220908151343-929b41ab4dc6
	github.com/buildkite/terminal-to-html/v3 v3.8.0
	github.com/cavaliergopher/cpio v1.0.1
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cockroachdb/pebble v1.1.1
	github.com/containerd/containerd v1.7.20
	github.com/coreos/go-oidc/v3 v3.10.0
	github.com/creack/pty v1.1.18
	github.com/crewjam/saml v0.4.14
	github.com/docker/distribution v2.8.2+incompatible
	github.com/docker/docker v26.1.5+incompatible
	github.com/docker/go-units v0.5.0
	github.com/dop251/goja v0.0.0-20230626124041-ba8a63e79201
	github.com/elastic/gosigar v0.14.2
	github.com/firecracker-microvm/firecracker-go-sdk v0.0.0-20241028184712-f74f43bb036d
	github.com/gabriel-vasile/mimetype v1.4.4
	github.com/go-enry/go-enry/v2 v2.8.7
	github.com/go-faker/faker/v4 v4.0.0-beta.3
	github.com/go-git/go-git/v5 v5.11.0
	github.com/go-redis/redis/extra/redisotel/v8 v8.11.5
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-sql-driver/mysql v1.8.1
	github.com/gobwas/glob v0.2.3
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/google/go-cmp v0.6.0
	github.com/google/go-containerregistry v0.17.0
	github.com/google/go-github/v59 v59.0.0
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/tink/go v1.7.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.7.3
	github.com/groob/plist v0.0.0-20220217120414-63fa881b19a5
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hanwen/go-fuse/v2 v2.7.2
	github.com/hashicorp/golang-lru v1.0.2
	github.com/hashicorp/memberlist v0.5.1
	github.com/hashicorp/serf v0.10.1
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa
	github.com/jackc/pgx/v5 v5.5.5
	github.com/jhump/protoreflect v1.15.1
	github.com/jonboulle/clockwork v0.4.0
	github.com/jotfs/fastcdc-go v0.2.0
	github.com/jsimonetti/rtnetlink v1.3.3
	github.com/klauspost/compress v1.17.9
	github.com/klauspost/cpuid/v2 v2.2.5
	github.com/lestrrat-go/jwx v1.2.29
	github.com/lni/dragonboat/v4 v4.0.0-20240618143154-6a1623140f27
	github.com/lni/goutils v1.4.0
	github.com/logrusorgru/aurora v2.0.3+incompatible
	github.com/manifoldco/promptui v0.9.0
	github.com/mattn/go-isatty v0.0.20
	github.com/mattn/go-shellwords v1.0.12
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/mdlayher/vsock v1.2.1
	github.com/mitchellh/go-ps v1.0.0
	github.com/mwitkow/grpc-proxy v0.0.0-20230212185441-f345521cb9c9
	github.com/nishanths/exhaustive v0.7.11
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.1.0
	github.com/opencontainers/runtime-spec v1.2.0
	github.com/otiai10/copy v1.14.0
	github.com/pkg/errors v0.9.1
	github.com/planetscale/vtprotobuf v0.6.1-0.20240409071808-615f978279ca
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/client_golang v1.19.1
	github.com/prometheus/client_model v0.6.1
	github.com/prometheus/common v0.54.0
	github.com/rantav/go-grpc-channelz v0.0.3
	github.com/rs/zerolog v1.29.1
	github.com/shirou/gopsutil/v3 v3.23.5
	github.com/shurcooL/githubv4 v0.0.0-20231126234147-1cffa1f02456
	github.com/sirupsen/logrus v1.9.3
	github.com/smacker/go-tree-sitter v0.0.0-20230501083651-a7d92773b3aa
	github.com/stretchr/testify v1.9.0
	github.com/tebeka/selenium v0.9.9
	github.com/throttled/throttled/v2 v2.12.0
	github.com/tklauser/go-sysconf v0.3.11
	github.com/vishvananda/netlink v1.2.1-beta.2
	github.com/xiam/s-expr v0.0.0-20240311144413-9d7e25c9d7d1
	github.com/zeebo/blake3 v0.2.3
	gitlab.com/arm-research/smarter/smarter-device-manager v1.20.11
	go.opentelemetry.io/contrib/detectors/gcp v1.21.1
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.54.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.56.0
	go.opentelemetry.io/otel v1.31.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/metric v1.31.0
	go.opentelemetry.io/otel/sdk v1.29.0
	go.opentelemetry.io/otel/trace v1.31.0
	go.uber.org/atomic v1.11.0
	golang.org/x/crypto v0.31.0
	golang.org/x/exp v0.0.0-20240613232115-7f521ea00fb8
	golang.org/x/mod v0.20.0
	golang.org/x/oauth2 v0.22.0
	golang.org/x/sync v0.10.0
	golang.org/x/sys v0.28.0
	golang.org/x/text v0.21.0
	golang.org/x/time v0.6.0
	golang.org/x/tools v0.24.0
	google.golang.org/api v0.196.0
	google.golang.org/genproto v0.0.0-20240903143218-8af14fe29dc1
	google.golang.org/genproto/googleapis/api v0.0.0-20240903143218-8af14fe29dc1
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20240903143218-8af14fe29dc1
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1
	google.golang.org/grpc v1.66.0
	google.golang.org/protobuf v1.34.2
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/driver/clickhouse v0.5.1
	gorm.io/driver/mysql v1.5.1
	gorm.io/driver/postgres v1.5.2
	gorm.io/driver/sqlite v1.5.2
	gorm.io/gorm v1.25.2
	honnef.co/go/tools v0.5.1
	kythe.io v0.0.72
)

require (
	bitbucket.org/creachadair/stringset v0.0.11 // indirect
	cel.dev/expr v0.15.0 // indirect
	cloud.google.com/go v0.115.1 // indirect
	cloud.google.com/go/auth v0.9.3 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.4 // indirect
	cloud.google.com/go/iam v1.2.0 // indirect
	dario.cat/mergo v1.0.0 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20230811130428-ced1acdcaa24 // indirect
	github.com/AdamKorcz/go-118-fuzz-build v0.0.0-20230306123547-8075edf89bb0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	github.com/ClickHouse/ch-go v0.57.0 // indirect
	github.com/DataDog/zstd v1.5.5 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.21.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/sprig/v3 v3.2.3 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/Microsoft/hcsshim v0.12.3 // indirect
	github.com/ProtonMail/go-crypto v0.0.0-20230828082145-3c4c8a2d2371 // indirect
	github.com/VictoriaMetrics/metrics v1.33.1 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.28 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.26 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.14.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.12.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.14.12 // indirect
	github.com/beevik/etree v1.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bgentry/go-netrc v0.0.0-20140422174119-9fd32a8b3d3d // indirect
	github.com/bits-and-blooms/bitset v1.14.3 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/bmatcuk/doublestar/v4 v4.6.1 // indirect
	github.com/bufbuild/protocompile v0.5.1 // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/chzyer/readline v1.5.1 // indirect
	github.com/cloudflare/circl v1.3.7 // indirect
	github.com/cncf/xds/go v0.0.0-20240423153145-555b57ec207b // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240616162244-4768e80dfb9a // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/containerd/cgroups/v3 v3.0.2 // indirect
	github.com/containerd/containerd/api v1.7.19 // indirect
	github.com/containerd/continuity v0.4.3 // indirect
	github.com/containerd/errdefs v0.1.0 // indirect
	github.com/containerd/fifo v1.1.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/containerd/stargz-snapshotter/estargz v0.14.3 // indirect
	github.com/containerd/ttrpc v1.2.5 // indirect
	github.com/containerd/typeurl/v2 v2.1.1 // indirect
	github.com/containernetworking/cni v1.2.3 // indirect
	github.com/containernetworking/plugins v1.5.1 // indirect
	github.com/crewjam/httperr v0.2.0 // indirect
	github.com/cyphar/filepath-securejoin v0.2.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dlclark/regexp2 v1.10.0 // indirect
	github.com/docker/cli v27.0.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.7.0 // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-events v0.0.0-20190806004212-e31b211e4f1c // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/envoyproxy/go-control-plane v0.12.1-0.20240621013728-1eb8caab5155 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.0.4 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/getsentry/sentry-go v0.28.1 // indirect
	github.com/go-chi/chi/v5 v5.0.7 // indirect
	github.com/go-enry/go-oniguruma v1.2.1 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.6.1 // indirect
	github.com/go-git/gcfg v1.5.1-0.20230307220236-3a3c6141e376 // indirect
	github.com/go-git/go-billy/v5 v5.5.0 // indirect
	github.com/go-jose/go-jose/v4 v4.0.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
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
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.1 // indirect
	github.com/golang/glog v1.2.1 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/codesearch v1.2.0 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/orderedcode v0.0.1 // indirect
	github.com/google/pprof v0.0.0-20240424215950-a892ee059fd6 // indirect
	github.com/google/s2a-go v0.1.8 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.3 // indirect
	github.com/googleapis/gax-go/v2 v2.13.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.6 // indirect
	github.com/hashicorp/go-version v1.6.0 // indirect
	github.com/huandu/xstrings v1.4.0 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
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
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.2 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/lni/vfs v0.2.1-0.20231221160030-d43d3c60804c // indirect
	github.com/lufia/plan9stats v0.0.0-20230326075908-cb1d2100619a // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattermost/xml-roundtrip-validator v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.11 // indirect
	github.com/mdlayher/netlink v1.7.2 // indirect
	github.com/mdlayher/socket v0.4.1 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/miekg/dns v1.1.61 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/locker v1.0.1 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/moby/sys/sequential v0.5.0 // indirect
	github.com/moby/sys/signal v0.7.0 // indirect
	github.com/moby/sys/user v0.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/opencontainers/selinux v1.11.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/paulmach/orb v0.9.2 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pjbgf/sha1cd v0.3.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/russellhaering/goxmldsig v1.4.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/sergi/go-diff v1.3.1 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/shurcooL/graphql v0.0.0-20230722043721-ed46e5a46466 // indirect
	github.com/skeema/knownhosts v1.2.1 // indirect
	github.com/spf13/cast v1.5.1 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/vbatts/tar-split v0.11.3 // indirect
	github.com/vishvananda/netns v0.0.4 // indirect
	github.com/xanzy/ssh-agent v0.3.3 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.etcd.io/bbolt v1.3.10 // indirect
	go.mongodb.org/mongo-driver v1.14.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/term v0.27.0 // indirect
	golang.org/x/tools/go/vcs v0.1.0-deprecated // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	k8s.io/kubelet v0.29.1 // indirect
	oras.land/oras-go/v2 v2.5.0 // indirect
)
