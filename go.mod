module github.com/buildbuddy-io/buildbuddy

go 1.18

replace (
	github.com/buildkite/terminal-to-html/v3 => github.com/buildbuddy-io/terminal-to-html/v3 v3.7.0-patched-1
	github.com/go-redsync/redsync/v4 v4.4.1 => github.com/bduffany/redsync/v4 v4.4.1-minimal
	github.com/lni/dragonboat/v3 => github.com/tylerwilliams/dragonboat/v3 v3.3.4-rc5
	github.com/throttled/throttled/v2 => github.com/buildbuddy-io/throttled/v2 v2.9.1-rc2
)

require (
	cloud.google.com/go/compute/metadata v0.2.1
	cloud.google.com/go/logging v1.5.0
	cloud.google.com/go/storage v1.27.0
	github.com/AlecAivazis/survey/v2 v2.3.4
	github.com/Azure/azure-storage-blob-go v0.14.0
	github.com/ClickHouse/clickhouse-go/v2 v2.2.0
	github.com/GoogleCloudPlatform/cloudsql-proxy v1.33.0
	github.com/armon/circbuf v0.0.0-20150827004946-bbbad097214e
	github.com/aws/aws-sdk-go v1.43.9
	github.com/bazelbuild/bazelisk v1.11.0
	github.com/bazelbuild/rules_go v0.29.0
	github.com/bazelbuild/rules_webtesting v0.2.0
	github.com/bduffany/godemon v0.0.0-20221115232931-09721d48e30e
	github.com/bojand/ghz v0.110.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/buildbuddy-io/tensorflow-proto v0.0.0-20220908151343-929b41ab4dc6
	github.com/buildkite/terminal-to-html/v3 v3.0.0-00010101000000-000000000000
	github.com/cavaliergopher/cpio v1.0.1
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/cockroachdb/pebble v0.0.0-20220408210401-5591b6b889f9
	github.com/containerd/stargz-snapshotter/estargz v0.12.1
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/creack/pty v1.1.17
	github.com/crewjam/saml v0.4.6
	github.com/docker/distribution v2.8.1+incompatible
	github.com/docker/docker v20.10.20+incompatible
	github.com/docker/go-units v0.5.0
	github.com/elastic/gosigar v0.11.0
	github.com/firecracker-microvm/firecracker-go-sdk v0.22.1-0.20220812215434-490e3369bc97
	github.com/go-faker/faker/v4 v4.0.0-beta.3
	github.com/go-git/go-git/v5 v5.2.0
	github.com/go-redis/redis/extra/redisotel/v8 v8.10.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-redsync/redsync/v4 v4.4.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gobwas/glob v0.2.3
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/google/go-cmp v0.5.9
	github.com/google/go-containerregistry v0.12.0
	github.com/google/go-github/v43 v43.0.0
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/tink/go v1.7.0
	github.com/google/uuid v1.3.0
	github.com/groob/plist v0.0.0-20210519001750-9f754062e6d6
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hashicorp/memberlist v0.3.1
	github.com/hashicorp/serf v0.9.6
	github.com/jhump/protoreflect v1.9.0
	github.com/jsimonetti/rtnetlink v0.0.0-20210714135244-af39de65d6ad
	github.com/klauspost/compress v1.15.12
	github.com/lestrrat-go/jwx v1.2.11
	github.com/lni/dragonboat/v3 v3.3.4
	github.com/logrusorgru/aurora v2.0.3+incompatible
	github.com/mattn/go-shellwords v1.0.12
	github.com/mattn/go-sqlite3 v1.14.12
	github.com/mdlayher/vsock v1.1.1
	github.com/mitchellh/go-ps v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.2
	github.com/prometheus/client_model v0.2.0
	github.com/rs/zerolog v1.20.0
	github.com/shirou/gopsutil/v3 v3.22.10
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.1
	github.com/tebeka/selenium v0.9.9
	github.com/throttled/throttled/v2 v2.0.0-00010101000000-000000000000
	github.com/vishvananda/netlink v1.1.1-0.20210330154013-f5de75959ad5
	go.opentelemetry.io/contrib/detectors/gcp v1.11.1
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.36.4
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.36.4
	go.opentelemetry.io/otel v1.11.1
	go.opentelemetry.io/otel/exporters/jaeger v1.11.1
	go.opentelemetry.io/otel/sdk v1.11.1
	go.opentelemetry.io/otel/trace v1.11.1
	golang.org/x/crypto v0.1.0
	golang.org/x/oauth2 v0.1.0
	golang.org/x/sync v0.1.0
	golang.org/x/sys v0.1.0
	golang.org/x/text v0.4.0
	golang.org/x/time v0.1.0
	google.golang.org/api v0.102.0
	google.golang.org/genproto v0.0.0-20221027153422-115e99e71e1c
	google.golang.org/grpc v1.50.1
	google.golang.org/protobuf v1.28.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/driver/clickhouse v0.4.2
	gorm.io/driver/mysql v1.3.4
	gorm.io/driver/sqlite v1.3.6
	gorm.io/gorm v1.23.7
)

require (
	cloud.google.com/go v0.105.0 // indirect
	cloud.google.com/go/compute v1.12.1 // indirect
	cloud.google.com/go/iam v0.6.0 // indirect
	cloud.google.com/go/longrunning v0.2.1 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/BurntSushi/toml v1.2.1 // indirect
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v0.34.1 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.1.1 // indirect
	github.com/Masterminds/sprig/v3 v3.2.2 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/VictoriaMetrics/metrics v1.23.0 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/asaskevich/govalidator v0.0.0-20210307081110-f21760c49a8d // indirect
	github.com/beevik/etree v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/bmatcuk/doublestar/v4 v4.3.0 // indirect
	github.com/census-instrumentation/opencensus-proto v0.2.1 // indirect
	github.com/cncf/udpa/go v0.0.0-20210930031921-04548b0d99d4 // indirect
	github.com/cncf/xds/go v0.0.0-20211011173535-cb28da3451f1 // indirect
	github.com/cockroachdb/errors v1.9.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/containerd/fifo v1.0.0 // indirect
	github.com/containernetworking/cni v1.1.2 // indirect
	github.com/containernetworking/plugins v1.1.1 // indirect
	github.com/crewjam/httperr v0.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/cli v20.10.21+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.7.0 // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/envoyproxy/go-control-plane v0.10.2-0.20220325020618-49ff273808a1 // indirect
	github.com/envoyproxy/protoc-gen-validate v0.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/getsentry/sentry-go v0.14.0 // indirect
	github.com/go-git/gcfg v1.5.0 // indirect
	github.com/go-git/go-billy/v5 v5.3.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.21.4 // indirect
	github.com/go-openapi/errors v0.20.3 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/loads v0.21.2 // indirect
	github.com/go-openapi/runtime v0.24.2 // indirect
	github.com/go-openapi/spec v0.20.7 // indirect
	github.com/go-openapi/strfmt v0.21.3 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/go-openapi/validate v0.22.0 // indirect
	github.com/go-redis/redis/extra/rediscmd/v8 v8.11.5 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.4.2 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-version v1.6.0 // indirect
	github.com/huandu/xstrings v1.3.1 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/jbenet/go-context v0.0.0-20150711004518-d14ea06fba99 // indirect
	github.com/jinzhu/configor v1.2.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jonboulle/clockwork v0.3.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/josharian/native v1.0.0 // indirect
	github.com/juju/ratelimit v1.0.2 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/kevinburke/ssh_config v1.2.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.1 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/lni/goutils v1.3.1-0.20220404072553-ddb2075d2587 // indirect
	github.com/lni/vfs v0.2.1-0.20220408085249-8be85be1c3c1 // indirect
	github.com/lufia/plan9stats v0.0.0-20220913051719-115f729f3c8c // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattermost/xml-roundtrip-validator v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.9 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mdlayher/netlink v1.6.0 // indirect
	github.com/mdlayher/socket v0.2.0 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/miekg/dns v1.1.50 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.1 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0-rc2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/paulmach/orb v0.7.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/pquerna/cachecontrol v0.1.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/russellhaering/goxmldsig v1.1.1 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.5.0 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/vbatts/tar-split v0.11.2 // indirect
	github.com/vishvananda/netns v0.0.0-20221028002847-e414ad8e04b4 // indirect
	github.com/xanzy/ssh-agent v0.3.2 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.mongodb.org/mongo-driver v1.10.4 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/otel/metric v0.33.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/goleak v1.1.12 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.23.0 // indirect
	golang.org/x/exp v0.0.0-20221031165847-c99f073a8326 // indirect
	golang.org/x/mod v0.6.0 // indirect
	golang.org/x/net v0.1.0 // indirect
	golang.org/x/term v0.1.0 // indirect
	golang.org/x/tools v0.2.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/square/go-jose.v2 v2.6.0 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
)
