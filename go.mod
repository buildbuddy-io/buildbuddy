module github.com/buildbuddy-io/buildbuddy

go 1.16

replace github.com/firecracker-microvm/firecracker-go-sdk => github.com/tylerwilliams/firecracker-go-sdk v0.22.1

require (
	cloud.google.com/go/storage v1.12.0
	github.com/GoogleCloudPlatform/cloudsql-proxy v1.17.0
	github.com/aws/aws-sdk-go v1.35.37
	github.com/bazelbuild/rules_go v0.24.3
	github.com/bojand/ghz v0.95.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/containerd/containerd v1.5.2
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/docker/distribution v2.7.1+incompatible
	github.com/docker/docker v20.10.7+incompatible
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0
	github.com/elastic/gosigar v0.11.0
	github.com/firecracker-microvm/firecracker-go-sdk v0.0.0-00010101000000-000000000000
	github.com/go-git/go-git/v5 v5.2.0
	github.com/go-redis/redis/extra/redisotel/v8 v8.10.0
	github.com/go-redis/redis/v8 v8.10.0
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.6
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.2.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/jhump/protoreflect v1.8.2
	github.com/jsimonetti/rtnetlink v0.0.0-20210714135244-af39de65d6ad
	github.com/klauspost/pgzip v1.2.5
	github.com/lib/pq v1.5.2 // indirect
	github.com/logrusorgru/aurora v2.0.3+incompatible
	github.com/mattn/go-shellwords v1.0.11
	github.com/mattn/go-sqlite3 v2.0.3+incompatible // indirect
	github.com/mdlayher/vsock v0.0.0-20210303205602-10d591861736
	github.com/moby/term v0.0.0-20201216013528-df9cb8a40635 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/opencontainers/runtime-spec v1.0.3-0.20200929063507-e6143ca7d51d
	github.com/pkg/errors v0.9.1
	github.com/pquerna/cachecontrol v0.0.0-20201205024021-ac21108117ac // indirect
	github.com/prometheus/client_golang v1.7.1
	github.com/rs/zerolog v1.20.0
	github.com/sirupsen/logrus v1.8.0
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/contrib/detectors/gcp v0.22.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.22.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.22.0
	go.opentelemetry.io/otel v1.0.0-RC2
	go.opentelemetry.io/otel/exporters/jaeger v1.0.0-RC2
	go.opentelemetry.io/otel/sdk v1.0.0-RC2
	go.opentelemetry.io/otel/trace v1.0.0-RC2
	golang.org/x/crypto v0.0.0-20210322153248-0c34fe9e7dc2
	golang.org/x/oauth2 v0.0.0-20210628180205-a41e5a781914
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	google.golang.org/api v0.50.0
	google.golang.org/genproto v0.0.0-20210721163202-f1cecdd8b78a
	google.golang.org/grpc v1.39.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v2 v2.4.0
	gorm.io/driver/mysql v1.0.4
	gorm.io/driver/sqlite v1.1.4
	gorm.io/gorm v1.20.12
)
