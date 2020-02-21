load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

def install_buildbuddy_dependencies():
    print("TYLER THE DEPENDENCIES THEY ARE LOADING")
    # gRPC
    go_repository(
	name = "org_golang_google_grpc",
	build_file_proto_mode = "disable",
	importpath = "google.golang.org/grpc",
	sum = "h1:2dTRdpdFEEhJYQD8EMLB61nnrzSCTbG38PhqdhvOltg=",
	version = "v1.26.0",
    )

    go_repository(
	name = "com_github_google_uuid",
	importpath = "github.com/google/uuid",
	sum = "h1:Gkbcsh/GbpXz7lPftLA3P6TYMwjCLYm83jiFQZF/3gY=",
	version = "v1.1.1",
    )

    # Needed for go dependencies.
    go_repository(
	name = "in_gopkg_check_v1",
	importpath = "gopkg.in/check.v1",
	sum = "h1:Co6ibVJAznAaIkqp8huTwlJQCZ016jof/cbN4VW5Yz0=",
	version = "v0.0.0-20161208181325-20d25e280405",
    )

    go_repository(
	name = "in_gopkg_yaml_v2",
	importpath = "gopkg.in/yaml.v2",
	sum = "h1:obN1ZagJSUGI0Ek/LBmuj4SNLPfIny3KsKFopxRdj10=",
	version = "v2.2.8",
    )

    go_repository(
	name = "com_github_denisenkom_go_mssqldb",
	importpath = "github.com/denisenkom/go-mssqldb",
	sum = "h1:83Wprp6ROGeiHFAP8WJdI2RoxALQYgdllERc3N5N2DM=",
	version = "v0.0.0-20191124224453-732737034ffd",
    )

    go_repository(
	name = "com_github_erikstmartin_go_testdb",
	importpath = "github.com/erikstmartin/go-testdb",
	sum = "h1:Yzb9+7DPaBjB8zlTR87/ElzFsnQfuHnVUVqpZZIcV5Y=",
	version = "v0.0.0-20160219214506-8d10e4a1bae5",
    )

    go_repository(
	name = "com_github_go_sql_driver_mysql",
	importpath = "github.com/go-sql-driver/mysql",
	sum = "h1:g24URVg0OFbNUTx9qqY1IRZ9D9z3iPyi5zKhQZpNwpA=",
	version = "v1.4.1",
    )

    go_repository(
	name = "com_github_golang_protobuf",
	importpath = "github.com/golang/protobuf",
	sum = "h1:P3YflyNX/ehuJFLhxviNdFxQPkGK5cDcApsge1SqnvM=",
	version = "v1.2.0",
    )

    go_repository(
	name = "com_github_golang_sql_civil",
	importpath = "github.com/golang-sql/civil",
	sum = "h1:lXe2qZdvpiX5WZkZR4hgp4KJVfY3nMkvmwbVkpv1rVY=",
	version = "v0.0.0-20190719163853-cb61b32ac6fe",
    )

    go_repository(
	name = "com_github_jinzhu_gorm",
	importpath = "github.com/jinzhu/gorm",
	sum = "h1:Drgk1clyWT9t9ERbzHza6Mj/8FY/CqMyVzOiHviMo6Q=",
	version = "v1.9.12",
    )

    go_repository(
	name = "com_github_jinzhu_inflection",
	importpath = "github.com/jinzhu/inflection",
	sum = "h1:K317FqzuhWc8YvSVlFMCCUb36O/S9MCKRDI7QkRKD/E=",
	version = "v1.0.0",
    )

    go_repository(
	name = "com_github_jinzhu_now",
	importpath = "github.com/jinzhu/now",
	sum = "h1:HjfetcXq097iXP0uoPCdnM4Efp5/9MsM0/M+XOTeR3M=",
	version = "v1.0.1",
    )

    go_repository(
	name = "com_github_lib_pq",
	importpath = "github.com/lib/pq",
	sum = "h1:sJZmqHoEaY7f+NPP8pgLB/WxulyR3fewgCM2qaSlBb4=",
	version = "v1.1.1",
    )

    go_repository(
	name = "com_github_mattn_go_sqlite3",
	importpath = "github.com/mattn/go-sqlite3",
	sum = "h1:xQ15muvnzGBHpIpdrNi1DA5x0+TcBZzsIDwmw9uTHzw=",
	version = "v2.0.1+incompatible",
    )

    go_repository(
	name = "org_golang_google_appengine",
	importpath = "google.golang.org/appengine",
	sum = "h1:/wp5JvzpHIxhs/dumFmF7BXTf3Z+dd4uXta4kVyO508=",
	version = "v1.4.0",
    )

    go_repository(
	name = "org_golang_x_crypto",
	importpath = "golang.org/x/crypto",
	sum = "h1:GGJVjV8waZKRHrgwvtH66z9ZGVurTD1MT0n1Bb+q4aM=",
	version = "v0.0.0-20191205180655-e7c4368fe9dd",
    )

    go_repository(
	name = "org_golang_x_net",
	importpath = "golang.org/x/net",
	sum = "h1:0GoQqolDA55aaLxZyTzK/Y2ePZzZTUrRacwib7cNsYQ=",
	version = "v0.0.0-20190404232315-eb5bcb51f2a3",
    )

    go_repository(
	name = "org_golang_x_sys",
	importpath = "golang.org/x/sys",
	sum = "h1:+R4KGOnez64A81RvjARKc4UT5/tI9ujCIVX+P5KiHuI=",
	version = "v0.0.0-20190412213103-97732733099d",
    )

    go_repository(
	name = "org_golang_x_text",
	importpath = "golang.org/x/text",
	sum = "h1:g61tztE5qeGQ89tm6NTjjM9VPIm088od1l6aSorWRWg=",
	version = "v0.3.0",
    )
