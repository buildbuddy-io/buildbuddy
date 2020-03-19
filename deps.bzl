load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

def install_buildbuddy_dependencies():
    # gRPC
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable",
        importpath = "google.golang.org/grpc",
        sum = "h1:bO/TA4OxCOummhSf10siHuG7vJOiwh7SpRpFZDkOgl4=",
        version = "v1.28.0",
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
        sum = "h1:qIbj1fsPNlZgppZ+VLlY7N33q108Sa+fhmuc+sWQYwY=",
        version = "v1.0.0-20180628173108-788fd7840127",
    )

    go_repository(
        name = "in_gopkg_yaml_v2",
        importpath = "gopkg.in/yaml.v2",
        sum = "h1:ZCJp+EgiOT7lHqUV2J862kp8Qj64Jo6az82+3Td9dZw=",
        version = "v2.2.2",
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
        sum = "h1:6nsPYzhq5kReh6QImI3k5qWzO4PEbvbIW2cwSfR/6xs=",
        version = "v1.3.2",
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
        sum = "h1:tycE03LOZYQNhDpS27tcQdAzLCVMaj7QT2SXxebnpCM=",
        version = "v1.6.5",
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
        sum = "h1:efeOvDhwQ29Dj3SdAV/MJf8oukgn+8D8WgaCaRMchF8=",
        version = "v0.0.0-20191209160850-c0dbc17a3553",
    )

    go_repository(
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        sum = "h1:JA8d3MPx/IToSyXZG/RhwYEtfrKO1Fxrqe8KrkiLXKM=",
        version = "v0.0.0-20191228213918-04cbcbbfeed8",
    )

    go_repository(
        name = "org_golang_x_text",
        importpath = "golang.org/x/text",
        sum = "h1:tW2bmiBqwgJj/UpqtC8EpXEZVYOwU0yG4iWbprSVAcs=",
        version = "v0.3.2",
    )

    go_repository(
	name = "co_honnef_go_tools",
	importpath = "honnef.co/go/tools",
	sum = "h1:3JgtbtFHMiCmsznwGVTUWbgGov+pVqnlf1dEJTNAXeM=",
	version = "v0.0.1-2019.2.3",
    )

    go_repository(
	name = "com_github_burntsushi_toml",
	importpath = "github.com/BurntSushi/toml",
	sum = "h1:WXkYYl6Yr3qBf1K79EBnL4mak0OimBfB0XUf9Vl28OQ=",
	version = "v0.3.1",
    )

    go_repository(
	name = "com_github_burntsushi_xgb",
	importpath = "github.com/BurntSushi/xgb",
	sum = "h1:1BDTz0u9nC3//pOCMdNH+CiXJVYJh5UQNCOBG7jbELc=",
	version = "v0.0.0-20160522181843-27f122750802",
    )

    go_repository(
	name = "com_github_census_instrumentation_opencensus_proto",
	importpath = "github.com/census-instrumentation/opencensus-proto",
	sum = "h1:glEXhBS5PSLLv4IXzLA5yPRVX4bilULVyxxbrfOtDAk=",
	version = "v0.2.1",
    )

    go_repository(
	name = "com_github_client9_misspell",
	importpath = "github.com/client9/misspell",
	sum = "h1:ta993UF76GwbvJcIo3Y68y/M3WxlpEHPWIGDkJYwzJI=",
	version = "v0.3.4",
    )

    go_repository(
	name = "com_github_davecgh_go_spew",
	importpath = "github.com/davecgh/go-spew",
	sum = "h1:ZDRjVQ15GmhC3fiQ8ni8+OwkZQO4DARzQgrnXU1Liz8=",
	version = "v1.1.0",
    )

    go_repository(
	name = "com_github_envoyproxy_go_control_plane",
	importpath = "github.com/envoyproxy/go-control-plane",
	sum = "h1:4cmBvAEBNJaGARUEs3/suWRyfyBfhf7I60WBZq+bv2w=",
	version = "v0.9.1-0.20191026205805-5f8ba28d4473",
    )

    go_repository(
	name = "com_github_envoyproxy_protoc_gen_validate",
	importpath = "github.com/envoyproxy/protoc-gen-validate",
	sum = "h1:EQciDnbrYxy13PgWoY8AqoxGiPrpgBZ1R8UNe3ddc+A=",
	version = "v0.1.0",
    )

    go_repository(
	name = "com_github_go_gl_glfw",
	importpath = "github.com/go-gl/glfw",
	sum = "h1:QbL/5oDUmRBzO9/Z7Seo6zf912W/a6Sr4Eu0G/3Jho0=",
	version = "v0.0.0-20190409004039-e6da0acd62b1",
    )

    go_repository(
	name = "com_github_go_gl_glfw_v3_3_glfw",
	importpath = "github.com/go-gl/glfw/v3.3/glfw",
	sum = "h1:b+9H1GAsx5RsjvDFLoS5zkNBzIQMuVKUYQDmxU3N5XE=",
	version = "v0.0.0-20191125211704-12ad95a8df72",
    )

    go_repository(
	name = "com_github_golang_glog",
	importpath = "github.com/golang/glog",
	sum = "h1:VKtxabqXZkF25pY9ekfRL6a582T4P37/31XEstQ5p58=",
	version = "v0.0.0-20160126235308-23def4e6c14b",
    )

    go_repository(
	name = "com_github_golang_groupcache",
	importpath = "github.com/golang/groupcache",
	sum = "h1:5ZkaAPbicIKTF2I64qf5Fh8Aa83Q/dnOafMYV0OMwjA=",
	version = "v0.0.0-20191227052852-215e87163ea7",
    )

    go_repository(
	name = "com_github_golang_mock",
	importpath = "github.com/golang/mock",
	sum = "h1:qGJ6qTW+x6xX/my+8YUVl4WNpX9B7+/l2tRsHGZ7f2s=",
	version = "v1.3.1",
    )

    go_repository(
	name = "com_github_google_btree",
	importpath = "github.com/google/btree",
	sum = "h1:0udJVsspx3VBr5FwtLhQQtuAsVc79tTq0ocGIPAU6qo=",
	version = "v1.0.0",
    )

    go_repository(
	name = "com_github_google_go_cmp",
	importpath = "github.com/google/go-cmp",
	sum = "h1:Xye71clBPdm5HgqGwUkwhbynsUJZhDbS20FvLhQ2izg=",
	version = "v0.3.1",
    )

    go_repository(
	name = "com_github_google_martian",
	importpath = "github.com/google/martian",
	sum = "h1:/CP5g8u/VJHijgedC/Legn3BAbAaWPgecwXBIDzw5no=",
	version = "v2.1.0+incompatible",
    )

    go_repository(
	name = "com_github_google_pprof",
	importpath = "github.com/google/pprof",
	sum = "h1:Jnx61latede7zDD3DiiP4gmNz33uK0U5HDUaF0a/HVQ=",
	version = "v0.0.0-20190515194954-54271f7e092f",
    )

    go_repository(
	name = "com_github_google_renameio",
	importpath = "github.com/google/renameio",
	sum = "h1:GOZbcHa3HfsPKPlmyPyN2KEohoMXOhdMbHrvbpl2QaA=",
	version = "v0.1.0",
    )

    go_repository(
	name = "com_github_googleapis_gax_go_v2",
	importpath = "github.com/googleapis/gax-go/v2",
	sum = "h1:sjZBwGj9Jlw33ImPtvFviGYvseOtDM7hkSKB7+Tv3SM=",
	version = "v2.0.5",
    )

    go_repository(
	name = "com_github_hashicorp_golang_lru",
	importpath = "github.com/hashicorp/golang-lru",
	sum = "h1:0hERBMJE1eitiLkihrMvRVBYAkpHzc/J3QdDN+dAcgU=",
	version = "v0.5.1",
    )

    go_repository(
	name = "com_github_jstemmer_go_junit_report",
	importpath = "github.com/jstemmer/go-junit-report",
	sum = "h1:6QPYqodiu3GuPL+7mfx+NwDdp2eTkp9IfEUpgAwUN0o=",
	version = "v0.9.1",
    )

    go_repository(
	name = "com_github_kisielk_gotool",
	importpath = "github.com/kisielk/gotool",
	sum = "h1:AV2c/EiW3KqPNT9ZKl07ehoAGi4C5/01Cfbblndcapg=",
	version = "v1.0.0",
    )

    go_repository(
	name = "com_github_kr_pretty",
	importpath = "github.com/kr/pretty",
	sum = "h1:L/CwN0zerZDmRFUapSPitk6f+Q3+0za1rQkzVuMiMFI=",
	version = "v0.1.0",
    )

    go_repository(
	name = "com_github_kr_pty",
	importpath = "github.com/kr/pty",
	sum = "h1:VkoXIwSboBpnk99O/KFauAEILuNHv5DVFKZMBN/gUgw=",
	version = "v1.1.1",
    )

    go_repository(
	name = "com_github_kr_text",
	importpath = "github.com/kr/text",
	sum = "h1:45sCR5RtlFHMR4UwH9sdQ5TC8v0qDQCHnXt+kaKSTVE=",
	version = "v0.1.0",
    )

    go_repository(
	name = "com_github_pmezard_go_difflib",
	importpath = "github.com/pmezard/go-difflib",
	sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
	version = "v1.0.0",
    )

    go_repository(
	name = "com_github_prometheus_client_model",
	importpath = "github.com/prometheus/client_model",
	sum = "h1:gQz4mCbXsO+nc9n1hCxHcGA3Zx3Eo+UHZoInFGUIXNM=",
	version = "v0.0.0-20190812154241-14fe0d1b01d4",
    )

    go_repository(
	name = "com_github_rogpeppe_go_internal",
	importpath = "github.com/rogpeppe/go-internal",
	sum = "h1:RR9dF3JtopPvtkroDZuVD7qquD0bnHlKSqaQhgwt8yk=",
	version = "v1.3.0",
    )

    go_repository(
	name = "com_github_stretchr_objx",
	importpath = "github.com/stretchr/objx",
	sum = "h1:4G4v2dO3VZwixGIRoQ5Lfboy6nUhCyYzaqnIAPPhYs4=",
	version = "v0.1.0",
    )

    go_repository(
	name = "com_github_stretchr_testify",
	importpath = "github.com/stretchr/testify",
	sum = "h1:2E4SXV/wtOkTonXsotYi4li6zVWxYlZuYNCXe9XRJyk=",
	version = "v1.4.0",
    )

    go_repository(
	name = "com_google_cloud_go",
	importpath = "cloud.google.com/go",
	sum = "h1:0E3eE8MX426vUOs7aHfI7aN1BrIzzzf4ccKCSfSjGmc=",
	version = "v0.50.0",
    )

    go_repository(
	name = "com_google_cloud_go_bigquery",
	importpath = "cloud.google.com/go/bigquery",
	sum = "h1:sAbMqjY1PEQKZBWfbu6Y6bsupJ9c4QdHnzg/VvYTLcE=",
	version = "v1.3.0",
    )

    go_repository(
	name = "com_google_cloud_go_datastore",
	importpath = "cloud.google.com/go/datastore",
	sum = "h1:Kt+gOPPp2LEPWp8CSfxhsM8ik9CcyE/gYu+0r+RnZvM=",
	version = "v1.0.0",
    )

    go_repository(
	name = "com_google_cloud_go_pubsub",
	importpath = "cloud.google.com/go/pubsub",
	sum = "h1:9/vpR43S4aJaROxqQHQ3nH9lfyKKV0dC3vOmnw8ebQQ=",
	version = "v1.1.0",
    )

    go_repository(
	name = "com_google_cloud_go_storage",
	importpath = "cloud.google.com/go/storage",
	sum = "h1:RPUcBvDeYgQFMfQu1eBMq6piD1SXmLH+vK3qjewZPus=",
	version = "v1.5.0",
    )

    go_repository(
	name = "com_shuralyov_dmitri_gpu_mtl",
	importpath = "dmitri.shuralyov.com/gpu/mtl",
	sum = "h1:VpgP7xuJadIUuKccphEpTJnWhS2jkQyMt6Y7pJCD7fY=",
	version = "v0.0.0-20190408044501-666a987793e9",
    )

    go_repository(
	name = "in_gopkg_errgo_v2",
	importpath = "gopkg.in/errgo.v2",
	sum = "h1:0vLT13EuvQ0hNvakwLuFZ/jYrLp5F3kcWHXdRggjCE8=",
	version = "v2.1.0",
    )

    go_repository(
	name = "io_opencensus_go",
	importpath = "go.opencensus.io",
	sum = "h1:75k/FF0Q2YM8QYo07VPddOLBslDt1MZOdEslOHvmzAs=",
	version = "v0.22.2",
    )

    go_repository(
	name = "io_rsc_binaryregexp",
	importpath = "rsc.io/binaryregexp",
	sum = "h1:HfqmD5MEmC0zvwBuF187nq9mdnXjXsSivRiXN7SmRkE=",
	version = "v0.2.0",
    )

    go_repository(
	name = "org_golang_google_api",
	importpath = "google.golang.org/api",
	sum = "h1:yzlyyDW/J0w8yNFJIhiAJy4kq74S+1DOLdawELNxFMA=",
	version = "v0.15.0",
    )

    go_repository(
	name = "org_golang_google_genproto",
	importpath = "google.golang.org/genproto",
	sum = "h1:ADPHZzpzM4tk4V4S5cnCrr5SwzvlrPRmqqCuJDB8UTs=",
	version = "v0.0.0-20191230161307-f3c370f40bfb",
    )

    go_repository(
	name = "org_golang_x_exp",
	importpath = "golang.org/x/exp",
	sum = "h1:zQpM52jfKHG6II1ISZY1ZcpygvuSFZpLwfluuF89XOg=",
	version = "v0.0.0-20191227195350-da58074b4299",
    )

    go_repository(
	name = "org_golang_x_image",
	importpath = "golang.org/x/image",
	sum = "h1:+qEpEAPhDZ1o0x3tHzZTQDArnOixOzGD9HUJfcg0mb4=",
	version = "v0.0.0-20190802002840-cff245a6509b",
    )

    go_repository(
	name = "org_golang_x_lint",
	importpath = "golang.org/x/lint",
	sum = "h1:J5lckAjkw6qYlOZNj90mLYNTEKDvWeuc1yieZ8qUzUE=",
	version = "v0.0.0-20191125180803-fdd1cda4f05f",
    )

    go_repository(
	name = "org_golang_x_mobile",
	importpath = "golang.org/x/mobile",
	sum = "h1:4+4C/Iv2U4fMZBiMCc98MG1In4gJY5YRhtpDNeDeHWs=",
	version = "v0.0.0-20190719004257-d2bd2a29d028",
    )

    go_repository(
	name = "org_golang_x_mod",
	importpath = "golang.org/x/mod",
	sum = "h1:WG0RUwxtNT4qqaXX3DPA8zHFNm/D9xaBpxzHt1WcA/E=",
	version = "v0.1.1-0.20191105210325-c90efee705ee",
    )

    go_repository(
	name = "org_golang_x_oauth2",
	importpath = "golang.org/x/oauth2",
	sum = "h1:pE8b58s1HRDMi8RDc79m0HISf9D4TzseP40cEA6IGfs=",
	version = "v0.0.0-20191202225959-858c2ad4c8b6",
    )

    go_repository(
	name = "org_golang_x_sync",
	importpath = "golang.org/x/sync",
	sum = "h1:vcxGaoTs7kV8m5Np9uUNQin4BrLOthgV7252N8V+FwY=",
	version = "v0.0.0-20190911185100-cd5d95a43a6e",
    )

    go_repository(
	name = "org_golang_x_time",
	importpath = "golang.org/x/time",
	sum = "h1:SvFZT6jyqRaOeXpc5h/JSfZenJ2O330aBsf7JfSUXmQ=",
	version = "v0.0.0-20190308202827-9d24e82272b4",
    )

    go_repository(
	name = "org_golang_x_tools",
	importpath = "golang.org/x/tools",
	sum = "h1:Toz2IK7k8rbltAXwNAxKcn9OzqyNfMUhUNjz3sL0NMk=",
	version = "v0.0.0-20191227053925-7b8e75db28f4",
    )

    go_repository(
	name = "org_golang_x_xerrors",
	importpath = "golang.org/x/xerrors",
	sum = "h1:/atklqdjdhuosWIl6AIbOeHJjicWYPqR9bpxqxYG2pA=",
	version = "v0.0.0-20191011141410-1b5146add898",
    )