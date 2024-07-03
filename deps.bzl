load("@bazel_gazelle//:deps.bzl", "go_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# The podman version used in tests and distributed with the buildbuddy executor image.
# When changing this version, a new release of podman-static may be needed.
# See dockerfiles/executor_image/README.md for instructions.
# The checksums below will also need to be updated.
PODMAN_VERSION = "v4.9.4"
PODMAN_STATIC_SHA256_AMD64 = "ac14152d2ef5cb25abbd615893cd56355f951f075a7ae663f0b2c1bc5d3fb77e"
PODMAN_STATIC_SHA256_ARM64 = "72ab4a9c6ae9d5d00200070cb43b0921117898925a284bfacbfb3873aa82e598"

# bazelisk run //:gazelle -- update-repos -from_file=go.mod -to_macro=deps.bzl%install_go_mod_dependencies -prune
def install_go_mod_dependencies(workspace_name = "buildbuddy"):
    go_repository(
        name = "cat_dario_mergo",
        importpath = "dario.cat/mergo",
        sum = "h1:AGCNq9Evsj31mOgNPcLyXc+4PNABt905YmuqPYYpBWk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "co_honnef_go_tools",
        importpath = "honnef.co/go/tools",
        sum = "h1:1kJlrWJLkaGXgcaeosRXViwviqjI7nkBvU2+sZW0AYc=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_alecaivazis_survey_v2",
        importpath = "github.com/AlecAivazis/survey/v2",
        sum = "h1:6I/u8FvytdGsgonrYsVn2t8t4QiRnh6QSTqkkhIiSjQ=",
        version = "v2.3.7",
    )
    go_repository(
        name = "com_github_alecthomas_template",
        importpath = "github.com/alecthomas/template",
        sum = "h1:JYp7IbQjafoB+tBA3gMyHYHrpOtNuDiK/uB5uXxq5wM=",
        version = "v0.0.0-20190718012654-fb15b899a751",
    )
    go_repository(
        name = "com_github_andybalholm_brotli",
        importpath = "github.com/andybalholm/brotli",
        sum = "h1:eLKJA0d02Lf0mVpIDgYnqXcUn0GqVmEFny3VuID1U3M=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_armon_circbuf",
        importpath = "github.com/armon/circbuf",
        sum = "h1:7Ip0wMmLHLRJdrloDxZfhMm0xrLXZS8+COSu2bXmEQs=",
        version = "v0.0.0-20190214190532-5111143e8da2",
    )
    go_repository(
        name = "com_github_armon_go_metrics",
        importpath = "github.com/armon/go-metrics",
        sum = "h1:hR91U9KYmb6bLBYLQjyM+3j+rcd/UhE+G78SFnF8gJA=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_asaskevich_govalidator",
        importpath = "github.com/asaskevich/govalidator",
        sum = "h1:DklsrG3dyBCFEj5IhUbnKptjxatkF07cF2ak3yi77so=",
        version = "v0.0.0-20230301143203-a9d515a09cc2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go",
        importpath = "github.com/aws/aws-sdk-go",
        sum = "h1:bLnBVutuyCGYZgQlu3wiXOJXtgI7EIWAaDIqVVudF3w=",
        version = "v1.44.286",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2",
        importpath = "github.com/aws/aws-sdk-go-v2",
        sum = "h1:+tefE750oAb7ZQGzla6bLkOwfcQCEtC5y2RqoqCeqKo=",
        version = "v1.18.1",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_aws_protocol_eventstream",
        importpath = "github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream",
        sum = "h1:dK82zF6kkPeCo8J1e+tGx4JdvDIQzj7ygIoLg8WMuGs=",
        version = "v1.4.10",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_config",
        importpath = "github.com/aws/aws-sdk-go-v2/config",
        sum = "h1:Az9uLwmssTE6OGTpsFqOnaGpLnKDqNYOJzWuC6UAYzA=",
        version = "v1.18.27",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_credentials",
        importpath = "github.com/aws/aws-sdk-go-v2/credentials",
        sum = "h1:qmU+yhKmOCyujmuPY7tf5MxR/RKyZrOPO3V4DobiTUk=",
        version = "v1.13.26",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_feature_ec2_imds",
        importpath = "github.com/aws/aws-sdk-go-v2/feature/ec2/imds",
        sum = "h1:LxK/bitrAr4lnh9LnIS6i7zWbCOdMsfzKFBI6LUCS0I=",
        version = "v1.13.4",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_feature_rds_auth",
        importpath = "github.com/aws/aws-sdk-go-v2/feature/rds/auth",
        sum = "h1:2v4gCEM/SfIMZiMT8luKNmgrngfZo61YPENzedO22n0=",
        version = "v1.2.12",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_feature_s3_manager",
        importpath = "github.com/aws/aws-sdk-go-v2/feature/s3/manager",
        sum = "h1:4bh28MeeXoBFTjb0JjQ5sVatzlf5xA1DziV8mZed9v4=",
        version = "v1.11.70",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_configsources",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/configsources",
        sum = "h1:A5UqQEmPaCFpedKouS4v+dHCTUo2sKqhoKO9U5kxyWo=",
        version = "v1.1.34",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_endpoints_v2",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/endpoints/v2",
        sum = "h1:srIVS45eQuewqz6fKKu6ZGXaq6FuFg5NzgQBAM6g8Y4=",
        version = "v2.4.28",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_ini",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/ini",
        sum = "h1:LWA+3kDM8ly001vJ1X1waCuLJdtTl48gwkPKWy9sosI=",
        version = "v1.3.35",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_v4a",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/v4a",
        sum = "h1:wscW+pnn3J1OYnanMnza5ZVYXLX4cKk5rAvUAl4Qu+c=",
        version = "v1.0.26",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_accept_encoding",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding",
        sum = "h1:y2+VQzC6Zh2ojtV2LoC0MNwHWc6qXv/j2vrQtlftkdA=",
        version = "v1.9.11",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_checksum",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/checksum",
        sum = "h1:zZSLP3v3riMOP14H7b4XP0uyfREDQOYv2cqIrvTXDNQ=",
        version = "v1.1.29",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_presigned_url",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/presigned-url",
        sum = "h1:bkRyG4a929RCnpVSTvLM2j/T4ls015ZhhYApbmYs15s=",
        version = "v1.9.28",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_s3shared",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/s3shared",
        sum = "h1:dBL3StFxHtpBzJJ/mNEsjXVgfO+7jR0dAIEwLqMapEA=",
        version = "v1.14.3",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_s3",
        importpath = "github.com/aws/aws-sdk-go-v2/service/s3",
        sum = "h1:ya7fmrN2fE7s1P2gaPbNg5MTkERVWfsH8ToP1YC4Z9o=",
        version = "v1.35.0",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_sso",
        importpath = "github.com/aws/aws-sdk-go-v2/service/sso",
        sum = "h1:nneMBM2p79PGWBQovYO/6Xnc2ryRMw3InnDJq1FHkSY=",
        version = "v1.12.12",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_ssooidc",
        importpath = "github.com/aws/aws-sdk-go-v2/service/ssooidc",
        sum = "h1:2qTR7IFk7/0IN/adSFhYu9Xthr0zVFTgBrmPldILn80=",
        version = "v1.14.12",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_sts",
        importpath = "github.com/aws/aws-sdk-go-v2/service/sts",
        sum = "h1:XFJ2Z6sNUUcAz9poj+245DMkrHE4h2j5I9/xD50RHfE=",
        version = "v1.19.2",
    )
    go_repository(
        name = "com_github_aws_smithy_go",
        importpath = "github.com/aws/smithy-go",
        sum = "h1:hgz0X/DX0dGqTYpGALqXJoRKRj5oQ7150i5FdTePzO8=",
        version = "v1.13.5",
    )
    go_repository(
        name = "com_github_awslabs_soci_snapshotter",
        build_directives = [
            "gazelle:go_grpc_compilers @io_bazel_rules_go//proto:go_proto,@io_bazel_rules_go//proto:go_grpc_v2",
        ],
        importpath = "github.com/awslabs/soci-snapshotter",
        patch_args = ["-p1"],
        patches = ["@{}//buildpatches:com_github_awslabs_soci_snapshotter.patch".format(workspace_name)],
        replace = "github.com/buildbuddy-io/soci-snapshotter",
        sum = "h1:skoAXrwJCa9YsOP/+ojXX2Wge/syyFRcP+imYwEGxWg=",
        version = "v0.0.8",
    )
    go_repository(
        name = "com_github_azure_azure_pipeline_go",
        importpath = "github.com/Azure/azure-pipeline-go",
        sum = "h1:7U9HBg1JFK3jHl5qmo4CTZKFTVgMwdFHMVtCdfBE21U=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_azure_azure_storage_blob_go",
        importpath = "github.com/Azure/azure-storage-blob-go",
        sum = "h1:rXtgp8tN1p29GvpGgfJetavIG0V7OgcSXPpwp3tx6qk=",
        version = "v0.15.0",
    )
    go_repository(
        name = "com_github_bazelbuild_bazel_gazelle",
        importpath = "github.com/bazelbuild/bazel-gazelle",
        sum = "h1:vCNhz75HxeeLUkDMhDkNeDSJfjUROMswex+NyYLPY6A=",
        version = "v0.37.0",
    )
    go_repository(
        name = "com_github_bazelbuild_bazelisk",
        build_file_generation = "on",
        importpath = "github.com/bazelbuild/bazelisk",
        sum = "h1:TOh9touXYcKKiZqE+Dj4XD/FHrzsqmu9iDvPTsW9jIc=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_github_bazelbuild_buildtools",
        importpath = "github.com/bazelbuild/buildtools",
        patch_args = ["-p1"],
        patches = ["@{}//buildpatches:buildifier.patch".format(workspace_name)],
        sum = "h1:3UwzfrfwoxlyGlPhbQR1O1HLOd4qNEyAwxHRSE+Yde4=",
        version = "v0.0.0-20240606140350-80f1f6802857",
    )
    go_repository(
        name = "com_github_bazelbuild_rules_go",
        importpath = "github.com/bazelbuild/rules_go",
        sum = "h1:fZgo6mCUKeL/+GQiMWy5/QU1FjNXGPnTd5bAeao1pbg=",
        version = "v0.48.0",
    )
    go_repository(
        name = "com_github_bazelbuild_rules_webtesting",
        importpath = "github.com/bazelbuild/rules_webtesting",
        sum = "h1:zmBkl2nxjRojoW9PtGVEtW/91kHieotlD7cL9vFq06c=",
        version = "v0.0.0-20210910170740-6b2ef24cfe95",
    )
    go_repository(
        name = "com_github_bduffany_godemon",
        importpath = "github.com/bduffany/godemon",
        sum = "h1:Th4pNly+xoH2szOq4aA+uAFmg5h37BCyq3pFqnZGJ70=",
        version = "v0.0.0-20221115232931-09721d48e30e",
    )
    go_repository(
        name = "com_github_beevik_etree",
        importpath = "github.com/beevik/etree",
        sum = "h1:l7WETslUG/T+xOPs47dtd6jov2Ii/8/OjCldk5fYfQw=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_beorn7_perks",
        importpath = "github.com/beorn7/perks",
        sum = "h1:VlbKKnNfV8bJzeqoa4cOKqO6bYr3WgKZxO8Z16+hsOM=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_bgentry_go_netrc",
        importpath = "github.com/bgentry/go-netrc",
        sum = "h1:xDfNPAt8lFiC1UJrqV3uuy861HCTo708pDMbjHHdCas=",
        version = "v0.0.0-20140422174119-9fd32a8b3d3d",
    )
    go_repository(
        name = "com_github_bits_and_blooms_bitset",
        importpath = "github.com/bits-and-blooms/bitset",
        sum = "h1:U/q1fAF7xXRhFCrhROzIfffYnu+dlS38vCZtmFVPHmA=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_github_blang_semver",
        importpath = "github.com/blang/semver",
        sum = "h1:cQNTCjp13qL8KC3Nbxr/y2Bqb63oX6wdnnjpJbkM4JQ=",
        version = "v3.5.1+incompatible",
    )
    go_repository(
        name = "com_github_bmatcuk_doublestar_v4",
        importpath = "github.com/bmatcuk/doublestar/v4",
        sum = "h1:FH9SifrbvJhnlQpztAx++wlkk70QBf0iBWDwNy7PA4I=",
        version = "v4.6.1",
    )
    go_repository(
        name = "com_github_bojand_ghz",
        build_directives = [
            "gazelle:resolve go github.com/prometheus/client_model/go @{}//proto:prometheus_client_go_proto".format(workspace_name),
        ],
        importpath = "github.com/bojand/ghz",
        sum = "h1:6F4wsmZVwFg5UnD+/R+IABWk6sKE/0OKIBdUQUZnOdo=",
        version = "v0.120.0",
    )
    go_repository(
        name = "com_github_bradfitz_gomemcache",
        importpath = "github.com/bradfitz/gomemcache",
        sum = "h1:Dr+ezPI5ivhMn/3WOoB86XzMhie146DNaBbhaQWZHMY=",
        version = "v0.0.0-20230611145640-acc696258285",
    )
    go_repository(
        name = "com_github_bufbuild_protocompile",
        importpath = "github.com/bufbuild/protocompile",
        sum = "h1:mixz5lJX4Hiz4FpqFREJHIXLfaLBntfaJv1h+/jS+Qg=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_github_buildbuddy_io_tensorflow_proto",
        build_directives = [
            "gazelle:go_grpc_compilers @io_bazel_rules_go//proto:go_proto,@io_bazel_rules_go//proto:go_grpc_v2",
        ],
        importpath = "github.com/buildbuddy-io/tensorflow-proto",
        sum = "h1:LcKnQdAYrT3LOZt0mTgw6uiEi7QVkH75cCxPj+AbkLA=",
        version = "v0.0.0-20220908151343-929b41ab4dc6",
    )
    go_repository(
        name = "com_github_buildkite_terminal_to_html_v3",
        importpath = "github.com/buildkite/terminal-to-html/v3",
        replace = "github.com/buildbuddy-io/terminal-to-html/v3",
        sum = "h1:XMnC81zIjWBw5SRc/TqPOP4vHPftVhdNl5azJrOwz+Y=",
        version = "v3.7.0-patched-1",
    )
    go_repository(
        name = "com_github_burntsushi_toml",
        importpath = "github.com/BurntSushi/toml",
        sum = "h1:o7IhLm0Msx3BaB+n3Ag7L8EVlByGnpq14C4YWiu/gL8=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_cavaliergopher_cpio",
        importpath = "github.com/cavaliergopher/cpio",
        sum = "h1:KQFSeKmZhv0cr+kawA3a0xTQCU4QxXF1vhU7P7av2KM=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        build_directives = [
            "gazelle:resolve go github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1 //gen-go/resource/v1:resource",
        ],
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sum = "h1:iKLQ0xPNFxR/2hzXZMrBo8f1j86j5WHzznCCQxV/b8g=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_cespare_xxhash_v2",
        importpath = "github.com/cespare/xxhash/v2",
        sum = "h1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=",
        version = "v2.3.0",
    )
    go_repository(
        name = "com_github_chzyer_readline",
        importpath = "github.com/chzyer/readline",
        sum = "h1:upd/6fQk4src78LMRzh5vItIt361/o4uq553V8B5sGI=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_clickhouse_ch_go",
        importpath = "github.com/ClickHouse/ch-go",
        sum = "h1:X/QmUmFhpUvLgPSQb7fWOSi1wvqGn6tJ7w2a59c4xsg=",
        version = "v0.57.0",
    )
    go_repository(
        name = "com_github_clickhouse_clickhouse_go_v2",
        importpath = "github.com/ClickHouse/clickhouse-go/v2",
        sum = "h1:WCnusqEeCO/9sLFVIv57le/O1ydUb+x9+SYYhJ11fsY=",
        version = "v2.10.1",
    )
    go_repository(
        name = "com_github_cloudflare_circl",
        importpath = "github.com/cloudflare/circl",
        patch_args = ["-p1"],
        patches = ["@{}//buildpatches:cloudflare_circl.patch".format(workspace_name)],
        sum = "h1:qlCDlTPz2n9fu58M0Nh1J/JzcFpfgkFHHX3O35r5vcU=",
        version = "v1.3.7",
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        importpath = "github.com/cncf/xds/go",
        sum = "h1:jQCWAUqqlij9Pgj2i/PB79y4KOPYVyFYdROxgaCwdTQ=",
        version = "v0.0.0-20231128003011-0fa0005c9caa",
    )
    go_repository(
        name = "com_github_cockroachdb_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/errors",
        sum = "h1:5bA+k2Y6r+oz/6Z/RFlNeVCesGARKuC6YymtcDrbC/I=",
        version = "v1.11.3",
    )
    go_repository(
        name = "com_github_cockroachdb_fifo",
        importpath = "github.com/cockroachdb/fifo",
        sum = "h1:f52TdbU4D5nozMAhO9TvTJ2ZMCXtN4VIAmfrrZ0JXQ4=",
        version = "v0.0.0-20240616162244-4768e80dfb9a",
    )
    go_repository(
        name = "com_github_cockroachdb_logtags",
        importpath = "github.com/cockroachdb/logtags",
        sum = "h1:r6VH0faHjZeQy818SGhaone5OnYfxFR/+AzdY3sf5aE=",
        version = "v0.0.0-20230118201751-21c54148d20b",
    )
    go_repository(
        name = "com_github_cockroachdb_pebble",
        importpath = "github.com/cockroachdb/pebble",
        sum = "h1:XnKU22oiCLy2Xn8vp1re67cXg4SAasg/WDt1NtcRFaw=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_cockroachdb_redact",
        importpath = "github.com/cockroachdb/redact",
        sum = "h1:u1PMllDkdFfPWaNGMyLD1+so+aq3uUItthCFqzwPJ30=",
        version = "v1.1.5",
    )
    go_repository(
        name = "com_github_cockroachdb_tokenbucket",
        importpath = "github.com/cockroachdb/tokenbucket",
        sum = "h1:zuQyyAKVxetITBuuhv3BI9cMrmStnpT18zmgmTxunpo=",
        version = "v0.0.0-20230807174530-cc333fc44b06",
    )
    go_repository(
        name = "com_github_containerd_containerd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/containerd",
        sum = "h1:H/XLzbnGuenZEGK+v0RkwTdv2u1QFAruMe5N0GNPJwA=",
        version = "v1.7.14",
    )
    go_repository(
        name = "com_github_containerd_fifo",
        importpath = "github.com/containerd/fifo",
        sum = "h1:4I2mbh5stb1u6ycIABlBw9zgtlK8viPI9QkQNRQEEmY=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_containerd_log",
        importpath = "github.com/containerd/log",
        sum = "h1:TCJt7ioM2cr/tfR8GPbGf9/VRAX8D2B4PjzCpfX540I=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_containerd_stargz_snapshotter_estargz",
        importpath = "github.com/containerd/stargz-snapshotter/estargz",
        sum = "h1:OqlDCK3ZVUO6C3B/5FSkDwbkEETK84kQgEeFwDC+62k=",
        version = "v0.14.3",
    )
    go_repository(
        name = "com_github_containernetworking_cni",
        importpath = "github.com/containernetworking/cni",
        sum = "h1:wtRGZVv7olUHMOqouPpn3cXJWpJgM6+EUl31EQbXALQ=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_containernetworking_plugins",
        importpath = "github.com/containernetworking/plugins",
        sum = "h1:QVNXMT6XloyMUoO2wUOqWTC1hWFV62Q6mVDp5H1HnjM=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_coreos_go_oidc",
        importpath = "github.com/coreos/go-oidc",
        sum = "h1:mh48q/BqXqgjVHpy2ZY7WnWAbenxRjsz9N1i1YxjHAk=",
        version = "v2.2.1+incompatible",
    )
    go_repository(
        name = "com_github_creack_pty",
        importpath = "github.com/creack/pty",
        sum = "h1:n56/Zwd5o6whRC5PMGretI4IdRLlmBXYNjScPaBgsbY=",
        version = "v1.1.18",
    )
    go_repository(
        name = "com_github_crewjam_httperr",
        importpath = "github.com/crewjam/httperr",
        sum = "h1:b2BfXR8U3AlIHwNeFFvZ+BV1LFvKLlzMjzaTnZMybNo=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_crewjam_saml",
        importpath = "github.com/crewjam/saml",
        sum = "h1:g9FBNx62osKusnFzs3QTN5L9CVA/Egfgm+stJShzw/c=",
        version = "v0.4.14",
    )
    go_repository(
        name = "com_github_cyphar_filepath_securejoin",
        importpath = "github.com/cyphar/filepath-securejoin",
        sum = "h1:Ugdm7cg7i6ZK6x3xDF1oEu1nfkyfH53EtKeQYTC3kyg=",
        version = "v0.2.4",
    )
    go_repository(
        name = "com_github_datadog_zstd",
        importpath = "github.com/DataDog/zstd",
        sum = "h1:oWf5W7GtOLgp6bciQYDmhHHjdhYkALu6S/5Ni9ZgSvQ=",
        version = "v1.5.5",
    )
    go_repository(
        name = "com_github_davecgh_go_spew",
        importpath = "github.com/davecgh/go-spew",
        sum = "h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_decred_dcrd_dcrec_secp256k1_v4",
        importpath = "github.com/decred/dcrd/dcrec/secp256k1/v4",
        sum = "h1:8UrgZ3GkP4i/CLijOJx79Yu+etlyjdBU4sfcs2WYQMs=",
        version = "v4.2.0",
    )
    go_repository(
        name = "com_github_dgryski_go_rendezvous",
        importpath = "github.com/dgryski/go-rendezvous",
        sum = "h1:lO4WD4F/rVNCu3HqELle0jiPLLBs70cWOduZpkS1E78=",
        version = "v0.0.0-20200823014737-9f7001d12a5f",
    )
    go_repository(
        name = "com_github_distribution_reference",
        importpath = "github.com/distribution/reference",
        sum = "h1:/FUIFXtfc/x2gpa5/VGfiGLuOIdYa1t65IKK2OFGvA0=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_dlclark_regexp2",
        importpath = "github.com/dlclark/regexp2",
        sum = "h1:+/GIL799phkJqYW+3YbOd8LCcbHzT0Pbo8zl70MHsq0=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_docker_cli",
        importpath = "github.com/docker/cli",
        sum = "h1:QdqR7znue1mtkXIJ+ruQMGQhpw2JzMJLRXp6zpzF6tM=",
        version = "v24.0.2+incompatible",
    )
    go_repository(
        name = "com_github_docker_distribution",
        importpath = "github.com/docker/distribution",
        sum = "h1:T3de5rq0dB1j30rp0sA2rER+m322EBzniBPB6ZIzuh8=",
        version = "v2.8.2+incompatible",
    )
    go_repository(
        name = "com_github_docker_docker",
        importpath = "github.com/docker/docker",
        sum = "h1:yGVmKUFGgcxA6PXWAokO0sQL22BrQ67cgVjko8tGdXE=",
        version = "v26.0.2+incompatible",
    )
    go_repository(
        name = "com_github_docker_docker_credential_helpers",
        importpath = "github.com/docker/docker-credential-helpers",
        sum = "h1:xtCHsjxogADNZcdv1pKUHXryefjlVRqWqIhk/uXJp0A=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_github_docker_go_connections",
        importpath = "github.com/docker/go-connections",
        sum = "h1:El9xVISelRB7BuFusrZozjnkIM5YnzCViNKohAFqRJQ=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_docker_go_units",
        importpath = "github.com/docker/go-units",
        sum = "h1:69rxXcBk27SvSaaxTtLh/8llcHD8vYHT7WSdRZ/jvr4=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_dop251_goja",
        importpath = "github.com/dop251/goja",
        sum = "h1:+9NRIliCUhliHMCixEO0mcXmrv3HYwxs9oxM1Z+qnYM=",
        version = "v0.0.0-20230626124041-ba8a63e79201",
    )
    go_repository(
        name = "com_github_dustin_go_humanize",
        importpath = "github.com/dustin/go-humanize",
        sum = "h1:GzkhY7T5VNhEkwH0PVJgjz+fX1rhBrR7pRT3mDkpeCY=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_elastic_gosigar",
        importpath = "github.com/elastic/gosigar",
        sum = "h1:Dg80n8cr90OZ7x+bAax/QjoW/XqTI11RmA79ZwIm9/4=",
        version = "v0.14.2",
    )
    go_repository(
        name = "com_github_emirpasic_gods",
        importpath = "github.com/emirpasic/gods",
        sum = "h1:FXtiHYKDGKCW2KzwZKx0iC0PQmdlorYgdFG9jPXJ1Bc=",
        version = "v1.18.1",
    )
    go_repository(
        name = "com_github_envoyproxy_go_control_plane",
        importpath = "github.com/envoyproxy/go-control-plane",
        sum = "h1:4X+VP1GHd1Mhj6IB5mMeGbLCleqxjletLK6K0rbxyZI=",
        version = "v0.12.0",
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sum = "h1:gVPz/FMfvh57HdSJQyvBtF00j8JU4zdyUgIUNhlgg0A=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_felixge_httpsnoop",
        importpath = "github.com/felixge/httpsnoop",
        sum = "h1:NFTV2Zj1bL4mc9sqWACXbQFVBBg2W3GPvqp8/ESS2Wg=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_firecracker_microvm_firecracker_go_sdk",
        importpath = "github.com/firecracker-microvm/firecracker-go-sdk",
        patch_args = ["-p1"],
        # TODO(bduffany): when
        # https://github.com/firecracker-microvm/firecracker-go-sdk/pull/510 is
        # merged, cherry-pick the final revision into our fork, and remove this
        # patch.
        patches = ["@{}//buildpatches:com_github_firecracker_microvm_firecracker_go_sdk_jailer.patch".format(workspace_name)],
        replace = "github.com/buildbuddy-io/firecracker-go-sdk",
        sum = "h1:Wx6fPNZOs0SJ9NpTsLbJsItORvEM9D94k/vr8ZwIBEg=",
        version = "v0.0.0-20230721-1d5c50b",
    )
    go_repository(
        name = "com_github_fsnotify_fsnotify",
        importpath = "github.com/fsnotify/fsnotify",
        sum = "h1:8JEhPFa5W2WU7YfeZzPNqzMP6Lwt7L2715Ggo0nosvA=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_gabriel_vasile_mimetype",
        importpath = "github.com/gabriel-vasile/mimetype",
        sum = "h1:QjV6pZ7/XZ7ryI2KuyeEDE8wnh7fHP9YnQy+R0LnH8I=",
        version = "v1.4.4",
    )
    go_repository(
        name = "com_github_getsentry_sentry_go",
        importpath = "github.com/getsentry/sentry-go",
        sum = "h1:zzaSm/vHmGllRM6Tpx1492r0YDzauArdBfkJRtY6P5k=",
        version = "v0.28.1",
    )
    go_repository(
        name = "com_github_go_chi_chi_v5",
        importpath = "github.com/go-chi/chi/v5",
        sum = "h1:rDTPXLDHGATaeHvVlLcR4Qe0zftYethFucbjVQ1PxU8=",
        version = "v5.0.7",
    )
    go_repository(
        name = "com_github_go_enry_go_enry_v2",
        importpath = "github.com/go-enry/go-enry/v2",
        sum = "h1:vbab0pcf5Yo1cHQLzbWZ+QomUh3EfEU8EiR5n7W0lnQ=",
        version = "v2.8.7",
    )
    go_repository(
        name = "com_github_go_enry_go_oniguruma",
        importpath = "github.com/go-enry/go-oniguruma",
        sum = "h1:k8aAMuJfMrqm/56SG2lV9Cfti6tC4x8673aHCcBk+eo=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_go_faker_faker_v4",
        importpath = "github.com/go-faker/faker/v4",
        sum = "h1:zjTxJMHn7Po7OCPKY+VjO6mNQ4ZzE7PoBjb2sUNHVPs=",
        version = "v4.0.0-beta.3",
    )
    go_repository(
        name = "com_github_go_faster_city",
        importpath = "github.com/go-faster/city",
        sum = "h1:4WAxSZ3V2Ws4QRDrscLEDcibJY8uf41H6AhXDrNDcGw=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_go_faster_errors",
        importpath = "github.com/go-faster/errors",
        sum = "h1:nNIPOBkprlKzkThvS/0YaX8Zs9KewLCOSFQS5BU06FI=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_go_git_gcfg",
        importpath = "github.com/go-git/gcfg",
        sum = "h1:+zs/tPmkDkHx3U66DAb0lQFJrpS6731Oaa12ikc+DiI=",
        version = "v1.5.1-0.20230307220236-3a3c6141e376",
    )
    go_repository(
        name = "com_github_go_git_go_billy_v5",
        importpath = "github.com/go-git/go-billy/v5",
        sum = "h1:yEY4yhzCDuMGSv83oGxiBotRzhwhNr8VZyphhiu+mTU=",
        version = "v5.5.0",
    )
    go_repository(
        name = "com_github_go_git_go_git_v5",
        importpath = "github.com/go-git/go-git/v5",
        sum = "h1:XIZc1p+8YzypNr34itUfSvYJcv+eYdTnTvOZ2vD3cA4=",
        version = "v5.11.0",
    )
    go_repository(
        name = "com_github_go_logr_logr",
        importpath = "github.com/go-logr/logr",
        sum = "h1:pKouT5E8xu9zeFC39JXRDukb6JFQPXM5p5I91188VAQ=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_github_go_logr_stdr",
        importpath = "github.com/go-logr/stdr",
        sum = "h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_go_ole_go_ole",
        importpath = "github.com/go-ole/go-ole",
        sum = "h1:/Fpf6oFPoeFik9ty7siob0G6Ke8QvQEuVcuChpwXzpY=",
        version = "v1.2.6",
    )
    go_repository(
        name = "com_github_go_openapi_analysis",
        importpath = "github.com/go-openapi/analysis",
        sum = "h1:ZDFLvSNxpDaomuCueM0BlSXxpANBlFYiBvr+GXrvIHc=",
        version = "v0.21.4",
    )
    go_repository(
        name = "com_github_go_openapi_errors",
        importpath = "github.com/go-openapi/errors",
        sum = "h1:unTcVm6PispJsMECE3zWgvG4xTiKda1LIR5rCRWLG6M=",
        version = "v0.20.4",
    )
    go_repository(
        name = "com_github_go_openapi_jsonpointer",
        importpath = "github.com/go-openapi/jsonpointer",
        sum = "h1:eCs3fxoIi3Wh6vtgmLTOjdhSpiqphQ+DaPn38N2ZdrE=",
        version = "v0.19.6",
    )
    go_repository(
        name = "com_github_go_openapi_jsonreference",
        importpath = "github.com/go-openapi/jsonreference",
        sum = "h1:3sVjiK66+uXK/6oQ8xgcRKcFgQ5KXa2KvnJRumpMGbE=",
        version = "v0.20.2",
    )
    go_repository(
        name = "com_github_go_openapi_loads",
        importpath = "github.com/go-openapi/loads",
        sum = "h1:r2a/xFIYeZ4Qd2TnGpWDIQNcP80dIaZgf704za8enro=",
        version = "v0.21.2",
    )
    go_repository(
        name = "com_github_go_openapi_runtime",
        importpath = "github.com/go-openapi/runtime",
        sum = "h1:HYOFtG00FM1UvqrcxbEJg/SwvDRvYLQKGhw2zaQjTcc=",
        version = "v0.26.0",
    )
    go_repository(
        name = "com_github_go_openapi_spec",
        importpath = "github.com/go-openapi/spec",
        sum = "h1:xnlYNQAwKd2VQRRfwTEI0DcK+2cbuvI/0c7jx3gA8/8=",
        version = "v0.20.9",
    )
    go_repository(
        name = "com_github_go_openapi_strfmt",
        importpath = "github.com/go-openapi/strfmt",
        sum = "h1:rspiXgNWgeUzhjo1YU01do6qsahtJNByjLVbPLNHb8k=",
        version = "v0.21.7",
    )
    go_repository(
        name = "com_github_go_openapi_swag",
        importpath = "github.com/go-openapi/swag",
        sum = "h1:QLMzNJnMGPRNDCbySlcj1x01tzU8/9LTTL9hZZZogBU=",
        version = "v0.22.4",
    )
    go_repository(
        name = "com_github_go_openapi_validate",
        importpath = "github.com/go-openapi/validate",
        sum = "h1:G+c2ub6q47kfX1sOBLwIQwzBVt8qmOAARyo/9Fqs9NU=",
        version = "v0.22.1",
    )
    go_repository(
        name = "com_github_go_redis_redis_extra_rediscmd_v8",
        importpath = "github.com/go-redis/redis/extra/rediscmd/v8",
        sum = "h1:ftG8tp8SG81xyuL2woNEx5t2RZ8mOJuC2+tumi+/NR8=",
        version = "v8.11.5",
    )
    go_repository(
        name = "com_github_go_redis_redis_extra_redisotel_v8",
        importpath = "github.com/go-redis/redis/extra/redisotel/v8",
        sum = "h1:BqyYJgvdSr2S/6O2l7zmCj26ocUTxDLgagsGIRfkS+Q=",
        version = "v8.11.5",
    )
    go_repository(
        name = "com_github_go_redis_redis_v8",
        importpath = "github.com/go-redis/redis/v8",
        sum = "h1:AcZZR7igkdvfVmQTPnu9WE37LRrO/YrBH5zWyjDC0oI=",
        version = "v8.11.5",
    )
    go_repository(
        name = "com_github_go_sourcemap_sourcemap",
        importpath = "github.com/go-sourcemap/sourcemap",
        sum = "h1:W1iEw64niKVGogNgBN3ePyLFfuisuzeidWPMPWmECqU=",
        version = "v2.1.3+incompatible",
    )
    go_repository(
        name = "com_github_go_sql_driver_mysql",
        importpath = "github.com/go-sql-driver/mysql",
        sum = "h1:lUIinVbN1DY0xBg0eMOzmmtGoHwWBbvnWubQUrtU8EI=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_github_gobwas_glob",
        importpath = "github.com/gobwas/glob",
        sum = "h1:A4xDbljILXROh+kObIiy5kIaPYD8e96x1tgBhUI5J+Y=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_goccy_go_json",
        importpath = "github.com/goccy/go-json",
        sum = "h1:CrxCmQqYDkv1z7lO7Wbh2HN93uovUHgrECaO5ZrCXAU=",
        version = "v0.10.2",
    )
    go_repository(
        name = "com_github_gogo_protobuf",
        build_file_generation = "off",
        importpath = "github.com/gogo/protobuf",
        patch_args = ["-p1"],
        patches = ["@io_bazel_rules_go//third_party:com_github_gogo_protobuf-gazelle.patch"],
        sum = "h1:Ov1cvc58UF3b5XjBnZv7+opcTcQFZebYjWzi34vdm4Q=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_golang_glog",
        importpath = "github.com/golang/glog",
        sum = "h1:uCdmnmatrKCgMBlM4rMuJZWOkPDqdbZPnrMXDY4gI68=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_golang_groupcache",
        importpath = "github.com/golang/groupcache",
        sum = "h1:oI5xCqsCo564l8iNU+DwB5epxmsaqB+rhGL0m5jtYqE=",
        version = "v0.0.0-20210331224755-41bb18bfe9da",
    )
    go_repository(
        name = "com_github_golang_jwt_jwt",
        importpath = "github.com/golang-jwt/jwt",
        sum = "h1:IfV12K8xAKAnZqdXVzCZ+TOjboZ2keLg81eXfW3O+oY=",
        version = "v3.2.2+incompatible",
    )
    go_repository(
        name = "com_github_golang_jwt_jwt_v4",
        importpath = "github.com/golang-jwt/jwt/v4",
        sum = "h1:7cYmW1XlMY7h7ii7UhUyChSgS5wUJEnm9uZVTGqOWzg=",
        version = "v4.5.0",
    )
    go_repository(
        name = "com_github_golang_protobuf",
        importpath = "github.com/golang/protobuf",
        sum = "h1:i7eJL8qZTpSEXOPTxNKhASYpMn+8e5Q6AdndVa1dWek=",
        version = "v1.5.4",
    )
    go_repository(
        name = "com_github_golang_snappy",
        importpath = "github.com/golang/snappy",
        sum = "h1:yAGX7huGHXlcLOEtBnF4w7FQwA26wojNCwOYAEhLjQM=",
        version = "v0.0.4",
    )
    go_repository(
        name = "com_github_google_btree",
        importpath = "github.com/google/btree",
        sum = "h1:xf4v41cLI2Z6FxbKm+8Bu+m8ifhj15JuZ9sa0jZCMUU=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_google_codesearch",
        importpath = "github.com/google/codesearch",
        sum = "h1:VlyAH+AntnIbGGArOUs6sEBdPVwYvf1e8Uw3/TC77cA=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_google_flatbuffers",
        importpath = "github.com/google/flatbuffers",
        sum = "h1:mTPHyMn3/qO7lvBcm5S9p0olWUQgtQhBf2QWiz1U3qA=",
        version = "v23.5.9+incompatible",
    )
    go_repository(
        name = "com_github_google_go_cmp",
        importpath = "github.com/google/go-cmp",
        sum = "h1:ofyhxvXcZhMsU5ulbFiLKl/XBFqE1GSq7atu8tAmTRI=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_google_go_containerregistry",
        importpath = "github.com/google/go-containerregistry",
        sum = "h1:MMkSh+tjSdnmJZO7ljvEqV1DjfekB6VUEAZgy3a+TQE=",
        version = "v0.15.2",
    )
    go_repository(
        name = "com_github_google_go_github_v59",
        importpath = "github.com/google/go-github/v59",
        sum = "h1:7h6bgpF5as0YQLLkEiVqpgtJqjimMYhBkD4jT5aN3VA=",
        version = "v59.0.0",
    )
    go_repository(
        name = "com_github_google_go_querystring",
        importpath = "github.com/google/go-querystring",
        sum = "h1:AnCroh3fv4ZBgVIf1Iwtovgjaw/GiKJo8M8yD/fhyJ8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_google_orderedcode",
        importpath = "github.com/google/orderedcode",
        sum = "h1:UzfcAexk9Vhv8+9pNOgRu41f16lHq725vPwnSeiG/Us=",
        version = "v0.0.1",
    )
    go_repository(
        name = "com_github_google_pprof",
        importpath = "github.com/google/pprof",
        sum = "h1:hR7/MlvK23p6+lIw9SN1TigNLn9ZnF3W4SYRKq2gAHs=",
        version = "v0.0.0-20230602150820-91b7bce49751",
    )
    go_repository(
        name = "com_github_google_s2a_go",
        importpath = "github.com/google/s2a-go",
        sum = "h1:60BLSyTrOV4/haCDW4zb1guZItoSq8foHCXrAnjBo/o=",
        version = "v0.1.7",
    )
    go_repository(
        name = "com_github_google_shlex",
        importpath = "github.com/google/shlex",
        sum = "h1:El6M4kTTCOh6aBiKaUGG7oYTSPP8MxqL4YI3kZKwcP4=",
        version = "v0.0.0-20191202100458-e7afc7fbc510",
    )
    go_repository(
        name = "com_github_google_tink_go",
        importpath = "github.com/google/tink/go",
        sum = "h1:6Eox8zONGebBFcCBqkVmt60LaWZa6xg1cl/DwAh/J1w=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_google_uuid",
        importpath = "github.com/google/uuid",
        sum = "h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_googleapis_enterprise_certificate_proxy",
        importpath = "github.com/googleapis/enterprise-certificate-proxy",
        sum = "h1:Vie5ybvEvT75RniqhfFxPRy3Bf7vr3h0cechB90XaQs=",
        version = "v0.3.2",
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        build_directives = [
            "gazelle:resolve proto go google/rpc/code.proto @org_golang_google_genproto_googleapis_rpc//code",
            "gazelle:resolve proto proto google/rpc/code.proto @googleapis//google/rpc:code_proto",
        ],
        importpath = "github.com/googleapis/gax-go/v2",
        sum = "h1:A+gCJKdRfqXkr+BIRGtZLibNXf0m1f9E4HG56etFpas=",
        version = "v2.12.0",
    )
    go_repository(
        name = "com_github_googlecloudplatform_cloudsql_proxy",
        importpath = "github.com/GoogleCloudPlatform/cloudsql-proxy",
        sum = "h1:PKOekn3HT5IdC0i/6xj/QGGXdCm/T57yP0RAbmtPa3k=",
        version = "v1.33.8",
    )
    go_repository(
        name = "com_github_googlecloudplatform_opentelemetry_operations_go_detectors_gcp",
        importpath = "github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp",
        sum = "h1:aNyyrkRcLMWFum5qgYbXl6Ut+MMOmfH/kLjZJ5YJP/I=",
        version = "v1.21.0",
    )
    go_repository(
        name = "com_github_gorilla_mux",
        importpath = "github.com/gorilla/mux",
        sum = "h1:gnP5JzjVOuiZD07fKKToCAOjS0yOpj/qPETTXCCS6hw=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_github_groob_plist",
        importpath = "github.com/groob/plist",
        sum = "h1:saaSiB25B1wgaxrshQhurfPKUGJ4It3OxNJUy0rdOjU=",
        version = "v0.0.0-20220217120414-63fa881b19a5",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_prometheus",
        importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
        sum = "h1:Ovs26xHkKqVztRpIrF/92BcuyuQ/YW4NSIpoGtfXNho=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_hanwen_go_fuse_v2",
        importpath = "github.com/hanwen/go-fuse/v2",
        patch_args = ["-p1"],
        patch_tool = "patch",
        patches = ["@{}//buildpatches:com_github_hanwen_go_fuse_v2.patch".format(workspace_name)],
        sum = "h1:t5ivNIH2PK+zw4OBul/iJjsoG9K6kXo4nMDoBpciC8A=",
        version = "v2.3.0",
    )
    go_repository(
        name = "com_github_hashicorp_errwrap",
        importpath = "github.com/hashicorp/errwrap",
        sum = "h1:OxrOeh75EUXMY8TBjag2fzXGZ40LB6IKw45YeGUDY2I=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_immutable_radix",
        importpath = "github.com/hashicorp/go-immutable-radix",
        sum = "h1:DKHmCUm2hRBK510BaiZlwvpD40f8bJFeZnpfm2KLowc=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_hashicorp_go_msgpack",
        importpath = "github.com/hashicorp/go-msgpack",
        sum = "h1:i9R9JSrqIz0QVLz3sz+i3YJdT7TTSLcfLLzJi9aZTuI=",
        version = "v0.5.5",
    )
    go_repository(
        name = "com_github_hashicorp_go_msgpack_v2",
        importpath = "github.com/hashicorp/go-msgpack/v2",
        sum = "h1:4Ee8FTp834e+ewB71RDrQ0VKpyFdrKOjvYtnQ/ltVj0=",
        version = "v2.1.2",
    )
    go_repository(
        name = "com_github_hashicorp_go_multierror",
        importpath = "github.com/hashicorp/go-multierror",
        sum = "h1:H5DkEtf6CXdFp0N0Em5UCwQpXMWke8IA0+lD48awMYo=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_hashicorp_go_sockaddr",
        importpath = "github.com/hashicorp/go-sockaddr",
        sum = "h1:RSG8rKU28VTUTvEKghe5gIhIQpv8evvNpnDEyqO4u9I=",
        version = "v1.0.6",
    )
    go_repository(
        name = "com_github_hashicorp_go_version",
        importpath = "github.com/hashicorp/go-version",
        sum = "h1:feTTfFNnjP967rlCxM/I9g701jU+RN74YKx2mOkIeek=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru",
        importpath = "github.com/hashicorp/golang-lru",
        sum = "h1:dV3g9Z/unq5DpblPpw+Oqcv4dU/1omnb4Ok8iPY6p1c=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_hashicorp_memberlist",
        importpath = "github.com/hashicorp/memberlist",
        sum = "h1:mk5dRuzeDNis2bi6LLoQIXfMH7JQvAzt3mQD0vNZZUo=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_github_hashicorp_serf",
        importpath = "github.com/hashicorp/serf",
        sum = "h1:Z1H2J60yRKvfDYAOZLd2MU0ND4AH/WDz7xYHDWQsIPY=",
        version = "v0.10.1",
    )
    go_repository(
        name = "com_github_huandu_xstrings",
        importpath = "github.com/huandu/xstrings",
        sum = "h1:D17IlohoQq4UcpqD7fDk80P7l+lwAmlFaBHgOipl2FU=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_github_imdario_mergo",
        importpath = "github.com/imdario/mergo",
        sum = "h1:wwQJbIsHYGMUyLSPrEq1CT16AhnhNJQ51+4fdHUnCl4=",
        version = "v0.3.16",
    )
    go_repository(
        name = "com_github_jackc_pgerrcode",
        importpath = "github.com/jackc/pgerrcode",
        sum = "h1:s+4MhCQ6YrzisK6hFJUX53drDT4UsSW3DEhKn0ifuHw=",
        version = "v0.0.0-20220416144525-469b46aa5efa",
    )
    go_repository(
        name = "com_github_jackc_pgpassfile",
        importpath = "github.com/jackc/pgpassfile",
        sum = "h1:/6Hmqy13Ss2zCq62VdNG8tM1wchn8zjSGOBJ6icpsIM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_jackc_pgservicefile",
        importpath = "github.com/jackc/pgservicefile",
        sum = "h1:bbPeKD0xmW/Y25WS6cokEszi5g+S0QxI/d45PkRi7Nk=",
        version = "v0.0.0-20221227161230-091c0ba34f0a",
    )
    go_repository(
        name = "com_github_jackc_pgx_v5",
        importpath = "github.com/jackc/pgx/v5",
        sum = "h1:amBjrZVmksIdNjxGW/IiIMzxMKZFelXbUoPNb+8sjQw=",
        version = "v5.5.5",
    )
    go_repository(
        name = "com_github_jackc_puddle_v2",
        importpath = "github.com/jackc/puddle/v2",
        sum = "h1:RhxXJtFG022u4ibrCSMSiu5aOq1i77R3OHKNJj77OAk=",
        version = "v2.2.1",
    )
    go_repository(
        name = "com_github_jbenet_go_context",
        importpath = "github.com/jbenet/go-context",
        sum = "h1:BQSFePA1RWJOlocH6Fxy8MmwDt+yVQYULKfN0RoTN8A=",
        version = "v0.0.0-20150711004518-d14ea06fba99",
    )
    go_repository(
        name = "com_github_jhump_protoreflect",
        build_directives = [
            "gazelle:resolve go github.com/jhump/protoreflect/internal/testprotos @com_github_jhump_protoreflect//internal/testprotos",
            "gazelle:proto disable",
        ],
        importpath = "github.com/jhump/protoreflect",
        sum = "h1:HUMERORf3I3ZdX05WaQ6MIpd/NJ434hTp5YiKgfCL6c=",
        version = "v1.15.1",
    )
    go_repository(
        name = "com_github_jinzhu_configor",
        importpath = "github.com/jinzhu/configor",
        sum = "h1:OKk9dsR8i6HPOCZR8BcMtcEImAFjIhbJFZNyn5GCZko=",
        version = "v1.2.1",
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
        sum = "h1:/o9tlHleP7gOFmsnYNz3RGnqzefHA47wQpKrrdTIwXQ=",
        version = "v1.1.5",
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath",
        importpath = "github.com/jmespath/go-jmespath",
        sum = "h1:BEgLn5cpjn8UN1mAw4NjwDrS35OdebyEtFe+9YPoQUg=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_jonboulle_clockwork",
        importpath = "github.com/jonboulle/clockwork",
        sum = "h1:p4Cf1aMWXnXAUh8lVfewRBx1zaTSYKrKMF2g3ST4RZ4=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_josharian_intern",
        importpath = "github.com/josharian/intern",
        sum = "h1:vlS4z54oSdjm0bgjRigI+G1HpF+tI+9rE5LLzOg8HmY=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_josharian_native",
        importpath = "github.com/josharian/native",
        sum = "h1:uuaP0hAbW7Y4l0ZRQ6C9zfb7Mg1mbFKry/xzDAfmtLA=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_jotfs_fastcdc_go",
        importpath = "github.com/jotfs/fastcdc-go",
        replace = "github.com/buildbuddy-io/fastcdc-go",
        sum = "h1:yu6OEmUHMQqiMQ3BOLlhYUe6O34nb4DJob7vt++E42k=",
        version = "v0.2.0-rc2",
    )
    go_repository(
        name = "com_github_jsimonetti_rtnetlink",
        importpath = "github.com/jsimonetti/rtnetlink",
        sum = "h1:ycpm3z8XlAzmaacVRjdUT3x6MM1o3YBXsXc7DXSRNCE=",
        version = "v1.3.3",
    )
    go_repository(
        name = "com_github_json_iterator_go",
        importpath = "github.com/json-iterator/go",
        sum = "h1:PV8peI4a0ysnczrg+LtxykD8LfKY9ML6u2jnxaEnrnM=",
        version = "v1.1.12",
    )
    go_repository(
        name = "com_github_kballard_go_shellquote",
        importpath = "github.com/kballard/go-shellquote",
        sum = "h1:Z9n2FFNUXsshfwJMBgNA0RU6/i7WVaAegv3PtuIHPMs=",
        version = "v0.0.0-20180428030007-95032a82bc51",
    )
    go_repository(
        name = "com_github_kevinburke_ssh_config",
        importpath = "github.com/kevinburke/ssh_config",
        sum = "h1:x584FjTGwHzMwvHx18PXxbBVzfnxogHaAReU4gf13a4=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_klauspost_compress",
        importpath = "github.com/klauspost/compress",
        sum = "h1:6KIumPrER1LHsvBVuDa0r5xaG0Es51mhhB9BQB2qeMA=",
        version = "v1.17.9",
    )
    go_repository(
        name = "com_github_klauspost_cpuid_v2",
        importpath = "github.com/klauspost/cpuid/v2",
        sum = "h1:0E5MSMDEoAulmXNFquVs//DdoomxaoTY1kUhbc/qbZg=",
        version = "v2.2.5",
    )
    go_repository(
        name = "com_github_kr_pretty",
        importpath = "github.com/kr/pretty",
        sum = "h1:flRD4NNwYAUpkphVc1HcthR4KEIFJ65n8Mw5qdRn3LE=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_kr_text",
        importpath = "github.com/kr/text",
        sum = "h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_lestrrat_go_backoff_v2",
        importpath = "github.com/lestrrat-go/backoff/v2",
        sum = "h1:oNb5E5isby2kiro9AgdHLv5N5tint1AnDVVf2E2un5A=",
        version = "v2.0.8",
    )
    go_repository(
        name = "com_github_lestrrat_go_blackmagic",
        importpath = "github.com/lestrrat-go/blackmagic",
        sum = "h1:Cg2gVSc9h7sz9NOByczrbUvLopQmXrfFx//N+AkAr5k=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_lestrrat_go_httpcc",
        importpath = "github.com/lestrrat-go/httpcc",
        sum = "h1:ydWCStUeJLkpYyjLDHihupbn2tYmZ7m22BGkcvZZrIE=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_lestrrat_go_iter",
        importpath = "github.com/lestrrat-go/iter",
        sum = "h1:gMXo1q4c2pHmC3dn8LzRhJfP1ceCbgSiT9lUydIzltI=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_lestrrat_go_jwx",
        importpath = "github.com/lestrrat-go/jwx",
        sum = "h1:QT0utmUJ4/12rmsVQrJ3u55bycPkKqGYuGT4tyRhxSQ=",
        version = "v1.2.29",
    )
    go_repository(
        name = "com_github_lestrrat_go_option",
        importpath = "github.com/lestrrat-go/option",
        sum = "h1:oAzP2fvZGQKWkvHa1/SAcFolBEca1oN+mQ7eooNBEYU=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_lni_dragonboat_v4",
        importpath = "github.com/lni/dragonboat/v4",
        replace = "github.com/buildbuddy-io/dragonboat/v4",
        sum = "h1:lbo2lPtfQvt7EnStYEyTRljRNdLzNCLuzvxQlObP/Pc=",
        version = "v4.0.1",
    )
    go_repository(
        name = "com_github_lni_goutils",
        importpath = "github.com/lni/goutils",
        sum = "h1:e1tNN+4zsbTpNvhG5cxirkH9Pdz96QAZ2j6+5tmjvqg=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_github_lni_vfs",
        importpath = "github.com/lni/vfs",
        replace = "github.com/buildbuddy-io/vfs",
        sum = "h1:MR//pf4kj1+XbgbmwJlUrm2tdfxToEGgW8z+1fl3phw=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_logrusorgru_aurora",
        importpath = "github.com/logrusorgru/aurora",
        sum = "h1:tOpm7WcpBTn4fjmVfgpQq0EfczGlG91VSDkswnjF5A8=",
        version = "v2.0.3+incompatible",
    )
    go_repository(
        name = "com_github_lufia_plan9stats",
        importpath = "github.com/lufia/plan9stats",
        sum = "h1:N9zuLhTvBSRt0gWSiJswwQ2HqDmtX/ZCDJURnKUt1Ik=",
        version = "v0.0.0-20230326075908-cb1d2100619a",
    )
    go_repository(
        name = "com_github_mailru_easyjson",
        importpath = "github.com/mailru/easyjson",
        sum = "h1:UGYAvKxe3sBsEDzO8ZeWOSlIQfWFlxbzLZe7hwFURr0=",
        version = "v0.7.7",
    )
    go_repository(
        name = "com_github_manifoldco_promptui",
        importpath = "github.com/manifoldco/promptui",
        sum = "h1:3V4HzJk1TtXW1MTZMP7mdlwbBpIinw3HztaIlYthEiA=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_github_masterminds_goutils",
        importpath = "github.com/Masterminds/goutils",
        sum = "h1:5nUrii3FMTL5diU80unEVvNevw1nH4+ZV4DSLVJLSYI=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_masterminds_semver_v3",
        importpath = "github.com/Masterminds/semver/v3",
        sum = "h1:RN9w6+7QoMeJVGyfmbcgs28Br8cvmnucEXnY0rYXWg0=",
        version = "v3.2.1",
    )
    go_repository(
        name = "com_github_masterminds_sprig_v3",
        importpath = "github.com/Masterminds/sprig/v3",
        sum = "h1:eL2fZNezLomi0uOLqjQoN6BfsDD+fyLtgbJMAj9n6YA=",
        version = "v3.2.3",
    )
    go_repository(
        name = "com_github_mattermost_xml_roundtrip_validator",
        importpath = "github.com/mattermost/xml-roundtrip-validator",
        sum = "h1:RXbVD2UAl7A7nOTR4u7E3ILa4IbtvKBHw64LDsmu9hU=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_mattn_go_colorable",
        importpath = "github.com/mattn/go-colorable",
        sum = "h1:fFA4WZxdEF4tXPZVKMLwD8oUnCTTo08duU7wxecdEvA=",
        version = "v0.1.13",
    )
    go_repository(
        name = "com_github_mattn_go_ieproxy",
        importpath = "github.com/mattn/go-ieproxy",
        sum = "h1:MQ/5BuGSgDAHZOJe6YY80IF2UVCfGkwfo6AeD7HtHYo=",
        version = "v0.0.11",
    )
    go_repository(
        name = "com_github_mattn_go_isatty",
        importpath = "github.com/mattn/go-isatty",
        sum = "h1:xfD0iDuEKnDkl03q4limB+vH+GxLEtL/jb4xVJSWWEY=",
        version = "v0.0.20",
    )
    go_repository(
        name = "com_github_mattn_go_shellwords",
        importpath = "github.com/mattn/go-shellwords",
        sum = "h1:M2zGm7EW6UQJvDeQxo4T51eKPurbeFbe8WtebGE2xrk=",
        version = "v1.0.12",
    )
    go_repository(
        name = "com_github_mattn_go_sqlite3",
        importpath = "github.com/mattn/go-sqlite3",
        patch_args = ["-p1"],
        patches = ["@{}//buildpatches:com_github_mattn_go_sqlite3.patch".format(workspace_name)],
        sum = "h1:mCRHCLDUBXgpKAqIKsaAaAsrAlbkeomtRFKXh2L6YIM=",
        version = "v1.14.17",
    )
    go_repository(
        name = "com_github_mdlayher_netlink",
        importpath = "github.com/mdlayher/netlink",
        sum = "h1:/UtM3ofJap7Vl4QWCPDGXY8d3GIY2UGSDbK+QWmY8/g=",
        version = "v1.7.2",
    )
    go_repository(
        name = "com_github_mdlayher_socket",
        importpath = "github.com/mdlayher/socket",
        sum = "h1:eM9y2/jlbs1M615oshPQOHZzj6R6wMT7bX5NPiQvn2U=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_mdlayher_vsock",
        importpath = "github.com/mdlayher/vsock",
        sum = "h1:pC1mTJTvjo1r9n9fbm7S1j04rCgCzhCOS5DY0zqHlnQ=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_mgutz_ansi",
        importpath = "github.com/mgutz/ansi",
        sum = "h1:5PJl274Y63IEHC+7izoQE9x6ikvDFZS2mDVS3drnohI=",
        version = "v0.0.0-20200706080929-d51e80ef957d",
    )
    go_repository(
        name = "com_github_microsoft_go_winio",
        importpath = "github.com/Microsoft/go-winio",
        sum = "h1:9/kr64B9VUZrLm5YYwbGtUJnMgqWVOdUAXu6Migciow=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_microsoft_hcsshim",
        importpath = "github.com/Microsoft/hcsshim",
        sum = "h1:68vKo2VN8DE9AdN4tnkWnmdhqdbpUFM8OF3Airm7fz8=",
        version = "v0.11.4",
    )
    go_repository(
        name = "com_github_miekg_dns",
        importpath = "github.com/miekg/dns",
        sum = "h1:nLxbwF3XxhwVSm8g9Dghm9MHPaUZuqhPiGL+675ZmEs=",
        version = "v1.1.61",
    )
    go_repository(
        name = "com_github_mitchellh_copystructure",
        importpath = "github.com/mitchellh/copystructure",
        sum = "h1:vpKXTN4ewci03Vljg/q9QvCGUDttBOGBIa15WveJJGw=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_mitchellh_go_homedir",
        importpath = "github.com/mitchellh/go-homedir",
        sum = "h1:lukF9ziXFxDFPkA1vsr5zpc1XuPDn/wFntq5mG+4E0Y=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_mitchellh_go_ps",
        importpath = "github.com/mitchellh/go-ps",
        sum = "h1:i6ampVEEF4wQFF+bkYfwYgY+F/uYJDktmvLPf7qIgjc=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mitchellh_mapstructure",
        importpath = "github.com/mitchellh/mapstructure",
        sum = "h1:jeMsZIYE/09sWLaz43PL7Gy6RuMjD2eJVyuac5Z2hdY=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_mitchellh_reflectwalk",
        importpath = "github.com/mitchellh/reflectwalk",
        sum = "h1:G2LzWKi524PWgd3mLHV8Y5k7s6XUvT0Gef6zxSIeXaQ=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_moby_docker_image_spec",
        importpath = "github.com/moby/docker-image-spec",
        sum = "h1:jMKff3w6PgbfSa69GfNg+zN/XLhfXJGnEx3Nl2EsFP0=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_modern_go_concurrent",
        importpath = "github.com/modern-go/concurrent",
        sum = "h1:TRLaZ9cD/w8PVh93nsPXa1VrQ6jlwL5oN8l14QlcNfg=",
        version = "v0.0.0-20180306012644-bacd9c7ef1dd",
    )
    go_repository(
        name = "com_github_modern_go_reflect2",
        importpath = "github.com/modern-go/reflect2",
        sum = "h1:xBagoLtFs94CBntxluKeaWgTMpvLxC4ur3nMaC9Gz0M=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_mschoch_smat",
        importpath = "github.com/mschoch/smat",
        sum = "h1:8imxQsjDm8yFEAVBe7azKmKSgzSkZXDuKkSq9374khM=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_mwitkow_grpc_proxy",
        importpath = "github.com/mwitkow/grpc-proxy",
        sum = "h1:62uLwA3l2JMH84liO4ZhnjTH5PjFyCYxbHLgXPaJMtI=",
        version = "v0.0.0-20230212185441-f345521cb9c9",
    )
    go_repository(
        name = "com_github_nishanths_exhaustive",
        importpath = "github.com/nishanths/exhaustive",
        sum = "h1:+ANTMqRNrqwInnP9aszg/0jDo+zbXa4x66U19Bx/oTk=",  # keep
        version = "v0.2.3",  # keep
    )
    go_repository(
        name = "com_github_oklog_ulid",
        importpath = "github.com/oklog/ulid",
        sum = "h1:EGfNDEx6MqHz8B3uNV6QAib1UR2Lm97sHi3ocA6ESJ4=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_opencontainers_go_digest",
        importpath = "github.com/opencontainers/go-digest",
        sum = "h1:apOUWs51W5PlhuyGyz9FCeeBIOUDA/6nW8Oi/yOhh5U=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_opencontainers_image_spec",
        importpath = "github.com/opencontainers/image-spec",
        sum = "h1:fzg1mXZFj8YdPeNkRXMg+zb88BFV0Ys52cJydRwBkb8=",
        version = "v1.1.0-rc3",
    )
    go_repository(
        name = "com_github_opencontainers_runtime_spec",
        importpath = "github.com/opencontainers/runtime-spec",
        sum = "h1:HHUyrt9mwHUjtasSbXSMvs4cyFxh+Bll4AjJ9odEGpg=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_opentracing_opentracing_go",
        importpath = "github.com/opentracing/opentracing-go",
        sum = "h1:uEJPy/1a5RIPAJ0Ov+OIO8OxWu77jEv+1B0VhjKrZUs=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_paulmach_orb",
        importpath = "github.com/paulmach/orb",
        sum = "h1:p/YWV2uJwamAynnDOJGNbPBVtDHj3vG51k9tR1rFwJE=",
        version = "v0.9.2",
    )
    go_repository(
        name = "com_github_pierrec_lz4_v4",
        importpath = "github.com/pierrec/lz4/v4",
        sum = "h1:yOVMLb6qSIDP67pl/5F7RepeKYu/VmTyEXvuMI5d9mQ=",
        version = "v4.1.21",
    )
    go_repository(
        name = "com_github_pjbgf_sha1cd",
        importpath = "github.com/pjbgf/sha1cd",
        sum = "h1:4D5XXmUUBUl/xQ6IjCkEAbqXskkq/4O7LmGn0AqMDs4=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_pkg_errors",
        importpath = "github.com/pkg/errors",
        sum = "h1:FEBLx1zS214owpjy7qsBeixbURkuhQAwrK5UwLGTwt4=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_github_planetscale_vtprotobuf",
        importpath = "github.com/planetscale/vtprotobuf",
        patch_args = ["-p1"],
        patches = ["@{}//buildpatches:vtprotobuf.patch".format(workspace_name)],
        sum = "h1:nBeETjudeJ5ZgBHUz1fVHvbqUKnYOXNhsIEabROxmNA=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_pmezard_go_difflib",
        importpath = "github.com/pmezard/go-difflib",
        sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_power_devops_perfstat",
        importpath = "github.com/power-devops/perfstat",
        sum = "h1:0LFwY6Q3gMACTjAbMZBjXAqTOzOwFaj2Ld6cjeQ7Rig=",
        version = "v0.0.0-20221212215047-62379fc7944b",
    )
    go_repository(
        name = "com_github_pquerna_cachecontrol",
        importpath = "github.com/pquerna/cachecontrol",
        sum = "h1:vBXSNuE5MYP9IJ5kjsdo8uq+w41jSPgvba2DEnkRx9k=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_prometheus_client_golang",
        build_directives = [
            "gazelle:resolve go github.com/prometheus/client_model/go @{}//proto:prometheus_client_go_proto".format(workspace_name),
        ],
        importpath = "github.com/prometheus/client_golang",
        sum = "h1:ygXvpU1AoN1MhdzckN+PyD9QJOSD4x7kmXYlnfbA6JU=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_github_prometheus_client_model",
        importpath = "github.com/prometheus/client_model",
        sum = "h1:ZKSh/rekM+n3CeS952MLRAdFwIKqeY8b62p8ais2e9E=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_prometheus_common",
        build_directives = [
            "gazelle:resolve go github.com/prometheus/client_model/go @{}//proto:prometheus_client_go_proto".format(workspace_name),
        ],
        importpath = "github.com/prometheus/common",
        sum = "h1:ZlZy0BgJhTwVZUn7dLOkwCZHUkrAqd3WYtcFCWnM1D8=",
        version = "v0.54.0",
    )
    go_repository(
        name = "com_github_prometheus_procfs",
        importpath = "github.com/prometheus/procfs",
        sum = "h1:YagwOFzUgYfKKHX6Dr+sHT7km/hxC76UB0learggepc=",
        version = "v0.15.1",
    )
    go_repository(
        name = "com_github_protonmail_go_crypto",
        importpath = "github.com/ProtonMail/go-crypto",
        sum = "h1:kkhsdkhsCvIsutKu5zLMgWtgh9YxGCNAw8Ad8hjwfYg=",
        version = "v0.0.0-20230828082145-3c4c8a2d2371",
    )
    go_repository(
        name = "com_github_rantav_go_grpc_channelz",
        importpath = "github.com/rantav/go-grpc-channelz",
        sum = "h1:svoYt8ZD0uO6B/EZVWGNIDRJY/JXfak2y5Ks+1xwaVo=",
        version = "v0.0.3",
    )
    go_repository(
        name = "com_github_roaringbitmap_roaring",
        importpath = "github.com/RoaringBitmap/roaring",
        sum = "h1:LXcSqGGGMKm+KAzUyWn7ZeREqoOkoMX+KwLOK1thc4I=",
        version = "v1.9.1",
    )
    go_repository(
        name = "com_github_rogpeppe_go_internal",
        importpath = "github.com/rogpeppe/go-internal",
        sum = "h1:exVL4IDcn6na9z1rAb56Vxr+CgyK3nn3O+epU5NdKM8=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_github_rs_xid",
        importpath = "github.com/rs/xid",
        sum = "h1:mKX4bl4iPYJtEIxp6CYiUuLQ/8DYMoz0PUdtGgMFRVc=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_rs_zerolog",
        importpath = "github.com/rs/zerolog",
        sum = "h1:cO+d60CHkknCbvzEWxP0S9K6KqyTjrCNUy1LdQLCGPc=",
        version = "v1.29.1",
    )
    go_repository(
        name = "com_github_russellhaering_goxmldsig",
        importpath = "github.com/russellhaering/goxmldsig",
        sum = "h1:8UcDh/xGyQiyrW+Fq5t8f+l2DLB1+zlhYzkPUJ7Qhys=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_github_sean_seed",
        importpath = "github.com/sean-/seed",
        sum = "h1:nn5Wsu0esKSJiIVhscUtVbo7ada43DJhG55ua/hjS5I=",
        version = "v0.0.0-20170313163322-e2103e2c3529",
    )
    go_repository(
        name = "com_github_segmentio_asm",
        importpath = "github.com/segmentio/asm",
        sum = "h1:9BQrFxC+YOHJlTlHGkTrFWf59nbL3XnCoFLTwDCI7ys=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_sergi_go_diff",
        importpath = "github.com/sergi/go-diff",
        sum = "h1:xkr+Oxo4BOQKmkn/B9eMK0g5Kg/983T9DqqPHwYqD+8=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_shirou_gopsutil_v3",
        importpath = "github.com/shirou/gopsutil/v3",
        sum = "h1:5SgDCeQ0KW0S4N0znjeM/eFHXXOKyv2dVNgRq/c9P6Y=",
        version = "v3.23.5",
    )
    go_repository(
        name = "com_github_shoenig_go_m1cpu",
        importpath = "github.com/shoenig/go-m1cpu",
        sum = "h1:nxdKQNcEB6vzgA2E2bvzKIYRuNj7XNJ4S/aRSwKzFtM=",
        version = "v0.1.6",
    )
    go_repository(
        name = "com_github_shopspring_decimal",
        importpath = "github.com/shopspring/decimal",
        sum = "h1:2Usl1nmF/WZucqkFZhnfFYxxxu8LG21F6nPQBE5gKV8=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_shurcool_githubv4",
        importpath = "github.com/shurcooL/githubv4",
        sum = "h1:6dExqsYngGEiixqa1vmtlUd+zbyISilg0Cf3GWVdeYM=",
        version = "v0.0.0-20231126234147-1cffa1f02456",
    )
    go_repository(
        name = "com_github_shurcool_graphql",
        importpath = "github.com/shurcooL/graphql",
        sum = "h1:17JxqqJY66GmZVHkmAsGEkcIu0oCe3AM420QDgGwZx0=",
        version = "v0.0.0-20230722043721-ed46e5a46466",
    )
    go_repository(
        name = "com_github_sirupsen_logrus",
        importpath = "github.com/sirupsen/logrus",
        sum = "h1:dueUQJ1C2q9oE3F7wvmSGAaVtTmUizReu6fjN8uqzbQ=",
        version = "v1.9.3",
    )
    go_repository(
        name = "com_github_skeema_knownhosts",
        importpath = "github.com/skeema/knownhosts",
        sum = "h1:SHWdIUa82uGZz+F+47k8SY4QhhI291cXCpopT1lK2AQ=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_smacker_go_tree_sitter",
        importpath = "github.com/smacker/go-tree-sitter",
        sum = "h1:exZ0FwfhblsYbgfqYH+W/3sFye821WD02NjBmc+ENhE=",
        version = "v0.0.0-20230501083651-a7d92773b3aa",
    )
    go_repository(
        name = "com_github_spf13_cast",
        importpath = "github.com/spf13/cast",
        sum = "h1:R+kOtfhWQE6TVQzY+4D7wJLBgkdVasCEFxSUBYBYIlA=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_stretchr_testify",
        importpath = "github.com/stretchr/testify",
        sum = "h1:HtqpIVDClZ4nwg75+f6Lvsy/wHu+3BoSGCbBAcpTsTg=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_github_tebeka_selenium",
        importpath = "github.com/tebeka/selenium",
        sum = "h1:cNziB+etNgyH/7KlNI7RMC1ua5aH1+5wUlFQyzeMh+w=",
        version = "v0.9.9",
    )
    go_repository(
        name = "com_github_throttled_throttled_v2",
        importpath = "github.com/throttled/throttled/v2",
        replace = "github.com/buildbuddy-io/throttled/v2",
        sum = "h1:l9PGL9DJwcCgQcVt/zFzVjJbSzb+BmpU8NBeo6leHKU=",
        version = "v2.9.1-rc2",
    )
    go_repository(
        name = "com_github_tklauser_go_sysconf",
        importpath = "github.com/tklauser/go-sysconf",
        sum = "h1:89WgdJhk5SNwJfu+GKyYveZ4IaJ7xAkecBo+KdJV0CM=",
        version = "v0.3.11",
    )
    go_repository(
        name = "com_github_tklauser_numcpus",
        importpath = "github.com/tklauser/numcpus",
        sum = "h1:ng9scYS7az0Bk4OZLvrNXNSAO2Pxr1XXRAPyjhIx+Fk=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_valyala_fastrand",
        importpath = "github.com/valyala/fastrand",
        sum = "h1:f+5HkLW4rsgzdNoleUOB69hyT9IlD2ZQh9GyDMfb5G8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_valyala_histogram",
        importpath = "github.com/valyala/histogram",
        sum = "h1:wyYGAZZt3CpwUiIb9AU/Zbllg1llXyrtApRS815OLoQ=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_vbatts_tar_split",
        importpath = "github.com/vbatts/tar-split",
        sum = "h1:hLFqsOLQ1SsppQNTMpkpPXClLDfC2A3Zgy9OUU+RVck=",
        version = "v0.11.3",
    )
    go_repository(
        name = "com_github_victoriametrics_metrics",
        importpath = "github.com/VictoriaMetrics/metrics",
        sum = "h1:CNV3tfm2Kpv7Y9W3ohmvqgFWPR55tV2c7M2U6OIo+UM=",
        version = "v1.33.1",
    )
    go_repository(
        name = "com_github_vishvananda_netlink",
        importpath = "github.com/vishvananda/netlink",
        sum = "h1:Llsql0lnQEbHj0I1OuKyp8otXp0r3q0mPkuhwHfStVs=",
        version = "v1.2.1-beta.2",
    )
    go_repository(
        name = "com_github_vishvananda_netns",
        importpath = "github.com/vishvananda/netns",
        sum = "h1:Oeaw1EM2JMxD51g9uhtC0D7erkIjgmj8+JZc26m1YX8=",
        version = "v0.0.4",
    )
    go_repository(
        name = "com_github_xanzy_ssh_agent",
        importpath = "github.com/xanzy/ssh-agent",
        sum = "h1:+/15pJfg/RsTxqYcX6fHqOXZwwMP+2VyYWJeWM2qQFM=",
        version = "v0.3.3",
    )
    go_repository(
        name = "com_github_xiam_s_expr",
        importpath = "github.com/xiam/s-expr",
        sum = "h1:GOOG0ytl7fQsJ2yyA0qJu4uSUyv2pbILu+7edPboSNk=",
        version = "v0.0.0-20240311144413-9d7e25c9d7d1",
    )
    go_repository(
        name = "com_github_yusufpapurcu_wmi",
        importpath = "github.com/yusufpapurcu/wmi",
        sum = "h1:E1ctvB7uKFMOJw3fdOW32DwGE9I7t++CRUEMKvFoFiw=",
        version = "v1.2.3",
    )
    go_repository(
        name = "com_github_zeebo_blake3",
        importpath = "github.com/zeebo/blake3",
        sum = "h1:TFoLXsjeXqRNFxSbk35Dk4YtszE/MQQGK10BH4ptoTg=",
        version = "v0.2.3",
    )

    # keep
    go_repository(
        name = "com_gitlab_arm_research_smarter_smarter_device_manager",
        build_directives = [
            "gazelle:resolve go google.golang.org/grpc @org_golang_google_grpc//:grpc",
        ],
        importpath = "gitlab.com/arm-research/smarter/smarter-device-manager",
        sha256 = "4926695678627f4f316cf6ec107d0b2847f5051a12cb05862a257703336d83ce",
        strip_prefix = "smarter-device-manager-v1.20.11",
        urls = ["https://gitlab.com/arm-research/smarter/smarter-device-manager/-/archive/v1.20.11/smarter-device-manager-v1.20.11.zip"],
    )
    go_repository(
        name = "com_google_cloud_go",
        importpath = "cloud.google.com/go",
        sum = "h1:tpFCD7hpHFlQ8yPwT3x+QeXqc2T6+n6T+hmABHfDUSM=",
        version = "v0.112.0",
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        importpath = "cloud.google.com/go/compute",
        sum = "h1:phWcR2eWzRJaL/kOiJwfFsPs4BaKq1j6vnpZrc1YlVg=",
        version = "v1.24.0",
    )
    go_repository(
        name = "com_google_cloud_go_compute_metadata",
        importpath = "cloud.google.com/go/compute/metadata",
        sum = "h1:mg4jlk7mCAj6xXp9UJ4fjI9VUI5rubuGBW5aJ7UnBMY=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        importpath = "cloud.google.com/go/iam",
        sum = "h1:bEa06k05IO4f4uJonbB5iAgKTPpABy1ayxaIZV/GHVc=",
        version = "v1.1.6",
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        importpath = "cloud.google.com/go/logging",
        sum = "h1:iEIOXFO9EmSiTjDmfpbRjOxECO7R8C7b8IXUGOj7xZw=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_longrunning",
        importpath = "cloud.google.com/go/longrunning",
        sum = "h1:GOE6pZFdSrTb4KAiKnXsJBtlE6mEyaW44oKyMILWnOg=",
        version = "v0.5.5",
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        importpath = "cloud.google.com/go/secretmanager",
        sum = "h1:82fpF5vBBvu9XW4qj0FU2C6qVMtj1RM/XHwKXUEAfYY=",
        version = "v1.11.5",
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        importpath = "cloud.google.com/go/storage",
        sum = "h1:P0mOkAcaJxhCTvAkMhxMfrTKiNcub4YmmPBtlhAyTr8=",
        version = "v1.36.0",
    )
    go_repository(
        name = "in_gopkg_square_go_jose_v2",
        importpath = "gopkg.in/square/go-jose.v2",
        sum = "h1:NGk74WTnPKBNUhNzQX7PYcTLUjoq7mzKk2OKbvwk2iI=",
        version = "v2.6.0",
    )
    go_repository(
        name = "in_gopkg_warnings_v0",
        importpath = "gopkg.in/warnings.v0",
        sum = "h1:wFXVbFY8DY5/xOe1ECiWdKCzZlxgshcYVNkBHstARME=",
        version = "v0.1.2",
    )
    go_repository(
        name = "in_gopkg_yaml_v2",
        importpath = "gopkg.in/yaml.v2",
        sum = "h1:D8xgwECY7CYvx+Y2n4sBz93Jn9JRvxdiyyo8CTfuKaY=",
        version = "v2.4.0",
    )
    go_repository(
        name = "in_gopkg_yaml_v3",
        importpath = "gopkg.in/yaml.v3",
        sum = "h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=",
        version = "v3.0.1",
    )
    go_repository(
        name = "io_etcd_go_bbolt",
        importpath = "go.etcd.io/bbolt",
        sum = "h1:j+zJOnnEjF/kyHlDDgGnVL/AIqIJPq8UoB2GSNfkUfQ=",
        version = "v1.3.7",
    )
    go_repository(
        name = "io_gorm_driver_clickhouse",
        importpath = "gorm.io/driver/clickhouse",
        sum = "h1:OJwu7RLRzeXXJjvfBciGC8RCwL2+OF/qFGlYGpiL81g=",
        version = "v0.5.1",
    )
    go_repository(
        name = "io_gorm_driver_mysql",
        importpath = "gorm.io/driver/mysql",
        sum = "h1:WUEH5VF9obL/lTtzjmML/5e6VfFR/788coz2uaVCAZw=",
        version = "v1.5.1",
    )
    go_repository(
        name = "io_gorm_driver_postgres",
        importpath = "gorm.io/driver/postgres",
        sum = "h1:ytTDxxEv+MplXOfFe3Lzm7SjG09fcdb3Z/c056DTBx0=",
        version = "v1.5.2",
    )
    go_repository(
        name = "io_gorm_driver_sqlite",
        importpath = "gorm.io/driver/sqlite",
        sum = "h1:TpQ+/dqCY4uCigCFyrfnrJnrW9zjpelWVoEVNy5qJkc=",
        version = "v1.5.2",
    )
    go_repository(
        name = "io_gorm_gorm",
        importpath = "gorm.io/gorm",
        sum = "h1:gs1o6Vsa+oVKG/a9ElL3XgyGfghFfkKA2SInQaCyMho=",
        version = "v1.25.2",
    )

    # keep
    go_repository(
        name = "io_k8s_kubelet",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/kubelet",
        sum = "h1:n6PHxrm0FBlAGi7f3hs3CrNqVr+x3ssfrbb0aKqsBzo=",
        version = "v0.21.2",
    )
    go_repository(
        name = "io_kythe",
        importpath = "kythe.io",
        patch_args = ["-p1"],
        patches = ["@{}//buildpatches:io_kythe.patch".format(workspace_name)],
        replace = "github.com/buildbuddy-io/kythe",
        sum = "h1:3lminP0qtiOsw+3pHEBsKoId8tlVSwf/g4uyIucmRk4=",
        version = "v0.0.70",
    )
    go_repository(
        name = "io_opencensus_go",
        importpath = "go.opencensus.io",
        sum = "h1:y73uSU6J157QMP2kn2r30vwW1A2W2WFwSCGnAVxeaD0=",
        version = "v0.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_detectors_gcp",
        importpath = "go.opentelemetry.io/contrib/detectors/gcp",
        sum = "h1:VLAa8mb2eMu2Iq9pnVhece0Yla2mIq1yKYLUoZ/ifJs=",
        version = "v1.21.1",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc",
        importpath = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        sum = "h1:UNQQKPfTDe1J81ViolILjTKPr9WetKW6uei2hFgJmFs=",
        version = "v0.47.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp",
        importpath = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
        sum = "h1:sv9kVfal0MK0wBMCOGr+HeJm9v803BkJxGrk2au7j08=",
        version = "v0.47.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        importpath = "go.opentelemetry.io/otel",
        sum = "h1:xS7Ku+7yTFvDfDraDIJVpw7XPyuHlB9MCiqqX5mcJ6Y=",
        version = "v1.22.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_jaeger",
        importpath = "go.opentelemetry.io/otel/exporters/jaeger",
        sum = "h1:D7UpUy2Xc2wsi1Ras6V40q806WM07rqoCWzXu7Sqy+4=",
        version = "v1.17.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        importpath = "go.opentelemetry.io/otel/metric",
        sum = "h1:lypMQnGyJYeuYPhOM/bgjbFM6WE44W1/T45er4d8Hhg=",
        version = "v1.22.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk",
        importpath = "go.opentelemetry.io/otel/sdk",
        sum = "h1:FTt8qirL1EysG6sTQRZ5TokkU8d0ugCj8htOgThZXQ8=",
        version = "v1.21.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        importpath = "go.opentelemetry.io/otel/trace",
        sum = "h1:Hg6pPujv0XG9QaVbGOBVHunyuLcCC3jN7WEhPx83XD0=",
        version = "v1.22.0",
    )
    go_repository(
        name = "land_oras_oras_go_v2",
        importpath = "oras.land/oras-go/v2",
        sum = "h1:1nS8BIeEP6CBVQifwxrsth2bkuD+cYfjp7Hf7smUcS8=",
        version = "v2.1.0",
    )
    go_repository(
        name = "org_bitbucket_creachadair_stringset",
        importpath = "bitbucket.org/creachadair/stringset",
        sum = "h1:6Sv4CCv14Wm+OipW4f3tWOb0SQVpBDLW0knnJqUnmZ8=",
        version = "v0.0.11",
    )
    go_repository(
        name = "org_golang_google_api",
        importpath = "google.golang.org/api",
        sum = "h1:Vhs54HkaEpkMBdgGdOT2P6F0csGG/vxDS0hWHJzmmps=",
        version = "v0.162.0",
    )
    go_repository(
        name = "org_golang_google_genproto",
        importpath = "google.golang.org/genproto",
        sum = "h1:F6qOa9AZTYJXOUEr4jDysRDLrm4PHePlge4v4TGAlxY=",
        version = "v0.0.0-20240227224415-6ceb2ff114de",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        importpath = "google.golang.org/genproto/googleapis/api",
        sum = "h1:jFNzHPIeuzhdRwVhbZdiym9q0ory/xY3sA+v2wPg8I0=",
        version = "v0.0.0-20240227224415-6ceb2ff114de",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        sum = "h1:weYsP+dNijSQVoLAb5bpUos3ciBpNU/NEVlHFKrk8pg=",
        version = "v0.0.0-20240125205218-1f4bbc51befe",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sum = "h1:cZGRis4/ot9uVm639a+rHCUaG0JJHEsdyzSQTMX+suY=",
        version = "v0.0.0-20240227224415-6ceb2ff114de",
    )

    # gRPC
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable",
        importpath = "google.golang.org/grpc",
        patch_args = ["-p1"],
        # Remove panic() from serverHandlerTransport.Drain
        # gRPC GracefulStop stops accepting new requests and lets any existing
        # requests finish. For "grpc-over-http" requests, gRPC does not control
        # the connection lifetime so they choose to panic in Drain if there are
        # inflight "grpc-over-http" requests. Since we also shutdown the HTTP
        # server gracefully, it's safe for us to allow gRPC to wait for all
        # ongoing requests to finish.
        patches = ["@{}//buildpatches:org_golang_google_grpc_remove_drain_panic.patch".format(workspace_name)],
        sum = "h1:WjKe+dnvABXyPJMD7KDNLxtoGk5tgk+YFWN6cBWjZE8=",
        version = "v1.63.0",
    )
    go_repository(
        name = "org_golang_google_protobuf",
        # As of v1.33.0, some proto files were added to the repo which will
        # trigger Gazelle to generate proto build targets and create a cyclic
        # dependency chain.
        #
        # Turn off proto targets generation and only use Go targets to avoid
        # this class of issue.
        # This could be removed when we moved to bzlmod as Gazelle will set
        # by default thanks to this change:
        #   https://github.com/bazelbuild/bazel-gazelle/pull/1758
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/protobuf",
        sum = "h1:6xV6lTsCfpGD21XK49h7MhtcApnLqkfYgPcdHftf6hg=",
        version = "v1.34.2",
    )
    go_repository(
        name = "org_golang_x_crypto",
        importpath = "golang.org/x/crypto",
        sum = "h1:mnl8DM0o513X8fdIkmyFE/5hTYxbwYOjDS/+rK6qpRI=",
        version = "v0.24.0",
    )
    go_repository(
        name = "org_golang_x_exp",
        importpath = "golang.org/x/exp",
        sum = "h1:yixxcjnhBmY0nkL253HFVIm0JsFHwrHdT3Yh6szTnfY=",
        version = "v0.0.0-20240613232115-7f521ea00fb8",
    )
    go_repository(
        name = "org_golang_x_exp_typeparams",
        importpath = "golang.org/x/exp/typeparams",
        sum = "h1:qyrTQ++p1afMkO4DPEeLGq/3oTsdlvdH4vqZUBWzUKM=",
        version = "v0.0.0-20220218215828-6cf2b201936e",
    )
    go_repository(
        name = "org_golang_x_mod",
        importpath = "golang.org/x/mod",
        sum = "h1:5+9lSbEzPSdWkH32vYPBwEpX8KwDbM52Ud9xBUvNlb0=",
        version = "v0.18.0",
    )
    go_repository(
        name = "org_golang_x_net",
        importpath = "golang.org/x/net",
        sum = "h1:soB7SVo0PWrY4vPW/+ay0jKDNScG2X9wFeYlXIvJsOQ=",
        version = "v0.26.0",
    )
    go_repository(
        name = "org_golang_x_oauth2",
        importpath = "golang.org/x/oauth2",
        sum = "h1:9+E/EZBCbTLNrbN35fHv/a/d/mOBatymz1zbtQrXpIg=",
        version = "v0.19.0",
    )
    go_repository(
        name = "org_golang_x_sync",
        importpath = "golang.org/x/sync",
        sum = "h1:YsImfSBoP9QPYL0xyKJPq0gcaJdG3rInoqxTWbfQu9M=",
        version = "v0.7.0",
    )
    go_repository(
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        sum = "h1:rF+pYz3DAGSQAxAu1CbC7catZg4ebC4UIeIhKxBZvws=",
        version = "v0.21.0",
    )
    go_repository(
        name = "org_golang_x_term",
        importpath = "golang.org/x/term",
        sum = "h1:WVXCp+/EBEHOj53Rvu+7KiT/iElMrO8ACK16SMZ3jaA=",
        version = "v0.21.0",
    )
    go_repository(
        name = "org_golang_x_text",
        importpath = "golang.org/x/text",
        sum = "h1:a94ExnEXNtEwYLGJSIUxnWoxoRz/ZcCsV63ROupILh4=",
        version = "v0.16.0",
    )
    go_repository(
        name = "org_golang_x_time",
        importpath = "golang.org/x/time",
        sum = "h1:o7cqy6amK/52YcAKIPlM3a+Fpj35zvRj2TP+e1xFSfk=",
        version = "v0.5.0",
    )
    go_repository(
        name = "org_golang_x_tools",
        importpath = "golang.org/x/tools",
        sum = "h1:gqSGLZqv+AI9lIQzniJ0nZDRG5GBPsSi+DRNHWNz6yA=",
        version = "v0.22.0",
    )
    go_repository(
        name = "org_golang_x_tools_go_vcs",
        importpath = "golang.org/x/tools/go/vcs",
        sum = "h1:cOIJqWBl99H1dH5LWizPa+0ImeeJq3t3cJjaeOWUAL4=",
        version = "v0.1.0-deprecated",
    )
    go_repository(
        name = "org_mongodb_go_mongo_driver",
        importpath = "go.mongodb.org/mongo-driver",
        sum = "h1:aPx33jmn/rQuJXPQLZQ8NtfPQG8CaqgLThFtqRb0PiE=",
        version = "v1.12.0",
    )
    go_repository(
        name = "org_uber_go_atomic",
        importpath = "go.uber.org/atomic",
        sum = "h1:ZvwS0R+56ePWxUNi+Atn9dWONBPp/AUETXlHW0DxSjE=",
        version = "v1.11.0",
    )
    go_repository(
        name = "org_uber_go_multierr",
        importpath = "go.uber.org/multierr",
        sum = "h1:blXXJkSxSSfBVBlC76pxqeO+LN3aDfLQo+309xJstO0=",
        version = "v1.11.0",
    )
    go_repository(
        name = "org_uber_go_zap",
        importpath = "go.uber.org/zap",
        sum = "h1:FiJd5l1UOLj0wCgbSE0rwwXHzEdAZS6hiiSnxJN/D60=",
        version = "v1.24.0",
    )

# Manually created
def install_static_dependencies(workspace_name = "buildbuddy"):
    http_archive(
        name = "com_github_buildbuddy_io_protoc_gen_protobufjs",
        sha256 = "81ce501fdcc10a08c84b8c9d9d1900ca35b62f581deaba8168bed2b96e7696b0",
        urls = ["https://github.com/buildbuddy-io/protoc-gen-protobufjs/releases/download/v0.0.12/protoc-gen-protobufjs-v0.0.12.tar.gz"],
    )

    http_archive(
        name = "com_github_firecracker_microvm_firecracker",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "firecracker", srcs = ["release-{release}/firecracker-{release}"])',
            'filegroup(name = "jailer", srcs = ["release-{release}/jailer-{release}"])',
        ]).format(release = "v1.7.0-x86_64"),
        sha256 = "55bd3e6d599fdd108e36e52f9aee2319f06c18a90f2fa49b64e93fdf06f5ff53",
        urls = ["https://github.com/firecracker-microvm/firecracker/releases/download/v1.7.0/firecracker-v1.7.0-x86_64.tgz"],
    )
    http_archive(
        name = "com_github_containerd_stargz_snapshotter-v0.11.4-linux-amd64",
        build_file_content = 'exports_files(["stargz-store"])',
        sha256 = "56933aa04a64d3bf6991f9b1be127ac9896fe597d2fba194c67c2dd4368bbae3",
        urls = ["https://github.com/containerd/stargz-snapshotter/releases/download/v0.11.4/stargz-snapshotter-v0.11.4-linux-amd64.tar.gz"],
    )

    http_file(
        name = "com_github_buildbuddy_io_soci_snapshotter-soci-store-linux-amd64",
        downloaded_file_path = "soci-store",
        executable = True,
        sha256 = "0896b1df3dccd97895a5fde4bfbde47f1e533753113e39634261e2e035c9e6a3",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/soci-snapshotter/soci-store-v0.0.15-linux-amd64"],
    )

    http_file(
        name = "com_github_buildbuddy_io_soci_snapshotter-soci-store-linux-amd64-race",
        downloaded_file_path = "soci-store-race",
        executable = True,
        sha256 = "8b420b9e94433f7fe74f71a26a34ee2c0bed6535bdb4c2176a91611e4054d0d0",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/soci-snapshotter/soci-store-v0.0.15-linux-amd64-race"],
    )

    http_file(
        name = "com_github_redis_redis-redis-server-v6.2.1-linux-x86_64",
        executable = True,
        sha256 = "6d9c268fa0f696c3fc71b3656936c777d02b3b1c6637674ac3173facaefa4a77",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/redis/redis-server-6.2.1-linux-x86_64"],
    )
    http_file(
        name = "com_github_redis_redis-redis-server-v6.2.1-linux-arm64",
        executable = True,
        sha256 = "c381660c79dcbe4835a243bab8a0d3c3ee5dc502c1b4a47f1761bd8f9e7be240",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/redis/redis-server-6.2.1-linux-arm64"],
    )
    http_file(
        name = "com_github_redis_redis-redis-server-v6.2.6-darwin-arm64",
        executable = True,
        sha256 = "a70261f7a3f455a9a7c9d845299d487a86c1faef2af1605b94f39db44c098e69",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/redis/redis-server-6.2.6-darwin-arm64"],
    )
    http_file(
        name = "com_github_redis_redis-redis-server-v6.2.6-darwin-x86_64",
        executable = True,
        sha256 = "398bcf2be83a30249ec81259c87a7fc5c3bf511ff9b222ed517d8e52c99733c6",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/redis/redis-server-6.2.6-darwin-x86_64"],
    )
    http_file(
        name = "io_bazel_bazel-5.3.2-darwin-x86_64",
        executable = True,
        sha256 = "fe01824013184899386a4807435e38811949ca13f46713e7fc39c70fa1528a17",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-darwin-x86_64"],
    )
    http_file(
        name = "io_bazel_bazel-5.3.2-linux-x86_64",
        executable = True,
        sha256 = "973e213b1e9207ccdd3ea4730c0f92cbef769ec112ac2b84980583220d8db845",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-linux-x86_64"],
    )
    http_file(
        name = "io_bazel_bazel-5.3.2-linux-arm64",
        executable = True,
        sha256 = "dcad413da286ac1d3f88e384ff05c2ed796f903be85b253591d170ce258db721",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-linux-arm64"],
    )
    http_file(
        name = "io_bazel_bazel-6.5.0-darwin-x86_64",
        executable = True,
        sha256 = "bbf9c2c03bac48e0514f46db0295027935535d91f6d8dcd960c53393559eab29",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.5.0/bazel-6.5.0-darwin-x86_64"],
    )
    http_file(
        name = "io_bazel_bazel-6.5.0-linux-x86_64",
        executable = True,
        sha256 = "a40ac69263440761199fcb8da47ad4e3f328cbe79ffbf4ecc14e5ba252857307",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/6.5.0/bazel-6.5.0-linux-x86_64"],
    )
    http_file(
        name = "io_bazel_bazel-7.1.0-darwin-x86_64",
        executable = True,
        sha256 = "52ad8d57c22e4f873c724473a09ecfd98966c3a2950e102a7bd7e8c612b8001c",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.1.0/bazel-7.1.0-darwin-x86_64"],
    )
    http_file(
        name = "io_bazel_bazel-7.1.0-linux-x86_64",
        executable = True,
        sha256 = "62d62c699c1eb9f9be6a88030912a54d19fe45ae29329c7e5c53aba787492522",
        urls = ["https://github.com/bazelbuild/bazel/releases/download/7.1.0/bazel-7.1.0-linux-x86_64"],
    )
    http_file(
        name = "io_bazel_bazelisk-1.17.0-darwin-amd64",
        executable = True,
        sha256 = "3cf03dab8f5ef7c29e354b8e9293c82098ace3634253f9c660c26168b9e34720",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.17.0/bazelisk-darwin-amd64"],
    )
    http_file(
        name = "io_bazel_bazelisk-1.17.0-darwin-arm64",
        executable = True,
        sha256 = "2d4c66d428176b6c65e284ff74951b074846f15d324b099959483c175dec5728",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.17.0/bazelisk-darwin-arm64"],
    )
    http_file(
        name = "io_bazel_bazelisk-1.17.0-linux-amd64",
        executable = True,
        sha256 = "61699e22abb2a26304edfa1376f65ad24191f94a4ffed68a58d42b6fee01e124",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.17.0/bazelisk-linux-amd64"],
    )
    http_file(
        name = "org_kernel_git_linux_kernel-vmlinux",
        executable = True,
        sha256 = "4582d9c5d572c0449f55cc1cf317bf154dc0ff25df97378991f7c5bc9554f14e",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-v5.15-4582d9c5d572c0449f55cc1cf317bf154dc0ff25df97378991f7c5bc9554f14e"],
    )

    # TODO: mac build
    http_file(
        name = "org_llvm_llvm_clang-format_linux-x86_64",
        executable = True,
        sha256 = "85b1c2591274422234955e906aee39e6f793a88a74f3efc49a1852a0646ce08f",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/clang-format/clang-format-14_linux-x86_64"],
    )

    http_file(
        name = "com_github_buildbuddy_io_podman_static_podman-linux-amd64",
        urls = ["https://github.com/buildbuddy-io/podman-static/releases/download/buildbuddy-{podman_version}/podman-linux-amd64.tar.gz".format(podman_version = PODMAN_VERSION)],
        sha256 = PODMAN_STATIC_SHA256_AMD64,
    )

    http_file(
        name = "com_github_buildbuddy_io_podman_static_podman-linux-arm64",
        urls = ["https://github.com/buildbuddy-io/podman-static/releases/download/buildbuddy-{podman_version}/podman-linux-arm64.tar.gz".format(podman_version = PODMAN_VERSION)],
        sha256 = PODMAN_STATIC_SHA256_ARM64,
    )

    http_file(
        name = "com_github_containers_crun_crun-linux-amd64",
        urls = ["https://github.com/containers/crun/releases/download/1.15/crun-1.15-linux-amd64-disable-systemd"],
        sha256 = "03fd3ec6a7799183eaeefba5ebd3f66f9b5fb41a5b080c196285879631ff5dc1",
        executable = True,
    )
    http_file(
        name = "com_github_containers_crun_crun-linux-arm64",
        urls = ["https://github.com/containers/crun/releases/download/1.15/crun-1.15-linux-arm64-disable-systemd"],
        sha256 = "1bd840c95e9ae8edc25654dcf2481309724b9ff18ce95dbcd2535da9b026a47d",
        executable = True,
    )
