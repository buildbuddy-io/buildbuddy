load("@bazel_gazelle//:deps.bzl", "go_repository")
load("@bazel_skylib//lib:modules.bzl", "modules")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# The podman version used in tests and distributed with the buildbuddy executor image.
# When changing this version, a new release of podman-static may be needed.
# See dockerfiles/executor_image/README.md for instructions.
# The checksums below will also need to be updated.
PODMAN_VERSION = "v5.4.0"
PODMAN_STATIC_SHA256_AMD64 = "6b58fe68608b2a54fd2319a56ae5b7510fbe284c726e308cc28b55091baf9a3b"
PODMAN_STATIC_SHA256_ARM64 = "0d285fe272e8d9bbfa14a42cecea5fe2306e6f9ddaa1088c1ff0378b8df1363b"

# bazelisk run //:gazelle -- update-repos -from_file=go.mod -to_macro=deps.bzl%install_go_mod_dependencies -prune
def install_go_mod_dependencies(workspace_name = "buildbuddy"):
    go_repository(
        name = "build_buf_gen_go_bufbuild_protovalidate_protocolbuffers_go",
        importpath = "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go",
        sum = "h1:tdpHgTbmbvEIARu+bixzmleMi14+3imnpoFXz+Qzjp4=",
        version = "v1.31.0-20230802163732-1c33ebd9ecfa.1",
    )
    go_repository(
        name = "build_buf_gen_go_open_feature_flagd_connectrpc_go",
        importpath = "buf.build/gen/go/open-feature/flagd/connectrpc/go",
        sum = "h1:dU3vvR6d5iVwHBVQVS14ci+i29+4lLT8HjYktYSTPF8=",
        version = "v1.17.0-20240906125204-0a6a901b42e8.1",
    )
    go_repository(
        name = "build_buf_gen_go_open_feature_flagd_grpc_go",
        importpath = "buf.build/gen/go/open-feature/flagd/grpc/go",
        sum = "h1:D3HI5RQbqgffyf+Z77+hReDx5kigFVAKGvttULD9/ms=",
        version = "v1.5.1-20250127221518-be6d1143b690.2",
    )
    go_repository(
        name = "build_buf_gen_go_open_feature_flagd_protocolbuffers_go",
        importpath = "buf.build/gen/go/open-feature/flagd/protocolbuffers/go",
        sum = "h1:eZKupK8gUTuc6zifAFQon8Gnt44fR4cd0GnTWjELvEw=",
        version = "v1.36.5-20250127221518-be6d1143b690.1",
    )
    go_repository(
        name = "cat_dario_mergo",
        importpath = "dario.cat/mergo",
        sum = "h1:AGCNq9Evsj31mOgNPcLyXc+4PNABt905YmuqPYYpBWk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "cc_mvdan_gofumpt",
        importpath = "mvdan.cc/gofumpt",
        sum = "h1:avhhrOmv0IuvQVK7fvwV91oFSGAk5/6Po8GXTzICeu8=",
        version = "v0.3.1",
    )
    go_repository(
        name = "cc_mvdan_interfacer",
        importpath = "mvdan.cc/interfacer",
        sum = "h1:WX1yoOaKQfddO/mLzdV4wptyWgoH/6hwLs7QHTixo0I=",
        version = "v0.0.0-20180901003855-c20040233aed",
    )
    go_repository(
        name = "cc_mvdan_lint",
        importpath = "mvdan.cc/lint",
        sum = "h1:DxJ5nJdkhDlLok9K6qO+5290kphDJbHOQO1DFFFTeBo=",
        version = "v0.0.0-20170908181259-adc824a0674b",
    )
    go_repository(
        name = "cc_mvdan_unparam",
        importpath = "mvdan.cc/unparam",
        sum = "h1:Jh3LAeMt1eGpxomyu3jVkmVZWW2MxZ1qIIV2TZ/nRio=",
        version = "v0.0.0-20211214103731-d0ef000c54e5",
    )
    go_repository(
        name = "co_honnef_go_tools",
        importpath = "honnef.co/go/tools",
        sum = "h1:4bH5o3b5ZULQ4UrBmP+63W9r7qIkqJClEA9ko5YKx+I=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_4d63_gochecknoglobals",
        importpath = "4d63.com/gochecknoglobals",
        sum = "h1:zeZSRqj5yCg28tCkIV/z/lWbwvNm5qnKVS15PI8nhD0=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_connectrpc_connect",
        importpath = "connectrpc.com/connect",
        sum = "h1:PAg7CjSAGvscaf6YZKUefjoih5Z/qYkyaTrBW8xvYPw=",
        version = "v1.18.1",
    )
    go_repository(
        name = "com_connectrpc_otelconnect",
        importpath = "connectrpc.com/otelconnect",
        sum = "h1:scO5pOb0i4yUE66CnNrHeK1x51yq0bE0ehPg6WvzXJY=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_github_aclements_go_moremath",
        importpath = "github.com/aclements/go-moremath",
        sum = "h1:xlwdaKcTNVW4PtpQb8aKA4Pjy0CdJHEqvFbAnvR5m2g=",
        version = "v0.0.0-20210112150236-f10218a38794",
    )
    go_repository(
        name = "com_github_adalogics_go_fuzz_headers",
        importpath = "github.com/AdaLogics/go-fuzz-headers",
        sum = "h1:bvDV9vkmnHYOMsOr4WLk+Vo07yKIzd94sVoIqshQ4bU=",
        version = "v0.0.0-20230811130428-ced1acdcaa24",
    )
    go_repository(
        name = "com_github_adamkorcz_go_118_fuzz_build",
        importpath = "github.com/AdamKorcz/go-118-fuzz-build",
        sum = "h1:59MxjQVfjXsBpLy+dbd2/ELV5ofnUkUZBvWSC85sheA=",
        version = "v0.0.0-20230306123547-8075edf89bb0",
    )
    go_repository(
        name = "com_github_agnivade_levenshtein",
        importpath = "github.com/agnivade/levenshtein",
        sum = "h1:3oJU7J3FGFmyhn8KHjmVaZCN5hxTr7GxgRue+sxIXdQ=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_ajg_form",
        importpath = "github.com/ajg/form",
        sum = "h1:t9c7v8JUKu/XxOGBU0yjNpaMloxGEJhUkqFRq0ibGeU=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_akavel_rsrc",
        importpath = "github.com/akavel/rsrc",
        sum = "h1:Zxm8V5eI1hW4gGaYsJQUhxpjkENuG91ki8B4zCrvEsw=",
        version = "v0.10.2",
    )
    go_repository(
        name = "com_github_alecaivazis_survey_v2",
        importpath = "github.com/AlecAivazis/survey/v2",
        sum = "h1:6I/u8FvytdGsgonrYsVn2t8t4QiRnh6QSTqkkhIiSjQ=",
        version = "v2.3.7",
    )
    go_repository(
        name = "com_github_alecthomas_kingpin",
        importpath = "github.com/alecthomas/kingpin",
        sum = "h1:NXM4vkjHeFp3bbp0z/u0AdQRLg6b5LrPeFwgjVHUW58=",
        version = "v1.3.8-0.20191105203113-8c96d1c22481",
    )
    go_repository(
        name = "com_github_alecthomas_kingpin_v2",
        importpath = "github.com/alecthomas/kingpin/v2",
        sum = "h1:f48lwail6p8zpO1bC4TxtqACaGqHYA22qkHjHpqDjYY=",
        version = "v2.4.0",
    )
    go_repository(
        name = "com_github_alecthomas_template",
        importpath = "github.com/alecthomas/template",
        sum = "h1:JYp7IbQjafoB+tBA3gMyHYHrpOtNuDiK/uB5uXxq5wM=",
        version = "v0.0.0-20190718012654-fb15b899a751",
    )
    go_repository(
        name = "com_github_alecthomas_units",
        importpath = "github.com/alecthomas/units",
        sum = "h1:s6gZFSlWYmbqAuRjVTiNNhvNRfY2Wxp9nhfyel4rklc=",
        version = "v0.0.0-20211218093645-b94a6e3cc137",
    )
    go_repository(
        name = "com_github_alexflint_go_filemutex",
        importpath = "github.com/alexflint/go-filemutex",
        sum = "h1:LgE+nTUWnQCyRKbpoceKZsPQbs84LivvgwUymZXdOcM=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_alexkohler_prealloc",
        importpath = "github.com/alexkohler/prealloc",
        sum = "h1:Hbq0/3fJPQhNkN0dR95AVrr6R7tou91y0uHG5pOcUuw=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_andybalholm_brotli",
        importpath = "github.com/andybalholm/brotli",
        sum = "h1:eLKJA0d02Lf0mVpIDgYnqXcUn0GqVmEFny3VuID1U3M=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_anmitsu_go_shlex",
        importpath = "github.com/anmitsu/go-shlex",
        sum = "h1:9AeTilPcZAjCFIImctFaOjnTIavg87rW78vTPkQqLI8=",
        version = "v0.0.0-20200514113438-38f4b401e2be",
    )
    go_repository(
        name = "com_github_antihax_optional",
        importpath = "github.com/antihax/optional",
        sum = "h1:xK2lYat7ZLaVVcIuj82J8kIro4V6kDe0AUDFboUCwcg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_antlr4_go_antlr_v4",
        importpath = "github.com/antlr4-go/antlr/v4",
        sum = "h1:lxCg3LAv+EUK6t1i0y1V6/SLeUi0eKEKdhQAlS8TVTI=",
        version = "v4.13.0",
    )
    go_repository(
        name = "com_github_antlr_antlr4_runtime_go_antlr_v4",
        importpath = "github.com/antlr/antlr4/runtime/Go/antlr/v4",
        sum = "h1:goHVqTbFX3AIo0tzGr14pgfAW2ZfPChKO21Z9MGf/gk=",
        version = "v4.0.0-20230512164433-5d1fd1a340c9",
    )
    go_repository(
        name = "com_github_antonboom_errname",
        importpath = "github.com/Antonboom/errname",
        sum = "h1:LzIJZlyLOCSu51o3/t2n9Ck7PcoP9wdbrdaW6J8fX24=",
        version = "v0.1.6",
    )
    go_repository(
        name = "com_github_antonboom_nilnil",
        importpath = "github.com/Antonboom/nilnil",
        sum = "h1:PHhrh5ANKFWRBh7TdYmyyq2gyT2lotnvFvvFbylF81Q=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_apache_beam",
        importpath = "github.com/apache/beam",
        sum = "h1:7C2/JDa+fiRJs8kAcfCHxVTf0xxwKsCFQYDMoRdr/dk=",
        version = "v2.31.0+incompatible",
    )
    go_repository(
        name = "com_github_armon_circbuf",
        importpath = "github.com/armon/circbuf",
        sum = "h1:7Ip0wMmLHLRJdrloDxZfhMm0xrLXZS8+COSu2bXmEQs=",
        version = "v0.0.0-20190214190532-5111143e8da2",
    )
    go_repository(
        name = "com_github_armon_consul_api",
        importpath = "github.com/armon/consul-api",
        sum = "h1:G1bPvciwNyF7IUmKXNt9Ak3m6u9DE1rF+RmtIkBpVdA=",
        version = "v0.0.0-20180202201655-eb2c6b5be1b6",
    )
    go_repository(
        name = "com_github_armon_go_metrics",
        importpath = "github.com/armon/go-metrics",
        sum = "h1:hR91U9KYmb6bLBYLQjyM+3j+rcd/UhE+G78SFnF8gJA=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_armon_go_radix",
        importpath = "github.com/armon/go-radix",
        sum = "h1:F4z6KzEeeQIMeLFa97iZU6vupzoecKdU5TX24SNppXI=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_armon_go_socks5",
        importpath = "github.com/armon/go-socks5",
        sum = "h1:0CwZNZbxp69SHPdPJAN/hZIm0C4OItdklCFmMRWYpio=",
        version = "v0.0.0-20160902184237-e75332964ef5",
    )
    go_repository(
        name = "com_github_asaskevich_govalidator",
        importpath = "github.com/asaskevich/govalidator",
        sum = "h1:DklsrG3dyBCFEj5IhUbnKptjxatkF07cF2ak3yi77so=",
        version = "v0.0.0-20230301143203-a9d515a09cc2",
    )
    go_repository(
        name = "com_github_ashanbrown_forbidigo",
        importpath = "github.com/ashanbrown/forbidigo",
        sum = "h1:VkYIwb/xxdireGAdJNZoo24O4lmnEWkactplBlWTShc=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_ashanbrown_makezero",
        importpath = "github.com/ashanbrown/makezero",
        sum = "h1:iCQ87C0V0vSyO+M9E/FZYbu65auqH0lnsOkf5FcB28s=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go",
        importpath = "github.com/aws/aws-sdk-go",
        sum = "h1:cSg4pvZ3m8dgYcgqB97MrcdjUmZ1BeMYKUxMMB89IPk=",
        version = "v1.55.6",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2",
        importpath = "github.com/aws/aws-sdk-go-v2",
        sum = "h1:mJoei2CxPutQVxaATCzDUjcZEjVRdpsiiXi2o38yqWM=",
        version = "v1.36.3",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_aws_protocol_eventstream",
        importpath = "github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream",
        sum = "h1:zAybnyUQXIZ5mok5Jqwlf58/TFE7uvd3IAsa1aF9cXs=",
        version = "v1.6.10",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_config",
        importpath = "github.com/aws/aws-sdk-go-v2/config",
        sum = "h1:Kg+fAYNaJeGXp1vmjtidss8O2uXIsXwaRqsQJKXVr+0=",
        version = "v1.29.9",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_credentials",
        importpath = "github.com/aws/aws-sdk-go-v2/credentials",
        sum = "h1:fvtQY3zFzYJ9CfixuAQ96IxDrBajbBWGqjNTCa79ocU=",
        version = "v1.17.62",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_feature_ec2_imds",
        importpath = "github.com/aws/aws-sdk-go-v2/feature/ec2/imds",
        sum = "h1:x793wxmUWVDhshP8WW2mlnXuFrO4cOd3HLBroh1paFw=",
        version = "v1.16.30",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_feature_rds_auth",
        importpath = "github.com/aws/aws-sdk-go-v2/feature/rds/auth",
        sum = "h1:qDk85oQdhwP4NR1RpkN+t40aN46/K96hF9J1vDRrkKM=",
        version = "v1.5.11",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_feature_s3_manager",
        importpath = "github.com/aws/aws-sdk-go-v2/feature/s3/manager",
        sum = "h1:MTLivtC3s89de7Fe3P8rzML/8XPNRfuyJhlRTsCEt0k=",
        version = "v1.17.66",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_configsources",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/configsources",
        sum = "h1:ZK5jHhnrioRkUNOc+hOgQKlUL5JeC3S6JgLxtQ+Rm0Q=",
        version = "v1.3.34",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_endpoints_v2",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/endpoints/v2",
        sum = "h1:SZwFm17ZUNNg5Np0ioo/gq8Mn6u9w19Mri8DnJ15Jf0=",
        version = "v2.6.34",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_ini",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/ini",
        sum = "h1:bIqFDwgGXXN1Kpp99pDOdKMTTb5d2KyU5X/BZxjOkRo=",
        version = "v1.8.3",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_v4a",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/v4a",
        sum = "h1:ZNTqv4nIdE/DiBfUUfXcLZ/Spcuz+RjeziUtNJackkM=",
        version = "v1.3.34",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_accept_encoding",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding",
        sum = "h1:eAh2A4b5IzM/lum78bZ590jy36+d/aFLgKF/4Vd1xPE=",
        version = "v1.12.3",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_checksum",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/checksum",
        sum = "h1:lguz0bmOoGzozP9XfRJR1QIayEYo+2vP/No3OfLF0pU=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_presigned_url",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/presigned-url",
        sum = "h1:dM9/92u2F1JbDaGooxTq18wmmFzbJRfXfVfy96/1CXM=",
        version = "v1.12.15",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_s3shared",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/s3shared",
        sum = "h1:moLQUoVq91LiqT1nbvzDukyqAlCv89ZmwaHw/ZFlFZg=",
        version = "v1.18.15",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_s3",
        importpath = "github.com/aws/aws-sdk-go-v2/service/s3",
        sum = "h1:jIiopHEV22b4yQP2q36Y0OmwLbsxNWdWwfZRR5QRRO4=",
        version = "v1.78.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_sso",
        importpath = "github.com/aws/aws-sdk-go-v2/service/sso",
        sum = "h1:8JdC7Gr9NROg1Rusk25IcZeTO59zLxsKgE0gkh5O6h0=",
        version = "v1.25.1",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_ssooidc",
        importpath = "github.com/aws/aws-sdk-go-v2/service/ssooidc",
        sum = "h1:KwuLovgQPcdjNMfFt9OhUd9a2OwcOKhxfvF4glTzLuA=",
        version = "v1.29.1",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_sts",
        importpath = "github.com/aws/aws-sdk-go-v2/service/sts",
        sum = "h1:PZV5W8yk4OtH1JAuhV2PXwwO9v5G5Aoj+eMCn4T+1Kc=",
        version = "v1.33.17",
    )
    go_repository(
        name = "com_github_aws_smithy_go",
        importpath = "github.com/aws/smithy-go",
        sum = "h1:6D9hW43xKFrRx/tXXfAlIZc4JI+yQe6snnWcQyxSyLQ=",
        version = "v1.22.2",
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
        sum = "h1:7y8/iC+qyw1U4m5OJgKhIZ+SCZtDRxUqd797Eg2qWFA=",
        version = "v0.9.0-buildbuddy",
    )
    go_repository(
        name = "com_github_aymerick_douceur",
        importpath = "github.com/aymerick/douceur",
        sum = "h1:Mv+mAeH1Q+n9Fr+oyamOlAkUNPWPlA8PPGR0QAaYuPk=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_azure_azure_pipeline_go",
        importpath = "github.com/Azure/azure-pipeline-go",
        sum = "h1:7U9HBg1JFK3jHl5qmo4CTZKFTVgMwdFHMVtCdfBE21U=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go",
        importpath = "github.com/Azure/azure-sdk-for-go",
        sum = "h1:KnPIugL51v3N3WwvaSmZbxukD1WuWXOiE9fRdu32f2I=",
        version = "v16.2.1+incompatible",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azcore",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azcore",
        sum = "h1:nyQWyZvwGTvunIMxi1Y9uXkcyr+I7TeNrr/foo4Kpk8=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_azidentity",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/azidentity",
        sum = "h1:tfLQ34V6F7tVSwoTf/4lH5sE0o6eCJuNDTmH09nDpbc=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_internal",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/internal",
        sum = "h1:ywEEhmNahHBihViHepv3xPBn1663uRv2t2q/ESv9seY=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go_sdk_storage_azblob",
        importpath = "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob",
        sum = "h1:YUUxeiOWgdAQE3pXt2H7QXzZs0q8UBjgRbl56qo8GYM=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_azure_azure_storage_blob_go",
        importpath = "github.com/Azure/azure-storage-blob-go",
        sum = "h1:rXtgp8tN1p29GvpGgfJetavIG0V7OgcSXPpwp3tx6qk=",
        version = "v0.15.0",
    )
    go_repository(
        name = "com_github_azure_go_ansiterm",
        importpath = "github.com/Azure/go-ansiterm",
        sum = "h1:UQHMgLO+TxOElx5B5HZ4hJQsoJ/PvUvKRhJHDQXO8P8=",
        version = "v0.0.0-20210617225240-d185dfc1b5a1",
    )
    go_repository(
        name = "com_github_azure_go_autorest",
        importpath = "github.com/Azure/go-autorest",
        sum = "h1:V5VMDjClD3GiElqLWO7mz2MxNAK/vTfRHdAubSIPRgs=",
        version = "v14.2.0+incompatible",
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest",
        importpath = "github.com/Azure/go-autorest/autorest",
        sum = "h1:90Y4srNYrwOtAgVo3ndrQkTYn6kf1Eg/AjTFJ8Is2aM=",
        version = "v0.11.18",
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_adal",
        importpath = "github.com/Azure/go-autorest/autorest/adal",
        sum = "h1:Mp5hbtOePIzM8pJVRa3YLrWWmZtoxRXqUEzCfJt3+/Q=",
        version = "v0.9.13",
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_date",
        importpath = "github.com/Azure/go-autorest/autorest/date",
        sum = "h1:7gUk1U5M/CQbp9WoqinNzJar+8KY+LPI6wiWrP/myHw=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_mocks",
        importpath = "github.com/Azure/go-autorest/autorest/mocks",
        sum = "h1:K0laFcLE6VLTOwNgSxaGbUcLPuGXlNkbVvq4cW4nIHk=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_to",
        importpath = "github.com/Azure/go-autorest/autorest/to",
        sum = "h1:oXVqrxakqqV1UZdSazDOPOLvOIz+XA683u8EctwboHk=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_azure_go_autorest_logger",
        importpath = "github.com/Azure/go-autorest/logger",
        sum = "h1:IG7i4p/mDa2Ce4TRyAO8IHnVhAVF3RFU+ZtXWSmf4Tg=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_azure_go_autorest_tracing",
        importpath = "github.com/Azure/go-autorest/tracing",
        sum = "h1:TYi4+3m5t6K48TGI9AUdb+IzbnSxvnvUMfuitfgcfuo=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_azuread_microsoft_authentication_library_for_go",
        importpath = "github.com/AzureAD/microsoft-authentication-library-for-go",
        sum = "h1:XHOnouVk1mxXfQidrMEnLlPk9UMeRtyBTnEFtxkV0kU=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_barkimedes_go_deepcopy",
        importpath = "github.com/barkimedes/go-deepcopy",
        sum = "h1:GSoSVRLoBaFpOOds6QyY1L8AX7uoY+Ln3BHc22W40X0=",
        version = "v0.0.0-20220514131651-17c30cfc62df",
    )
    go_repository(
        name = "com_github_bazelbuild_bazel_gazelle",
        importpath = "github.com/bazelbuild/bazel-gazelle",
        sum = "h1:BpkUzE3H2l6buJYFTKgzVMecJimQgWwYud25qVIx0SQ=",
        version = "v0.42.0",
    )
    go_repository(
        name = "com_github_bazelbuild_bazelisk",
        build_file_generation = "on",
        importpath = "github.com/bazelbuild/bazelisk",
        sum = "h1:ewduqIuhzpKB6Uw887ecLrnC9abSQHqofklqlfJgs3k=",
        version = "v1.25.1-0.20250219134847-cdb99bfb1b7d",
    )
    go_repository(
        name = "com_github_bazelbuild_buildtools",
        importpath = "github.com/bazelbuild/buildtools",
        patch_args = ["-p1"],
        patches = ["@{}//buildpatches:buildifier.patch".format(workspace_name)],
        sum = "h1:FGzENZi+SX9I7h9xvMtRA3rel8hCEfyzSixteBgn7MU=",
        version = "v0.0.0-20240918101019-be1c24cc9a44",
    )
    go_repository(
        name = "com_github_bazelbuild_rules_go",
        importpath = "github.com/bazelbuild/rules_go",
        sum = "h1:+ozpngVAW67pCAwfhepaXSSrG3yHcj8K9hNAxSYBno4=",
        version = "v0.52.0",
    )
    go_repository(
        name = "com_github_bazelbuild_rules_webtesting",
        importpath = "github.com/bazelbuild/rules_webtesting",
        sum = "h1:8iFEccri5HBi43hKpNGesGtekRZNZIMB0vVaHXB2QXs=",
        version = "v0.2.1-0.20250212231324-7a1c88f61e35",
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
        sum = "h1:PmQJDDYahBGNKDcpdX8uPy1xRCwoCGVUiW669MEirVI=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_github_benbjohnson_clock",
        importpath = "github.com/benbjohnson/clock",
        sum = "h1:Q92kusRqC1XV2MjkWETPvjJVqKetz1OzxZB7mHJLju8=",
        version = "v1.1.0",
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
        name = "com_github_bgentry_speakeasy",
        importpath = "github.com/bgentry/speakeasy",
        sum = "h1:ByYyxL9InA1OWqxJqqp2A5pYHUrCiAL6K3J+LKSsQkY=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_bitly_go_simplejson",
        importpath = "github.com/bitly/go-simplejson",
        sum = "h1:6IH+V8/tVMab511d5bn4M7EwGXZf9Hj6i2xSwkNEM+Y=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_bits_and_blooms_bitset",
        importpath = "github.com/bits-and-blooms/bitset",
        sum = "h1:Gd2c8lSNf9pKXom5JtD7AaKO8o7fGQ2LtFj1436qilA=",
        version = "v1.14.3",
    )
    go_repository(
        name = "com_github_bits_and_blooms_bloom_v3",
        importpath = "github.com/bits-and-blooms/bloom/v3",
        sum = "h1:VfknkqV4xI+PsaDIsoHueyxVDZrfvMn56jeWUzvzdls=",
        version = "v3.7.0",
    )
    go_repository(
        name = "com_github_bkaradzic_go_lz4",
        importpath = "github.com/bkaradzic/go-lz4",
        sum = "h1:RXc4wYsyz985CkXXeX04y4VnZFGG8Rd43pRaHsOXAKk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_bketelsen_crypt",
        importpath = "github.com/bketelsen/crypt",
        sum = "h1:+0HFd5KSZ/mm3JmhmrDukiId5iR6w4+BdFtfSy4yWIc=",
        version = "v0.0.3-0.20200106085610-5cbc8cc4026c",
    )
    go_repository(
        name = "com_github_bkielbasa_cyclop",
        importpath = "github.com/bkielbasa/cyclop",
        sum = "h1:7Jmnh0yL2DjKfw28p86YTd/B4lRGcNuu12sKE35sM7A=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_blang_semver",
        importpath = "github.com/blang/semver",
        sum = "h1:cQNTCjp13qL8KC3Nbxr/y2Bqb63oX6wdnnjpJbkM4JQ=",
        version = "v3.5.1+incompatible",
    )
    go_repository(
        name = "com_github_blang_semver_v4",
        importpath = "github.com/blang/semver/v4",
        sum = "h1:1PFHFE6yCCTv8C1TeyNNarDzntLi7wMI5i/pzqYIsAM=",
        version = "v4.0.0",
    )
    go_repository(
        name = "com_github_blizzy78_varnamelen",
        importpath = "github.com/blizzy78/varnamelen",
        sum = "h1:oqSblyuQvFsW1hbBHh1zfwrKe3kcSj0rnXkKzsQ089M=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_github_bmatcuk_doublestar_v4",
        importpath = "github.com/bmatcuk/doublestar/v4",
        sum = "h1:fdDeAqgT47acgwd9bd9HxJRDmc9UAmPpc+2m0CXv75Q=",
        version = "v4.7.1",
    )
    go_repository(
        name = "com_github_bmizerany_assert",
        importpath = "github.com/bmizerany/assert",
        sum = "h1:DDGfHa7BWjL4YnC6+E63dPcxHo2sUxDIu8g3QgEJdRY=",
        version = "v0.0.0-20160611221934-b7ed37b82869",
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
        name = "com_github_bojand_hri",
        importpath = "github.com/bojand/hri",
        sum = "h1:OIv6AtbPjYv9A7qjUqylU11mbcP610JWsWCwvpc3w3U=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_bombsimon_wsl_v3",
        importpath = "github.com/bombsimon/wsl/v3",
        sum = "h1:Mka/+kRLoQJq7g2rggtgQsjuI/K5Efd87WX96EWFxjM=",
        version = "v3.3.0",
    )
    go_repository(
        name = "com_github_bradfitz_gomemcache",
        importpath = "github.com/bradfitz/gomemcache",
        sum = "h1:Dr+ezPI5ivhMn/3WOoB86XzMhie146DNaBbhaQWZHMY=",
        version = "v0.0.0-20230611145640-acc696258285",
    )
    go_repository(
        name = "com_github_breml_bidichk",
        importpath = "github.com/breml/bidichk",
        sum = "h1:qe6ggxpTfA8E75hdjWPZ581sY3a2lnl0IRxLQFelECI=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_bshuster_repo_logrus_logstash_hook",
        importpath = "github.com/bshuster-repo/logrus-logstash-hook",
        sum = "h1:pgAtgj+A31JBVtEHu2uHuEx0n+2ukqUJnS2vVe5pQNA=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_bufbuild_protocompile",
        importpath = "github.com/bufbuild/protocompile",
        sum = "h1:mixz5lJX4Hiz4FpqFREJHIXLfaLBntfaJv1h+/jS+Qg=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_github_bufbuild_protovalidate_go",
        importpath = "github.com/bufbuild/protovalidate-go",
        sum = "h1:pJr07sYhliyfj/STAM7hU4J3FKpVeLVKvOBmOTN8j+s=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_buger_jsonparser",
        importpath = "github.com/buger/jsonparser",
        sum = "h1:2PnMjfWD7wBILjqQbt530v576A/cAbQvEW9gGIpYMUs=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_bugsnag_bugsnag_go",
        importpath = "github.com/bugsnag/bugsnag-go",
        sum = "h1:rFt+Y/IK1aEZkEHchZRSq9OQbsSzIT/OrI8YFFmRIng=",
        version = "v0.0.0-20141110184014-b1d153021fcd",
    )
    go_repository(
        name = "com_github_bugsnag_osext",
        importpath = "github.com/bugsnag/osext",
        sum = "h1:otBG+dV+YK+Soembjv71DPz3uX/V/6MMlSyD9JBQ6kQ=",
        version = "v0.0.0-20130617224835-0dd3f918b21b",
    )
    go_repository(
        name = "com_github_bugsnag_panicwrap",
        importpath = "github.com/bugsnag/panicwrap",
        sum = "h1:nvj0OLI3YqYXer/kZD8Ri1aaunCxIEsOst1BVJswV0o=",
        version = "v0.0.0-20151223152923-e2c28503fcd0",
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
        sum = "h1:pxW6RcqyfI9/kWtOwnv/G+AzdKuy2ZrqINhenH4HyNs=",
        version = "v1.4.1-0.20240526193622-a339e1f7089c",
    )
    go_repository(
        name = "com_github_burntsushi_xgb",
        importpath = "github.com/BurntSushi/xgb",
        sum = "h1:1BDTz0u9nC3//pOCMdNH+CiXJVYJh5UQNCOBG7jbELc=",
        version = "v0.0.0-20160522181843-27f122750802",
    )
    go_repository(
        name = "com_github_burntsushi_xgbutil",
        importpath = "github.com/BurntSushi/xgbutil",
        sum = "h1:4ZrkT/RzpnROylmoQL57iVUL57wGKTR5O6KpVnbm2tA=",
        version = "v0.0.0-20160919175755-f7c97cef3b4e",
    )
    go_repository(
        name = "com_github_butuzov_ireturn",
        importpath = "github.com/butuzov/ireturn",
        sum = "h1:QvrO2QF2+/Cx1WA/vETCIYBKtRjc30vesdoPUNo1EbY=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_bwesterb_go_ristretto",
        importpath = "github.com/bwesterb/go-ristretto",
        sum = "h1:1w53tCkGhCQ5djbat3+MH0BAQ5Kfgbt56UZQ/JMzngw=",
        version = "v1.2.3",
    )
    go_repository(
        name = "com_github_cavaliergopher_cpio",
        importpath = "github.com/cavaliergopher/cpio",
        sum = "h1:KQFSeKmZhv0cr+kawA3a0xTQCU4QxXF1vhU7P7av2KM=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_cenkalti_backoff_v4",
        importpath = "github.com/cenkalti/backoff/v4",
        sum = "h1:MyRJ/UdXutAwSAT+s3wNd7MfTIcy71VQueUuFK343L8=",
        version = "v4.3.0",
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
        name = "com_github_certifi_gocertifi",
        importpath = "github.com/certifi/gocertifi",
        sum = "h1:uH66TXeswKn5PW5zdZ39xEwfS9an067BirqA+P4QaLI=",
        version = "v0.0.0-20200922220541-2c3bb06c6054",
    )
    go_repository(
        name = "com_github_cespare_xxhash",
        importpath = "github.com/cespare/xxhash",
        sum = "h1:a6HrQnmkObjyL+Gs60czilIUGqrzKutQD6XZog3p+ko=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_cespare_xxhash_v2",
        importpath = "github.com/cespare/xxhash/v2",
        sum = "h1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=",
        version = "v2.3.0",
    )
    go_repository(
        name = "com_github_charithe_durationcheck",
        importpath = "github.com/charithe/durationcheck",
        sum = "h1:mPP4ucLrf/rKZiIG/a9IPXHGlh8p4CzgpyTy6EEutYk=",
        version = "v0.0.9",
    )
    go_repository(
        name = "com_github_charmbracelet_lipgloss",
        importpath = "github.com/charmbracelet/lipgloss",
        sum = "h1:768h64EFkGUr8V5yAKV7/Ta0NiVceiPaV+PphaW1K9g=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_chavacava_garif",
        importpath = "github.com/chavacava/garif",
        sum = "h1:tFXjAxje9thrTF4h57Ckik+scJjTWdwAtZqZPtOT48M=",
        version = "v0.0.0-20220316182200-5cad0b5181d4",
    )
    go_repository(
        name = "com_github_checkpoint_restore_go_criu_v4",
        importpath = "github.com/checkpoint-restore/go-criu/v4",
        sum = "h1:WW2B2uxx9KWF6bGlHqhm8Okiafwwx7Y2kcpn8lCpjgo=",
        version = "v4.1.0",
    )
    go_repository(
        name = "com_github_checkpoint_restore_go_criu_v5",
        importpath = "github.com/checkpoint-restore/go-criu/v5",
        sum = "h1:wpFFOoomK3389ue2lAb0Boag6XPht5QYpipxmSNL4d8=",
        version = "v5.3.0",
    )
    go_repository(
        name = "com_github_chzyer_logex",
        importpath = "github.com/chzyer/logex",
        sum = "h1:XHDu3E6q+gdHgsdTPH6ImJMIp436vR6MPtH8gP05QzM=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_chzyer_readline",
        importpath = "github.com/chzyer/readline",
        sum = "h1:upd/6fQk4src78LMRzh5vItIt361/o4uq553V8B5sGI=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_chzyer_test",
        importpath = "github.com/chzyer/test",
        sum = "h1:p3BQDXSxOhOG0P9z6/hGnII4LGiEPOYBhs8asl/fC04=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_cilium_ebpf",
        importpath = "github.com/cilium/ebpf",
        sum = "h1:nk5HPMeoBXtOzbkZBWym+ZWq1GIiHUsBFXxwewXAHLQ=",
        version = "v0.10.0",
    )
    go_repository(
        name = "com_github_circonus_labs_circonus_gometrics",
        importpath = "github.com/circonus-labs/circonus-gometrics",
        sum = "h1:C29Ae4G5GtYyYMm1aztcyj/J5ckgJm2zwdDajFbx1NY=",
        version = "v2.3.1+incompatible",
    )
    go_repository(
        name = "com_github_circonus_labs_circonusllhist",
        importpath = "github.com/circonus-labs/circonusllhist",
        sum = "h1:TJH+oke8D16535+jHExHj4nQvzlZrj7ug5D7I/orNUA=",
        version = "v0.1.3",
    )
    go_repository(
        name = "com_github_clickhouse_ch_go",
        importpath = "github.com/ClickHouse/ch-go",
        sum = "h1:X/QmUmFhpUvLgPSQb7fWOSi1wvqGn6tJ7w2a59c4xsg=",
        version = "v0.57.0",
    )
    go_repository(
        name = "com_github_clickhouse_clickhouse_go",
        importpath = "github.com/ClickHouse/clickhouse-go",
        sum = "h1:cKjXeYLNWVJIx2J1K6H2CqyRmfwVJVY1OV1coaaFcI0=",
        version = "v1.5.4",
    )
    go_repository(
        name = "com_github_clickhouse_clickhouse_go_v2",
        importpath = "github.com/ClickHouse/clickhouse-go/v2",
        sum = "h1:WCnusqEeCO/9sLFVIv57le/O1ydUb+x9+SYYhJ11fsY=",
        version = "v2.10.1",
    )
    go_repository(
        name = "com_github_client9_misspell",
        importpath = "github.com/client9/misspell",
        sum = "h1:ta993UF76GwbvJcIo3Y68y/M3WxlpEHPWIGDkJYwzJI=",
        version = "v0.3.4",
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
        name = "com_github_cloudflare_golz4",
        importpath = "github.com/cloudflare/golz4",
        sum = "h1:F1EaeKL/ta07PY/k9Os/UFtwERei2/XzGemhpGnBKNg=",
        version = "v0.0.0-20150217214814-ef862a3cdc58",
    )
    go_repository(
        name = "com_github_cloudykit_fastprinter",
        importpath = "github.com/CloudyKit/fastprinter",
        sum = "h1:sR+/8Yb4slttB4vD+b9btVEnWgL3Q00OBTzVT8B9C0c=",
        version = "v0.0.0-20200109182630-33d98a066a53",
    )
    go_repository(
        name = "com_github_cloudykit_jet_v6",
        importpath = "github.com/CloudyKit/jet/v6",
        sum = "h1:EpcZ6SR9n28BUGtNJSvlBqf90IpjeFr36Tizxhn/oME=",
        version = "v6.2.0",
    )
    go_repository(
        name = "com_github_cncf_udpa_go",
        importpath = "github.com/cncf/udpa/go",
        sum = "h1:QQ3GSy+MqSHxm/d8nCtnAiZdYFd45cYZPs8vOOIYKfk=",
        version = "v0.0.0-20220112060539-c52dc94e7fbe",
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        build_file_generation = "clean",
        importpath = "github.com/cncf/xds/go",
        sum = "h1:QVw89YDxXxEe+l8gU8ETbOasdwEV+avkR75ZzsVV9WI=",
        version = "v0.0.0-20240905190251-b4127c9b8d78",
    )
    go_repository(
        name = "com_github_cockroachdb_datadriven",
        importpath = "github.com/cockroachdb/datadriven",
        sum = "h1:otljaYPt5hWxV3MUfO5dFPFiOXg9CyG5/kCfayTqsJ4=",
        version = "v1.0.3-0.20230413201302-be42291fc80f",
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
        sum = "h1:5II1uEP4MyHLDnsrbv/EZ36arcb9Mxg3n+owhZ3GrG8=",
        version = "v1.1.4",
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
        name = "com_github_codegangsta_inject",
        importpath = "github.com/codegangsta/inject",
        sum = "h1:sDMmm+q/3+BukdIpxwO365v/Rbspp2Nt5XntgQRXq8Q=",
        version = "v0.0.0-20150114235600-33e0aa1cb7c0",
    )
    go_repository(
        name = "com_github_containerd_aufs",
        importpath = "github.com/containerd/aufs",
        sum = "h1:2oeJiwX5HstO7shSrPZjrohJZLzK36wvpdmzDRkL/LY=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_containerd_btrfs",
        importpath = "github.com/containerd/btrfs",
        sum = "h1:osn1exbzdub9L5SouXO5swW4ea/xVdJZ3wokxN5GrnA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_containerd_btrfs_v2",
        importpath = "github.com/containerd/btrfs/v2",
        sum = "h1:FN4wsx7KQrYoLXN7uLP0vBV4oVWHOIKDRQ1G2Z0oL5M=",
        version = "v2.0.0",
    )
    go_repository(
        name = "com_github_containerd_cgroups",
        importpath = "github.com/containerd/cgroups",
        sum = "h1:v8rEWFl6EoqHB+swVNjVoCJE8o3jX7e8nqBGPLaDFBM=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_containerd_cgroups_v3",
        importpath = "github.com/containerd/cgroups/v3",
        sum = "h1:f5WFqIVSgo5IZmtTT3qVBo6TzI1ON6sycSBKkymb9L0=",
        version = "v3.0.2",
    )
    go_repository(
        name = "com_github_containerd_console",
        importpath = "github.com/containerd/console",
        sum = "h1:lIr7SlA5PxZyMV30bDW0MGbiOPXwc63yRuCP0ARubLw=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_containerd_containerd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/containerd",
        sum = "h1:yFyEyojddO3MIGVER2xJLWoCIn+Up4GaHFquP7hsFII=",
        version = "v1.7.27",
    )
    go_repository(
        name = "com_github_containerd_containerd_api",
        build_directives = [
            "gazelle:proto file",
            "gazelle:proto_import_prefix github.com/containerd/containerd/api",
            "gazelle:resolve proto go github.com/containerd/containerd/api/types //types:types",
            "gazelle:resolve go github.com/containerd/containerd/api/types/descriptor //types:types",
            "gazelle:resolve go github.com/containerd/containerd/api/types/fieldpath //types:types",
            "gazelle:resolve go github.com/containerd/containerd/api/types/metrics //types:types",
            "gazelle:resolve go github.com/containerd/containerd/api/types/mount //types:types",
            "gazelle:resolve go github.com/containerd/containerd/api/types/platform //types:types",
            "gazelle:resolve go github.com/containerd/containerd/api/types/sandbox //types:types",
            "gazelle:resolve go github.com/containerd/containerd/api/types/task //types/task:task",
            "gazelle:resolve proto google/rpc/status.proto @googleapis//google/rpc:status_proto",
            "gazelle:resolve proto go google/rpc/status.proto @org_golang_google_genproto_googleapis_rpc//status",
        ],
        importpath = "github.com/containerd/containerd/api",
        patches = ["@{}//buildpatches:com_github_containerd_containerd_api.patch".format(workspace_name)],
        sum = "h1:hVTNJKR8fMc/2Tiw60ZRijntNMd1U+JVMyTRdsD2bS0=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_github_containerd_continuity",
        importpath = "github.com/containerd/continuity",
        sum = "h1:ZRoN1sXq9u7V6QoHMcVWGhOwDFqZ4B9i5H6un1Wh0x4=",
        version = "v0.4.5",
    )
    go_repository(
        name = "com_github_containerd_errdefs",
        importpath = "github.com/containerd/errdefs",
        sum = "h1:tg5yIfIlQIrxYtu9ajqY42W3lpS19XqdxRQeEwYG8PI=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_containerd_fifo",
        importpath = "github.com/containerd/fifo",
        sum = "h1:4I2mbh5stb1u6ycIABlBw9zgtlK8viPI9QkQNRQEEmY=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_containerd_go_cni",
        importpath = "github.com/containerd/go-cni",
        sum = "h1:ORi7P1dYzCwVM6XPN4n3CbkuOx/NZ2DOqy+SHRdo9rU=",
        version = "v1.1.9",
    )
    go_repository(
        name = "com_github_containerd_go_runc",
        importpath = "github.com/containerd/go-runc",
        sum = "h1:oU+lLv1ULm5taqgV/CJivypVODI4SUz1znWjv3nNYS0=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_containerd_imgcrypt",
        importpath = "github.com/containerd/imgcrypt",
        sum = "h1:ZS7TuywcRNLoHpU0g+v4/PsKynl6TYlw5xDVWWoIyFA=",
        version = "v1.1.8",
    )
    go_repository(
        name = "com_github_containerd_log",
        importpath = "github.com/containerd/log",
        sum = "h1:TCJt7ioM2cr/tfR8GPbGf9/VRAX8D2B4PjzCpfX540I=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_containerd_nri",
        importpath = "github.com/containerd/nri",
        sum = "h1:n1S753B9lX8RFrHYeSgwVvS1yaUcHjxbB+f+xzEncRI=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_github_containerd_platforms",
        importpath = "github.com/containerd/platforms",
        sum = "h1:zvwtM3rz2YHPQsF2CHYM8+KtB5dvhISiXh5ZpSBQv6A=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_containerd_protobuild",
        importpath = "github.com/containerd/protobuild",
        sum = "h1:RIyEIu+D+iIha6E1PREBPAXspSMFaDVam81JlolZWpg=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_containerd_stargz_snapshotter_estargz",
        importpath = "github.com/containerd/stargz-snapshotter/estargz",
        sum = "h1:OqlDCK3ZVUO6C3B/5FSkDwbkEETK84kQgEeFwDC+62k=",
        version = "v0.14.3",
    )
    go_repository(
        name = "com_github_containerd_ttrpc",
        build_directives = [
            "gazelle:proto file",
            "gazelle:resolve proto proto/status.proto @googleapis//google/rpc:status_proto",
            "gazelle:resolve proto go proto/status.proto @org_golang_google_genproto_googleapis_rpc//status",
        ],
        importpath = "github.com/containerd/ttrpc",
        patches = ["@{}//buildpatches:com_github_containerd_ttrpc.patch".format(workspace_name)],
        sum = "h1:qIrroQvuOL9HQ1X6KHe2ohc7p+HP/0VE6XPU7elJRqQ=",
        version = "v1.2.7",
    )
    go_repository(
        name = "com_github_containerd_typeurl",
        importpath = "github.com/containerd/typeurl",
        sum = "h1:Chlt8zIieDbzQFzXzAeBEF92KhExuE4p9p92/QmY7aY=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_containerd_typeurl_v2",
        importpath = "github.com/containerd/typeurl/v2",
        sum = "h1:3Q4Pt7i8nYwy2KmQWIw2+1hTvwTE/6w9FqcttATPO/4=",
        version = "v2.1.1",
    )
    go_repository(
        name = "com_github_containerd_zfs",
        importpath = "github.com/containerd/zfs",
        sum = "h1:n7OZ7jZumLIqNJqXrEc/paBM840mORnmGdJDmAmJZHM=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_containernetworking_cni",
        importpath = "github.com/containernetworking/cni",
        sum = "h1:hhOcjNVUQTnzdRJ6alC5XF+wd9mfGIUaj8FuJbEslXM=",
        version = "v1.2.3",
    )
    go_repository(
        name = "com_github_containernetworking_plugins",
        importpath = "github.com/containernetworking/plugins",
        sum = "h1:T5ji+LPYjjgW0QM+KyrigZbLsZ8jaX+E5J/EcKOE4gQ=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_containers_ocicrypt",
        importpath = "github.com/containers/ocicrypt",
        sum = "h1:r7UR6o8+lyhkEywetubUUgcKFjOWOaWz8cEBrCPX0ic=",
        version = "v1.1.10",
    )
    go_repository(
        name = "com_github_coreos_bbolt",
        importpath = "github.com/coreos/bbolt",
        sum = "h1:wZwiHHUieZCquLkDL0B8UhzreNWsPHooDAG3q34zk0s=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_coreos_etcd",
        importpath = "github.com/coreos/etcd",
        sum = "h1:8F3hqu9fGYLBifCmRCJsicFqDx/D68Rt3q1JMazcgBQ=",
        version = "v3.3.13+incompatible",
    )
    go_repository(
        name = "com_github_coreos_go_iptables",
        importpath = "github.com/coreos/go-iptables",
        sum = "h1:XWM3V+MPRr5/q51NuWSgU0fqMad64Zyxs8ZUoMsamr8=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_github_coreos_go_oidc",
        importpath = "github.com/coreos/go-oidc",
        sum = "h1:sdJrfw8akMnCuUlaZU3tE/uYXFgfqom8DBE9so9EBsM=",
        version = "v2.1.0+incompatible",
    )
    go_repository(
        name = "com_github_coreos_go_oidc_v3",
        importpath = "github.com/coreos/go-oidc/v3",
        sum = "h1:tDnXHnLyiTVyT/2zLDGj09pFPkhND8Gl8lnTRhoEaJU=",
        version = "v3.10.0",
    )
    go_repository(
        name = "com_github_coreos_go_semver",
        importpath = "github.com/coreos/go-semver",
        sum = "h1:wkHLiw0WNATZnSG7epLsujiMCgPAc9xhjJ4tgnAxmfM=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_coreos_go_systemd",
        importpath = "github.com/coreos/go-systemd",
        sum = "h1:Wf6HqHfScWJN9/ZjdUKyjop4mf3Qdd+1TvvltAvM3m8=",
        version = "v0.0.0-20190321100706-95778dfbb74e",
    )
    go_repository(
        name = "com_github_coreos_go_systemd_v22",
        importpath = "github.com/coreos/go-systemd/v22",
        sum = "h1:RrqgGjYQKalulkV8NGVIfkXQf6YYmOyiJKk8iXXhfZs=",
        version = "v22.5.0",
    )
    go_repository(
        name = "com_github_coreos_pkg",
        importpath = "github.com/coreos/pkg",
        sum = "h1:lBNOc5arjvs8E5mO2tbpBpLoyyu8B6e44T7hJy6potg=",
        version = "v0.0.0-20180928190104-399ea9e2e55f",
    )
    go_repository(
        name = "com_github_cpuguy83_go_md2man_v2",
        importpath = "github.com/cpuguy83/go-md2man/v2",
        sum = "h1:p1EgwI/C7NhT0JmVkwCD2ZBK8j4aeHQX2pMHHBfMQ6w=",
        version = "v2.0.2",
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
        name = "com_github_cucumber_gherkin_go_v26",
        importpath = "github.com/cucumber/gherkin/go/v26",
        sum = "h1:EgIjePLWiPeslwIWmNQ3XHcypPsWAHoMCz/YEBKP4GI=",
        version = "v26.2.0",
    )
    go_repository(
        name = "com_github_cucumber_godog",
        importpath = "github.com/cucumber/godog",
        sum = "h1:51AL8lBXF3f0cyA5CV4TnJFCTHpgiy+1x1Hb3TtZUmo=",
        version = "v0.15.0",
    )
    go_repository(
        name = "com_github_cucumber_messages_go_v21",
        importpath = "github.com/cucumber/messages/go/v21",
        sum = "h1:wzA0LxwjlWQYZd32VTlAVDTkW6inOFmSM+RuOwHZiMI=",
        version = "v21.0.1",
    )
    go_repository(
        name = "com_github_cyphar_filepath_securejoin",
        importpath = "github.com/cyphar/filepath-securejoin",
        sum = "h1:4d9N5ykBnSp5Xn2JkhocYDkOpURL/18CYMpo6xB9uWM=",
        version = "v0.3.6",
    )
    go_repository(
        name = "com_github_d2g_dhcp4",
        importpath = "github.com/d2g/dhcp4",
        sum = "h1:Xo2rK1pzOm0jO6abTPIQwbAmqBIOj132otexc1mmzFc=",
        version = "v0.0.0-20170904100407-a1d1b6c41b1c",
    )
    go_repository(
        name = "com_github_d2g_dhcp4client",
        importpath = "github.com/d2g/dhcp4client",
        sum = "h1:suYBsYZIkSlUMEz4TAYCczKf62IA2UWC+O8+KtdOhCo=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_d2g_dhcp4server",
        importpath = "github.com/d2g/dhcp4server",
        sum = "h1:+CpLbZIeUn94m02LdEKPcgErLJ347NUwxPKs5u8ieiY=",
        version = "v0.0.0-20181031114812-7d4a0a7f59a5",
    )
    go_repository(
        name = "com_github_d2g_hardwareaddr",
        importpath = "github.com/d2g/hardwareaddr",
        sum = "h1:itqmmf1PFpC4n5JW+j4BU7X4MTfVurhYRTjODoPb2Y8=",
        version = "v0.0.0-20190221164911-e7d9fbe030e4",
    )
    go_repository(
        name = "com_github_daixiang0_gci",
        importpath = "github.com/daixiang0/gci",
        sum = "h1:iwJvwQpBZmMg31w+QQ6jsyZ54KEATn6/nfARbBNW294=",
        version = "v0.2.9",
    )
    go_repository(
        name = "com_github_danieljoos_wincred",
        importpath = "github.com/danieljoos/wincred",
        sum = "h1:QLdCxFs1/Yl4zduvBdcHB8goaYk9RARS2SgLLRuAyr0=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_datadog_datadog_go",
        importpath = "github.com/DataDog/datadog-go",
        sum = "h1:qSG2N4FghB1He/r2mFrWKCaL7dXCilEuNEeAn20fdD4=",
        version = "v3.2.0+incompatible",
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
        sum = "h1:U9qPSI2PIWSS1VwoXQT9A3Wy9MM3WgvqSxFWenqJduM=",
        version = "v1.1.2-0.20180830191138-d8f796af33cc",
    )
    go_repository(
        name = "com_github_davecgh_go_xdr",
        importpath = "github.com/davecgh/go-xdr",
        sum = "h1:qg9VbHo1TlL0KDM0vYvBG9EY0X0Yku5WYIPoFWt8f6o=",
        version = "v0.0.0-20161123171359-e6a2ba005892",
    )
    go_repository(
        name = "com_github_dchest_uniuri",
        importpath = "github.com/dchest/uniuri",
        sum = "h1:koIcOUdrTIivZgSLhHQvKgqdWZq5d7KdMEWF1Ud6+5g=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_decred_dcrd_crypto_blake256",
        importpath = "github.com/decred/dcrd/crypto/blake256",
        sum = "h1:7PltbUIQB7u/FfZ39+DGa/ShuMyJ5ilcvdfma9wOH6Y=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_decred_dcrd_dcrec_secp256k1_v4",
        importpath = "github.com/decred/dcrd/dcrec/secp256k1/v4",
        sum = "h1:8UrgZ3GkP4i/CLijOJx79Yu+etlyjdBU4sfcs2WYQMs=",
        version = "v4.2.0",
    )
    go_repository(
        name = "com_github_denis_tingajkin_go_header",
        importpath = "github.com/denis-tingajkin/go-header",
        sum = "h1:jEeSF4sdv8/3cT/WY8AgDHUoItNSoEZ7qg9dX7pc218=",
        version = "v0.4.2",
    )
    go_repository(
        name = "com_github_denverdino_aliyungo",
        importpath = "github.com/denverdino/aliyungo",
        sum = "h1:p6poVbjHDkKa+wtC8frBMwQtT3BmqGYBjzMwJ63tuR4=",
        version = "v0.0.0-20190125010748-a747050bb1ba",
    )
    go_repository(
        name = "com_github_dgrijalva_jwt_go",
        importpath = "github.com/dgrijalva/jwt-go",
        sum = "h1:7qlOGliEKZXTDg6OTjfoBKDXWrumCAMpl/TFQ4/5kLM=",
        version = "v3.2.0+incompatible",
    )
    go_repository(
        name = "com_github_dgryski_go_rendezvous",
        importpath = "github.com/dgryski/go-rendezvous",
        sum = "h1:lO4WD4F/rVNCu3HqELle0jiPLLBs70cWOduZpkS1E78=",
        version = "v0.0.0-20200823014737-9f7001d12a5f",
    )
    go_repository(
        name = "com_github_dgryski_go_sip13",
        importpath = "github.com/dgryski/go-sip13",
        sum = "h1:RMLoZVzv4GliuWafOuPuQDKSm1SJph7uCRnnS61JAn4=",
        version = "v0.0.0-20181026042036-e10d5fee7954",
    )
    go_repository(
        name = "com_github_diegoholiveira_jsonlogic_v3",
        importpath = "github.com/diegoholiveira/jsonlogic/v3",
        sum = "h1:92HSmB9bwM/o0ZvrCpcvTP2EsPXSkKtAniIr2W/dcIM=",
        version = "v3.7.4",
    )
    go_repository(
        name = "com_github_distribution_reference",
        importpath = "github.com/distribution/reference",
        sum = "h1:0IXCQ5g4/QMHHkarYzh5l+u8T3t73zM5QvfrDyIgxBk=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_djarvur_go_err113",
        importpath = "github.com/Djarvur/go-err113",
        sum = "h1:sHglBQTwgx+rWPdisA5ynNEsoARbiCBOyGcJM4/OzsM=",
        version = "v0.0.0-20210108212216-aea10b59be24",
    )
    go_repository(
        name = "com_github_dlclark_regexp2",
        importpath = "github.com/dlclark/regexp2",
        sum = "h1:+/GIL799phkJqYW+3YbOd8LCcbHzT0Pbo8zl70MHsq0=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_dmarkham_enumer",
        importpath = "github.com/dmarkham/enumer",
        sum = "h1:fIF11F9l5jyD++YYvxcSH5WgHfeaSGPaN/T4kOQ4qEM=",
        version = "v1.5.8",
    )
    go_repository(
        name = "com_github_dnaeon_go_vcr",
        importpath = "github.com/dnaeon/go-vcr",
        sum = "h1:r8L/HqC0Hje5AXMu1ooW8oyQyOFv4GxqpL0nRP7SLLY=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_dnephin_pflag",
        importpath = "github.com/dnephin/pflag",
        sum = "h1:oxONGlWxhmUct0YzKTgrpQv9AUA1wtPBn7zuSjJqptk=",
        version = "v1.0.7",
    )
    go_repository(
        name = "com_github_docker_cli",
        importpath = "github.com/docker/cli",
        sum = "h1:aMphQkcGtpHixwwhAXJT1rrK/detk2JIvDaFkLctbGM=",
        version = "v27.5.0+incompatible",
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
        sum = "h1:NEAxTwEjxV6VbBMBoGG3zPqbiJosIApZjxlbrG9q3/g=",
        version = "v26.1.5+incompatible",
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
        name = "com_github_docker_go_events",
        importpath = "github.com/docker/go-events",
        sum = "h1:+pKlWGMw7gf6bQ+oDZB4KHQFypsfjYlq/C4rfL7D3g8=",
        version = "v0.0.0-20190806004212-e31b211e4f1c",
    )
    go_repository(
        name = "com_github_docker_go_metrics",
        importpath = "github.com/docker/go-metrics",
        sum = "h1:AgB/0SvBxihN0X8OR4SjsblXkbMvalQ8cjmtKQ2rQV8=",
        version = "v0.0.1",
    )
    go_repository(
        name = "com_github_docker_go_units",
        importpath = "github.com/docker/go-units",
        sum = "h1:69rxXcBk27SvSaaxTtLh/8llcHD8vYHT7WSdRZ/jvr4=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_docker_libtrust",
        importpath = "github.com/docker/libtrust",
        sum = "h1:ZClxb8laGDf5arXfYcAtECDFgAgHklGI8CxgjHnXKJ4=",
        version = "v0.0.0-20150114040149-fa567046d9b1",
    )
    go_repository(
        name = "com_github_docker_spdystream",
        importpath = "github.com/docker/spdystream",
        sum = "h1:cenwrSVm+Z7QLSV/BsnenAOcDXdX4cMv4wP0B/5QbPg=",
        version = "v0.0.0-20160310174837-449fdfce4d96",
    )
    go_repository(
        name = "com_github_docopt_docopt_go",
        importpath = "github.com/docopt/docopt-go",
        sum = "h1:bWDMxwH3px2JBh6AyO7hdCn/PkvCZXii8TGj7sbtEbQ=",
        version = "v0.0.0-20180111231733-ee0de3bc6815",
    )
    go_repository(
        name = "com_github_dop251_goja",
        importpath = "github.com/dop251/goja",
        sum = "h1:+9NRIliCUhliHMCixEO0mcXmrv3HYwxs9oxM1Z+qnYM=",
        version = "v0.0.0-20230626124041-ba8a63e79201",
    )
    go_repository(
        name = "com_github_dop251_goja_nodejs",
        importpath = "github.com/dop251/goja_nodejs",
        sum = "h1:W1n4DvpzZGOISgp7wWNtraLcHtnmnTwBlJidqtMIuwQ=",
        version = "v0.0.0-20211022123610-8dd9abb0616d",
    )
    go_repository(
        name = "com_github_dustin_go_humanize",
        importpath = "github.com/dustin/go-humanize",
        sum = "h1:GzkhY7T5VNhEkwH0PVJgjz+fX1rhBrR7pRT3mDkpeCY=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_eknkc_amber",
        importpath = "github.com/eknkc/amber",
        sum = "h1:clC1lXBpe2kTj2VHdaIu9ajZQe4kcEY9j0NsnDDBZ3o=",
        version = "v0.0.0-20171010120322-cdade1c07385",
    )
    go_repository(
        name = "com_github_elastic_gosigar",
        importpath = "github.com/elastic/gosigar",
        sum = "h1:Dg80n8cr90OZ7x+bAax/QjoW/XqTI11RmA79ZwIm9/4=",
        version = "v0.14.2",
    )
    go_repository(
        name = "com_github_elazarl_goproxy",
        importpath = "github.com/elazarl/goproxy",
        sum = "h1:xwIyKHbaP5yfT6O9KIeYJR5549MXRQkoQMRXGztz8YQ=",
        version = "v1.2.3",
    )
    go_repository(
        name = "com_github_emicklei_go_restful",
        importpath = "github.com/emicklei/go-restful",
        sum = "h1:spTtZBk5DYEvbxMVutUuTyh1Ao2r4iyvLdACqsl/Ljk=",
        version = "v2.9.5+incompatible",
    )
    go_repository(
        name = "com_github_emicklei_go_restful_v3",
        importpath = "github.com/emicklei/go-restful/v3",
        sum = "h1:y2DdzBAURM29NFF94q6RaY4vjIH1rtwDapwQtU84iWk=",
        version = "v3.12.0",
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
        sum = "h1:vPfJZCkob6yTMEgS+0TwfTUfbHjfy/6vOJ8hUWX/uXE=",
        version = "v0.13.1",
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sum = "h1:tntQDh69XqOCOZsDz0lVJQez/2L6Uu2PdjCQwWCJ3bM=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_esimonov_ifshort",
        importpath = "github.com/esimonov/ifshort",
        sum = "h1:6SID4yGWfRae/M7hkVDVVyppy8q/v9OuxNdmjLQStBA=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_ettle_strcase",
        importpath = "github.com/ettle/strcase",
        sum = "h1:htFueZyVeE1XNnMEfbqp5r67qAN/4r6ya1ysq8Q+Zcw=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_evanphx_json_patch",
        importpath = "github.com/evanphx/json-patch",
        sum = "h1:jBYDEEiFBPxA0v50tFdvOzQQTCvpL6mnFh5mB2/l16U=",
        version = "v5.6.0+incompatible",
    )
    go_repository(
        name = "com_github_evanphx_json_patch_v5",
        importpath = "github.com/evanphx/json-patch/v5",
        sum = "h1:kcBlZQbplgElYIlo/n1hJbls2z/1awpXxpRi0/FOJfg=",
        version = "v5.9.0",
    )
    go_repository(
        name = "com_github_fatih_color",
        importpath = "github.com/fatih/color",
        sum = "h1:8LOYc1KYPPmyKMuN8QV2DNRWNbLo6LZ0iLs8+mlH53w=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_github_fatih_structs",
        importpath = "github.com/fatih/structs",
        sum = "h1:Q7juDM0QtcnhCpeyLGQKyg4TOIghuNXrkL32pHAUMxo=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_fatih_structtag",
        importpath = "github.com/fatih/structtag",
        sum = "h1:/OdNE99OxoI/PqaW/SuSK9uxxT3f/tcSZgon/ssNSx4=",
        version = "v1.2.0",
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
        # TODO(bduffany): remove these patches when the corresponding PRs are merged
        patches = [
            # https://github.com/firecracker-microvm/firecracker-go-sdk/pull/510
            "@{}//buildpatches:com_github_firecracker_microvm_firecracker_go_sdk_jailer.patch".format(workspace_name),
            # https://github.com/firecracker-microvm/firecracker-go-sdk/pull/600
            # TODO: this PR only includes the CgroupArgs part of the patch.
            # Need another PR for the ParentCgroup part.
            "@{}//buildpatches:com_github_firecracker_microvm_firecracker_go_sdk_cgroup.patch".format(workspace_name),
        ],
        sum = "h1:n9Q3BKnUAW0x1D1x2I2QIj0T/5K6UYL6JKkPvwwARw0=",
        version = "v0.0.0-20241028184712-f74f43bb036d",
    )
    go_repository(
        name = "com_github_flosch_pongo2_v4",
        importpath = "github.com/flosch/pongo2/v4",
        sum = "h1:gv+5Pe3vaSVmiJvh/BZa82b7/00YUGm0PIyVVLop0Hw=",
        version = "v4.0.2",
    )
    go_repository(
        name = "com_github_form3tech_oss_jwt_go",
        importpath = "github.com/form3tech-oss/jwt-go",
        sum = "h1:7ZaBxOI7TMoYBfyA3cQHErNNyAWIKUMIwqxEtgHOs5c=",
        version = "v3.2.3+incompatible",
    )
    go_repository(
        name = "com_github_frankban_quicktest",
        importpath = "github.com/frankban/quicktest",
        sum = "h1:g2rn0vABPOOXmZUj+vbmUp0lPoXEMuhTpIluN0XL9UY=",
        version = "v1.14.4",
    )
    go_repository(
        name = "com_github_fsnotify_fsnotify",
        importpath = "github.com/fsnotify/fsnotify",
        sum = "h1:dAwr6QBTBZIkG8roQaJjGof0pp0EeF+tNV7YBP3F/8M=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_github_fullsailor_pkcs7",
        importpath = "github.com/fullsailor/pkcs7",
        sum = "h1:RDBNVkRviHZtvDvId8XSGPu3rmpmSe+wKRcEWNgsfWU=",
        version = "v0.0.0-20190404230743-d7302db945fa",
    )
    go_repository(
        name = "com_github_fxamacker_cbor_v2",
        importpath = "github.com/fxamacker/cbor/v2",
        sum = "h1:iM5WgngdRBanHcxugY4JySA0nk1wZorNOpTgCMedv5E=",
        version = "v2.7.0",
    )
    go_repository(
        name = "com_github_fzipp_gocyclo",
        importpath = "github.com/fzipp/gocyclo",
        sum = "h1:L66amyuYogbxl0j2U+vGqJXusPF2IkduvXLnYD5TFgw=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_github_gabriel_vasile_mimetype",
        importpath = "github.com/gabriel-vasile/mimetype",
        sum = "h1:QjV6pZ7/XZ7ryI2KuyeEDE8wnh7fHP9YnQy+R0LnH8I=",
        version = "v1.4.4",
    )
    go_repository(
        name = "com_github_garyburd_redigo",
        importpath = "github.com/garyburd/redigo",
        sum = "h1:LofdAjjjqCSXMwLGgOgnE+rdPuvX9DxCqaHwKy7i/ko=",
        version = "v0.0.0-20150301180006-535138d7bcd7",
    )
    go_repository(
        name = "com_github_getsentry_raven_go",
        importpath = "github.com/getsentry/raven-go",
        sum = "h1:no+xWJRb5ZI7eE8TWgIq1jLulQiIoLG0IfYxv5JYMGs=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_getsentry_sentry_go",
        importpath = "github.com/getsentry/sentry-go",
        sum = "h1:zzaSm/vHmGllRM6Tpx1492r0YDzauArdBfkJRtY6P5k=",
        version = "v0.28.1",
    )
    go_repository(
        name = "com_github_ghemawat_stream",
        importpath = "github.com/ghemawat/stream",
        sum = "h1:r5GgOLGbza2wVHRzK7aAj6lWZjfbAwiu/RDCVOKjRyM=",
        version = "v0.0.0-20171120220530-696b145b53b9",
    )
    go_repository(
        name = "com_github_ghodss_yaml",
        importpath = "github.com/ghodss/yaml",
        sum = "h1:wQHKEahhL6wmXdzwWG11gIVCkOv05bNOh+Rxn0yngAk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_gin_contrib_sse",
        importpath = "github.com/gin-contrib/sse",
        sum = "h1:Y/yl/+YNO8GZSjAhjMsSuLt29uWRFHdHYUb5lYOV9qE=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_gin_gonic_gin",
        importpath = "github.com/gin-gonic/gin",
        sum = "h1:4+fr/el88TOO3ewCmQr8cx/CtZ/umlIRIs5M4NTNjf8=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_gliderlabs_ssh",
        importpath = "github.com/gliderlabs/ssh",
        sum = "h1:a4YXD1V7xMF9g5nTkdfnja3Sxy1PVDCj1Zg4Wb8vY6c=",
        version = "v0.3.8",
    )
    go_repository(
        name = "com_github_go_chi_chi_v5",
        importpath = "github.com/go-chi/chi/v5",
        sum = "h1:rDTPXLDHGATaeHvVlLcR4Qe0zftYethFucbjVQ1PxU8=",
        version = "v5.0.7",
    )
    go_repository(
        name = "com_github_go_critic_go_critic",
        importpath = "github.com/go-critic/go-critic",
        sum = "h1:abibh5XYBTASawfTQ0rA7dVtQT+6KzpGqb/J+DxRDaw=",
        version = "v0.6.3",
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
        name = "com_github_go_errors_errors",
        importpath = "github.com/go-errors/errors",
        sum = "h1:J6MZopCL4uSllY1OfXM374weqZFFItUbrImctkmUxIA=",
        version = "v1.4.2",
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
        sum = "h1:u+dcrgaguSSkbjzHwelEjc0Yj300NUevrrPphk/SoRA=",
        version = "v5.6.1",
    )
    go_repository(
        name = "com_github_go_git_go_git_fixtures_v4",
        importpath = "github.com/go-git/go-git-fixtures/v4",
        sum = "h1:eMje31YglSBqCdIqdhKBW8lokaMrL3uTkpGYlE2OOT4=",
        version = "v4.3.2-0.20231010084843-55a94097c399",
    )
    go_repository(
        name = "com_github_go_git_go_git_v5",
        importpath = "github.com/go-git/go-git/v5",
        sum = "h1:DAQ9APonnlvSWpvolXWIuV6Q6zXy2wHbN4cVlNR5Q+M=",
        version = "v5.13.1",
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
        sum = "h1:WtGNWLvXpe6ZudgnXrq0barxBImvnnJoMEhXAzcbM0I=",
        version = "v0.0.0-20200222043503-6f7a984d4dc4",
    )
    go_repository(
        name = "com_github_go_ini_ini",
        importpath = "github.com/go-ini/ini",
        sum = "h1:Mujh4R/dH6YL8bxuISne3xX2+qcQ9p0IxKAP6ExWoUo=",
        version = "v1.25.4",
    )
    go_repository(
        name = "com_github_go_jose_go_jose_v3",
        importpath = "github.com/go-jose/go-jose/v3",
        sum = "h1:fFKWeig/irsp7XD2zBxvnmA/XaRWp5V3CBsZXJF7G7k=",
        version = "v3.0.3",
    )
    go_repository(
        name = "com_github_go_jose_go_jose_v4",
        importpath = "github.com/go-jose/go-jose/v4",
        sum = "h1:M6T8+mKZl/+fNNuFHvGIzDz7BTLQPIounk/b9dw3AaE=",
        version = "v4.0.5",
    )
    go_repository(
        name = "com_github_go_kit_kit",
        importpath = "github.com/go-kit/kit",
        sum = "h1:wDJmvq38kDhkVxi50ni9ykkdUr1PKgqKOoi01fa0Mdk=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_github_go_kit_log",
        importpath = "github.com/go-kit/log",
        sum = "h1:DGJh0Sm43HbOeYDNnVZFl8BvcYVvjD5bqYJvp0REbwQ=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_go_logfmt_logfmt",
        importpath = "github.com/go-logfmt/logfmt",
        sum = "h1:TrB8swr/68K7m9CcGut2g3UOihhbcbiMAYiuTXdEih4=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_go_logr_logr",
        importpath = "github.com/go-logr/logr",
        sum = "h1:6pFjapn8bFcIbiKo3XT4j/BhANplGihG6tvd+8rYgrY=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_go_logr_stdr",
        importpath = "github.com/go-logr/stdr",
        sum = "h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_go_logr_zapr",
        importpath = "github.com/go-logr/zapr",
        sum = "h1:XGdV8XW8zdwFiwOA2Dryh1gj2KRQyOOoNmBy4EplIcQ=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_go_martini_martini",
        importpath = "github.com/go-martini/martini",
        sum = "h1:xveKWz2iaueeTaUgdetzel+U7exyigDYBryyVfV/rZk=",
        version = "v0.0.0-20170121215854-22fa46961aab",
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
        sum = "h1:aGday7OWupfMs+LbmLZG4k0MYXIANxcuBTYUC03zFCU=",
        version = "v0.23.0",
    )
    go_repository(
        name = "com_github_go_openapi_errors",
        importpath = "github.com/go-openapi/errors",
        sum = "h1:c4xY/OLxUBSTiepAg3j/MHuAv5mJhnf53LLMWFB+u/w=",
        version = "v0.22.0",
    )
    go_repository(
        name = "com_github_go_openapi_jsonpointer",
        importpath = "github.com/go-openapi/jsonpointer",
        sum = "h1:YgdVicSA9vH5RiHs9TZW5oyafXZFc6+2Vc1rr/O9oNQ=",
        version = "v0.21.0",
    )
    go_repository(
        name = "com_github_go_openapi_jsonreference",
        importpath = "github.com/go-openapi/jsonreference",
        sum = "h1:Rs+Y7hSXT83Jacb7kFyjn4ijOuVGSvOdF2+tg1TRrwQ=",
        version = "v0.21.0",
    )
    go_repository(
        name = "com_github_go_openapi_loads",
        importpath = "github.com/go-openapi/loads",
        sum = "h1:ECPGd4jX1U6NApCGG1We+uEozOAvXvJSF4nnwHZ8Aco=",
        version = "v0.22.0",
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
        sum = "h1:LTVzPc3p/RzRnkQqLRndbAzjY0d0BCL72A6j3CdL9ZY=",
        version = "v0.21.0",
    )
    go_repository(
        name = "com_github_go_openapi_strfmt",
        importpath = "github.com/go-openapi/strfmt",
        sum = "h1:nlUS6BCqcnAk0pyhi9Y+kdDVZdZMHfEKQiS4HaMgO/c=",
        version = "v0.23.0",
    )
    go_repository(
        name = "com_github_go_openapi_swag",
        importpath = "github.com/go-openapi/swag",
        sum = "h1:vsEVJDUo2hPJ2tu0/Xc+4noaxyEffXNIs3cOULZ+GrE=",
        version = "v0.23.0",
    )
    go_repository(
        name = "com_github_go_openapi_validate",
        importpath = "github.com/go-openapi/validate",
        sum = "h1:LdfDKwNbpB6Vn40xhTdNZAnfLECL81w+VX3BumrGD58=",
        version = "v0.24.0",
    )
    go_repository(
        name = "com_github_go_ping_ping",
        importpath = "github.com/go-ping/ping",
        sum = "h1:3MCGhVX4fyEUuhsfwPrsEdQw6xspHkv5zHsiSoDFZYw=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_go_playground_locales",
        importpath = "github.com/go-playground/locales",
        sum = "h1:EWaQ/wswjilfKLTECiXz7Rh+3BjFhfDFKv/oXslEjJA=",
        version = "v0.14.1",
    )
    go_repository(
        name = "com_github_go_playground_universal_translator",
        importpath = "github.com/go-playground/universal-translator",
        sum = "h1:82dyy6p4OuJq4/CByFNOn/jYrnRPArHwAcmLoJZxyho=",
        version = "v0.18.0",
    )
    go_repository(
        name = "com_github_go_playground_validator",
        importpath = "github.com/go-playground/validator",
        sum = "h1:UA72EPEogEnq76ehGdEDp4Mit+3FDh548oRqwVgNsHA=",
        version = "v9.31.0+incompatible",
    )
    go_repository(
        name = "com_github_go_playground_validator_v10",
        importpath = "github.com/go-playground/validator/v10",
        sum = "h1:prmOlTVv+YjZjmRmNSF3VmspqJIxJWXmqUsHwfTRRkQ=",
        version = "v10.11.1",
    )
    go_repository(
        name = "com_github_go_redis_redis",
        importpath = "github.com/go-redis/redis",
        sum = "h1:BKZuG6mCnRj5AOaWJXoCgf6rqTYnYJLe4en2hxT7r9o=",
        version = "v6.15.8+incompatible",
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
        sum = "h1:LedoTUt/eveggdHS9qUFC1EFSa8bU2+1pZjSRpvNJ1Y=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_go_stack_stack",
        importpath = "github.com/go-stack/stack",
        sum = "h1:5SgMzNM5HxrEjV0ww2lTmX6E2Izsfxas4+YHWRs3Lsk=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_github_go_task_slim_sprig",
        importpath = "github.com/go-task/slim-sprig",
        sum = "h1:p104kn46Q8WdvHunIJ9dAyjPVtrBPhSr3KT2yUst43I=",
        version = "v0.0.0-20210107165309-348f09dbbbc0",
    )
    go_repository(
        name = "com_github_go_task_slim_sprig_v3",
        importpath = "github.com/go-task/slim-sprig/v3",
        sum = "h1:sUs3vkvUymDpBKi3qH1YSqBQk9+9D/8M2mN1vB6EwHI=",
        version = "v3.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astcast",
        importpath = "github.com/go-toolsmith/astcast",
        sum = "h1:JojxlmI6STnFVG9yOImLeGREv8W2ocNUM+iOhR6jE7g=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astcopy",
        importpath = "github.com/go-toolsmith/astcopy",
        sum = "h1:OMgl1b1MEpjFQ1m5ztEO06rz5CUd3oBv9RF7+DyvdG8=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astequal",
        importpath = "github.com/go-toolsmith/astequal",
        sum = "h1:JbSszi42Jiqu36Gnf363HWS9MTEAz67vTQLponh3Moc=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_go_toolsmith_astfmt",
        importpath = "github.com/go-toolsmith/astfmt",
        sum = "h1:A0vDDXt+vsvLEdbMFJAUBI/uTbRw1ffOPnxsILnFL6k=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_astp",
        importpath = "github.com/go-toolsmith/astp",
        sum = "h1:alXE75TXgcmupDsMK1fRAy0YUzLzqPVvBKoyWV+KPXg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_strparse",
        importpath = "github.com/go-toolsmith/strparse",
        sum = "h1:Vcw78DnpCAKlM20kSbAyO4mPfJn/lyYA4BJUDxe2Jb4=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_toolsmith_typep",
        importpath = "github.com/go-toolsmith/typep",
        sum = "h1:8xdsa1+FSIH/RhEkgnD1j2CJOy5mNllW1Q9tRiYwvlk=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_go_xmlfmt_xmlfmt",
        importpath = "github.com/go-xmlfmt/xmlfmt",
        sum = "h1:khEcpUM4yFcxg4/FHQWkvVRmgijNXRfzkIDHh23ggEo=",
        version = "v0.0.0-20191208150333-d5b6f63a941b",
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
        name = "com_github_godbus_dbus",
        importpath = "github.com/godbus/dbus",
        sum = "h1:BWhy2j3IXJhjCbC68FptL43tDKIq8FladmaTs3Xs7Z8=",
        version = "v0.0.0-20190422162347-ade71ed3457e",
    )
    go_repository(
        name = "com_github_godbus_dbus_v5",
        importpath = "github.com/godbus/dbus/v5",
        sum = "h1:4KLkAxT3aOY8Li4FRJe/KvhoNFFxo0m6fNuFUO8QJUk=",
        version = "v5.1.0",
    )
    go_repository(
        name = "com_github_gofiber_fiber_v2",
        importpath = "github.com/gofiber/fiber/v2",
        sum = "h1:b0rYH6b06Df+4NyrbdptQL8ifuxw/Tf2DgfkZkDaxEo=",
        version = "v2.52.2",
    )
    go_repository(
        name = "com_github_gofrs_flock",
        importpath = "github.com/gofrs/flock",
        sum = "h1:MTLVXXHf8ekldpJk3AKicLij9MdwOWkZ+a/jHHZby9E=",
        version = "v0.12.1",
    )
    go_repository(
        name = "com_github_gofrs_uuid",
        importpath = "github.com/gofrs/uuid",
        sum = "h1:3qXRTX8/NbyulANqlc0lchS1gqAVxRgsuW1YrTJupqA=",
        version = "v4.4.0+incompatible",
    )
    go_repository(
        name = "com_github_gogo_googleapis",
        build_file_proto_mode = "disable",
        importpath = "github.com/gogo/googleapis",
        sum = "h1:1Yx4Myt7BxzvUr5ldGSbwYiZG6t9wGBZ+8/fX3Wvtq0=",
        version = "v1.4.1",
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
        name = "com_github_gogo_status",
        importpath = "github.com/gogo/status",
        sum = "h1:+eIkrewn5q6b30y+g/BJINVVdi2xH7je5MPJ3ZPK3JA=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_golang_glog",
        importpath = "github.com/golang/glog",
        sum = "h1:CNNw5U8lSiiBk7druxtSHHTsRWcxKoac6kZKm2peBBc=",
        version = "v1.2.4",
    )
    go_repository(
        name = "com_github_golang_groupcache",
        importpath = "github.com/golang/groupcache",
        sum = "h1:f+oWsMOmNPc8JmEHVZIycC7hBoQxHH9pNKQORJNozsQ=",
        version = "v0.0.0-20241129210726-2c02b8208cf8",
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
        sum = "h1:JdqV9zKUdtaa9gdPlywC3aeoEsR681PlKC+4F5gQgeo=",
        version = "v4.5.1",
    )
    go_repository(
        name = "com_github_golang_jwt_jwt_v5",
        importpath = "github.com/golang-jwt/jwt/v5",
        sum = "h1:OuVbFODueb089Lh128TAcimifWaLhJwVflnrgM17wHk=",
        version = "v5.2.1",
    )
    go_repository(
        name = "com_github_golang_mock",
        importpath = "github.com/golang/mock",
        # patch_args = ["-p1"],
        # patches = ["@io_bazel_rules_go//third_party:com_github_golang_mock-gazelle.patch"],
        sum = "h1:YojYx61/OLFsiv6Rw1Z96LpldJIy31o+UHmwAUMJ6/U=",
        version = "v1.7.0-rc.1",
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
        name = "com_github_golang_sql_civil",
        importpath = "github.com/golang-sql/civil",
        sum = "h1:au07oEsX2xN0ktxqI+Sida1w446QrXBRJ0nee3SNZlA=",
        version = "v0.0.0-20220223132316-b832511892a9",
    )
    go_repository(
        name = "com_github_golang_sql_sqlexp",
        importpath = "github.com/golang-sql/sqlexp",
        sum = "h1:ZCD6MBpcuOVfGVqsEmY5/4FtYiKz6tSyUv9LPEDei6A=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_golangci_check",
        importpath = "github.com/golangci/check",
        sum = "h1:23T5iq8rbUYlhpt5DB4XJkc6BU31uODLD1o1gKvZmD0=",
        version = "v0.0.0-20180506172741-cfe4005ccda2",
    )
    go_repository(
        name = "com_github_golangci_dupl",
        importpath = "github.com/golangci/dupl",
        sum = "h1:w8hkcTqaFpzKqonE9uMCefW1WDie15eSP/4MssdenaM=",
        version = "v0.0.0-20180902072040-3e9179ac440a",
    )
    go_repository(
        name = "com_github_golangci_go_misc",
        importpath = "github.com/golangci/go-misc",
        sum = "h1:6RGUuS7EGotKx6J5HIP8ZtyMdiDscjMLfRBSPuzVVeo=",
        version = "v0.0.0-20220329215616-d24fe342adfe",
    )
    go_repository(
        name = "com_github_golangci_gofmt",
        importpath = "github.com/golangci/gofmt",
        sum = "h1:iR3fYXUjHCR97qWS8ch1y9zPNsgXThGwjKPrYfqMPks=",
        version = "v0.0.0-20190930125516-244bba706f1a",
    )
    go_repository(
        name = "com_github_golangci_golangci_lint",
        importpath = "github.com/golangci/golangci-lint",
        sum = "h1:SLwZFEmDgopqZpfP495zCtV9REUf551JJlJ51Ql7NZA=",
        version = "v1.43.0",
    )
    go_repository(
        name = "com_github_golangci_lint_1",
        importpath = "github.com/golangci/lint-1",
        sum = "h1:MfyDlzVjl1hoaPzPD4Gpb/QgoRfSBR0jdhwGyAWwMSA=",
        version = "v0.0.0-20191013205115-297bf364a8e0",
    )
    go_repository(
        name = "com_github_golangci_maligned",
        importpath = "github.com/golangci/maligned",
        sum = "h1:kNY3/svz5T29MYHubXix4aDDuE3RWHkPvopM/EDv/MA=",
        version = "v0.0.0-20180506175553-b1d89398deca",
    )
    go_repository(
        name = "com_github_golangci_misspell",
        importpath = "github.com/golangci/misspell",
        sum = "h1:pLzmVdl3VxTOncgzHcvLOKirdvcx/TydsClUQXTehjo=",
        version = "v0.3.5",
    )
    go_repository(
        name = "com_github_golangci_revgrep",
        importpath = "github.com/golangci/revgrep",
        sum = "h1:SgM7GDZTxtTTQPU84heOxy34iG5Du7F2jcoZnvp+fXI=",
        version = "v0.0.0-20210930125155-c22e5001d4f2",
    )
    go_repository(
        name = "com_github_golangci_unconvert",
        importpath = "github.com/golangci/unconvert",
        sum = "h1:zwtduBRr5SSWhqsYNgcuWO2kFlpdOZbP0+yRjmvPGys=",
        version = "v0.0.0-20180507085042-28b1c447d1f4",
    )
    go_repository(
        name = "com_github_gomodule_redigo",
        importpath = "github.com/gomodule/redigo",
        sum = "h1:K/R+8tc58AaqLkqG2Ol3Qk+DR/TlNuhuh457pBFPtt0=",
        version = "v2.0.0+incompatible",
    )
    go_repository(
        name = "com_github_google_brotli_go_cbrotli",
        importpath = "github.com/google/brotli/go/cbrotli",
        sum = "h1:Cl8UPT6eo1fkL+TFS6LSW3J6gUGgD0rFKZVAgw/0iwY=",
        version = "v0.0.0-20230919092154-53947c15f577",
    )
    go_repository(
        name = "com_github_google_btree",
        importpath = "github.com/google/btree",
        sum = "h1:xf4v41cLI2Z6FxbKm+8Bu+m8ifhj15JuZ9sa0jZCMUU=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_google_cel_go",
        importpath = "github.com/google/cel-go",
        sum = "h1:nDx9r8S3L4pE61eDdt8igGj8rf5kjYR3ILxWIpWNi84=",
        version = "v0.20.1",
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
        sum = "h1:ubBKR94NR4pXUCY/MUsRVzd9umNW7ht7EG9hHfS9FX8=",
        version = "v24.12.23+incompatible",
    )
    go_repository(
        name = "com_github_google_gnostic",
        importpath = "github.com/google/gnostic",
        sum = "h1:FhTMOKj2VhjpouxvWJAV1TL304uMlb9zcDqkl6cEI54=",
        version = "v0.5.7-v3refs",
    )
    go_repository(
        name = "com_github_google_gnostic_models",
        importpath = "github.com/google/gnostic-models",
        sum = "h1:yo/ABAfM5IMRsS1VnXjTBvUb61tFIHozhlYvRgGre9I=",
        version = "v0.6.8",
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
        sum = "h1:5p+zYs/R4VGHkhyvgWurWrpJ2hW4Vv9fQI+GzdcwXLk=",
        version = "v0.17.0",
    )
    go_repository(
        name = "com_github_google_go_github_v27",
        importpath = "github.com/google/go-github/v27",
        sum = "h1:N/EEqsvJLgqTbepTiMBz+12KhwLovv6YvwpRezd+4Fg=",
        version = "v27.0.4",
    )
    go_repository(
        name = "com_github_google_go_github_v59",
        importpath = "github.com/google/go-github/v59",
        sum = "h1:7h6bgpF5as0YQLLkEiVqpgtJqjimMYhBkD4jT5aN3VA=",
        version = "v59.0.0",
    )
    go_repository(
        name = "com_github_google_go_pkcs11",
        importpath = "github.com/google/go-pkcs11",
        sum = "h1:PVRnTgtArZ3QQqTGtbtjtnIkzl2iY2kt24yqbrf7td8=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_google_go_querystring",
        importpath = "github.com/google/go-querystring",
        sum = "h1:AnCroh3fv4ZBgVIf1Iwtovgjaw/GiKJo8M8yD/fhyJ8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_google_gofuzz",
        importpath = "github.com/google/gofuzz",
        sum = "h1:xRy4A+RhZaiKjJ1bPfwQ8sedCA+YS2YcCHW6ec7JMi0=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_google_martian",
        importpath = "github.com/google/martian",
        sum = "h1:/CP5g8u/VJHijgedC/Legn3BAbAaWPgecwXBIDzw5no=",
        version = "v2.1.0+incompatible",
    )
    go_repository(
        name = "com_github_google_martian_v3",
        importpath = "github.com/google/martian/v3",
        sum = "h1:DIhPTQrbPkgs2yJYdXU/eNACCG5DVQjySNRNlflZ9Fc=",
        version = "v3.3.3",
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
        sum = "h1:kmjWCqn2qkEml422C2Rrd27c3VGxi6a/6HNq8QmHRKM=",
        version = "v0.0.0-20240525223248-4bfdf5a9a2af",
    )
    go_repository(
        name = "com_github_google_renameio",
        importpath = "github.com/google/renameio",
        sum = "h1:GOZbcHa3HfsPKPlmyPyN2KEohoMXOhdMbHrvbpl2QaA=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_google_s2a_go",
        importpath = "github.com/google/s2a-go",
        sum = "h1:zZDs9gcbt9ZPLV0ndSyQk6Kacx2g/X+SKYovpnz3SMM=",
        version = "v0.1.8",
    )
    go_repository(
        name = "com_github_google_shlex",
        importpath = "github.com/google/shlex",
        sum = "h1:El6M4kTTCOh6aBiKaUGG7oYTSPP8MxqL4YI3kZKwcP4=",
        version = "v0.0.0-20191202100458-e7afc7fbc510",
    )
    go_repository(
        name = "com_github_google_subcommands",
        importpath = "github.com/google/subcommands",
        sum = "h1:vWQspBTo2nEqTUFita5/KeEWlUL8kQObDFbub/EN9oE=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_google_uuid",
        importpath = "github.com/google/uuid",
        sum = "h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_google_wire",
        importpath = "github.com/google/wire",
        sum = "h1:HBkoIh4BdSxoyo9PveV8giw7ZsaBOvzWKfcg/6MrVwI=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_googleapis_enterprise_certificate_proxy",
        importpath = "github.com/googleapis/enterprise-certificate-proxy",
        sum = "h1:QRje2j5GZimBzlbhGA2V2QlGNgL8G6e+wGo/+/2bWI0=",
        version = "v0.3.3",
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        build_directives = [
            "gazelle:resolve proto go google/rpc/code.proto @org_golang_google_genproto_googleapis_rpc//code",
            "gazelle:resolve proto proto google/rpc/code.proto @googleapis//google/rpc:code_proto",
        ],
        importpath = "github.com/googleapis/gax-go/v2",
        sum = "h1:yitjD5f7jQHhyDsnhKEBU52NdvvdSeGzlAnDPT0hH1s=",
        version = "v2.13.0",
    )
    go_repository(
        name = "com_github_googleapis_gnostic",
        importpath = "github.com/googleapis/gnostic",
        sum = "h1:9fHAtK0uDfpveeqqo1hkEZJcFvYXAiCN3UutL8F9xHw=",
        version = "v0.5.5",
    )
    go_repository(
        name = "com_github_googlecloudplatform_cloudsql_proxy",
        importpath = "github.com/GoogleCloudPlatform/cloudsql-proxy",
        sum = "h1:EpJ0Ecs8VWw1dsmn7AfACq9wpDG3cotb03+GkeQ2dqQ=",
        version = "v1.35.0",
    )
    go_repository(
        name = "com_github_googlecloudplatform_opentelemetry_operations_go_detectors_gcp",
        importpath = "github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp",
        sum = "h1:3c8yed4lgqTt+oTQ+JNMDo+F4xprBf+O/il4ZC0nRLw=",
        version = "v1.25.0",
    )
    go_repository(
        name = "com_github_gopherjs_gopherjs",
        importpath = "github.com/gopherjs/gopherjs",
        sum = "h1:EGx4pi6eqNxGaHF6qqu48+N2wcFQ5qg5FXgOdqsJ5d8=",
        version = "v0.0.0-20181017120253-0766667cb4d1",
    )
    go_repository(
        name = "com_github_gordonklaus_ineffassign",
        importpath = "github.com/gordonklaus/ineffassign",
        sum = "h1:PVRE9d4AQKmbelZ7emNig1+NT27DUmKZn5qXxfio54U=",
        version = "v0.0.0-20210914165742-4cc7213b9bc8",
    )
    go_repository(
        name = "com_github_gorilla_css",
        importpath = "github.com/gorilla/css",
        sum = "h1:BQqNyPTi50JCFMTw/b67hByjMVXZRwGha6wxVGkeihY=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_gorilla_handlers",
        importpath = "github.com/gorilla/handlers",
        sum = "h1:0QniY0USkHQ1RGCLfKxeNHK9bkDHGRYGNDFBCS+YARg=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_gorilla_mux",
        importpath = "github.com/gorilla/mux",
        sum = "h1:TuBL49tXwgrFYWhqrNgrUNEY92u81SPhu7sTdzQEiWY=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_gorilla_websocket",
        importpath = "github.com/gorilla/websocket",
        sum = "h1:PPwGk2jz7EePpoHN/+ClbZu8SPxiqlu12wZP/3sWmnc=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_gostaticanalysis_analysisutil",
        importpath = "github.com/gostaticanalysis/analysisutil",
        sum = "h1:ZMCjoue3DtDWQ5WyU16YbjbQEQ3VuzwxALrpYd+HeKk=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_github_gostaticanalysis_comment",
        importpath = "github.com/gostaticanalysis/comment",
        sum = "h1:hlnx5+S2fY9Zo9ePo4AhgYsYHbM2+eAv8m/s1JiCd6Q=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_gostaticanalysis_forcetypeassert",
        importpath = "github.com/gostaticanalysis/forcetypeassert",
        sum = "h1:6eUflI3DiGusXGK6X7cCcIgVCpZ2CiZ1Q7jl6ZxNV70=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_gostaticanalysis_nilerr",
        importpath = "github.com/gostaticanalysis/nilerr",
        sum = "h1:ThE+hJP0fEp4zWLkWHWcRyI2Od0p7DlgYG3Uqrmrcpk=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_gregjones_httpcache",
        importpath = "github.com/gregjones/httpcache",
        sum = "h1:pdN6V1QBWetyv/0+wjACpqVH+eVULgEjkurDLq3goeM=",
        version = "v0.0.0-20180305231024-9cad4c3443a7",
    )
    go_repository(
        name = "com_github_groob_plist",
        importpath = "github.com/groob/plist",
        sum = "h1:saaSiB25B1wgaxrshQhurfPKUGJ4It3OxNJUy0rdOjU=",
        version = "v0.0.0-20220217120414-63fa881b19a5",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_middleware",
        importpath = "github.com/grpc-ecosystem/go-grpc-middleware",
        sum = "h1:+9834+KizmvFV7pXQGSXQTsaWhq2GjuNUt0aUU0YBYw=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_middleware_providers_prometheus",
        importpath = "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus",
        sum = "h1:qnpSQwGEnkcRpTqNOIR6bJbR0gAorgP9CSALpRcKoAA=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_middleware_v2",
        importpath = "github.com/grpc-ecosystem/go-grpc-middleware/v2",
        sum = "h1:pRhl55Yx1eC7BZ1N+BBWwnKaMyD8uC+34TLdndZMAKk=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_prometheus",
        importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
        sum = "h1:Ovs26xHkKqVztRpIrF/92BcuyuQ/YW4NSIpoGtfXNho=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway",
        importpath = "github.com/grpc-ecosystem/grpc-gateway",
        sum = "h1:gmcG1KaJ57LophUzW0Hy8NmPhnMZb4M0+kPpLofRdBo=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway_v2",
        importpath = "github.com/grpc-ecosystem/grpc-gateway/v2",
        sum = "h1:VNqngBF40hVlDloBruUehVYC3ArSgIyScOAyMRqBxRg=",
        version = "v2.25.1",
    )
    go_repository(
        name = "com_github_guptarohit_asciigraph",
        importpath = "github.com/guptarohit/asciigraph",
        sum = "h1:ccFnUF8xYIOUPPY3tmdvRyHqmn1MYI9iv1pLKX+/ZkQ=",
        version = "v0.5.5",
    )
    go_repository(
        name = "com_github_hanwen_go_fuse",
        importpath = "github.com/hanwen/go-fuse",
        sum = "h1:GxS9Zrn6c35/BnfiVsZVWmsG803xwE7eVRDvcf/BEVc=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hanwen_go_fuse_v2",
        importpath = "github.com/hanwen/go-fuse/v2",
        sum = "h1:SbJP1sUP+n1UF8NXBA14BuojmTez+mDgOk0bC057HQw=",
        version = "v2.7.2",
    )
    go_repository(
        name = "com_github_hashicorp_consul_api",
        importpath = "github.com/hashicorp/consul/api",
        sum = "h1:BNQPM9ytxj6jbjjdRPioQ94T6YXriSopn0i8COv6SRA=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_hashicorp_consul_sdk",
        importpath = "github.com/hashicorp/consul/sdk",
        sum = "h1:LnuDWGNsoajlhGyHJvuWW6FVqRl8JOTPqS6CPTsYjhY=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_hashicorp_errwrap",
        importpath = "github.com/hashicorp/errwrap",
        sum = "h1:OxrOeh75EUXMY8TBjag2fzXGZ40LB6IKw45YeGUDY2I=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_cleanhttp",
        importpath = "github.com/hashicorp/go-cleanhttp",
        sum = "h1:035FKYIWjmULyFRBKPs8TBQoi0x6d9G4xc9neXJWAZQ=",
        version = "v0.5.2",
    )
    go_repository(
        name = "com_github_hashicorp_go_immutable_radix",
        importpath = "github.com/hashicorp/go-immutable-radix",
        sum = "h1:DKHmCUm2hRBK510BaiZlwvpD40f8bJFeZnpfm2KLowc=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_hashicorp_go_memdb",
        importpath = "github.com/hashicorp/go-memdb",
        sum = "h1:XSL3NR682X/cVk2IeV0d70N4DZ9ljI885xAEU8IoK3c=",
        version = "v1.3.4",
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
        name = "com_github_hashicorp_go_net",
        importpath = "github.com/hashicorp/go.net",
        sum = "h1:sNCoNyDEvN1xa+X0baata4RdcpKwcMS6DH+xwfqPgjw=",
        version = "v0.0.1",
    )
    go_repository(
        name = "com_github_hashicorp_go_retryablehttp",
        importpath = "github.com/hashicorp/go-retryablehttp",
        sum = "h1:C8hUCYzor8PIfXHa4UrZkU4VvK8o9ISHxT2Q8+VepXU=",
        version = "v0.7.7",
    )
    go_repository(
        name = "com_github_hashicorp_go_rootcerts",
        importpath = "github.com/hashicorp/go-rootcerts",
        sum = "h1:Rqb66Oo1X/eSV1x66xbDccZjhJigjg0+e82kpwzSwCI=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_sockaddr",
        importpath = "github.com/hashicorp/go-sockaddr",
        sum = "h1:RSG8rKU28VTUTvEKghe5gIhIQpv8evvNpnDEyqO4u9I=",
        version = "v1.0.6",
    )
    go_repository(
        name = "com_github_hashicorp_go_syslog",
        importpath = "github.com/hashicorp/go-syslog",
        sum = "h1:KaodqZuhUoZereWVIYmpUgZysurB1kBLX2j0MwMrUAE=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_go_uuid",
        importpath = "github.com/hashicorp/go-uuid",
        sum = "h1:cfejS+Tpcp13yd5nYHWDI6qVCny6wyX2Mt5SGur2IGE=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_hashicorp_go_version",
        importpath = "github.com/hashicorp/go-version",
        sum = "h1:5tqGy27NaOTB8yJKUZELlFAS/LTKJkrmONwQKeRZfjY=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru",
        importpath = "github.com/hashicorp/golang-lru",
        sum = "h1:dV3g9Z/unq5DpblPpw+Oqcv4dU/1omnb4Ok8iPY6p1c=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru_v2",
        importpath = "github.com/hashicorp/golang-lru/v2",
        sum = "h1:a+bsQ5rvGLjzHuww6tVxozPZFVghXaHOwFs4luLUK2k=",
        version = "v2.0.7",
    )
    go_repository(
        name = "com_github_hashicorp_hcl",
        importpath = "github.com/hashicorp/hcl",
        sum = "h1:0Anlzjpi4vEasTeNFn2mLJgTSwt0+6sfsiTG8qcWGx4=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_logutils",
        importpath = "github.com/hashicorp/logutils",
        sum = "h1:dLEQVugN8vlakKOUE3ihGLTZJRB4j+M2cdTm/ORI65Y=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hashicorp_mdns",
        importpath = "github.com/hashicorp/mdns",
        sum = "h1:sY0CMhFmjIPDMlTB+HfymFHCaYLhgifZ0QhjaYKD/UQ=",
        version = "v1.0.4",
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
        name = "com_github_hdrhistogram_hdrhistogram_go",
        importpath = "github.com/HdrHistogram/hdrhistogram-go",
        sum = "h1:5IcZpTvzydCQeHzK4Ef/D5rrSqwxob0t8PQPMybUNFM=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_hinshun_vt10x",
        importpath = "github.com/hinshun/vt10x",
        sum = "h1:qv2VnGeEQHchGaZ/u7lxST/RaJw+cv273q79D81Xbog=",
        version = "v0.0.0-20220119200601-820417d04eec",
    )
    go_repository(
        name = "com_github_hpcloud_tail",
        importpath = "github.com/hpcloud/tail",
        sum = "h1:nfCOvKYfkgYP8hkirhJocXT2+zOD8yUNjXaWfTlyFKI=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_huandu_xstrings",
        importpath = "github.com/huandu/xstrings",
        sum = "h1:D17IlohoQq4UcpqD7fDk80P7l+lwAmlFaBHgOipl2FU=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_github_hydrogen18_memlistener",
        importpath = "github.com/hydrogen18/memlistener",
        sum = "h1:JR7eDj8HD6eXrc5fWLbSUnfcQFL06PYvCc0DKQnWfaU=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_iancoleman_strcase",
        importpath = "github.com/iancoleman/strcase",
        sum = "h1:nTXanmYxhfFAMjZL34Ov6gkzEsSJZ5DbhxWjvSASxEI=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_ianlancetaylor_demangle",
        importpath = "github.com/ianlancetaylor/demangle",
        sum = "h1:KwWnWVWCNtNq/ewIX7HIKnELmEx2nDP42yskD/pi7QE=",
        version = "v0.0.0-20240312041847-bd984b5ce465",
    )
    go_repository(
        name = "com_github_imdario_mergo",
        importpath = "github.com/imdario/mergo",
        sum = "h1:wwQJbIsHYGMUyLSPrEq1CT16AhnhNJQ51+4fdHUnCl4=",
        version = "v0.3.16",
    )
    go_repository(
        name = "com_github_imkira_go_interpol",
        importpath = "github.com/imkira/go-interpol",
        sum = "h1:KIiKr0VSG2CUW1hl1jpiyuzuJeKUUpC8iM1AIE7N1Vk=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_inconshreveable_mousetrap",
        importpath = "github.com/inconshreveable/mousetrap",
        sum = "h1:wN+x4NVGpMsO7ErUn/mUI3vEoE6Jt13X2s0bqwp9tc8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_intel_goresctrl",
        importpath = "github.com/intel/goresctrl",
        sum = "h1:kcDhjE3ZF/mNrJuRzLS3LY2Hp6atFaF1XVFBT7SVL2g=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_iris_contrib_httpexpect_v2",
        importpath = "github.com/iris-contrib/httpexpect/v2",
        sum = "h1:3cTZSyBBen/kfjCtgNFoUKi1u0FVXNaAjyRJOo6AVS4=",
        version = "v2.12.1",
    )
    go_repository(
        name = "com_github_iris_contrib_schema",
        importpath = "github.com/iris-contrib/schema",
        sum = "h1:CPSBLyx2e91H2yJzPuhGuifVRnZBBJ3pCOMbOvPZaTw=",
        version = "v0.0.6",
    )
    go_repository(
        name = "com_github_j_keck_arping",
        importpath = "github.com/j-keck/arping",
        sum = "h1:hlLhuXgQkzIJTZuhMigvG/CuSkaspeaD9hRDk2zuiMI=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_jackc_chunkreader_v2",
        importpath = "github.com/jackc/chunkreader/v2",
        sum = "h1:i+RDz65UE+mmpjTfyz0MoVTnzeYxroil2G82ki7MGG8=",
        version = "v2.0.1",
    )
    go_repository(
        name = "com_github_jackc_pgconn",
        importpath = "github.com/jackc/pgconn",
        sum = "h1:bVoTr12EGANZz66nZPkMInAV/KHD2TxH9npjXXgiB3w=",
        version = "v1.14.3",
    )
    go_repository(
        name = "com_github_jackc_pgerrcode",
        importpath = "github.com/jackc/pgerrcode",
        sum = "h1:s+4MhCQ6YrzisK6hFJUX53drDT4UsSW3DEhKn0ifuHw=",
        version = "v0.0.0-20220416144525-469b46aa5efa",
    )
    go_repository(
        name = "com_github_jackc_pgio",
        importpath = "github.com/jackc/pgio",
        sum = "h1:g12B9UwVnzGhueNavwioyEEpAmqMe1E/BN9ES+8ovkE=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_jackc_pgpassfile",
        importpath = "github.com/jackc/pgpassfile",
        sum = "h1:/6Hmqy13Ss2zCq62VdNG8tM1wchn8zjSGOBJ6icpsIM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_jackc_pgproto3_v2",
        importpath = "github.com/jackc/pgproto3/v2",
        sum = "h1:1HLSx5H+tXR9pW3in3zaztoEwQYRC9SQaYUHjTSUOag=",
        version = "v2.3.3",
    )
    go_repository(
        name = "com_github_jackc_pgservicefile",
        importpath = "github.com/jackc/pgservicefile",
        sum = "h1:bbPeKD0xmW/Y25WS6cokEszi5g+S0QxI/d45PkRi7Nk=",
        version = "v0.0.0-20221227161230-091c0ba34f0a",
    )
    go_repository(
        name = "com_github_jackc_pgtype",
        importpath = "github.com/jackc/pgtype",
        sum = "h1:y+xUdabmyMkJLyApYuPj38mW+aAIqCe5uuBB51rH3Vw=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_github_jackc_pgx_v4",
        importpath = "github.com/jackc/pgx/v4",
        sum = "h1:dE2/TrEsGX3RBprb3qryqSV9Y60iZN1C6i8IrmW9/BA=",
        version = "v4.18.3",
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
        name = "com_github_jgautheron_goconst",
        importpath = "github.com/jgautheron/goconst",
        sum = "h1:HxVbL1MhydKs8R8n/HE5NPvzfaYmQJA3o879lE4+WcM=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_jhump_gopoet",
        importpath = "github.com/jhump/gopoet",
        sum = "h1:gYjOPnzHd2nzB37xYQZxj4EIQNpBrBskRqQQ3q4ZgSg=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_jhump_goprotoc",
        importpath = "github.com/jhump/goprotoc",
        sum = "h1:Y1UgUX+txUznfqcGdDef8ZOVlyQvnV0pKWZH08RmZuo=",
        version = "v0.5.0",
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
        name = "com_github_jingyugao_rowserrcheck",
        importpath = "github.com/jingyugao/rowserrcheck",
        sum = "h1:zibz55j/MJtLsjP1OF4bSdgXxwL1b+Vn7Tjzq7gFzUs=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_jinzhu_configor",
        importpath = "github.com/jinzhu/configor",
        sum = "h1:OKk9dsR8i6HPOCZR8BcMtcEImAFjIhbJFZNyn5GCZko=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_jinzhu_gorm",
        importpath = "github.com/jinzhu/gorm",
        sum = "h1:+IyIjPEABKRpsu/F8OvDPy9fyQlgsg2luMV2ZIH5i5o=",
        version = "v1.9.16",
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
        name = "com_github_jirfag_go_printf_func_name",
        importpath = "github.com/jirfag/go-printf-func-name",
        sum = "h1:KA9BjwUk7KlCh6S9EAGWBt1oExIUv9WyNCiRz5amv48=",
        version = "v0.0.0-20200119135958-7558a9eaa5af",
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath",
        importpath = "github.com/jmespath/go-jmespath",
        sum = "h1:BEgLn5cpjn8UN1mAw4NjwDrS35OdebyEtFe+9YPoQUg=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath_internal_testify",
        importpath = "github.com/jmespath/go-jmespath/internal/testify",
        sum = "h1:shLQSRRSCCPj3f2gpwzGwWFoC7ycTf1rcQZHOlsJ6N8=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_jmhodges_levigo",
        importpath = "github.com/jmhodges/levigo",
        sum = "h1:q5EC36kV79HWeTBWsod3mG11EgStG3qArTKcvlksN1U=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_jmoiron_sqlx",
        importpath = "github.com/jmoiron/sqlx",
        sum = "h1:41Ip0zITnmWNR/vHV+S4m+VoUivnWY5E4OJfLZjCJMA=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_joefitzgerald_rainbow_reporter",
        importpath = "github.com/joefitzgerald/rainbow-reporter",
        sum = "h1:AuMG652zjdzI0YCCnXAqATtRBpGXMcAnrajcaTrSeuo=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_johanneskaufmann_html_to_markdown",
        importpath = "github.com/JohannesKaufmann/html-to-markdown",
        sum = "h1:CMAl6hz2MRfs03ZGAwYqQTC43Egi3vbc9SVo6nEKUE0=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_github_joker_jade",
        importpath = "github.com/Joker/jade",
        sum = "h1:Qbeh12Vq6BxURXT1qZBRHsDxeURB8ztcL6f3EXSGeHk=",
        version = "v1.1.3",
    )
    go_repository(
        name = "com_github_jonboulle_clockwork",
        importpath = "github.com/jonboulle/clockwork",
        sum = "h1:p4Cf1aMWXnXAUh8lVfewRBx1zaTSYKrKMF2g3ST4RZ4=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_josephspurrier_goversioninfo",
        importpath = "github.com/josephspurrier/goversioninfo",
        sum = "h1:Puhl12NSHUSALHSuzYwPYQkqa2E1+7SrtAPJorKK0C8=",
        version = "v1.4.0",
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
        name = "com_github_jpillora_backoff",
        importpath = "github.com/jpillora/backoff",
        sum = "h1:uvFg412JmmHBHw7iwprIxkPMI+sGQ4kzOWsMeHnm2EA=",
        version = "v1.0.0",
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
        name = "com_github_jstemmer_go_junit_report",
        importpath = "github.com/jstemmer/go-junit-report",
        sum = "h1:6QPYqodiu3GuPL+7mfx+NwDdp2eTkp9IfEUpgAwUN0o=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_github_jtolds_gls",
        importpath = "github.com/jtolds/gls",
        sum = "h1:xdiiI2gbIgH/gLH7ADydsJ1uDOEzR8yvV7C0MuV77Wo=",
        version = "v4.20.0+incompatible",
    )
    go_repository(
        name = "com_github_julienschmidt_httprouter",
        importpath = "github.com/julienschmidt/httprouter",
        sum = "h1:U0609e9tgbseu3rBINet9P48AI/D3oJs4dN7jwJOQ1U=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_julz_importas",
        importpath = "github.com/julz/importas",
        sum = "h1:F78HnrsjY3cR7j0etXy5+TU1Zuy7Xt08X/1aJnH5xXY=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_kataras_blocks",
        importpath = "github.com/kataras/blocks",
        sum = "h1:cF3RDY/vxnSRezc7vLFlQFTYXG/yAr1o7WImJuZbzC4=",
        version = "v0.0.7",
    )
    go_repository(
        name = "com_github_kataras_golog",
        importpath = "github.com/kataras/golog",
        sum = "h1:isP8th4PJH2SrbkciKnylaND9xoTtfxv++NB+DF0l9g=",
        version = "v0.1.8",
    )
    go_repository(
        name = "com_github_kataras_iris_v12",
        importpath = "github.com/kataras/iris/v12",
        sum = "h1:WzDY5nGuW/LgVaFS5BtTkW3crdSKJ/FEgWnxPnIVVLI=",
        version = "v12.2.0",
    )
    go_repository(
        name = "com_github_kataras_pio",
        importpath = "github.com/kataras/pio",
        sum = "h1:kqreJ5KOEXGMwHAWHDwIl+mjfNCPhAwZPa8gK7MKlyw=",
        version = "v0.0.11",
    )
    go_repository(
        name = "com_github_kataras_sitemap",
        importpath = "github.com/kataras/sitemap",
        sum = "h1:w71CRMMKYMJh6LR2wTgnk5hSgjVNB9KL60n5e2KHvLY=",
        version = "v0.0.6",
    )
    go_repository(
        name = "com_github_kataras_tunnel",
        importpath = "github.com/kataras/tunnel",
        sum = "h1:sCAqWuJV7nPzGrlb0os3j49lk2JhILT0rID38NHNLpA=",
        version = "v0.0.4",
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
        name = "com_github_kisielk_errcheck",
        importpath = "github.com/kisielk/errcheck",
        sum = "h1:YTDO4pNy7AUN/021p+JGHycQyYNIyMoenM1YDVK6RlY=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_kisielk_gotool",
        importpath = "github.com/kisielk/gotool",
        sum = "h1:AV2c/EiW3KqPNT9ZKl07ehoAGi4C5/01Cfbblndcapg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_klauspost_compress",
        importpath = "github.com/klauspost/compress",
        sum = "h1:In6xLpyWOi1+C7tXUUWv2ot1QvBjxevKAaI6IXrJmUc=",
        version = "v1.17.11",
    )
    go_repository(
        name = "com_github_klauspost_cpuid_v2",
        importpath = "github.com/klauspost/cpuid/v2",
        sum = "h1:ZWSB3igEs+d0qvnxR/ZBzXVmxkgt8DdzP6m9pfuVLDM=",
        version = "v2.2.7",
    )
    go_repository(
        name = "com_github_konsorten_go_windows_terminal_sequences",
        importpath = "github.com/konsorten/go-windows-terminal-sequences",
        sum = "h1:CE8S1cTafDpPvMhIxNJKvHsGVBgn1xWYf1NbHQhywc8=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_kr_logfmt",
        importpath = "github.com/kr/logfmt",
        sum = "h1:T+h1c/A9Gawja4Y9mFVWj2vyii2bbUNDw3kt9VxK2EY=",
        version = "v0.0.0-20140226030751-b84e30acd515",
    )
    go_repository(
        name = "com_github_kr_pretty",
        importpath = "github.com/kr/pretty",
        sum = "h1:flRD4NNwYAUpkphVc1HcthR4KEIFJ65n8Mw5qdRn3LE=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_kr_pty",
        importpath = "github.com/kr/pty",
        sum = "h1:hyz3dwM5QLc1Rfoz4FuWJQG5BN7tc6K1MndAUnGpQr4=",
        version = "v1.1.5",
    )
    go_repository(
        name = "com_github_kr_text",
        importpath = "github.com/kr/text",
        sum = "h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_kulti_thelper",
        importpath = "github.com/kulti/thelper",
        sum = "h1:K4xulKkwOCnT1CDms6Ex3uG1dvSMUUQe9zxgYQgbRXs=",
        version = "v0.6.2",
    )
    go_repository(
        name = "com_github_kunwardeep_paralleltest",
        importpath = "github.com/kunwardeep/paralleltest",
        sum = "h1:UdKIkImEAXjR1chUWLn+PNXqWUGs//7tzMeWuP7NhmI=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_kylelemons_godebug",
        importpath = "github.com/kylelemons/godebug",
        sum = "h1:RPNrshWIDI6G2gRW9EHilWtl7Z6Sb1BR0xunSBf0SNc=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_kyoh86_exportloopref",
        importpath = "github.com/kyoh86/exportloopref",
        sum = "h1:5Ry/at+eFdkX9Vsdw3qU4YkvGtzuVfzT4X7S77LoN/M=",
        version = "v0.1.8",
    )
    go_repository(
        name = "com_github_labstack_echo",
        importpath = "github.com/labstack/echo",
        sum = "h1:pGRcYk231ExFAyoAjAfD85kQzRJCRI8bbnE7CX5OEgg=",
        version = "v3.3.10+incompatible",
    )
    go_repository(
        name = "com_github_labstack_echo_v4",
        importpath = "github.com/labstack/echo/v4",
        sum = "h1:5CiyngihEO4HXsz3vVsJn7f8xAlWwRr3aY6Ih280ZKA=",
        version = "v4.10.0",
    )
    go_repository(
        name = "com_github_labstack_gommon",
        importpath = "github.com/labstack/gommon",
        sum = "h1:y7cvthEAEbU0yHOf4axH8ZG2NH8knB9iNSoTO8dyIk8=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_ldez_gomoddirectives",
        importpath = "github.com/ldez/gomoddirectives",
        sum = "h1:y7MBaisZVDYmKvt9/l1mjNCiSA1BVn34U0ObUcJwlhA=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_ldez_tagliatelle",
        importpath = "github.com/ldez/tagliatelle",
        sum = "h1:3BqVVlReVUZwafJUwQ+oxbx2BEX2vUG4Yu/NOfMiKiM=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_leodido_go_urn",
        importpath = "github.com/leodido/go-urn",
        sum = "h1:BqpAaACuzVSgi/VLzGZIobT2z4v53pjosyNd9Yv6n/w=",
        version = "v1.2.1",
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
        name = "com_github_lib_pq",
        importpath = "github.com/lib/pq",
        sum = "h1:YXG7RB+JIjhP29X+OtkiDnYaXQwpS4JEWq7dtCCRUEw=",
        version = "v1.10.9",
    )
    go_repository(
        name = "com_github_linuxkit_virtsock",
        importpath = "github.com/linuxkit/virtsock",
        sum = "h1:jUp75lepDg0phMUJBCmvaeFDldD2N3S1lBuPwUTszio=",
        version = "v0.0.0-20201010232012-f8cee7dfc7a3",
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
        name = "com_github_lucasb_eyer_go_colorful",
        importpath = "github.com/lucasb-eyer/go-colorful",
        sum = "h1:1nnpGOrhyZZuNyfu1QjKiUICQ74+3FNCN69Aj6K7nkY=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_lufia_plan9stats",
        importpath = "github.com/lufia/plan9stats",
        sum = "h1:N9zuLhTvBSRt0gWSiJswwQ2HqDmtX/ZCDJURnKUt1Ik=",
        version = "v0.0.0-20230326075908-cb1d2100619a",
    )
    go_repository(
        name = "com_github_lyft_protoc_gen_star_v2",
        importpath = "github.com/lyft/protoc-gen-star/v2",
        sum = "h1:sIXJOMrYnQZJu7OB7ANSF4MYri2fTEGIsRLz6LwI4xE=",
        version = "v2.0.4-0.20230330145011-496ad1ac90a4",
    )
    go_repository(
        name = "com_github_magiconair_properties",
        importpath = "github.com/magiconair/properties",
        sum = "h1:5ibWZ6iY0NctNGWo87LalDlEZ6R41TqbbDamhfG/Qzo=",
        version = "v1.8.6",
    )
    go_repository(
        name = "com_github_mailgun_raymond_v2",
        importpath = "github.com/mailgun/raymond/v2",
        sum = "h1:5dmlB680ZkFG2RN/0lvTAghrSxIESeu9/2aeDqACtjw=",
        version = "v2.0.48",
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
        name = "com_github_maratori_testpackage",
        importpath = "github.com/maratori/testpackage",
        sum = "h1:QtJ5ZjqapShm0w5DosRjg0PRlSdAdlx+W6cCKoALdbQ=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_marstr_guid",
        importpath = "github.com/marstr/guid",
        sum = "h1:/M4H/1G4avsieL6BbUwCOBzulmoeKVP5ux/3mQNnbyI=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_masterminds_goutils",
        importpath = "github.com/Masterminds/goutils",
        sum = "h1:5nUrii3FMTL5diU80unEVvNevw1nH4+ZV4DSLVJLSYI=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_masterminds_semver",
        importpath = "github.com/Masterminds/semver",
        sum = "h1:H65muMkzWKEuNDnfl9d70GUjFniHKHRbFPGBuZ3QEww=",
        version = "v1.5.0",
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
        name = "com_github_matoous_godox",
        importpath = "github.com/matoous/godox",
        sum = "h1:pWxk9e//NbPwfxat7RXkts09K+dEBJWakUWwICVqYbA=",
        version = "v0.0.0-20210227103229-6504466cf951",
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
        name = "com_github_mattn_go_runewidth",
        importpath = "github.com/mattn/go-runewidth",
        sum = "h1:UNAjwbU9l54TA3KzvqLGxwWjHmMgBUVhBiTjelZgg3U=",
        version = "v0.0.15",
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
        sum = "h1:2gZY6PC6kBnID23Tichd1K+Z0oS6nE/XwU+Vz/5o4kU=",
        version = "v1.14.22",
    )
    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions",
        importpath = "github.com/matttproud/golang_protobuf_extensions",
        sum = "h1:mmDVorXM7PCGKw94cs5zkfA9PSy5pEvNWRP0ET0TIVo=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_maxbrunsfeld_counterfeiter_v6",
        importpath = "github.com/maxbrunsfeld/counterfeiter/v6",
        sum = "h1:g+4J5sZg6osfvEfkRZxJ1em0VT95/UOZgi/l7zi1/oE=",
        version = "v6.2.2",
    )
    go_repository(
        name = "com_github_mbilski_exhaustivestruct",
        importpath = "github.com/mbilski/exhaustivestruct",
        sum = "h1:wCBmUnSYufAHO6J4AVWY6ff+oxWxsVFrwgOdMUQePUo=",
        version = "v1.2.0",
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
        name = "com_github_mfridman_tparse",
        importpath = "github.com/mfridman/tparse",
        sum = "h1:rEub2HkmqNzAsGviYkwUDYHe2bkmPq7LdM4XC+kMUuI=",
        version = "v0.11.1",
    )
    go_repository(
        name = "com_github_mgechev_dots",
        importpath = "github.com/mgechev/dots",
        sum = "h1:zpIH83+oKzcpryru8ceC6BxnoG8TBrhgAvRg8obzup0=",
        version = "v0.0.0-20210922191527-e955255bf517",
    )
    go_repository(
        name = "com_github_mgechev_revive",
        importpath = "github.com/mgechev/revive",
        sum = "h1:GjFml7ZsoR0IrQ2E2YIvWFNS5GPDV7xNwvA5GM1HZC4=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_mgutz_ansi",
        importpath = "github.com/mgutz/ansi",
        sum = "h1:5PJl274Y63IEHC+7izoQE9x6ikvDFZS2mDVS3drnohI=",
        version = "v0.0.0-20200706080929-d51e80ef957d",
    )
    go_repository(
        name = "com_github_microcosm_cc_bluemonday",
        importpath = "github.com/microcosm-cc/bluemonday",
        sum = "h1:SMZe2IGa0NuHvnVNAZ+6B38gsTbi5e4sViiWJyDDqFY=",
        version = "v1.0.23",
    )
    go_repository(
        name = "com_github_microsoft_cosesign1go",
        importpath = "github.com/Microsoft/cosesign1go",
        sum = "h1:JnHY2wQkIK4HmstaK5rMdM4S83nIC7fJmD1phOLj9qo=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_microsoft_didx509go",
        importpath = "github.com/Microsoft/didx509go",
        sum = "h1:x1b3Hp1svlSgj4e4191cDtjYCgQIwQXZgudftw7VKtE=",
        version = "v0.0.2",
    )
    go_repository(
        name = "com_github_microsoft_go_mssqldb",
        importpath = "github.com/microsoft/go-mssqldb",
        sum = "h1:sgMPW0HA6Ihd37Yx0MzHyKD726C2kY/8KJsQtXHNaAs=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_microsoft_go_winio",
        importpath = "github.com/Microsoft/go-winio",
        sum = "h1:F2VQgta7ecxGYO8k3ZZz3RS8fVIXVxONVUPlNERoyfY=",
        version = "v0.6.2",
    )
    go_repository(
        name = "com_github_microsoft_hcsshim",
        importpath = "github.com/Microsoft/hcsshim",
        sum = "h1:LS9NXqXhMoqNCplK1ApmVSfB4UnVLRDWRapB6EIlxE0=",
        version = "v0.12.3",
    )
    go_repository(
        name = "com_github_microsoft_hcsshim_test",
        importpath = "github.com/Microsoft/hcsshim/test",
        sum = "h1:4FA+QBaydEHlwxg0lMN3rhwoDaQy6LKhVWR4qvq4BuA=",
        version = "v0.0.0-20210227013316-43a75bb4edd3",
    )
    go_repository(
        name = "com_github_miekg_dns",
        importpath = "github.com/miekg/dns",
        sum = "h1:nLxbwF3XxhwVSm8g9Dghm9MHPaUZuqhPiGL+675ZmEs=",
        version = "v1.1.61",
    )
    go_repository(
        name = "com_github_miekg_pkcs11",
        importpath = "github.com/miekg/pkcs11",
        sum = "h1:Ugu9pdy6vAYku5DEpVWVFPYnzV+bxB+iRdbuFSu7TvU=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_minio_highwayhash",
        importpath = "github.com/minio/highwayhash",
        sum = "h1:Aak5U0nElisjDCfPSG79Tgzkn2gl66NxOMspRrKnA/g=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_minio_sha256_simd",
        importpath = "github.com/minio/sha256-simd",
        sum = "h1:v1ta+49hkWZyvaKwrQB8elexRqm6Y0aMLjCNsrYxo6g=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mistifyio_go_zfs",
        importpath = "github.com/mistifyio/go-zfs",
        sum = "h1:aKW/4cBs+yK6gpqU3K/oIwk9Q/XICqd3zOX/UFuvqmk=",
        version = "v2.1.2-0.20190413222219-f784269be439+incompatible",
    )
    go_repository(
        name = "com_github_mistifyio_go_zfs_v3",
        importpath = "github.com/mistifyio/go-zfs/v3",
        sum = "h1:YaoXgBePoMA12+S1u/ddkv+QqxcfiZK4prI6HPnkFiU=",
        version = "v3.0.1",
    )
    go_repository(
        name = "com_github_mitchellh_cli",
        importpath = "github.com/mitchellh/cli",
        sum = "h1:OxRIeJXpAMztws/XHlN2vu6imG5Dpq+j61AzAX5fLng=",
        version = "v1.1.5",
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
        name = "com_github_mitchellh_go_testing_interface",
        importpath = "github.com/mitchellh/go-testing-interface",
        sum = "h1:fzU/JVNcaqHQEcVFAKeR41fkiLdIPrefOvVG1VZ96U0=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mitchellh_go_wordwrap",
        importpath = "github.com/mitchellh/go-wordwrap",
        sum = "h1:TLuKupo69TCn6TQSyGxwI1EblZZEsQ0vMlAFQflz0v0=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_mitchellh_gox",
        importpath = "github.com/mitchellh/gox",
        sum = "h1:lfGJxY7ToLJQjHHwi0EX6uYBdK78egf954SQl13PQJc=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_mitchellh_iochan",
        importpath = "github.com/mitchellh/iochan",
        sum = "h1:C+X3KsSTLFVBr/tK1eYN/vs4rJcvsiLU338UhYPJWeY=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mitchellh_mapstructure",
        importpath = "github.com/mitchellh/mapstructure",
        sum = "h1:jeMsZIYE/09sWLaz43PL7Gy6RuMjD2eJVyuac5Z2hdY=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_mitchellh_osext",
        importpath = "github.com/mitchellh/osext",
        sum = "h1:2+myh5ml7lgEU/51gbeLHfKGNfgEQQIWrlbdaOsidbQ=",
        version = "v0.0.0-20151018003038-5e2d6d41470f",
    )
    go_repository(
        name = "com_github_mitchellh_reflectwalk",
        importpath = "github.com/mitchellh/reflectwalk",
        sum = "h1:G2LzWKi524PWgd3mLHV8Y5k7s6XUvT0Gef6zxSIeXaQ=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_mkevac_debugcharts",
        importpath = "github.com/mkevac/debugcharts",
        sum = "h1:/mD+ABZyXD39BzJI2XyRJlqdZG11gXFo0SSynL+OFeU=",
        version = "v0.0.0-20191222103121-ae1c48aa8615",
    )
    go_repository(
        name = "com_github_mmcloughlin_avo",
        importpath = "github.com/mmcloughlin/avo",
        sum = "h1:nAco9/aI9Lg2kiuROBY6BhCI/z0t5jEvJfjWbL8qXLU=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_moby_docker_image_spec",
        importpath = "github.com/moby/docker-image-spec",
        sum = "h1:jMKff3w6PgbfSa69GfNg+zN/XLhfXJGnEx3Nl2EsFP0=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_moby_locker",
        importpath = "github.com/moby/locker",
        sum = "h1:fOXqR41zeveg4fFODix+1Ch4mj/gT0NE1XJbp/epuBg=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_moby_spdystream",
        importpath = "github.com/moby/spdystream",
        sum = "h1:Vy79D6mHeJJjiPdFEL2yku1kl0chZpJfZcPpb16BRl8=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_moby_sys_mount",
        importpath = "github.com/moby/sys/mount",
        sum = "h1:fX1SVkXFJ47XWDoeFW4Sq7PdQJnV2QIDZAqjNqgEjUs=",
        version = "v0.3.3",
    )
    go_repository(
        name = "com_github_moby_sys_mountinfo",
        importpath = "github.com/moby/sys/mountinfo",
        sum = "h1:1shs6aH5s4o5H2zQLn796ADW1wMrIwHsyJ2v9KouLrg=",
        version = "v0.7.2",
    )
    go_repository(
        name = "com_github_moby_sys_sequential",
        importpath = "github.com/moby/sys/sequential",
        sum = "h1:OPvI35Lzn9K04PBbCLW0g4LcFAJgHsvXsRyewg5lXtc=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_moby_sys_signal",
        importpath = "github.com/moby/sys/signal",
        sum = "h1:25RW3d5TnQEoKvRbEKUGay6DCQ46IxAVTT9CUMgmsSI=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_github_moby_sys_symlink",
        importpath = "github.com/moby/sys/symlink",
        sum = "h1:tk1rOM+Ljp0nFmfOIBtlV3rTDlWOwFRhjEeAhZB0nZc=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_moby_sys_user",
        importpath = "github.com/moby/sys/user",
        sum = "h1:9ni5DlcW5an3SvRSx4MouotOygvzaXbaSrc/wGDFWPo=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_moby_sys_userns",
        importpath = "github.com/moby/sys/userns",
        sum = "h1:tVLXkFOxVu9A64/yh59slHVv9ahO9UIev4JZusOLG/g=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_moby_term",
        importpath = "github.com/moby/term",
        sum = "h1:HfkjXDfhgVaN5rmueG8cL8KKeFNecRCXFhaJ2qZ5SKA=",
        version = "v0.0.0-20221205130635-1aeaba878587",
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
        name = "com_github_montanaflynn_stats",
        importpath = "github.com/montanaflynn/stats",
        sum = "h1:etflOAAHORrCC44V+aR6Ftzort912ZU+YLiSTuV8eaE=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_github_moricho_tparallel",
        importpath = "github.com/moricho/tparallel",
        sum = "h1:95FytivzT6rYzdJLdtfn6m1bfFJylOJK41+lgv/EHf4=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_morikuni_aec",
        importpath = "github.com/morikuni/aec",
        sum = "h1:nP9CBfwrvYnBRgY6qfDQkygYDmYwOilePFkwzv4dU8A=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_mrunalp_fileutils",
        importpath = "github.com/mrunalp/fileutils",
        sum = "h1:NKzVxiH7eSk+OQ4M+ZYW1K6h27RUV3MI6NUTsHhU6Z4=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_mschoch_smat",
        importpath = "github.com/mschoch/smat",
        sum = "h1:8imxQsjDm8yFEAVBe7azKmKSgzSkZXDuKkSq9374khM=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_muesli_reflow",
        importpath = "github.com/muesli/reflow",
        sum = "h1:IFsN6K9NfGtjeggFP+68I4chLZV2yIKsXJFNZ+eWh6s=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_muesli_termenv",
        importpath = "github.com/muesli/termenv",
        sum = "h1:fwNUbu2mfWlgicwG7qYzs06aOI8Z/zKPAv8J4uKbT+o=",
        version = "v0.11.0",
    )
    go_repository(
        name = "com_github_munnerz_goautoneg",
        importpath = "github.com/munnerz/goautoneg",
        sum = "h1:C3w9PqII01/Oq1c1nUAm88MOHcQC9l5mIlSMApZMrHA=",
        version = "v0.0.0-20191010083416-a7dc8b61c822",
    )
    go_repository(
        name = "com_github_mwitkow_go_conntrack",
        importpath = "github.com/mwitkow/go-conntrack",
        sum = "h1:KUppIJq7/+SVif2QVs3tOP0zanoHgBEVAwHxUSIzRqU=",
        version = "v0.0.0-20190716064945-2f068394615f",
    )
    go_repository(
        name = "com_github_mwitkow_grpc_proxy",
        importpath = "github.com/mwitkow/grpc-proxy",
        sum = "h1:62uLwA3l2JMH84liO4ZhnjTH5PjFyCYxbHLgXPaJMtI=",
        version = "v0.0.0-20230212185441-f345521cb9c9",
    )
    go_repository(
        name = "com_github_mxk_go_flowrate",
        importpath = "github.com/mxk/go-flowrate",
        sum = "h1:y5//uYreIhSUg3J1GEMiLbxo1LJaP8RfCpH6pymGZus=",
        version = "v0.0.0-20140419014527-cca7078d478f",
    )
    go_repository(
        name = "com_github_nakabonne_nestif",
        importpath = "github.com/nakabonne/nestif",
        sum = "h1:wm28nZjhQY5HyYPx+weN3Q65k6ilSBxDb8v5S81B81U=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_nbutton23_zxcvbn_go",
        importpath = "github.com/nbutton23/zxcvbn-go",
        sum = "h1:4kuARK6Y6FxaNu/BnU2OAaLF86eTVhP2hjTB6iMvItA=",
        version = "v0.0.0-20210217022336-fa2cb2858354",
    )
    go_repository(
        name = "com_github_ncw_swift",
        importpath = "github.com/ncw/swift",
        sum = "h1:4DQRPj35Y41WogBxyhOXlrI37nzGlyEcsforeudyYPQ=",
        version = "v1.0.47",
    )
    go_repository(
        name = "com_github_netflix_go_expect",
        importpath = "github.com/Netflix/go-expect",
        sum = "h1:+vx7roKuyA63nhn5WAunQHLTznkw5W8b1Xc0dNjp83s=",
        version = "v0.0.0-20220104043353-73e0943537d2",
    )
    go_repository(
        name = "com_github_networkplumbing_go_nft",
        importpath = "github.com/networkplumbing/go-nft",
        sum = "h1:kExVMwXW48DOAukkBwyI16h4uhE5lN9iMvQd52lpTyU=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_niemeyer_pretty",
        importpath = "github.com/niemeyer/pretty",
        sum = "h1:fD57ERR4JtEqsWbfPhv4DMiApHyliiK5xCTNVSPiaAs=",
        version = "v0.0.0-20200227124842-a10e7caefd8e",
    )
    go_repository(
        name = "com_github_nishanths_exhaustive",
        importpath = "github.com/nishanths/exhaustive",
        sum = "h1:+ANTMqRNrqwInnP9aszg/0jDo+zbXa4x66U19Bx/oTk=",  # keep
        version = "v0.2.3",  # keep
    )
    go_repository(
        name = "com_github_nishanths_predeclared",
        importpath = "github.com/nishanths/predeclared",
        sum = "h1:V2EPdZPliZymNAn79T8RkNApBjMmVKh5XRpLm/w98Vk=",
        version = "v0.2.2",
    )
    go_repository(
        name = "com_github_nxadm_tail",
        importpath = "github.com/nxadm/tail",
        sum = "h1:8feyoE3OzPrcshW5/MJ4sGESc5cqmGkGCWlco4l0bqY=",
        version = "v1.4.11",
    )
    go_repository(
        name = "com_github_nytimes_gziphandler",
        importpath = "github.com/NYTimes/gziphandler",
        sum = "h1:ZUDjpQae29j0ryrS0u/B8HZfJBtBQHjqw2rQ2cqUQ3I=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_oklog_ulid",
        importpath = "github.com/oklog/ulid",
        sum = "h1:EGfNDEx6MqHz8B3uNV6QAib1UR2Lm97sHi3ocA6ESJ4=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_github_olekukonko_tablewriter",
        importpath = "github.com/olekukonko/tablewriter",
        sum = "h1:P2Ga83D34wi1o9J6Wh1mRuqd4mF/x/lgBS7N7AbDhec=",
        version = "v0.0.5",
    )
    go_repository(
        name = "com_github_oneofone_xxhash",
        importpath = "github.com/OneOfOne/xxhash",
        sum = "h1:31czK/TI9sNkxIKfaUfGlU47BAxQ0ztGgd9vPyqimf8=",
        version = "v1.2.8",
    )
    go_repository(
        name = "com_github_onsi_ginkgo",
        importpath = "github.com/onsi/ginkgo",
        sum = "h1:8xi0RTUf59SOSfEtZMvwTvXYMzG4gV23XVHOZiXNtnE=",
        version = "v1.16.5",
    )
    go_repository(
        name = "com_github_onsi_ginkgo_v2",
        importpath = "github.com/onsi/ginkgo/v2",
        sum = "h1:9Cnnf7UHo57Hy3k6/m5k3dRfGTMXGvxhHFvkDTCTpvA=",
        version = "v2.19.0",
    )
    go_repository(
        name = "com_github_onsi_gomega",
        importpath = "github.com/onsi/gomega",
        sum = "h1:EUMJIKUjM8sKjYbtxQI9A4z2o+rruxnzNvpknOXie6k=",
        version = "v1.34.1",
    )
    go_repository(
        name = "com_github_open_feature_flagd_core",
        importpath = "github.com/open-feature/flagd/core",
        sum = "h1:3LAuLR2vXpBF80RwwCAu9JX898JasfPH7ErJEf5C5YA=",
        version = "v0.11.2",
    )
    go_repository(
        name = "com_github_open_feature_flagd_schemas",
        importpath = "github.com/open-feature/flagd-schemas",
        sum = "h1:0RKCLYeQpvSsKR95kc894tm8GAZmq7bcG48v0KJ0HCs=",
        version = "v0.2.9-0.20250127221449-bb763438abc5",
    )
    go_repository(
        name = "com_github_open_feature_go_sdk",
        importpath = "github.com/open-feature/go-sdk",
        sum = "h1:jcxjCIG5Up3XkgYwWN5Y/WWfc6XobOhqrIwjyDBsoQo=",
        version = "v1.14.1",
    )
    go_repository(
        name = "com_github_open_feature_go_sdk_contrib_providers_flagd",
        importpath = "github.com/open-feature/go-sdk-contrib/providers/flagd",
        sum = "h1:5LZkT79u5JEkh4KLYqO92vog2E1aTAhCjbMZQvBIXOo=",
        version = "v0.2.6",
    )
    go_repository(
        name = "com_github_open_feature_go_sdk_contrib_tests_flagd",
        importpath = "github.com/open-feature/go-sdk-contrib/tests/flagd",
        sum = "h1:kFAeX4qLSdcm//s9fvyBQ9CvfBZMtTkwEN46rjuxx+E=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_github_open_feature_open_feature_operator_apis",
        importpath = "github.com/open-feature/open-feature-operator/apis",
        sum = "h1:0r4Z+RnJltuHdRBv79NFgAckhna6/M3Wcec6gzNX5vI=",
        version = "v0.2.44",
    )
    go_repository(
        name = "com_github_open_policy_agent_opa",
        importpath = "github.com/open-policy-agent/opa",
        sum = "h1:qocVAKyjrqMjCqsU02S/gHyLr4AQQ9xMtuV1kKnnyhM=",
        version = "v0.42.2",
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
        sum = "h1:8SG7/vwALn54lVB/0yZ/MMwhFrPYtpEHQb2IpWsCzug=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_opencontainers_runc",
        importpath = "github.com/opencontainers/runc",
        sum = "h1:BOIssBaW1La0/qbNZHXOOa71dZfZEQOzW7dqQf3phss=",
        version = "v1.1.12",
    )
    go_repository(
        name = "com_github_opencontainers_runtime_spec",
        importpath = "github.com/opencontainers/runtime-spec",
        sum = "h1:z97+pHb3uELt/yiAWD691HNHQIF07bE7dzrbT927iTk=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_opencontainers_runtime_tools",
        importpath = "github.com/opencontainers/runtime-tools",
        sum = "h1:DmNGcqH3WDbV5k8OJ+esPWbqUOX5rMLR2PMvziDMJi0=",
        version = "v0.9.1-0.20221107090550-2e043c6bd626",
    )
    go_repository(
        name = "com_github_opencontainers_selinux",
        importpath = "github.com/opencontainers/selinux",
        sum = "h1:+5Zbo97w3Lbmb3PeqQtpmTkMwsW5nRI3YaLpt7tQ7oU=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_github_openpeedeep_depguard",
        importpath = "github.com/OpenPeeDeeP/depguard",
        sum = "h1:pjK9nLPS1FwQYGGpPxoMYpe7qACHOhAWQMQzV71i49o=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_opentracing_opentracing_go",
        importpath = "github.com/opentracing/opentracing-go",
        sum = "h1:uEJPy/1a5RIPAJ0Ov+OIO8OxWu77jEv+1B0VhjKrZUs=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_otiai10_copy",
        importpath = "github.com/otiai10/copy",
        sum = "h1:dCI/t1iTdYGtkvCuBG2BgR6KZa83PTclw4U5n2wAllU=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_github_otiai10_mint",
        importpath = "github.com/otiai10/mint",
        sum = "h1:XaPLeE+9vGbuyEHem1JNk3bYc7KKqyI/na0/mLd/Kks=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_pascaldekloe_goe",
        importpath = "github.com/pascaldekloe/goe",
        sum = "h1:cBOtyMzM9HTpWjXfbbunk26uA6nG3a8n06Wieeh0MwY=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_pascaldekloe_name",
        importpath = "github.com/pascaldekloe/name",
        sum = "h1:9lnXOHeqeHHnWLbKfH6X98+4+ETVqFqxN09UXSjcMb0=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_paulmach_orb",
        importpath = "github.com/paulmach/orb",
        sum = "h1:p/YWV2uJwamAynnDOJGNbPBVtDHj3vG51k9tR1rFwJE=",
        version = "v0.9.2",
    )
    go_repository(
        name = "com_github_paulmach_protoscan",
        importpath = "github.com/paulmach/protoscan",
        sum = "h1:rM0FpcTjUMvPUNk2BhPJrreDKetq43ChnL+x1sRg8O8=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_pelletier_go_toml",
        importpath = "github.com/pelletier/go-toml",
        sum = "h1:4yBQzkHv+7BHq2PQUZF3Mx0IYxG7LsP222s7Agd3ve8=",
        version = "v1.9.5",
    )
    go_repository(
        name = "com_github_pelletier_go_toml_v2",
        importpath = "github.com/pelletier/go-toml/v2",
        sum = "h1:ipoSadvV8oGUjnUbMub59IDPPwfxF694nG/jwbMiyQg=",
        version = "v2.0.5",
    )
    go_repository(
        name = "com_github_peterbourgon_diskv",
        importpath = "github.com/peterbourgon/diskv",
        sum = "h1:UBdAOUP5p4RWqPBg048CAvpKN+vxiaj6gdUUzhl4XmI=",
        version = "v2.0.1+incompatible",
    )
    go_repository(
        name = "com_github_phayes_checkstyle",
        importpath = "github.com/phayes/checkstyle",
        sum = "h1:CdDQnGF8Nq9ocOS/xlSptM1N3BbrA6/kmaep5ggwaIA=",
        version = "v0.0.0-20170904204023-bfd46e6a821d",
    )
    go_repository(
        name = "com_github_philhofer_fwd",
        importpath = "github.com/philhofer/fwd",
        sum = "h1:bnDivRJ1EWPjUIRXV5KfORO897HTbpFAQddBdE8t7Gw=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_pierrec_lz4",
        importpath = "github.com/pierrec/lz4",
        sum = "h1:2xWsjqPFWcplujydGg4WmhC/6fZqK42wMM8aXeqhl0I=",
        version = "v2.0.5+incompatible",
    )
    go_repository(
        name = "com_github_pierrec_lz4_v4",
        importpath = "github.com/pierrec/lz4/v4",
        sum = "h1:yOVMLb6qSIDP67pl/5F7RepeKYu/VmTyEXvuMI5d9mQ=",
        version = "v4.1.21",
    )
    go_repository(
        name = "com_github_pingcap_errors",
        importpath = "github.com/pingcap/errors",
        sum = "h1:lFuQV/oaUMGcD2tqt+01ROSmJs75VG1ToEOkZIZ4nE4=",
        version = "v0.11.4",
    )
    go_repository(
        name = "com_github_pjbgf_sha1cd",
        importpath = "github.com/pjbgf/sha1cd",
        sum = "h1:4D5XXmUUBUl/xQ6IjCkEAbqXskkq/4O7LmGn0AqMDs4=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_pkg_browser",
        importpath = "github.com/pkg/browser",
        sum = "h1:+mdjkGKdHQG3305AYmdv1U2eRNDiU2ErMBj1gwrq8eQ=",
        version = "v0.0.0-20240102092130-5ac0b6a4141c",
    )
    go_repository(
        name = "com_github_pkg_diff",
        importpath = "github.com/pkg/diff",
        sum = "h1:aoZm08cpOy4WuID//EZDgcC4zIxODThtZNPirFr42+A=",
        version = "v0.0.0-20210226163009-20ebb0f2a09e",
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
        sum = "h1:1sLMdKq4gNANTj0dUibycTLzpIEKVnLnbaEkxws78nw=",
        version = "v0.6.1-0.20241121165744-79df5c4772f2",
    )
    go_repository(
        name = "com_github_pmezard_go_difflib",
        importpath = "github.com/pmezard/go-difflib",
        sum = "h1:Jamvg5psRIccs7FGNTlIRMkT8wgtp5eCXdBlqhYGL6U=",
        version = "v1.0.1-0.20181226105442-5d4384ee4fb2",
    )
    go_repository(
        name = "com_github_polyfloyd_go_errorlint",
        importpath = "github.com/polyfloyd/go-errorlint",
        sum = "h1:pDrQG0lrh68e602Wfp68BlUTRFoHn8PZYAjLgt2LFsM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_posener_complete",
        importpath = "github.com/posener/complete",
        sum = "h1:NP0eAhjcjImqslEwo/1hq7gpajME0fTLTezBKDqfXqo=",
        version = "v1.2.3",
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
        sum = "h1:0XM1XL/OFFJjXsYXlG30spTkV/E9+gmd5GD1w2HE8xM=",
        version = "v0.0.0-20171018203845-0dec1b30a021",
    )
    go_repository(
        name = "com_github_pquerna_ffjson",
        importpath = "github.com/pquerna/ffjson",
        sum = "h1:xoIK0ctDddBMnc74udxJYBqlo9Ylnsp1waqjLsnef20=",
        version = "v0.0.0-20190930134022-aa0246cd15f7",
    )
    go_repository(
        name = "com_github_prometheus_client_golang",
        build_directives = [
            "gazelle:resolve go github.com/prometheus/client_model/go @{}//proto:prometheus_client_go_proto".format(workspace_name),
        ],
        importpath = "github.com/prometheus/client_golang",
        sum = "h1:DIsaGmiaBkSangBgMtWdNfxbMNdku5IK6iNhrEqWvdA=",
        version = "v1.21.0",
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
        sum = "h1:xasJaQlnWAeyHdUBeGjXmutelfJHWMRr+Fg4QszZ2Io=",
        version = "v0.62.0",
    )
    go_repository(
        name = "com_github_prometheus_procfs",
        importpath = "github.com/prometheus/procfs",
        sum = "h1:YagwOFzUgYfKKHX6Dr+sHT7km/hxC76UB0learggepc=",
        version = "v0.15.1",
    )
    go_repository(
        name = "com_github_prometheus_tsdb",
        importpath = "github.com/prometheus/tsdb",
        sum = "h1:YZcsG11NqnK4czYLrWd9mpEuAJIHVQLwdrleYfszMAA=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_github_protonmail_go_crypto",
        importpath = "github.com/ProtonMail/go-crypto",
        sum = "h1:nRBOetoydLeUb4nHajyO2bKqMLfWQ/ZPwkXqXxPxCFk=",
        version = "v1.1.3",
    )
    go_repository(
        name = "com_github_puerkitobio_purell",
        importpath = "github.com/PuerkitoBio/purell",
        sum = "h1:WEQqlqaGbrPkxLJWfBwQmfEAE1Z7ONdDLqrN38tNFfI=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_puerkitobio_urlesc",
        importpath = "github.com/PuerkitoBio/urlesc",
        sum = "h1:d+Bc7a5rLufV/sSk/8dngufqelfh6jnri85riMAaF/M=",
        version = "v0.0.0-20170810143723-de5bf2ad4578",
    )
    go_repository(
        name = "com_github_quasilyte_go_ruleguard",
        importpath = "github.com/quasilyte/go-ruleguard",
        sum = "h1:sWFavxtIctGrVs5SYZ5Ml1CvrDAs8Kf5kx2PI3C41dA=",
        version = "v0.3.16-0.20220213074421-6aa060fab41a",
    )
    go_repository(
        name = "com_github_quasilyte_gogrep",
        importpath = "github.com/quasilyte/gogrep",
        sum = "h1:PDWGei+Rf2bBiuZIbZmM20J2ftEy9IeUCHA8HbQqed8=",
        version = "v0.0.0-20220120141003-628d8b3623b5",
    )
    go_repository(
        name = "com_github_quasilyte_regex_syntax",
        importpath = "github.com/quasilyte/regex/syntax",
        sum = "h1:L8QM9bvf68pVdQ3bCFZMDmnt9yqcMBro1pC7F+IPYMY=",
        version = "v0.0.0-20200407221936-30656e2c4a95",
    )
    go_repository(
        name = "com_github_quasilyte_stdinfo",
        importpath = "github.com/quasilyte/stdinfo",
        sum = "h1:M8mH9eK4OUR4lu7Gd+PU1fV2/qnDNfzT635KRSObncs=",
        version = "v0.0.0-20220114132959-f7386bf02567",
    )
    go_repository(
        name = "com_github_rakyll_statik",
        importpath = "github.com/rakyll/statik",
        sum = "h1:OF3QCZUuyPxuGEP7B4ypUa7sB/iHtqOTDYZXGM8KOdQ=",
        version = "v0.1.7",
    )
    go_repository(
        name = "com_github_rantav_go_grpc_channelz",
        importpath = "github.com/rantav/go-grpc-channelz",
        sum = "h1:svoYt8ZD0uO6B/EZVWGNIDRJY/JXfak2y5Ks+1xwaVo=",
        version = "v0.0.3",
    )
    go_repository(
        name = "com_github_rcrowley_go_metrics",
        importpath = "github.com/rcrowley/go-metrics",
        sum = "h1:MkV+77GLUNo5oJ0jf870itWm3D0Sjh7+Za9gazKc5LQ=",
        version = "v0.0.0-20200313005456-10cdbea86bc0",
    )
    go_repository(
        name = "com_github_rivo_uniseg",
        importpath = "github.com/rivo/uniseg",
        sum = "h1:S1pD9weZBuJdFmowNwbpi7BJ8TNftyUImj/0WQi72jY=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_roaringbitmap_roaring",
        importpath = "github.com/RoaringBitmap/roaring",
        sum = "h1:LXcSqGGGMKm+KAzUyWn7ZeREqoOkoMX+KwLOK1thc4I=",
        version = "v1.9.1",
    )
    go_repository(
        name = "com_github_robfig_cron",
        importpath = "github.com/robfig/cron",
        sum = "h1:ZjScXvvxeQ63Dbyxy76Fj3AT3Ut0aKsyd2/tl3DTMuQ=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_rogpeppe_fastuuid",
        importpath = "github.com/rogpeppe/fastuuid",
        sum = "h1:Ppwyp6VYCF1nvBTXL3trRso7mXMlRrw9ooo375wvi2s=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_rogpeppe_go_internal",
        importpath = "github.com/rogpeppe/go-internal",
        sum = "h1:KvO1DLK/DRN07sQ1LQKScxyZJuNnedQ5/wKSR38lUII=",
        version = "v1.13.1",
    )
    go_repository(
        name = "com_github_rs_xid",
        importpath = "github.com/rs/xid",
        sum = "h1:fV591PaemRlL6JfRxGDEPl69wICngIQ3shQtzfy2gxU=",
        version = "v1.6.0",
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
        name = "com_github_russross_blackfriday",
        importpath = "github.com/russross/blackfriday",
        sum = "h1:KqfZb0pUVN2lYqZUYRddxF4OR8ZMURnJIG5Y3VRLtww=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_russross_blackfriday_v2",
        importpath = "github.com/russross/blackfriday/v2",
        sum = "h1:JIOH55/0cWyOuilr9/qlrm0BSXldqnqwMsf35Ld67mk=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_ryancurrah_gomodguard",
        importpath = "github.com/ryancurrah/gomodguard",
        sum = "h1:ww2fsjqocGCAFamzvv/b8IsRduuHHeK2MHTcTxZTQX8=",
        version = "v1.2.3",
    )
    go_repository(
        name = "com_github_ryanrolds_sqlclosecheck",
        importpath = "github.com/ryanrolds/sqlclosecheck",
        sum = "h1:AZx+Bixh8zdUBxUA1NxbxVAS78vTPq4rCb8OUZI9xFw=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_ryanuber_columnize",
        importpath = "github.com/ryanuber/columnize",
        sum = "h1:C89EOx/XBWwIXl8wm8OPJBd7kPF25UfsK2X7Ph/zCAk=",
        version = "v2.1.2+incompatible",
    )
    go_repository(
        name = "com_github_safchain_ethtool",
        importpath = "github.com/safchain/ethtool",
        sum = "h1:vq1i2HCjshJNywOXFZ1BpwIjyeFR/kvNdHiRzqSElDI=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_sanity_io_litter",
        importpath = "github.com/sanity-io/litter",
        sum = "h1:iE+sBxPBzoK6uaEP5Lt3fHNgpKcHXc/A2HGETy0uJQo=",
        version = "v1.5.5",
    )
    go_repository(
        name = "com_github_sanposhiho_wastedassign_v2",
        importpath = "github.com/sanposhiho/wastedassign/v2",
        sum = "h1:+6/hQIHKNJAUixEj6EmOngGIisyeI+T3335lYTyxRoA=",
        version = "v2.0.6",
    )
    go_repository(
        name = "com_github_satori_go_uuid",
        importpath = "github.com/satori/go.uuid",
        sum = "h1:0uYX9dsZ2yD7q2RtLRtPSdGDWzjeM3TbMJP9utgA0ww=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_schollz_closestmatch",
        importpath = "github.com/schollz/closestmatch",
        sum = "h1:Uel2GXEpJqOWBrlyI+oY9LTiyyjYS17cCYRqP13/SHk=",
        version = "v2.1.0+incompatible",
    )
    go_repository(
        name = "com_github_sclevine_agouti",
        importpath = "github.com/sclevine/agouti",
        sum = "h1:8IBJS6PWz3uTlMP3YBIR5f+KAldcGuOeFkFbUWfBgK4=",
        version = "v3.0.0+incompatible",
    )
    go_repository(
        name = "com_github_sclevine_spec",
        importpath = "github.com/sclevine/spec",
        sum = "h1:1Jwdf9jSfDl9NVmt8ndHqbTZ7XCCPbh1jI3hkDBHVYA=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_sean_seed",
        importpath = "github.com/sean-/seed",
        sum = "h1:nn5Wsu0esKSJiIVhscUtVbo7ada43DJhG55ua/hjS5I=",
        version = "v0.0.0-20170313163322-e2103e2c3529",
    )
    go_repository(
        name = "com_github_seccomp_libseccomp_golang",
        importpath = "github.com/seccomp/libseccomp-golang",
        sum = "h1:RpforrEYXWkmGwJHIGnLZ3tTWStkjVVstwzNGqxX2Ds=",
        version = "v0.9.2-0.20220502022130-f33da4d89646",
    )
    go_repository(
        name = "com_github_securego_gosec_v2",
        importpath = "github.com/securego/gosec/v2",
        sum = "h1:+PDkpzR41OI2jrw1q6AdXZCbsNGNGT7pQjal0H0cArI=",
        version = "v2.11.0",
    )
    go_repository(
        name = "com_github_segmentio_asm",
        importpath = "github.com/segmentio/asm",
        sum = "h1:9BQrFxC+YOHJlTlHGkTrFWf59nbL3XnCoFLTwDCI7ys=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_sereal_sereal_go_sereal",
        importpath = "github.com/Sereal/Sereal/Go/sereal",
        sum = "h1:5kUcJJAKWWI82Xnp/CaU0eu5hLlHkmm9acjowSkwCd0=",
        version = "v0.0.0-20231009093132-b9187f1a92c6",
    )
    go_repository(
        name = "com_github_sergi_go_diff",
        importpath = "github.com/sergi/go-diff",
        sum = "h1:n661drycOFuPLCN3Uc8sB6B/s6Z4t2xvBgU1htSHuq8=",
        version = "v1.3.2-0.20230802210424-5b0b94c5c0d3",
    )
    go_repository(
        name = "com_github_shazow_go_diff",
        importpath = "github.com/shazow/go-diff",
        sum = "h1:W65qqJCIOVP4jpqPQ0YvHYKwcMEMVWIzWC5iNQQfBTU=",
        version = "v0.0.0-20160112020656-b6b7b6733b8c",
    )
    go_repository(
        name = "com_github_shirou_gopsutil",
        importpath = "github.com/shirou/gopsutil",
        sum = "h1:+1+c1VGhc88SSonWP6foOcLhvnKlUeu/erjjvaPEYiI=",
        version = "v3.21.11+incompatible",
    )
    go_repository(
        name = "com_github_shirou_gopsutil_v3",
        importpath = "github.com/shirou/gopsutil/v3",
        sum = "h1:5SgDCeQ0KW0S4N0znjeM/eFHXXOKyv2dVNgRq/c9P6Y=",
        version = "v3.23.5",
    )
    go_repository(
        name = "com_github_shirou_w32",
        importpath = "github.com/shirou/w32",
        sum = "h1:udFKJ0aHUL60LboW/A+DfgoHVedieIzIXE8uylPue0U=",
        version = "v0.0.0-20160930032740-bb4de0191aa4",
    )
    go_repository(
        name = "com_github_shoenig_go_m1cpu",
        importpath = "github.com/shoenig/go-m1cpu",
        sum = "h1:nxdKQNcEB6vzgA2E2bvzKIYRuNj7XNJ4S/aRSwKzFtM=",
        version = "v0.1.6",
    )
    go_repository(
        name = "com_github_shoenig_test",
        importpath = "github.com/shoenig/test",
        sum = "h1:kVTaSd7WLz5WZ2IaoM0RSzRsUD+m8wRR+5qvntpn4LU=",
        version = "v0.6.4",
    )
    go_repository(
        name = "com_github_shopify_goreferrer",
        importpath = "github.com/Shopify/goreferrer",
        sum = "h1:KkH3I3sJuOLP3TjA/dfr4NAY8bghDwnXiU7cTKxQqo0=",
        version = "v0.0.0-20220729165902-8cddb4f5de06",
    )
    go_repository(
        name = "com_github_shopify_logrus_bugsnag",
        importpath = "github.com/Shopify/logrus-bugsnag",
        sum = "h1:UrqY+r/OJnIp5u0s1SbQ8dVfLCZJsnvazdBP5hS4iRs=",
        version = "v0.0.0-20171204204709-577dee27f20d",
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
        name = "com_github_shurcool_sanitized_anchor_name",
        importpath = "github.com/shurcooL/sanitized_anchor_name",
        sum = "h1:PdmoCO6wvbs+7yrJyMORt4/BmY5IYyJwS/kOiWx8mHo=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_sirupsen_logrus",
        importpath = "github.com/sirupsen/logrus",
        sum = "h1:dueUQJ1C2q9oE3F7wvmSGAaVtTmUizReu6fjN8uqzbQ=",
        version = "v1.9.3",
    )
    go_repository(
        name = "com_github_sivchari_tenv",
        importpath = "github.com/sivchari/tenv",
        sum = "h1:wxW0mFpKI6DIb3s6m1jCDYvkWXCskrimXMuGd0K/kSQ=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_skeema_knownhosts",
        importpath = "github.com/skeema/knownhosts",
        sum = "h1:AM+y0rI04VksttfwjkSTNQorvGqmwATnvnAHpSgc0LY=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_smacker_go_tree_sitter",
        importpath = "github.com/smacker/go-tree-sitter",
        sum = "h1:exZ0FwfhblsYbgfqYH+W/3sFye821WD02NjBmc+ENhE=",
        version = "v0.0.0-20230501083651-a7d92773b3aa",
    )
    go_repository(
        name = "com_github_smartystreets_assertions",
        importpath = "github.com/smartystreets/assertions",
        sum = "h1:zE9ykElWQ6/NYmHa3jpm/yHnI4xSofP+UP6SpjHcSeM=",
        version = "v0.0.0-20180927180507-b2de0cb4f26d",
    )
    go_repository(
        name = "com_github_smartystreets_goconvey",
        importpath = "github.com/smartystreets/goconvey",
        sum = "h1:fv0U8FUIMPNf1L9lnHLvLhgicrIVChEkdzIKYqbNC9s=",
        version = "v1.6.4",
    )
    go_repository(
        name = "com_github_soheilhy_cmux",
        importpath = "github.com/soheilhy/cmux",
        sum = "h1:jjzc5WVemNEDTLwv9tlmemhC73tI08BNOIGwBOo10Js=",
        version = "v0.1.5",
    )
    go_repository(
        name = "com_github_sonatard_noctx",
        importpath = "github.com/sonatard/noctx",
        sum = "h1:VC1Qhl6Oxx9vvWo3UDgrGXYCeKCe3Wbw7qAWL6FrmTY=",
        version = "v0.0.1",
    )
    go_repository(
        name = "com_github_sourcegraph_go_diff",
        importpath = "github.com/sourcegraph/go-diff",
        sum = "h1:hmA1LzxW0n1c3Q4YbrFgg4P99GSnebYa3x8gr0HZqLQ=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_sourcegraph_go_langserver",
        importpath = "github.com/sourcegraph/go-langserver",
        sum = "h1:lj2sRU7ZMIkW372IDVGb6fE8VAY4c/EMsiDzrB9vmiU=",
        version = "v2.0.0+incompatible",
    )
    go_repository(
        name = "com_github_sourcegraph_jsonrpc2",
        importpath = "github.com/sourcegraph/jsonrpc2",
        sum = "h1:KjN/dC4fP6aN9030MZCJs9WQbTOjWHhrtKVpzzSrr/U=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_spaolacci_murmur3",
        importpath = "github.com/spaolacci/murmur3",
        sum = "h1:qLC7fQah7D6K1B0ujays3HV9gkFtllcxhzImRR7ArPQ=",
        version = "v0.0.0-20180118202830-f09979ecbc72",
    )
    go_repository(
        name = "com_github_spf13_afero",
        importpath = "github.com/spf13/afero",
        sum = "h1:EaGW2JJh15aKOejeuJ+wpFSHnbd7GE6Wvp3TsNhb6LY=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_spf13_cast",
        importpath = "github.com/spf13/cast",
        sum = "h1:R+kOtfhWQE6TVQzY+4D7wJLBgkdVasCEFxSUBYBYIlA=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_spf13_cobra",
        importpath = "github.com/spf13/cobra",
        sum = "h1:e5/vxKd/rZsfSJMUX1agtjeTDf+qv1/JdBF8gg5k9ZM=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_spf13_jwalterweatherman",
        importpath = "github.com/spf13/jwalterweatherman",
        sum = "h1:ue6voC5bR5F8YxI5S67j9i582FU4Qvo2bmqnqMYADFk=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_spf13_pflag",
        importpath = "github.com/spf13/pflag",
        sum = "h1:iy+VFUOCP1a+8yFto/drg2CJ5u0yRoB7fZw3DKv/JXA=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_github_spf13_viper",
        importpath = "github.com/spf13/viper",
        sum = "h1:7OX/1FS6n7jHD1zGrZTM7WtY13ZELRyosK4k93oPr44=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_github_ssgreg_nlreturn_v2",
        importpath = "github.com/ssgreg/nlreturn/v2",
        sum = "h1:X4XDI7jstt3ySqGU86YGAURbxw3oTDPK9sPEi6YEwQ0=",
        version = "v2.2.1",
    )
    go_repository(
        name = "com_github_stackexchange_wmi",
        importpath = "github.com/StackExchange/wmi",
        sum = "h1:G0m3OIz70MZUWq3EgK3CesDbo8upS2Vm9/P3FtgI+Jk=",
        version = "v0.0.0-20190523213315-cbe66965904d",
    )
    go_repository(
        name = "com_github_stefanberger_go_pkcs11uri",
        importpath = "github.com/stefanberger/go-pkcs11uri",
        sum = "h1:pnnLyeX7o/5aX8qUQ69P/mLojDqwda8hFOCBTmP/6hw=",
        version = "v0.0.0-20230803200340-78284954bff6",
    )
    go_repository(
        name = "com_github_stoewer_go_strcase",
        importpath = "github.com/stoewer/go-strcase",
        sum = "h1:g0eASXYtp+yvN9fK8sH94oCIk0fau9uV1/ZdJ0AVEzs=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_stretchr_objx",
        importpath = "github.com/stretchr/objx",
        sum = "h1:xuMeJ0Sdp5ZMRXx/aWO6RZxdr3beISkG5/G/aIRr3pY=",
        version = "v0.5.2",
    )
    go_repository(
        name = "com_github_stretchr_testify",
        importpath = "github.com/stretchr/testify",
        sum = "h1:Xv5erBjTwe/5IxqUQTdXv5kgmIvbHo3QQyRwhJsOfJA=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_subosito_gotenv",
        importpath = "github.com/subosito/gotenv",
        sum = "h1:Slr1R9HxAlEKefgq5jn9U+DnETlIUa6HfgEzj0g5d7s=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_sylvia7788_contextcheck",
        importpath = "github.com/sylvia7788/contextcheck",
        sum = "h1:MsiVqROAdr0efZc/fOCt0c235qm9XJqHtWwM+2h2B04=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_syndtr_gocapability",
        importpath = "github.com/syndtr/gocapability",
        sum = "h1:kdXcSzyDtseVEc4yCz2qF8ZrQvIDBJLl4S1c3GCXmoI=",
        version = "v0.0.0-20200815063812-42c35b437635",
    )
    go_repository(
        name = "com_github_syndtr_goleveldb",
        importpath = "github.com/syndtr/goleveldb",
        sum = "h1:fBdIW9lB4Iz0n9khmH8w27SJ3QEJ7+IgjPEwGSZiFdE=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_tchap_go_patricia",
        importpath = "github.com/tchap/go-patricia",
        sum = "h1:JvoDL7JSoIP2HDE8AbDH3zC8QBPxmzYe32HHy5yQ+Ck=",
        version = "v2.2.6+incompatible",
    )
    go_repository(
        name = "com_github_tchap_go_patricia_v2",
        importpath = "github.com/tchap/go-patricia/v2",
        sum = "h1:6rQp39lgIYZ+MHmdEq4xzuk1t7OdC35z/xm0BGhTkes=",
        version = "v2.3.1",
    )
    go_repository(
        name = "com_github_tdakkota_asciicheck",
        importpath = "github.com/tdakkota/asciicheck",
        sum = "h1:PKzG7JUTUmVspQTDqtkX9eSiLGossXTybutHwTXuO0A=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_tdewolff_minify_v2",
        importpath = "github.com/tdewolff/minify/v2",
        sum = "h1:kejsHQMM17n6/gwdw53qsi6lg0TGddZADVyQOz1KMdE=",
        version = "v2.12.4",
    )
    go_repository(
        name = "com_github_tdewolff_parse_v2",
        importpath = "github.com/tdewolff/parse/v2",
        sum = "h1:KCkDvNUMof10e3QExio9OPZJT8SbdKojLBumw8YZycQ=",
        version = "v2.6.4",
    )
    go_repository(
        name = "com_github_tebeka_selenium",
        importpath = "github.com/tebeka/selenium",
        sum = "h1:cNziB+etNgyH/7KlNI7RMC1ua5aH1+5wUlFQyzeMh+w=",
        version = "v0.9.9",
    )
    go_repository(
        name = "com_github_testcontainers_testcontainers_go",
        importpath = "github.com/testcontainers/testcontainers-go",
        sum = "h1:h0D5GaYG9mhOWr2qHdEKDXpkce/VlvaYOCzTRi6UBi8=",
        version = "v0.14.0",
    )
    go_repository(
        name = "com_github_tetafro_godot",
        importpath = "github.com/tetafro/godot",
        sum = "h1:BVoBIqAf/2QdbFmSwAWnaIqDivZdOV0ZRwEm6jivLKw=",
        version = "v1.4.11",
    )
    go_repository(
        name = "com_github_throttled_throttled_v2",
        importpath = "github.com/throttled/throttled/v2",
        replace = "github.com/buildbuddy-io/throttled/v2",
        sum = "h1:l9PGL9DJwcCgQcVt/zFzVjJbSzb+BmpU8NBeo6leHKU=",
        version = "v2.9.1-rc2",
    )
    go_repository(
        name = "com_github_tidwall_pretty",
        importpath = "github.com/tidwall/pretty",
        sum = "h1:HsD+QiTn7sK6flMKIvNmpqz1qrpP3Ps6jOKIKMooyg4=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_timakin_bodyclose",
        importpath = "github.com/timakin/bodyclose",
        sum = "h1:kl4KhGNsJIbDHS9/4U9yQo1UcPQM0kOMJHn29EoH/Ro=",
        version = "v0.0.0-20210704033933-f49887972144",
    )
    go_repository(
        name = "com_github_tink_crypto_tink_go_awskms_v2",
        importpath = "github.com/tink-crypto/tink-go-awskms/v2",
        sum = "h1:N9UxlsOzu5mttdjhxkDLbzwtEecuXmlxZVo/ds7JKJI=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_tink_crypto_tink_go_gcpkms_v2",
        importpath = "github.com/tink-crypto/tink-go-gcpkms/v2",
        sum = "h1:3B9i6XBXNTRspfkTC0asN5W0K6GhOSgcujNiECNRNb0=",
        version = "v2.2.0",
    )
    go_repository(
        name = "com_github_tink_crypto_tink_go_v2",
        importpath = "github.com/tink-crypto/tink-go/v2",
        sum = "h1:4/TA0lw0lA/iVKBL9f8R5eP7397bfc4antAMXF5JRhs=",
        version = "v2.3.0",
    )
    go_repository(
        name = "com_github_tinylib_msgp",
        importpath = "github.com/tinylib/msgp",
        sum = "h1:FCXC1xanKO4I8plpHGH2P7koL/RzZs12l/+r7vakfm0=",
        version = "v1.1.8",
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
        name = "com_github_tmc_grpc_websocket_proxy",
        importpath = "github.com/tmc/grpc-websocket-proxy",
        sum = "h1:uruHq4dN7GR16kFc5fp3d1RIYzJW5onx8Ybykw2YQFA=",
        version = "v0.0.0-20201229170055-e5319fda7802",
    )
    go_repository(
        name = "com_github_tomarrell_wrapcheck_v2",
        importpath = "github.com/tomarrell/wrapcheck/v2",
        sum = "h1:Cf4a/iwuMp9s7kKrh74GTgijRVim0wEpKjgAsT7Wctw=",
        version = "v2.6.1",
    )
    go_repository(
        name = "com_github_tommy_muehle_go_mnd_v2",
        importpath = "github.com/tommy-muehle/go-mnd/v2",
        sum = "h1:iAj0a8e6+dXSL7Liq0aXPox36FiN1dBbjA6lt9fl65s=",
        version = "v2.5.0",
    )
    go_repository(
        name = "com_github_tv42_httpunix",
        importpath = "github.com/tv42/httpunix",
        sum = "h1:u6SKchux2yDvFQnDHS3lPnIRmfVJ5Sxy3ao2SIdysLQ=",
        version = "v0.0.0-20191220191345-2ba4b9c3382c",
    )
    go_repository(
        name = "com_github_twmb_murmur3",
        importpath = "github.com/twmb/murmur3",
        sum = "h1:8Yt9taO/WN3l08xErzjeschgZU2QSrwm1kclYq+0aRg=",
        version = "v1.1.8",
    )
    go_repository(
        name = "com_github_ugorji_go",
        importpath = "github.com/ugorji/go",
        sum = "h1:j4s+tAvLfL3bZyefP2SEWmhBzmuIlH/eqNuPdFPgngw=",
        version = "v1.1.4",
    )
    go_repository(
        name = "com_github_ugorji_go_codec",
        importpath = "github.com/ugorji/go/codec",
        sum = "h1:YPXUKf7fYbp/y8xloBqZOw2qaVggbfwMlI8WM3wZUJ0=",
        version = "v1.2.7",
    )
    go_repository(
        name = "com_github_ultraware_funlen",
        importpath = "github.com/ultraware/funlen",
        sum = "h1:5ylVWm8wsNwH5aWo9438pwvsK0QiqVuUrt9bn7S/iLA=",
        version = "v0.0.3",
    )
    go_repository(
        name = "com_github_ultraware_whitespace",
        importpath = "github.com/ultraware/whitespace",
        sum = "h1:hh+/cpIcopyMYbZNVov9iSxvJU3OYQg78Sfaqzi/CzI=",
        version = "v0.0.5",
    )
    go_repository(
        name = "com_github_urfave_cli",
        importpath = "github.com/urfave/cli",
        sum = "h1:ebbhrRiGK2i4naQJr+1Xj92HXZCrK7MsyTS/ob3HnAk=",
        version = "v1.22.14",
    )
    go_repository(
        name = "com_github_urfave_cli_v2",
        importpath = "github.com/urfave/cli/v2",
        sum = "h1:JTTnM6wKzdA0Jqodd966MVj4vWbbquZykeX1sKbe2C4=",
        version = "v2.2.0",
    )
    go_repository(
        name = "com_github_urfave_negroni",
        importpath = "github.com/urfave/negroni",
        sum = "h1:kIimOitoypq34K7TG7DUaJ9kq/N4Ofuwi1sjz0KipXc=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_uudashr_gocognit",
        importpath = "github.com/uudashr/gocognit",
        sum = "h1:rrSex7oHr3/pPLQ0xoWq108XMU8s678FJcQ+aSfOHa4=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_github_valyala_bytebufferpool",
        importpath = "github.com/valyala/bytebufferpool",
        sum = "h1:GqA5TC/0021Y/b9FG4Oi9Mr3q7XYx6KllzawFIhcdPw=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_valyala_fasthttp",
        importpath = "github.com/valyala/fasthttp",
        sum = "h1:wqBQpxH71XW0e2g+Og4dzQM8pk34aFYlA1Ga8db7gU0=",
        version = "v1.52.0",
    )
    go_repository(
        name = "com_github_valyala_fastrand",
        importpath = "github.com/valyala/fastrand",
        sum = "h1:f+5HkLW4rsgzdNoleUOB69hyT9IlD2ZQh9GyDMfb5G8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_valyala_fasttemplate",
        importpath = "github.com/valyala/fasttemplate",
        sum = "h1:lxLXG0uE3Qnshl9QyaK6XJxMXlQZELvChBOCmQD0Loo=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_valyala_histogram",
        importpath = "github.com/valyala/histogram",
        sum = "h1:wyYGAZZt3CpwUiIb9AU/Zbllg1llXyrtApRS815OLoQ=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_valyala_tcplisten",
        importpath = "github.com/valyala/tcplisten",
        sum = "h1:rBHj/Xf+E1tRGZyWIWwJDiRY0zc1Js+CV5DqwacVSA8=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_vbatts_tar_split",
        importpath = "github.com/vbatts/tar-split",
        sum = "h1:hLFqsOLQ1SsppQNTMpkpPXClLDfC2A3Zgy9OUU+RVck=",
        version = "v0.11.3",
    )
    go_repository(
        name = "com_github_vektah_gqlparser_v2",
        importpath = "github.com/vektah/gqlparser/v2",
        sum = "h1:C02NsyEsL4TXJB7ndonqTfuQOL4XPIu0aAWugdmTgmc=",
        version = "v2.4.5",
    )
    go_repository(
        name = "com_github_veraison_go_cose",
        importpath = "github.com/veraison/go-cose",
        sum = "h1:Ok0Hr3GMAf8K/1NB4sV65QGgCiukG1w1QD+H5tmt0Ow=",
        version = "v1.2.0",
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
        name = "com_github_vmihailenco_msgpack_v5",
        importpath = "github.com/vmihailenco/msgpack/v5",
        sum = "h1:5gO0H1iULLWGhs2H5tbAHIZTV8/cYafcFOr9znI5mJU=",
        version = "v5.3.5",
    )
    go_repository(
        name = "com_github_vmihailenco_tagparser_v2",
        importpath = "github.com/vmihailenco/tagparser/v2",
        sum = "h1:y09buUbR+b5aycVFQs/g70pqKVZNBmxwAhO7/IwNM9g=",
        version = "v2.0.0",
    )
    go_repository(
        name = "com_github_willf_bitset",
        importpath = "github.com/willf/bitset",
        sum = "h1:N7Z7E9UvjW+sGsEl7k/SJrvY2reP1A07MrGuCjIOjRE=",
        version = "v1.1.11",
    )
    go_repository(
        name = "com_github_x448_float16",
        importpath = "github.com/x448/float16",
        sum = "h1:qLwI1I70+NjRFUR3zs1JPUCgaCXSh3SW62uAKT1mSBM=",
        version = "v0.8.4",
    )
    go_repository(
        name = "com_github_xanzy_ssh_agent",
        importpath = "github.com/xanzy/ssh-agent",
        sum = "h1:+/15pJfg/RsTxqYcX6fHqOXZwwMP+2VyYWJeWM2qQFM=",
        version = "v0.3.3",
    )
    go_repository(
        name = "com_github_xdg_go_pbkdf2",
        importpath = "github.com/xdg-go/pbkdf2",
        sum = "h1:Su7DPu48wXMwC3bs7MCNG+z4FhcyEuz5dlvchbq0B0c=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_xdg_go_scram",
        importpath = "github.com/xdg-go/scram",
        sum = "h1:FHX5I5B4i4hKRVRBCFRxq1iQRej7WO3hhBuJf+UUySY=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_xdg_go_stringprep",
        importpath = "github.com/xdg-go/stringprep",
        sum = "h1:XLI/Ng3O1Atzq0oBs3TWm+5ZVgkq2aqdlvP9JtoZ6c8=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonpointer",
        importpath = "github.com/xeipuuv/gojsonpointer",
        sum = "h1:zGWFAtiMcyryUHoUjUJX0/lt1H2+i2Ka2n+D3DImSNo=",
        version = "v0.0.0-20190905194746-02993c407bfb",
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonreference",
        importpath = "github.com/xeipuuv/gojsonreference",
        sum = "h1:EzJWgHovont7NscjpAxXsDA8S8BMYve8Y5+7cuRE7R0=",
        version = "v0.0.0-20180127040603-bd5ef7bd5415",
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonschema",
        importpath = "github.com/xeipuuv/gojsonschema",
        sum = "h1:LhYJRs+L4fBtjZUfuSZIKGeVu0QRy8e5Xi7D17UxZ74=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_xhit_go_str2duration_v2",
        importpath = "github.com/xhit/go-str2duration/v2",
        sum = "h1:lxklc02Drh6ynqX+DdPyp5pCKLUQpRT8bp8Ydu2Bstc=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_xiam_s_expr",
        importpath = "github.com/xiam/s-expr",
        sum = "h1:GOOG0ytl7fQsJ2yyA0qJu4uSUyv2pbILu+7edPboSNk=",
        version = "v0.0.0-20240311144413-9d7e25c9d7d1",
    )
    go_repository(
        name = "com_github_xiang90_probing",
        importpath = "github.com/xiang90/probing",
        sum = "h1:eY9dn8+vbi4tKz5Qo6v2eYzo7kUS51QINcR5jNpbZS8=",
        version = "v0.0.0-20190116061207-43a291ad63a2",
    )
    go_repository(
        name = "com_github_xordataexchange_crypt",
        importpath = "github.com/xordataexchange/crypt",
        sum = "h1:ESFSdwYZvkeru3RtdrYueztKhOBCSAAzS4Gf+k0tEow=",
        version = "v0.0.3-0.20170626215501-b2862e3d0a77",
    )
    go_repository(
        name = "com_github_yalp_jsonpath",
        importpath = "github.com/yalp/jsonpath",
        sum = "h1:6fRhSjgLCkTD3JnJxvaJ4Sj+TYblw757bqYgZaOq5ZY=",
        version = "v0.0.0-20180802001716-5cc68e5049a0",
    )
    go_repository(
        name = "com_github_yashtewari_glob_intersection",
        importpath = "github.com/yashtewari/glob-intersection",
        sum = "h1:6gJvMYQlTDOL3dMsPF6J0+26vwX9MB8/1q3uAdhmTrg=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_yeya24_promlinter",
        importpath = "github.com/yeya24/promlinter",
        sum = "h1:xFKDQ82orCU5jQujdaD8stOHiv8UN68BSdn2a8u8Y3o=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_yosssi_ace",
        importpath = "github.com/yosssi/ace",
        sum = "h1:tUkIP/BLdKqrlrPwcmH0shwEEhTRHoGnc1wFIWmaBUA=",
        version = "v0.0.5",
    )
    go_repository(
        name = "com_github_youmark_pkcs8",
        importpath = "github.com/youmark/pkcs8",
        sum = "h1:splanxYIlg+5LfHAM6xpdFEAYOk8iySO56hMFq6uLyA=",
        version = "v0.0.0-20181117223130-1be2e3e5546d",
    )
    go_repository(
        name = "com_github_yudai_gojsondiff",
        importpath = "github.com/yudai/gojsondiff",
        sum = "h1:27cbfqXLVEJ1o8I6v3y9lg8Ydm53EKqHXAOMxEGlCOA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_yudai_golcs",
        importpath = "github.com/yudai/golcs",
        sum = "h1:BHyfKlQyqbsFN5p3IfnEUduWvb9is428/nNb5L3U01M=",
        version = "v0.0.0-20170316035057-ecda9a501e82",
    )
    go_repository(
        name = "com_github_yuin_goldmark",
        importpath = "github.com/yuin/goldmark",
        sum = "h1:fVcFKWvrslecOb/tg+Cc05dkeYx540o0FuFt3nUVDoE=",
        version = "v1.4.13",
    )
    go_repository(
        name = "com_github_yusufpapurcu_wmi",
        importpath = "github.com/yusufpapurcu/wmi",
        sum = "h1:E1ctvB7uKFMOJw3fdOW32DwGE9I7t++CRUEMKvFoFiw=",
        version = "v1.2.3",
    )
    go_repository(
        name = "com_github_yvasiyarov_go_metrics",
        importpath = "github.com/yvasiyarov/go-metrics",
        sum = "h1:+lm10QQTNSBd8DVTNGHx7o/IKu9HYDvLMffDhbyLccI=",
        version = "v0.0.0-20140926110328-57bccd1ccd43",
    )
    go_repository(
        name = "com_github_yvasiyarov_gorelic",
        importpath = "github.com/yvasiyarov/gorelic",
        sum = "h1:hlE8//ciYMztlGpl/VA+Zm1AcTPHYkHJPbHqE6WJUXE=",
        version = "v0.0.0-20141212073537-a9bba5b9ab50",
    )
    go_repository(
        name = "com_github_yvasiyarov_newrelic_platform_go",
        importpath = "github.com/yvasiyarov/newrelic_platform_go",
        sum = "h1:ERexzlUfuTvpE74urLSbIQW0Z/6hF9t8U4NsJLaioAY=",
        version = "v0.0.0-20140908184405-b21fdbd4370f",
    )
    go_repository(
        name = "com_github_zeebo_assert",
        importpath = "github.com/zeebo/assert",
        sum = "h1:g7C04CbJuIDKNPFHmsk4hwZDO5O+kntRxzaUoNXj+IQ=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_zeebo_blake3",
        importpath = "github.com/zeebo/blake3",
        sum = "h1:TFoLXsjeXqRNFxSbk35Dk4YtszE/MQQGK10BH4ptoTg=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_zeebo_pcg",
        importpath = "github.com/zeebo/pcg",
        sum = "h1:lyqfGeWiv4ahac6ttHs+I5hwtH/+1mrhlCtVNQM2kHo=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_zeebo_xxh3",
        importpath = "github.com/zeebo/xxh3",
        sum = "h1:xZmwmqxHZA8AI603jOQ0tMqmBr9lPeFwGg6d+xy9DC0=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_zenazn_goji",
        importpath = "github.com/zenazn/goji",
        sum = "h1:4lbD8Mx2h7IvloP7r2C0D6ltZP6Ufip8Hn0wmSK5LR8=",
        version = "v1.0.1",
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
        sum = "h1:Jo0SM9cQnSkYfp44+v+NQXHpcHqlnRJk2qxh6yvxxxQ=",
        version = "v0.115.1",
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        importpath = "cloud.google.com/go/accessapproval",
        sum = "h1:DLU5ua2WQXvdUL6yd/D4XFPXyd6acv1hNJJBMdt6Fh0=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sum = "h1:K0zCbd23A64sdJmOZDaW39dEMB6JVnGz2uycwd8PTu0=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        importpath = "cloud.google.com/go/aiplatform",
        sum = "h1:EPPqgHDJpBZKRvv+OsB3cr0jYz3EL2pZ+802rBPcG8U=",
        version = "v1.68.0",
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        importpath = "cloud.google.com/go/analytics",
        sum = "h1:vJdNlQCfvgwxHl7bn5pqON6k5/PzVA+Uk5m5nWaFh84=",
        version = "v0.25.0",
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        importpath = "cloud.google.com/go/apigateway",
        sum = "h1:KfvzagH2g7O6K/egrXopAbpvsxeDGNf2dbG7F39/4Jk=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        importpath = "cloud.google.com/go/apigeeconnect",
        sum = "h1:/1m2gI6xt9146I8dhTtBKR8dGLb4WqYbMT7SViCkKEU=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        importpath = "cloud.google.com/go/apigeeregistry",
        sum = "h1:nDQW/S5YjH3wmHoUy53x4CIcccbukxjqEq0EjMJUYoA=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_appengine",
        importpath = "cloud.google.com/go/appengine",
        sum = "h1:8i/2xM6NrMI6Js2P5Ojsbr3J4SNOjLHpuqNeMsbvpp4=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        importpath = "cloud.google.com/go/area120",
        sum = "h1:HdrEaFo0n28gOrPlB6BXEVPQ2x9GZep2rkCvPcMINIg=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        importpath = "cloud.google.com/go/artifactregistry",
        sum = "h1:fHsfq5+Vir1FjEMGn7lbiSygyG+TXdtb1uRXQ72SDg4=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        importpath = "cloud.google.com/go/asset",
        sum = "h1:2kHJKyVUEbuisDjvOK9+XQrvoosEBqsb6yaEzq5gaWY=",
        version = "v1.20.0",
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        importpath = "cloud.google.com/go/assuredworkloads",
        sum = "h1:ZolaDkCGpBFrvrEc/lvdJ584NRmIahc8MMC9vqPkWrc=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_auth",
        importpath = "cloud.google.com/go/auth",
        sum = "h1:VOEUIAADkkLtyfr3BLa3R8Ed/j6w1jTBmARx+wb5w5U=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_auth_oauth2adapt",
        importpath = "cloud.google.com/go/auth/oauth2adapt",
        sum = "h1:0GWE/FUsXhf6C+jAkWgYm7X9tK8cuEIfy19DBn6B6bY=",
        version = "v0.2.4",
    )
    go_repository(
        name = "com_google_cloud_go_automl",
        importpath = "cloud.google.com/go/automl",
        sum = "h1:SkYEYiiYHDZmnIdk5ZnCo6pTgOwKdpcOFma27ZDjh0o=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        importpath = "cloud.google.com/go/baremetalsolution",
        sum = "h1:Ht+IPfbdjNGsOAaZqSkPJ+w/to4e7CzDZ1z9GaBkYV4=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        importpath = "cloud.google.com/go/batch",
        sum = "h1:yE/0VZZtAhsaPTEc3OqDXmsjFX/739HcRMfAy0OuiUU=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        importpath = "cloud.google.com/go/beyondcorp",
        sum = "h1:xETx1qRUghe5f5pPIFLSmN+I3g3FNGiiftEX7cTYztI=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        importpath = "cloud.google.com/go/bigquery",
        sum = "h1:SYEA2f7fKqbSRRBHb7g0iHTtZvtPSPYdXfmqsjpsBwo=",
        version = "v1.62.0",
    )
    go_repository(
        name = "com_google_cloud_go_bigtable",
        importpath = "cloud.google.com/go/bigtable",
        sum = "h1:/uVLxGVRbK4mxK/iO89VqXcL/zoTSmkltVfIDYVBluQ=",
        version = "v1.31.0",
    )
    go_repository(
        name = "com_google_cloud_go_billing",
        importpath = "cloud.google.com/go/billing",
        sum = "h1:hnFBA+u/O7mP9a1z5Um4oZ5dONrKV9XSCrlpMGP73wk=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        importpath = "cloud.google.com/go/binaryauthorization",
        sum = "h1:O6O31WTrJhuRsXfTAOPj5iRD4WUOeKskHrpuWKSiCP8=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        importpath = "cloud.google.com/go/certificatemanager",
        sum = "h1:dQQxtb+HnS9GbylmAImWkC9QR8zy/xwRiQrIaewbwgE=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        importpath = "cloud.google.com/go/channel",
        sum = "h1:VqNv/GBepVRSaCKtD92bMZd/3siLlc8KFk4u21cUK/U=",
        version = "v1.18.0",
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        importpath = "cloud.google.com/go/cloudbuild",
        sum = "h1:tMKv6Gh7jPH3TM3QBHkdPzls80kQbUSH9Q20Hn3QD40=",
        version = "v1.17.0",
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        importpath = "cloud.google.com/go/clouddms",
        sum = "h1:BBPvI//wT1n3ruqVGDgCxBdpu9/era/b3B/HsiY8SyU=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        importpath = "cloud.google.com/go/cloudtasks",
        sum = "h1:rKVSsQwh0CI68n3RalLoGuW7sOtq2eil2gVZK4Pyi40=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        importpath = "cloud.google.com/go/compute",
        sum = "h1:XwPcZjgMCnU2tkwY10VleUjSAfpTj9RDn+kGrbYsi8o=",
        version = "v1.28.1",
    )
    go_repository(
        name = "com_google_cloud_go_compute_metadata",
        importpath = "cloud.google.com/go/compute/metadata",
        sum = "h1:UxK4uu/Tn+I3p2dYWTfiX4wva7aYlKixAHn3fyqngqo=",
        version = "v0.5.2",
    )
    go_repository(
        name = "com_google_cloud_go_contactcenterinsights",
        importpath = "cloud.google.com/go/contactcenterinsights",
        sum = "h1:hZSiEb53tyULmOSBlDPhEWPEe+vQ0F2gPQjOka1E5oE=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_container",
        importpath = "cloud.google.com/go/container",
        sum = "h1:Q1oW01ENxkkG3uf1oYoTmHPdvP+yhFCIuCJ4mk2RwkQ=",
        version = "v1.39.0",
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        importpath = "cloud.google.com/go/containeranalysis",
        sum = "h1:+pm0vZAyHlkflBQRcpTWowroofRcNyn6gr5gUivI6Cc=",
        version = "v0.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        importpath = "cloud.google.com/go/datacatalog",
        sum = "h1:7e5/0B2LYbNx0BcUJbiCT8K2wCtcB5993z/v1JeLIdc=",
        version = "v1.22.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        importpath = "cloud.google.com/go/dataflow",
        sum = "h1:AKl2CoSEGRTCYV03YREEDYVcncjg3G6tf8CZx4k4cRo=",
        version = "v0.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        importpath = "cloud.google.com/go/dataform",
        sum = "h1:yJ4RbaIw9ivlHrWCTxPxojM1VtCMSNCsF3xR8ohMGKc=",
        version = "v0.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        importpath = "cloud.google.com/go/datafusion",
        sum = "h1:87ukhTCPlWBYQSCExQRJcAa6HSp/g40hZsImQre9Ng8=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        importpath = "cloud.google.com/go/datalabeling",
        sum = "h1:7pIXkYYp6kya0XG27zOB0Tsnn3rjJ05xBIqvN4edzp4=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        importpath = "cloud.google.com/go/dataplex",
        sum = "h1:l3xIynMeZZi8U7bFTNTzPUiCrnBZLJMp62vlrcBRyeE=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataproc_v2",
        importpath = "cloud.google.com/go/dataproc/v2",
        sum = "h1:YjgcpuzUYX+Q/xCbh/+5+Nwx0DGzsO9nss/O7Usy79c=",
        version = "v2.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataqna",
        importpath = "cloud.google.com/go/dataqna",
        sum = "h1:Wk8s3XHBwmk+5pUtevnFxwQ57bXnTtvSswdJnEjssR4=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        importpath = "cloud.google.com/go/datastore",
        sum = "h1:p5H3bUQltOa26GcMRAxPoNwoqGkq5v8ftx9/ZBB35MI=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_google_cloud_go_datastream",
        importpath = "cloud.google.com/go/datastream",
        sum = "h1:vSdeXl6b6apcOTWT6/NiHglc0eIEJ4z5yJXAsXT0WBo=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        importpath = "cloud.google.com/go/deploy",
        sum = "h1:fkBCtx9Qzr/vVokIALFdXb5cLVT0VJEfKWfOZftaGkI=",
        version = "v1.22.0",
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        importpath = "cloud.google.com/go/dialogflow",
        sum = "h1:tdeHPAjpTe+z5+YyYqEJwoZiOwgP+bML+zimEXo/6O0=",
        version = "v1.57.0",
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        importpath = "cloud.google.com/go/dlp",
        sum = "h1:wPts74+F848F/ACZqU+c32Xh91DaBXXZaE66vpN6FQA=",
        version = "v1.18.0",
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        importpath = "cloud.google.com/go/documentai",
        sum = "h1:MQbd4Bk3o7ckIiKZooXCVJfIDweO+B/XRAVsLdnSD/A=",
        version = "v1.33.0",
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        importpath = "cloud.google.com/go/domains",
        sum = "h1:+UAfUhEO9aINLqZjVkOKEG28+JKD+Zio0GmnOAXqKVY=",
        version = "v0.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        importpath = "cloud.google.com/go/edgecontainer",
        sum = "h1:szfTtWNXKviueQ58eZ2EDzbjvEQkfx4QNQa3KIaCweU=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_errorreporting",
        importpath = "cloud.google.com/go/errorreporting",
        sum = "h1:E/gLk+rL7u5JZB9oq72iL1bnhVlLrnfslrgcptjJEUE=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_google_cloud_go_essentialcontacts",
        importpath = "cloud.google.com/go/essentialcontacts",
        sum = "h1:Ddirv7AYVEQiTKpboCwNVpC9HHLYSaW7wAk2u/OXJHo=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        importpath = "cloud.google.com/go/eventarc",
        sum = "h1:ok7KHtdTSu8F7D8Sb+Ug5lrKcQk/1+Xq7cZjeydeXEo=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        importpath = "cloud.google.com/go/filestore",
        sum = "h1:pI1dZzLjmH3NnuUKf13So4jP80mdZoWXB9etiV2zpso=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        importpath = "cloud.google.com/go/firestore",
        sum = "h1:YwmDHcyrxVRErWcgxunzEaZxtNbc8QoFYA/JOEwDPgc=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_google_cloud_go_functions",
        importpath = "cloud.google.com/go/functions",
        sum = "h1:bO55p91lPY5JLg5MBdmt6G9n4kNeClX0lA9hdusDU6M=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkebackup",
        importpath = "cloud.google.com/go/gkebackup",
        sum = "h1:MhJ+vTgc+UtU9Y0uRmWs1XpcN6qmUUzyn+tTBiKAjxk=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        importpath = "cloud.google.com/go/gkeconnect",
        sum = "h1:JY9V0rYzRAXHpwwVfBvlphWP2CCUUiJrtCyZCMxYXEY=",
        version = "v0.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        importpath = "cloud.google.com/go/gkehub",
        sum = "h1:pA1mYF5jSC8C/oyjzsfBaznjejWwpxUFIlRjWUZKB/s=",
        version = "v0.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        importpath = "cloud.google.com/go/gkemulticloud",
        sum = "h1:4wJPaNK7HFYLniVqMue+Eo/SpX+yf+aMvRITjUpirgM=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sum = "h1:k+DNTzjW+hG+lfGsNbNCopicaUIyT0Q4B0xLYCwEnpo=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        importpath = "cloud.google.com/go/iam",
        sum = "h1:kZKMKVNk/IsSSc/udOb83K0hL/Yh/Gcqpz+oAkoIFN8=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        importpath = "cloud.google.com/go/iap",
        sum = "h1:Er5DF68/1MMBQo62Vfs3XvOPmyqj9JgQniSCNKLHYK8=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        importpath = "cloud.google.com/go/ids",
        sum = "h1:s14XF62E/BNe4jErHtgWaN1m5vrLkTMuG4gNRV0GRks=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        importpath = "cloud.google.com/go/iot",
        sum = "h1:Q2GdWBly1+5Enm1TcEWvmu3YTRw9IyS1PYR4gJHepVY=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        importpath = "cloud.google.com/go/kms",
        sum = "h1:x0OVJDl6UH1BSX4THKlMfdcFWoE4ruh90ZHuilZekrU=",
        version = "v1.19.0",
    )
    go_repository(
        name = "com_google_cloud_go_language",
        importpath = "cloud.google.com/go/language",
        sum = "h1:7e62MAtxUkjYzL3PnD5ZjJn81KV2hnau4EcS4LN73Lg=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        importpath = "cloud.google.com/go/lifesciences",
        sum = "h1:NxYwUD3BcxXKttOlbKsn84PacxjQPdCFSt6Kk4l6UW8=",
        version = "v0.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        importpath = "cloud.google.com/go/logging",
        sum = "h1:v3ktVzXMV7CwHq1MBF65wcqLMA7i+z3YxbUsoK7mOKs=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_longrunning",
        importpath = "cloud.google.com/go/longrunning",
        sum = "h1:mM1ZmaNsQsnb+5n1DNPeL0KwQd9jQRqSqSDEkBZr+aI=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        importpath = "cloud.google.com/go/managedidentities",
        sum = "h1:GRZPWk8g9g99CunSn6yvjCgWGmYlJImLb/oHHxNwujU=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        importpath = "cloud.google.com/go/maps",
        sum = "h1:i+IeDDYWxW98EmLNQIuFdEVYBYc/VdQCKKrMa7W4Iyw=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        importpath = "cloud.google.com/go/mediatranslation",
        sum = "h1:bA2Qid3TwvcAcOZp9Yj7GJSAi0N24G8cw+tlLBcIkV8=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        importpath = "cloud.google.com/go/memcache",
        sum = "h1:pbBvVeTgYKSJ0sxT2k+9OmDS1Kp+QnZkjzPg1+PvVN4=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        importpath = "cloud.google.com/go/metastore",
        sum = "h1:m7ICE3M+5jTwM6nHyv4/YcA1eeSYg3XtKpdQsB7QKP0=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        importpath = "cloud.google.com/go/monitoring",
        sum = "h1:EMc0tB+d3lUewT2NzKC/hr8cSR9WsUieVywzIHetGro=",
        version = "v1.21.0",
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        importpath = "cloud.google.com/go/networkconnectivity",
        sum = "h1:NgR2Qz/d8TwT9k3E9Dw2DtdeCASlIsli9o/r9VSgUKk=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        importpath = "cloud.google.com/go/networkmanagement",
        sum = "h1:HGbqaS352q7JLOpdmqHUXBeaJc6S6DtUSRJF9URtTBY=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        importpath = "cloud.google.com/go/networksecurity",
        sum = "h1:SA+W7/GNJnrf1gINnIar4zpsRrQk9dLNN5uTuY4TO90=",
        version = "v0.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        importpath = "cloud.google.com/go/notebooks",
        sum = "h1:80/UfGbQeaY1zMcqf8za2T8u94wjMTTjeXEj5Bim1Q4=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        importpath = "cloud.google.com/go/optimization",
        sum = "h1:yDun269GdHx6gvcNJZeklF7uQw19a0LldSSiYtOYiR0=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        importpath = "cloud.google.com/go/orchestration",
        sum = "h1:cGj9njLm+uUa/YPxGx6X7OU5RBh2VbpDktetkaHOEPQ=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        importpath = "cloud.google.com/go/orgpolicy",
        sum = "h1:WaabiSAxtyi4JNFATvsPmQS2IWRjr1+pwU3/Bihj7eA=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        importpath = "cloud.google.com/go/osconfig",
        sum = "h1:7XGKH/O0PGIoPIIYc+Ja5WD5Sc1nK0y5DT7jvSfyJVc=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        importpath = "cloud.google.com/go/oslogin",
        sum = "h1:tgeFPXRtrXuS6MbBsevnntls4kQeD/QP3VUB9ZxmmMg=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        importpath = "cloud.google.com/go/phishingprotection",
        sum = "h1:TSua7OZWGInbjd9DiSNH1v4UqhrKwpw3q4RM6rzm+0I=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sum = "h1:TUxMBu2SAWmo8RtWhKcgv7LjbmUEA2t5U4+abA3lXik=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        importpath = "cloud.google.com/go/privatecatalog",
        sum = "h1:9ZescTLuQE6idHyXAGh7nDxer8UJXfACW7DLIuhtvOs=",
        version = "v0.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        importpath = "cloud.google.com/go/pubsub",
        sum = "h1:PVTbzorLryFL5ue8esTS2BfehUs0ahyNOY9qcd+HMOs=",
        version = "v1.42.0",
    )
    go_repository(
        name = "com_google_cloud_go_pubsublite",
        importpath = "cloud.google.com/go/pubsublite",
        sum = "h1:jLQozsEVr+c6tOU13vDugtnaBSUy/PD5zK6mhm+uF1Y=",
        version = "v1.8.2",
    )
    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise_v2",
        importpath = "cloud.google.com/go/recaptchaenterprise/v2",
        sum = "h1:aXFHIGiFseHKdYxPtBaM18BvvK6CrG6yYM+IoWu3WDQ=",
        version = "v2.17.0",
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        importpath = "cloud.google.com/go/recommendationengine",
        sum = "h1:w8YMuIJdZxMEptNXSiXYdqaiCL2rTweOoO71RNtcxNE=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        importpath = "cloud.google.com/go/recommender",
        sum = "h1:ZyKUB5CddfFGD1qiTKbF3t59OxXPIE7+3lzy59i7hvs=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        importpath = "cloud.google.com/go/redis",
        sum = "h1:YItghJ0VY98gJperCaTVEe7g+QZWz1nsN5ioJcSxkDY=",
        version = "v1.17.0",
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        importpath = "cloud.google.com/go/resourcemanager",
        sum = "h1:oqO6UInOJ1ZBBEYTKPJms2+FKdGmZEYAYBKyt0oqpEI=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        importpath = "cloud.google.com/go/resourcesettings",
        sum = "h1:r2AHqVv9E6Toxiuwo905fFjy50pfKamehDBlQfhqblM=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        importpath = "cloud.google.com/go/retail",
        sum = "h1:8Ck0ZsfHzEdhd4BAzHJ0YcPq7poCPMMHM3BLJ0yk4WE=",
        version = "v1.18.0",
    )
    go_repository(
        name = "com_google_cloud_go_run",
        importpath = "cloud.google.com/go/run",
        sum = "h1:1hfJ4418lukwslnbuMZx/t4MxBd0FDo4d/38NvAP5Yo=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        importpath = "cloud.google.com/go/scheduler",
        sum = "h1:9Hc+L8YEgci20BFkQNsgsb5UJFfUbylfHAKgfXkNRWU=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        importpath = "cloud.google.com/go/secretmanager",
        sum = "h1:P2RRu2NEsQyOjplhUPvWKqzDXUKzwejHLuSUBHI8c4w=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_security",
        importpath = "cloud.google.com/go/security",
        sum = "h1:CjBd67GVb+Oenjt4VsUw0RUQktSIgexTJN3UQta7XRE=",
        version = "v1.18.0",
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        importpath = "cloud.google.com/go/securitycenter",
        sum = "h1:XsBzOeMRGs0/JkXXkbjhjjtAtlVGPR1GZ235gH25XMk=",
        version = "v1.35.0",
    )
    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        importpath = "cloud.google.com/go/servicedirectory",
        sum = "h1:uieHG59ROehbCEtd+YINNgjXEDyidH0ye+REZzDVe6Y=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_shell",
        importpath = "cloud.google.com/go/shell",
        sum = "h1:kCkIEXYPqrhHay46HjyhuOk/C1x7Qva4Lw968UVPcEo=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        importpath = "cloud.google.com/go/spanner",
        sum = "h1:h8xfobxh5lQu4qJVMPH+wSiyU+ZM6ZTxRNqGeu9iIVA=",
        version = "v1.67.0",
    )
    go_repository(
        name = "com_google_cloud_go_speech",
        importpath = "cloud.google.com/go/speech",
        sum = "h1:q/ZPuG5G//DHm9hBehaP5c/wuD2qP77OpiPQrE7hEbg=",
        version = "v1.25.0",
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        importpath = "cloud.google.com/go/storage",
        sum = "h1:CcxnSohZwizt4LCzQHWvBf1/kvtHUn7gk9QERXPyXFs=",
        version = "v1.43.0",
    )
    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        importpath = "cloud.google.com/go/storagetransfer",
        sum = "h1:URBQgN/5pyGGq/kE6z2HUYzAG2lhsvJWcjrNnqrjaxU=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        importpath = "cloud.google.com/go/talent",
        sum = "h1:2zqgG97bPfr259+xsJu1coFlvXzq+D9OD8mL8atgWeU=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        importpath = "cloud.google.com/go/texttospeech",
        sum = "h1:ZDftBGozfB/ITwvYiYHHeSDQ5Yc9azNphHMjIzakwVQ=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        importpath = "cloud.google.com/go/tpu",
        sum = "h1:mRFFdrJ/DuymJehZ0SJXKgMQ1eoWtOhgrStf/eQrlsw=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        importpath = "cloud.google.com/go/trace",
        sum = "h1:UHX6cOJm45Zw/KIbqHe4kII8PupLt/V5tscZUkeiJVI=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        importpath = "cloud.google.com/go/translate",
        sum = "h1:NoO50ycJWq7GPZEjuPz8Ye926uLko/gbxWnQ9mtQrDs=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_video",
        importpath = "cloud.google.com/go/video",
        sum = "h1:DTnNFkbpmPunk+V3WKmjs46EbdW5QevSy0KJ9JmlUus=",
        version = "v1.23.0",
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        importpath = "cloud.google.com/go/videointelligence",
        sum = "h1:nM9O0Pw3XcQpLHHfSSlzbytBRXh5ATrEnY4Cbmv4RVs=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_vision_v2",
        importpath = "cloud.google.com/go/vision/v2",
        sum = "h1:q3psn2Ea+EgUH7nefR0S9k9u08QTYhUI3PPm44FNqnM=",
        version = "v2.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        importpath = "cloud.google.com/go/vmmigration",
        sum = "h1:YH4XwJirujDvpPWVjzAxLUc97UfKs48+RDNpHzodRyc=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        importpath = "cloud.google.com/go/vmwareengine",
        sum = "h1:Yd8NnmkjUTWouvtQySzZJKzzUO+21hRnlji/oHjWRrc=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        importpath = "cloud.google.com/go/vpcaccess",
        sum = "h1:jJ6cyLNDcdQYZBXdqucqneR9D3MQzAoEmokE9gD5uVU=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        importpath = "cloud.google.com/go/webrisk",
        sum = "h1:Knhx8eILUwXmH6UnKF5h5GLuLy1eRrasm21s6DQtOHQ=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        importpath = "cloud.google.com/go/websecurityscanner",
        sum = "h1:2+X6oSpyKlCPN43j5xXVHMBLajVsh4BYFUpQNUk6Y+Q=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        importpath = "cloud.google.com/go/workflows",
        sum = "h1:LHZQw+fkCWN/zRSHEWcwEnh8xHGt76yd/4Gf6Pt0zbU=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_shuralyov_dmitri_gpu_mtl",
        importpath = "dmitri.shuralyov.com/gpu/mtl",
        sum = "h1:VpgP7xuJadIUuKccphEpTJnWhS2jkQyMt6Y7pJCD7fY=",
        version = "v0.0.0-20190408044501-666a987793e9",
    )
    go_repository(
        name = "dev_cel_expr",
        build_directives = [
            "gazelle:proto file",
            "gazelle:proto_strip_import_prefix /proto/cel/expr",
            "gazelle:proto_import_prefix cel/expr",
            "gazelle:resolve proto google/rpc/status.proto @googleapis//google/rpc:status_proto",
            "gazelle:resolve proto go google/rpc/status.proto @org_golang_google_genproto_googleapis_rpc//status",
        ],
        build_file_generation = "clean",
        importpath = "cel.dev/expr",
        sum = "h1:lXuo+nDhpyJSpWxpPVi5cPUwzKb+dsdOiw6IreM5yt0=",
        version = "v0.19.0",
    )
    go_repository(
        name = "dev_gocloud",
        importpath = "gocloud.dev",
        sum = "h1:f8LgP+4WDqOG/RXoUcyLpeIAGOcAbZrZbDQCUee10ng=",
        version = "v0.40.0",
    )
    go_repository(
        name = "in_gopkg_airbrake_gobrake_v2",
        importpath = "gopkg.in/airbrake/gobrake.v2",
        sum = "h1:7z2uVWwn7oVeeugY1DtlPAy5H+KYgB1KeKTnqjNatLo=",
        version = "v2.0.9",
    )
    go_repository(
        name = "in_gopkg_alecthomas_kingpin_v2",
        importpath = "gopkg.in/alecthomas/kingpin.v2",
        sum = "h1:jMFz6MfLP0/4fUyZle81rXUoxOBFi19VUFKVDOQfozc=",
        version = "v2.2.6",
    )

    # Needed for go dependencies.
    go_repository(
        name = "in_gopkg_check_v1",
        importpath = "gopkg.in/check.v1",
        sum = "h1:Hei/4ADfdWqJk1ZMxUNpqntNwaWcugrBjAiHlqqRiVk=",
        version = "v1.0.0-20201130134442-10cb98267c6c",
    )
    go_repository(
        name = "in_gopkg_cheggaaa_pb_v1",
        importpath = "gopkg.in/cheggaaa/pb.v1",
        sum = "h1:Ev7yu1/f6+d+b3pi5vPdRPc6nNtP1umSfcWiEfRqv6I=",
        version = "v1.0.25",
    )
    go_repository(
        name = "in_gopkg_errgo_v2",
        importpath = "gopkg.in/errgo.v2",
        sum = "h1:0vLT13EuvQ0hNvakwLuFZ/jYrLp5F3kcWHXdRggjCE8=",
        version = "v2.1.0",
    )
    go_repository(
        name = "in_gopkg_evanphx_json_patch_v4",
        importpath = "gopkg.in/evanphx/json-patch.v4",
        sum = "h1:n6jtcsulIzXPJaxegRbvFNNrZDjbij7ny3gmSPG+6V4=",
        version = "v4.12.0",
    )
    go_repository(
        name = "in_gopkg_fsnotify_v1",
        importpath = "gopkg.in/fsnotify.v1",
        sum = "h1:xOHLXZwVvI9hhs+cLKq5+I5onOuwQLhQwiu63xxlHs4=",
        version = "v1.4.7",
    )
    go_repository(
        name = "in_gopkg_gemnasium_logrus_airbrake_hook_v2",
        importpath = "gopkg.in/gemnasium/logrus-airbrake-hook.v2",
        sum = "h1:OAj3g0cR6Dx/R07QgQe8wkA9RNjB2u4i700xBkIT4e0=",
        version = "v2.1.2",
    )
    go_repository(
        name = "in_gopkg_go_playground_assert_v1",
        importpath = "gopkg.in/go-playground/assert.v1",
        sum = "h1:xoYuJVE7KT85PYWrN730RguIQO0ePzVRfFMXadIrXTM=",
        version = "v1.2.1",
    )
    go_repository(
        name = "in_gopkg_inf_v0",
        importpath = "gopkg.in/inf.v0",
        sum = "h1:73M5CoZyi3ZLMOyDlQh031Cx6N9NDJ2Vvfl76EDAgDc=",
        version = "v0.9.1",
    )
    go_repository(
        name = "in_gopkg_ini_v1",
        importpath = "gopkg.in/ini.v1",
        sum = "h1:Dgnx+6+nfE+IfzjUEISNeydPJh9AXNNsWbGP9KzCsOA=",
        version = "v1.67.0",
    )
    go_repository(
        name = "in_gopkg_mgo_v2",
        importpath = "gopkg.in/mgo.v2",
        sum = "h1:VpOs+IwYnYBaFnrNAeB8UUWtL3vEUnzSCL1nVjPhqrw=",
        version = "v2.0.0-20190816093944-a6b53ec6cb22",
    )
    go_repository(
        name = "in_gopkg_natefinch_lumberjack_v2",
        importpath = "gopkg.in/natefinch/lumberjack.v2",
        sum = "h1:1Lc07Kr7qY4U2YPouBjpCLxpiyxIVoxqXgkXLknAOE8=",
        version = "v2.0.0",
    )
    go_repository(
        name = "in_gopkg_resty_v1",
        importpath = "gopkg.in/resty.v1",
        sum = "h1:CuXP0Pjfw9rOuY6EP+UvtNvt5DSqHpIxILZKT/quCZI=",
        version = "v1.12.0",
    )
    go_repository(
        name = "in_gopkg_square_go_jose_v2",
        importpath = "gopkg.in/square/go-jose.v2",
        sum = "h1:7odma5RETjNHWJnR32wx8t+Io4djHE1PqxCFx3iiZ2w=",
        version = "v2.5.1",
    )
    go_repository(
        name = "in_gopkg_tomb_v1",
        importpath = "gopkg.in/tomb.v1",
        sum = "h1:uRGJdciOHaEIrze2W8Q3AKkepLTh2hOroT7a+7czfdQ=",
        version = "v1.0.0-20141024135613-dd632973f1e7",
    )
    go_repository(
        name = "in_gopkg_vmihailenco_msgpack_v2",
        importpath = "gopkg.in/vmihailenco/msgpack.v2",
        sum = "h1:gjPqo9orRVlSAH/065qw3MsFCDpH7fa1KpiizXyllY4=",
        version = "v2.9.2",
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
        name = "io_cncf_tags_container_device_interface",
        importpath = "tags.cncf.io/container-device-interface",
        sum = "h1:c0jN4Mt6781jD67NdPajmZlD1qrqQyov/Xfoab37lj0=",
        version = "v0.8.1",
    )
    go_repository(
        name = "io_cncf_tags_container_device_interface_specs_go",
        importpath = "tags.cncf.io/container-device-interface/specs-go",
        sum = "h1:QYGFzGxvYK/ZLMrjhvY0RjpUavIn4KcmRmVP/JjdBTA=",
        version = "v0.8.0",
    )
    go_repository(
        name = "io_etcd_go_bbolt",
        importpath = "go.etcd.io/bbolt",
        sum = "h1:yGEzV1wPz2yVCLsD8ZAiGHhHVlczyC9d1rP43/VCRJ0=",
        version = "v1.3.11",
    )
    go_repository(
        name = "io_etcd_go_etcd",
        importpath = "go.etcd.io/etcd",
        sum = "h1:1JFLBqwIgdyHN1ZtgjTBwO+blA6gVOmZurpiMEsETKo=",
        version = "v0.5.0-alpha.5.0.20200910180754-dd1b699fc489",
    )
    go_repository(
        name = "io_etcd_go_etcd_api_v3",
        importpath = "go.etcd.io/etcd/api/v3",
        sum = "h1:GsV3S+OfZEOCNXdtNkBSR7kgLobAa/SO6tCxRa0GAYw=",
        version = "v3.5.0",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_pkg_v3",
        importpath = "go.etcd.io/etcd/client/pkg/v3",
        sum = "h1:2aQv6F436YnN7I4VbI8PPYrBhu+SmrTaADcf8Mi/6PU=",
        version = "v3.5.0",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v2",
        importpath = "go.etcd.io/etcd/client/v2",
        sum = "h1:ftQ0nOOHMcbMS3KIaDQ0g5Qcd6bhaBrQT6b89DfwLTs=",
        version = "v2.305.0",
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v3",
        importpath = "go.etcd.io/etcd/client/v3",
        sum = "h1:62Eh0XOro+rDwkrypAGDfgmNh5Joq+z+W9HZdlXMzek=",
        version = "v3.5.0",
    )
    go_repository(
        name = "io_etcd_go_etcd_pkg_v3",
        importpath = "go.etcd.io/etcd/pkg/v3",
        sum = "h1:ntrg6vvKRW26JRmHTE0iNlDgYK6JX3hg/4cD62X0ixk=",
        version = "v3.5.0",
    )
    go_repository(
        name = "io_etcd_go_etcd_raft_v3",
        importpath = "go.etcd.io/etcd/raft/v3",
        sum = "h1:kw2TmO3yFTgE+F0mdKkG7xMxkit2duBDa2Hu6D/HMlw=",
        version = "v3.5.0",
    )
    go_repository(
        name = "io_etcd_go_etcd_server_v3",
        importpath = "go.etcd.io/etcd/server/v3",
        sum = "h1:jk8D/lwGEDlQU9kZXUFMSANkE22Sg5+mW27ip8xcF9E=",
        version = "v3.5.0",
    )
    go_repository(
        name = "io_etcd_go_gofail",
        importpath = "go.etcd.io/gofail",
        sum = "h1:XItAMIhOojXFQMgrxjnd2EIIHun/d5qL0Pf7FzVTkFg=",
        version = "v0.1.0",
    )
    go_repository(
        name = "io_filippo_edwards25519",
        importpath = "filippo.io/edwards25519",
        sum = "h1:FNf4tywRC1HmFuKW5xopWpigGjJKiJSV0Cqo0cJWDaA=",
        version = "v1.1.0",
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
    go_repository(
        name = "io_k8s_api",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/api",
        sum = "h1:I2QNzitPVsPeLQvexMEsj945QumYraqv9m74isPDKhM=",
        version = "v0.31.4",
    )
    go_repository(
        name = "io_k8s_apiextensions_apiserver",
        importpath = "k8s.io/apiextensions-apiserver",
        sum = "h1:fZgCVhGwsclj3qCw1buVXCV6khjRzKC5eCFt24kyLSk=",
        version = "v0.31.0",
    )
    go_repository(
        name = "io_k8s_apimachinery",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/apimachinery",
        sum = "h1:8xjE2C4CzhYVm9DGf60yohpNUh5AEBnPxCryPBECmlM=",
        version = "v0.31.4",
    )
    go_repository(
        name = "io_k8s_apiserver",
        importpath = "k8s.io/apiserver",
        sum = "h1:p+2dgJjy+bk+B1Csz+mc2wl5gHwvNkC9QJV+w55LVrY=",
        version = "v0.31.0",
    )
    go_repository(
        name = "io_k8s_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/client-go",
        sum = "h1:t4QEXt4jgHIkKKlx06+W3+1JOwAFU/2OPiOo7H92eRQ=",
        version = "v0.31.4",
    )
    go_repository(
        name = "io_k8s_code_generator",
        importpath = "k8s.io/code-generator",
        sum = "h1:kM/68Y26Z/u//TFc1ggVVcg62te8A2yQh57jBfD0FWQ=",
        version = "v0.19.7",
    )
    go_repository(
        name = "io_k8s_component_base",
        importpath = "k8s.io/component-base",
        sum = "h1:/KIzGM5EvPNQcYgwq5NwoQBaOlVFrghoVGr8lG6vNRs=",
        version = "v0.31.0",
    )
    go_repository(
        name = "io_k8s_cri_api",
        importpath = "k8s.io/cri-api",
        sum = "h1:pQwYDahnAX9K8KtdV8PD1eeNexMJojEj1t/5kAMX61E=",
        version = "v0.29.1",
    )
    go_repository(
        name = "io_k8s_gengo",
        importpath = "k8s.io/gengo",
        sum = "h1:JApXBKYyB7l9xx+DK7/+mFjC7A9Bt5A93FPvFD0HIFE=",
        version = "v0.0.0-20201113003025-83324d819ded",
    )
    go_repository(
        name = "io_k8s_klog_v2",
        importpath = "k8s.io/klog/v2",
        sum = "h1:n9Xl7H1Xvksem4KFG4PYbdQCQxqc/tTUyrgXaOhHSzk=",
        version = "v2.130.1",
    )
    go_repository(
        name = "io_k8s_kube_openapi",
        importpath = "k8s.io/kube-openapi",
        sum = "h1:w6nThEmGo9zcL+xH1Tu6pjxJ3K1jXFW+V0u4peqN8ks=",
        version = "v0.0.0-20240403164606-bc84c2ddaf99",
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
        name = "io_k8s_kubernetes",
        importpath = "k8s.io/kubernetes",
        sum = "h1:qTfB+u5M92k2fCCCVP2iuhgwwSOv1EkAkvQY1tQODD8=",
        version = "v1.13.0",
    )
    go_repository(
        name = "io_k8s_sigs_apiserver_network_proxy_konnectivity_client",
        importpath = "sigs.k8s.io/apiserver-network-proxy/konnectivity-client",
        sum = "h1:2770sDpzrjjsAtVhSeUFseziht227YAWYHLGNM8QPwY=",
        version = "v0.30.3",
    )
    go_repository(
        name = "io_k8s_sigs_controller_runtime",
        importpath = "sigs.k8s.io/controller-runtime",
        sum = "h1:nWVM7aq+Il2ABxwiCizrVDSlmDcshi9llbaFbC0ji/Q=",
        version = "v0.19.0",
    )
    go_repository(
        name = "io_k8s_sigs_json",
        importpath = "sigs.k8s.io/json",
        sum = "h1:EDPBXCAspyGV4jQlpZSudPeMmr1bNJefnuqLsRAsHZo=",
        version = "v0.0.0-20221116044647-bc3834ca7abd",
    )
    go_repository(
        name = "io_k8s_sigs_structured_merge_diff_v4",
        importpath = "sigs.k8s.io/structured-merge-diff/v4",
        sum = "h1:150L+0vs/8DA78h1u02ooW1/fFq/Lwr+sGiqlzvrtq4=",
        version = "v4.4.1",
    )
    go_repository(
        name = "io_k8s_sigs_yaml",
        importpath = "sigs.k8s.io/yaml",
        sum = "h1:Mk1wCc2gy/F0THH0TAp1QYyJNzRm2KCLy3o5ASXVI5E=",
        version = "v1.4.0",
    )
    go_repository(
        name = "io_k8s_utils",
        importpath = "k8s.io/utils",
        sum = "h1:pUdcCO1Lk/tbT5ztQWOBi5HBgbBP1J8+AsQnQCKsi8A=",
        version = "v0.0.0-20240711033017-18e509b52bc8",
    )
    go_repository(
        name = "io_kythe",
        importpath = "kythe.io",
        patch_args = ["-p1"],
        patches = ["@{}//buildpatches:io_kythe.patch".format(workspace_name)],
        replace = "github.com/buildbuddy-io/kythe",
        sum = "h1:fga2YWn6WRkXuxnoXMj6/fxXZqsoVOkmW4tUpT/VcJY=",
        version = "v0.0.72",
    )
    go_repository(
        name = "io_moul_http2curl_v2",
        importpath = "moul.io/http2curl/v2",
        sum = "h1:9r3JfDzWPcbIklMOs2TnIFzDYvfAZvjeavG6EzP7jYs=",
        version = "v2.3.0",
    )
    go_repository(
        name = "io_opencensus_go",
        importpath = "go.opencensus.io",
        sum = "h1:y73uSU6J157QMP2kn2r30vwW1A2W2WFwSCGnAVxeaD0=",
        version = "v0.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_auto_sdk",
        importpath = "go.opentelemetry.io/auto/sdk",
        sum = "h1:cH53jehLUN6UFLY71z+NDOiNJqDdPRaXzTel0sJySYA=",
        version = "v1.1.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib",
        importpath = "go.opentelemetry.io/contrib",
        sum = "h1:ubFQUn0VCZ0gPwIoJfBJVpeBlyRMxu8Mm/huKWYd9p0=",
        version = "v0.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_detectors_gcp",
        importpath = "go.opentelemetry.io/contrib/detectors/gcp",
        sum = "h1:P78qWqkLSShicHmAzfECaTgvslqHxblNE9j62Ws1NK8=",
        version = "v1.32.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc",
        importpath = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        sum = "h1:r6I7RJCN86bpD/FQwedZ0vSixDpwuWREjW9oRMsmqDc=",
        version = "v0.54.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp",
        importpath = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
        sum = "h1:UP6IpuHFkUgOQL9FFQFrZ+5LiwhhYRbi7VZSIx6Nj5s=",
        version = "v0.56.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        importpath = "go.opentelemetry.io/otel",
        sum = "h1:zRLXxLCgL1WyKsPVrgbSdMN4c0FMkDAskSTQP+0hdUY=",
        version = "v1.34.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_jaeger",
        importpath = "go.opentelemetry.io/otel/exporters/jaeger",
        sum = "h1:D7UpUy2Xc2wsi1Ras6V40q806WM07rqoCWzXu7Sqy+4=",
        version = "v1.17.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp",
        importpath = "go.opentelemetry.io/otel/exporters/otlp",
        sum = "h1:PTNgq9MRmQqqJY0REVbZFvwkYOA85vbdQU/nVfxDyqg=",
        version = "v0.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_internal_retry",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/internal/retry",
        sum = "h1:R/OBkMoGgfy2fLhs2QhkCI1w4HLEQX92GCcJB6SSdNk=",
        version = "v1.3.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlpmetric_otlpmetricgrpc",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc",
        sum = "h1:ajl4QczuJVA2TU9W9AGw++86Xga/RKt//16z/yxPgdk=",
        version = "v1.34.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace",
        sum = "h1:OeNbIYk/2C15ckl7glBlOBp5+WlYsOElzTNmiPW/x60=",
        version = "v1.34.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc",
        sum = "h1:tgJ0uaNS4c98WRNUEx5U3aDlrDOI5Rs+1Vifcw4DJ8U=",
        version = "v1.34.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracehttp",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp",
        sum = "h1:IeMeyr1aBvBiPVYihXIaeIZba6b8E1bYp7lbdxK8CQg=",
        version = "v1.19.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_prometheus",
        importpath = "go.opentelemetry.io/otel/exporters/prometheus",
        sum = "h1:GnCIi0QyG0yy2MrJLzVrIM7laaJstj//flf1zEJCG+E=",
        version = "v0.56.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        importpath = "go.opentelemetry.io/otel/metric",
        sum = "h1:+eTR3U0MyfWjRDhmFMxe2SsW64QrZ84AOhvqS7Y+PoQ=",
        version = "v1.34.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_oteltest",
        importpath = "go.opentelemetry.io/otel/oteltest",
        sum = "h1:HiITxCawalo5vQzdHfKeZurV8x7ljcqAgiWzF6Vaeaw=",
        version = "v0.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk",
        importpath = "go.opentelemetry.io/otel/sdk",
        sum = "h1:95zS4k/2GOy069d321O8jWgYsW3MzVV+KuSPKp7Wr1A=",
        version = "v1.34.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk_export_metric",
        importpath = "go.opentelemetry.io/otel/sdk/export/metric",
        sum = "h1:c5VRjxCXdQlx1HjzwGdQHzZaVI82b5EbBgOu2ljD92g=",
        version = "v0.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk_metric",
        importpath = "go.opentelemetry.io/otel/sdk/metric",
        sum = "h1:5CeK9ujjbFVL5c1PhLuStg1wxA7vQv7ce1EK0Gyvahk=",
        version = "v1.34.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        importpath = "go.opentelemetry.io/otel/trace",
        sum = "h1:+ouXS2V8Rd4hp4580a8q23bg0azF2nI8cqLYnC8mh/k=",
        version = "v1.34.0",
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        importpath = "go.opentelemetry.io/proto/otlp",
        sum = "h1:xJvq7gMzB31/d406fB8U5CBdyQGw4P399D1aQWU/3i4=",
        version = "v1.5.0",
    )
    go_repository(
        name = "io_rsc_binaryregexp",
        importpath = "rsc.io/binaryregexp",
        sum = "h1:HfqmD5MEmC0zvwBuF187nq9mdnXjXsSivRiXN7SmRkE=",
        version = "v0.2.0",
    )
    go_repository(
        name = "io_rsc_quote_v3",
        importpath = "rsc.io/quote/v3",
        sum = "h1:9JKUTTIUgS6kzR9mK1YuGKv6Nl+DijDNIc0ghT58FaY=",
        version = "v3.1.0",
    )
    go_repository(
        name = "io_rsc_sampler",
        importpath = "rsc.io/sampler",
        sum = "h1:7uVkIFmeBqHfdjD+gZwtXXI+RODJ2Wc4O7MPEh/QiW4=",
        version = "v1.3.0",
    )
    go_repository(
        name = "land_oras_oras_go_v2",
        importpath = "oras.land/oras-go/v2",
        sum = "h1:o8Me9kLY74Vp5uw07QXPiitjsw7qNXi8Twd+19Zf02c=",
        version = "v2.5.0",
    )
    go_repository(
        name = "net_starlark_go",
        importpath = "go.starlark.net",
        sum = "h1:xwwDQW5We85NaTk2APgoN9202w/l0DVGp+GZMfsrh7s=",
        version = "v0.0.0-20210223155950-e043a3d3c984",
    )
    go_repository(
        name = "org_bazil_fuse",
        importpath = "bazil.org/fuse",
        sum = "h1:SRsZGA7aFnCZETmov57jwPrWuTmaZK6+4R4v5FUe1/c=",
        version = "v0.0.0-20200407214033-5883e5a4b512",
    )
    go_repository(
        name = "org_bitbucket_creachadair_shell",
        importpath = "bitbucket.org/creachadair/shell",
        sum = "h1:Z96pB6DkSb7F3Y3BBnJeOZH2gazyMTWlvecSD4vDqfk=",
        version = "v0.0.7",
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
        sum = "h1:k/RafYqebaIJBO3+SMnfEGtFVlvp5vSgqTUF54UN/zg=",
        version = "v0.196.0",
    )
    go_repository(
        name = "org_golang_google_appengine",
        importpath = "google.golang.org/appengine",
        sum = "h1:IhEN5q69dyKagZPYMSdIjS2HqprW324FRQZJcGqPAsM=",
        version = "v1.6.8",
    )
    go_repository(
        name = "org_golang_google_cloud",
        importpath = "google.golang.org/cloud",
        sum = "h1:Cpp2P6TPjujNoC5M2KHY6g7wfyLYfIWRZaSdIKfDasA=",
        version = "v0.0.0-20151119220103-975617b05ea8",
    )
    go_repository(
        name = "org_golang_google_genproto",
        importpath = "google.golang.org/genproto",
        sum = "h1:BulPr26Jqjnd4eYDVe+YvyR7Yc2vJGkO5/0UxD0/jZU=",
        version = "v0.0.0-20240903143218-8af14fe29dc1",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        importpath = "google.golang.org/genproto/googleapis/api",
        sum = "h1:gap6+3Gk41EItBuyi4XX/bp4oqJ3UwuIMl25yGinuAA=",
        version = "v0.0.0-20250115164207-1a7da9e5054f",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        sum = "h1:W0PHii1rtgc5UgBtJif8xGePValKeZRomnuC5hatKME=",
        version = "v0.0.0-20240903143218-8af14fe29dc1",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sum = "h1:OxYkA3wjPsZyBylwymxSHa7ViiW1Sml4ToBrncvFehI=",
        version = "v0.0.0-20250115164207-1a7da9e5054f",
    )

    # gRPC
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable",
        importpath = "google.golang.org/grpc",
        patch_args = ["-p1"],
        patches = [
            # Remove panic() from serverHandlerTransport.Drain
            # gRPC GracefulStop stops accepting new requests and lets any existing
            # requests finish. For "grpc-over-http" requests, gRPC does not control
            # the connection lifetime so they choose to panic in Drain if there are
            # inflight "grpc-over-http" requests. Since we also shutdown the HTTP
            # server gracefully, it's safe for us to allow gRPC to wait for all
            # ongoing requests to finish.
            "@{}//buildpatches:org_golang_google_grpc_remove_drain_panic.patch".format(workspace_name),
        ],
        sum = "h1:pWFv03aZoHzlRKHWicjsZytKAiYCtNS0dHbXnIdq7jQ=",
        version = "v1.70.0",
    )
    go_repository(
        name = "org_golang_google_grpc_cmd_protoc_gen_go_grpc",
        importpath = "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
        sum = "h1:rNBFJjBCOgVr9pWD7rs/knKL4FRTKgpZmsRfV214zcA=",
        version = "v1.3.0",
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
        sum = "h1:tPhr+woSbjfYvY6/GPufUoYizxw1cF/yFoxJ2fmpwlM=",
        version = "v1.36.5",
    )
    go_repository(
        name = "org_golang_x_crypto",
        importpath = "golang.org/x/crypto",
        sum = "h1:b15kiHdrGCHrP6LvwaQ3c03kgNhhiMgvlhxHQhmg2Xs=",
        version = "v0.35.0",
    )
    go_repository(
        name = "org_golang_x_exp",
        importpath = "golang.org/x/exp",
        sum = "h1:l5+whBCLH3iH2ZNHYLbAe58bo7yrN4mVcnkHDYz5vvs=",
        version = "v0.0.0-20250210185358-939b2ce775ac",
    )
    go_repository(
        name = "org_golang_x_exp_typeparams",
        importpath = "golang.org/x/exp/typeparams",
        sum = "h1:1P7xPZEwZMoBoz0Yze5Nx2/4pxj6nw9ZqHWXqP0iRgQ=",
        version = "v0.0.0-20231108232855-2478ac86f678",
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
        sum = "h1:VLliZ0d+/avPrXXH+OakdXhpJuEoBZuwh1m2j7U6Iug=",
        version = "v0.0.0-20210508222113-6edffad5e616",
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
        sum = "h1:Zb7khfcRGKk+kqfxFaP5tZqCnDZMjC5VtUBs87Hr6QM=",
        version = "v0.23.0",
    )
    go_repository(
        name = "org_golang_x_net",
        importpath = "golang.org/x/net",
        sum = "h1:vWF2fRbw4qslQsQzgFqZff+BItCvGFQqKzKIzx1rmoA=",
        version = "v0.36.0",
    )
    go_repository(
        name = "org_golang_x_oauth2",
        importpath = "golang.org/x/oauth2",
        sum = "h1:KTBBxWqUa0ykRPLtV69rRto9TLXcqYkeswu48x/gvNE=",
        version = "v0.24.0",
    )
    go_repository(
        name = "org_golang_x_perf",
        importpath = "golang.org/x/perf",
        sum = "h1:ObuXPmIgI4ZMyQLIz48cJYgSyWdjUXc2SZAdyJMwEAU=",
        version = "v0.0.0-20230113213139-801c7ef9e5c5",
    )
    go_repository(
        name = "org_golang_x_sync",
        importpath = "golang.org/x/sync",
        sum = "h1:GGz8+XQP4FvTTrjZPzNKTMFtSXH80RAzG+5ghFPgK9w=",
        version = "v0.11.0",
    )
    go_repository(
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        sum = "h1:QjkSwP/36a20jFYWkSue1YwXzLmsV5Gfq7Eiy72C1uc=",
        version = "v0.30.0",
    )
    go_repository(
        name = "org_golang_x_telemetry",
        importpath = "golang.org/x/telemetry",
        sum = "h1:zf5N6UOrA487eEFacMePxjXAJctxKmyjKUsjA11Uzuk=",
        version = "v0.0.0-20240521205824-bda55230c457",
    )
    go_repository(
        name = "org_golang_x_term",
        importpath = "golang.org/x/term",
        sum = "h1:L6pJp37ocefwRRtYPKSWOWzOtWSxVajvz2ldH/xi3iU=",
        version = "v0.29.0",
    )
    go_repository(
        name = "org_golang_x_text",
        importpath = "golang.org/x/text",
        sum = "h1:bofq7m3/HAFvbF51jz3Q9wLg3jkvSPuiZu/pD1XwgtM=",
        version = "v0.22.0",
    )
    go_repository(
        name = "org_golang_x_time",
        importpath = "golang.org/x/time",
        sum = "h1:EsRrnYcQiGH+5FfbgvV4AP7qEZstoyrHB0DzarOQ4ZY=",
        version = "v0.9.0",
    )
    go_repository(
        name = "org_golang_x_tools",
        importpath = "golang.org/x/tools",
        sum = "h1:BgcpHewrV5AUp2G9MebG4XPFI1E2W41zU1SaqVA9vJY=",
        version = "v0.30.0",
    )
    go_repository(
        name = "org_golang_x_tools_go_vcs",
        importpath = "golang.org/x/tools/go/vcs",
        sum = "h1:cOIJqWBl99H1dH5LWizPa+0ImeeJq3t3cJjaeOWUAL4=",
        version = "v0.1.0-deprecated",
    )
    go_repository(
        name = "org_golang_x_xerrors",
        importpath = "golang.org/x/xerrors",
        sum = "h1:LLhsEBxRTBLuKlQxFBYUOU8xyFgXv6cOTp2HASDlsDk=",
        version = "v0.0.0-20240716161551-93cc26a95ae9",
    )
    go_repository(
        name = "org_mongodb_go_mongo_driver",
        importpath = "go.mongodb.org/mongo-driver",
        sum = "h1:P98w8egYRjYe3XDjxhYJagTokP/H6HzlsnojRgZRd80=",
        version = "v1.14.0",
    )
    go_repository(
        name = "org_mozilla_go_pkcs7",
        importpath = "go.mozilla.org/pkcs7",
        sum = "h1:A/5uWzF44DlIgdm/PQFwfMkW0JX+cIcQi/SwLAmZP5M=",
        version = "v0.0.0-20200128120323-432b2356ecb1",
    )
    go_repository(
        name = "org_uber_go_atomic",
        importpath = "go.uber.org/atomic",
        sum = "h1:ZvwS0R+56ePWxUNi+Atn9dWONBPp/AUETXlHW0DxSjE=",
        version = "v1.11.0",
    )
    go_repository(
        name = "org_uber_go_goleak",
        importpath = "go.uber.org/goleak",
        sum = "h1:2K3zAYmnTNqV73imy9J1T3WC+gmCePx2hEGkimedGto=",
        version = "v1.3.0",
    )
    go_repository(
        name = "org_uber_go_mock",
        importpath = "go.uber.org/mock",
        sum = "h1:KAMbZvZPyBPWgD14IrIQ38QCyjwpvVVV6K/bHl1IwQU=",
        version = "v0.5.0",
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
        sum = "h1:aJMhYGrd5QSmlpLMr2MftRKl7t8J8PTZPA732ud/XR8=",
        version = "v1.27.0",
    )
    go_repository(
        name = "tools_gotest",
        importpath = "gotest.tools",
        sum = "h1:VsBPFP1AI068pPrMxtb/S8Zkgf9xEmTLJjfM+P5UIEo=",
        version = "v2.2.0+incompatible",
    )
    go_repository(
        name = "tools_gotest_gotestsum",
        importpath = "gotest.tools/gotestsum",
        sum = "h1:szU3TaSz8wMx/uG+w/A2+4JUPwH903YYaMI9yOOYAyI=",
        version = "v1.8.2",
    )
    go_repository(
        name = "tools_gotest_v3",
        importpath = "gotest.tools/v3",
        sum = "h1:MfDY1b1/0xN1CyMlQDac0ziEy9zJQd9CXBRRDHw2jJo=",
        version = "v3.3.0",
    )
    go_repository(
        name = "xyz_gomodules_jsonpatch_v2",
        importpath = "gomodules.xyz/jsonpatch/v2",
        sum = "h1:Ci3iUJyx9UeRx7CeFN8ARgGbkESwJK+KB9lLcWxY/Zw=",
        version = "v2.4.0",
    )

# Manually created
def install_static_dependencies(workspace_name = "buildbuddy"):
    http_archive(
        name = "com_github_buildbuddy_io_protoc_gen_protobufjs",
        sha256 = "81ce501fdcc10a08c84b8c9d9d1900ca35b62f581deaba8168bed2b96e7696b0",
        urls = ["https://github.com/buildbuddy-io/protoc-gen-protobufjs/releases/download/v0.0.12/protoc-gen-protobufjs-v0.0.12.tar.gz"],
    )

    # This is firecracker v1.8 with https://github.com/firecracker-microvm/firecracker/commit/9536eaa6247c844aea6194ef5b7e1111431074d9
    # cherry-picked, which is required to use the balloon.
    http_archive(
        name = "com_github_firecracker_microvm_firecracker",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "firecracker", srcs = ["firecracker-{release}"])',
            'filegroup(name = "jailer", srcs = ["jailer-{release}"])',
        ]).format(release = "v1.8.0-with_uffd_balloon_patch-20250206-9a75dc3b53e49b04289d91c1f16be4af95a7be7b"),
        sha256 = "2507011c1bd922c593dc58122fa603e0781fdcb99fd72b6ce60946c83c7e4923",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/firecracker/firecracker-v1.8.0-with_uffd_balloon_patch-20250206-9a75dc3b53e49b04289d91c1f16be4af95a7be7b.tgz"],
    )

    http_archive(
        name = "com_github_firecracker_microvm_firecracker_arm64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "firecracker", srcs = ["firecracker-{release}"])',
            'filegroup(name = "jailer", srcs = ["jailer-{release}"])',
        ]).format(release = "v1.8.0-with_uffd_balloon_patch-20250206-9a75dc3b53e49b04289d91c1f16be4af95a7be7b"),
        sha256 = "dfab786e53e48f9399c91db86b41641ecd946dbc0e69a1024158680c6cdf5c70",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/firecracker/firecracker-arm64-v1.8.0-with_uffd_balloon_patch-20250206-9a75dc3b53e49b04289d91c1f16be4af95a7be7b.tgz"],
    )

    http_archive(
        name = "com_github_containerd_stargz_snapshotter-v0.11.4-linux-amd64",
        build_file_content = 'exports_files(["stargz-store"])',
        sha256 = "56933aa04a64d3bf6991f9b1be127ac9896fe597d2fba194c67c2dd4368bbae3",
        urls = ["https://github.com/containerd/stargz-snapshotter/releases/download/v0.11.4/stargz-snapshotter-v0.11.4-linux-amd64.tar.gz"],
    )

    http_archive(
        name = "com_github_buildbuddy_io_soci_snapshotter-static-linux-amd64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "soci-store.bin", srcs = ["soci-store"])',
            'filegroup(name = "soci-store-race.bin", srcs = ["soci-store-race"])',
        ]),
        urls = ["https://github.com/buildbuddy-io/soci-snapshotter/releases/download/v0.9.0-buildbuddy/soci-snapshotter-0.9.0-buildbuddy-linux-amd64-static.tar.gz"],
        sha256 = "5d69374b72776955e123485f0a5ab048f113c39d0d2192d04028d1ce18553b8b",
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
        name = "com_github_bazelbuild_bazelisk-bazelisk-darwin-amd64",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-darwin-amd64"],
        sha256 = "0af019eeb642fa70744419d02aa32df55e6e7a084105d49fb26801a660aa56d3",
        executable = True,
    )
    http_file(
        name = "com_github_bazelbuild_bazelisk-bazelisk-darwin-arm64",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-darwin-arm64"],
        sha256 = "b13dd89c6ecd90944ca3539f5a2c715a18f69b7458878c471a902a8e482ceb4b",
        executable = True,
    )
    http_file(
        name = "com_github_bazelbuild_bazelisk-bazelisk-linux-amd64",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-linux-amd64"],
        sha256 = "fd8fdff418a1758887520fa42da7e6ae39aefc788cf5e7f7bb8db6934d279fc4",
        executable = True,
    )
    http_file(
        name = "com_github_bazelbuild_bazelisk-bazelisk-linux-arm64",
        urls = ["https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-linux-arm64"],
        sha256 = "4c8d966e40ac2c4efcc7f1a5a5cceef2c0a2f16b957e791fa7a867cce31e8fcb",
        executable = True,
    )
    http_file(
        name = "org_kernel_git_linux_kernel-vmlinux",
        sha256 = "3fd19c602f2b11969ad563d4d4855c9147cf13c34238537c1e434097a11aa6b7",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-v5.15-3fd19c602f2b11969ad563d4d4855c9147cf13c34238537c1e434097a11aa6b7"],
        executable = True,
    )
    http_file(
        name = "org_kernel_git_linux_kernel-vmlinux-arm64",
        sha256 = "43dd6dff759ab7b8abfb5a9d34b4ef1ea97de735141436cc77db1a90359991c3",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-aarch64-v5.10-43dd6dff759ab7b8abfb5a9d34b4ef1ea97de735141436cc77db1a90359991c3"],
        executable = True,
    )

    http_file(
        name = "org_llvm_llvm_clang-format_linux-x86_64",
        executable = True,
        integrity = "sha256-BQxgAlbiJeq+lgjSj0kv6Gc8bn9d6sWcbalzIjx2TWw=",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/clang-format/clang-format-15_linux-x86_64"],
    )

    http_file(
        name = "org_llvm_llvm_clang-format_macos-x86_64",
        executable = True,
        integrity = "sha256-lxFvZNl/socLSqKXWLuo+w/n87HtikvBL6qSfs/ewZY=",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/clang-format/clang-format-15_darwin-x86_64"],
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

    # NOTE: crun 1.16.1 has a double-free bug. Before upgrading, be sure the
    # release includes a fix for https://github.com/containers/crun/issues/1537
    http_file(
        name = "com_github_containers_crun_crun-linux-amd64",
        urls = ["https://github.com/containers/crun/releases/download/1.15/crun-1.15-linux-amd64-disable-systemd"],
        sha256 = "03fd3ec6a7799183eaeefba5ebd3f66f9b5fb41a5b080c196285879631ff5dc1",
        downloaded_file_path = "crun",
        executable = True,
    )
    http_file(
        name = "com_github_containers_crun_crun-linux-arm64",
        urls = ["https://github.com/containers/crun/releases/download/1.15/crun-1.15-linux-arm64-disable-systemd"],
        sha256 = "1bd840c95e9ae8edc25654dcf2481309724b9ff18ce95dbcd2535da9b026a47d",
        downloaded_file_path = "crun",
        executable = True,
    )

    # busybox static builds (see tools/build_busybox.sh)
    http_file(
        name = "net_busybox_busybox-linux-amd64",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/busybox/busybox-1.36.1_linux-amd64"],
        sha256 = "f1c20582a7efb70382d63ec71fbeb6f422d7338592ff28ffe0bdff55c839a94c",
        downloaded_file_path = "busybox",
        executable = True,
    )
    http_file(
        name = "net_busybox_busybox-linux-arm64",
        urls = ["https://storage.googleapis.com/buildbuddy-tools/binaries/busybox/busybox-1.36.1_linux-arm64"],
        sha256 = "19601547c937e9636f64ae2dbd7d2d318dc5bbd58f7d5624bfa452f22cfe33d1",
        downloaded_file_path = "busybox",
        executable = True,
    )

    http_file(
        name = "com_github_krallin_tini_tini-linux-amd64",
        urls = ["https://github.com/krallin/tini/releases/download/v0.19.0/tini-amd64"],
        sha256 = "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c",
        downloaded_file_path = "tini",
        executable = True,
    )
    http_file(
        name = "com_github_krallin_tini_tini-linux-arm64",
        urls = ["https://github.com/krallin/tini/releases/download/v0.19.0/tini-arm64"],
        sha256 = "07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81",
        downloaded_file_path = "tini",
        executable = True,
    )

    http_archive(
        name = "com_github_googlecloudplatform_docker-credential-gcr-linux-amd64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "docker-credential-gcr.bin", srcs = ["docker-credential-gcr"])',
        ]),
        urls = ["https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.22/docker-credential-gcr_linux_amd64-2.1.22.tar.gz"],
        sha256 = "443e897dc383d69e55e6dbcb13802f4ec88444848612e83f0381df2ddd721694",
    )
    http_archive(
        name = "com_github_googlecloudplatform_docker-credential-gcr-linux-arm64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "docker-credential-gcr.bin", srcs = ["docker-credential-gcr"])',
        ]),
        urls = ["https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.22/docker-credential-gcr_linux_arm64-2.1.22.tar.gz"],
        sha256 = "b607dfb7516dc1ca6a2a05322d938cea58cc5975e2063efc3695ee6ddb2dccc7",
    )

    http_archive(
        name = "com_github_rootless_containers_rootlesskit-linux-amd64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            # 'filegroup(name = "rootlessctl.bin", srcs = ["rootlessctl"])',
            'filegroup(name = "rootlesskit.bin", srcs = ["rootlesskit"])',
        ]),
        urls = ["https://github.com/rootless-containers/rootlesskit/releases/download/v2.3.2/rootlesskit-x86_64.tar.gz"],
        sha256 = "5d402d7995f1e2c369240de3c6f8eb4cc2a3d1f0f4877ac5362044b2e83962e9",
    )
    http_archive(
        name = "com_github_rootless_containers_rootlesskit-linux-arm64",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            # 'filegroup(name = "rootlessctl.bin", srcs = ["rootlessctl"])',
            'filegroup(name = "rootlesskit.bin", srcs = ["rootlesskit"])',
        ]),
        urls = ["https://github.com/rootless-containers/rootlesskit/releases/download/v2.3.2/rootlesskit-aarch64.tar.gz"],
        sha256 = "0a4ed18c6794bfe5821cc6548f52b26b3b2296170f05df194c2073545200d968",
    )

    http_archive(
        name = "com_github_containerd_containerd-linux-amd64",
        strip_prefix = "bin",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "containerd.bin", srcs = ["containerd"])',
            'filegroup(name = "containerd-shim-runc-v2.bin", srcs = ["containerd-shim-runc-v2"])',
            'filegroup(name = "ctr.bin", srcs = ["ctr"])',
        ]),
        urls = ["https://github.com/containerd/containerd/releases/download/v2.0.2/containerd-2.0.2-linux-amd64.tar.gz"],
        sha256 = "9bd5b6a1bdf505d520d9a329c520258ed0a17faa9fe3db12712ee858ad59aae3",
    )
    http_archive(
        name = "com_github_containerd_containerd-linux-arm64",
        strip_prefix = "bin",
        build_file_content = "\n".join([
            'package(default_visibility = ["//visibility:public"])',
            'filegroup(name = "containerd.bin", srcs = ["containerd"])',
            'filegroup(name = "containerd-shim-runc-v2.bin", srcs = ["containerd-shim-runc-v2"])',
            'filegroup(name = "ctr.bin", srcs = ["ctr"])',
        ]),
        urls = ["https://github.com/containerd/containerd/releases/download/v2.0.2/containerd-2.0.2-linux-arm64.tar.gz"],
        sha256 = "14a2a9f7f75f73e5bcfb8b183d0b84830c54b98ef8c5f6ed70e51f1a230c673e",
    )

    http_file(
        name = "com_github_opencontainers_runc_runc-linux-amd64",
        urls = ["https://github.com/opencontainers/runc/releases/download/v1.2.5/runc.amd64"],
        sha256 = "fbd851fce6a8e0d67a9d184ea544c2abf67c9fd29b80fcc1adf67dfe9eb036a1",
        downloaded_file_path = "runc",
        executable = True,
    )
    http_file(
        name = "com_github_opencontainers_runc_runc-linux-arm64",
        urls = ["https://github.com/opencontainers/runc/releases/download/v1.2.5/runc.arm64"],
        sha256 = "bfc6575f4c601740539553b639ad6f635c23f76695ed484171bd864df6a23f76",
        downloaded_file_path = "runc",
        executable = True,
    )

install_static_dependencies_ext = modules.as_extension(install_static_dependencies)
