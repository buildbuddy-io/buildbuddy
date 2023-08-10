Result

```
goos: linux
goarch: amd64
cpu: Intel(R) Xeon(R) CPU @ 3.10GHz

+----------------------------------------+--------+-------------------------+-------------------------+-------------------------+------------------------+
|                                        | Go     | V1 (default)            | V2                      | V3                      | V4                     |
+----------------------------------------+--------+-------------------------+-------------------------+-------------------------+------------------------+
|BenchmarkDigestCompute/SHA256/1-30      | 1.20.6 | 179136      5882 ns/op  | 124694      9934 ns/op  | 192846      5993 ns/op  | 189498      5734 ns/op |
|BenchmarkDigestCompute/BLAKE3/1-30      | 1.20.6 | 113416      9730 ns/op  |  80197     15101 ns/op  | 110010      9602 ns/op  | 111120      9801 ns/op |
|BenchmarkDigestCompute/SHA256/10-30     | 1.20.6 | 211686      5667 ns/op  | 172010      6050 ns/op  | 197144      5842 ns/op  | 203642      5938 ns/op |
|BenchmarkDigestCompute/BLAKE3/10-30     | 1.20.6 | 108561      9803 ns/op  | 100317     10686 ns/op  | 111651      9737 ns/op  |  95474     10881 ns/op |
|BenchmarkDigestCompute/SHA256/100-30    | 1.20.6 | 204547      5887 ns/op  | 202862      5866 ns/op  | 198572      6088 ns/op  | 175352      6641 ns/op |
|BenchmarkDigestCompute/BLAKE3/100-30    | 1.20.6 | 104535     10291 ns/op  | 104095     10765 ns/op  | 107217      9921 ns/op  | 116206     10578 ns/op |
|BenchmarkDigestCompute/SHA256/1000-30   | 1.20.6 | 150646      8687 ns/op  | 149869      8309 ns/op  | 149089      8132 ns/op  | 145069      8410 ns/op |
|BenchmarkDigestCompute/BLAKE3/1000-30   | 1.20.6 |  65079     17070 ns/op  |  83692     14930 ns/op  |  81699     13492 ns/op  |  79120     13649 ns/op |
|BenchmarkDigestCompute/SHA256/10000-30  | 1.20.6 |  36711     31821 ns/op  |  33878     31934 ns/op  |  40624     29399 ns/op  |  34269     29485 ns/op |
|BenchmarkDigestCompute/BLAKE3/10000-30  | 1.20.6 |  41028     28839 ns/op  |  34753     34579 ns/op  |  39136     29736 ns/op  |  38966     30680 ns/op |
|BenchmarkDigestCompute/SHA256/100000-30 | 1.20.6 |   4870    246121 ns/op  |   4024    254265 ns/op  |   4930    241585 ns/op  |   4788    241926 ns/op |
|BenchmarkDigestCompute/BLAKE3/100000-30 | 1.20.6 |   7570    157361 ns/op  |   7762    159516 ns/op  |   7950    152194 ns/op  |   7906    160134 ns/op |
+----------------------------------------+--------+-------------------------+-------------------------+-------------------------+------------------------+
|BenchmarkDigestCompute/SHA256/1-30      | 1.21.0 | 194114      6180 ns/op  | 193610      6618 ns/op  | 174316      6741 ns/op  | 179653      6438 ns/op |
|BenchmarkDigestCompute/BLAKE3/1-30      | 1.21.0 | 109233     10516 ns/op  | 107288     10925 ns/op  | 101058     11138 ns/op  |  97424     10827 ns/op |
|BenchmarkDigestCompute/SHA256/10-30     | 1.21.0 | 199699      6447 ns/op  | 197600      6196 ns/op  | 112714      9932 ns/op  | 180054      6699 ns/op |
|BenchmarkDigestCompute/BLAKE3/10-30     | 1.21.0 | 120045     10299 ns/op  | 115908     11777 ns/op  |  90782     11209 ns/op  |  98275     11197 ns/op |
|BenchmarkDigestCompute/SHA256/100-30    | 1.21.0 | 199070      6731 ns/op  | 178394      6316 ns/op  | 185082      6663 ns/op  | 183381      6688 ns/op |
|BenchmarkDigestCompute/BLAKE3/100-30    | 1.21.0 | 118980     10133 ns/op  |  96954     10666 ns/op  | 103338     11811 ns/op  |  96330     11648 ns/op |
|BenchmarkDigestCompute/SHA256/1000-30   | 1.21.0 | 148444      8268 ns/op  | 147850      8226 ns/op  | 140574      8349 ns/op  | 136345      8872 ns/op |
|BenchmarkDigestCompute/BLAKE3/1000-30   | 1.21.0 |  86718     12829 ns/op  |  91364     14130 ns/op  |  80214     14230 ns/op  |  74690     15546 ns/op |
|BenchmarkDigestCompute/SHA256/10000-30  | 1.21.0 |  40066     29888 ns/op  |  35396     31246 ns/op  |  38924     31039 ns/op  |  40167     30380 ns/op |
|BenchmarkDigestCompute/BLAKE3/10000-30  | 1.21.0 |  40238     29330 ns/op  |  37699     31413 ns/op  |  37322     32380 ns/op  |  35989     31762 ns/op |
|BenchmarkDigestCompute/SHA256/100000-30 | 1.21.0 |   4944    253678 ns/op  |   4918    241070 ns/op  |   4952    240906 ns/op  |   4131    243320 ns/op |
|BenchmarkDigestCompute/BLAKE3/100000-30 | 1.21.0 |   7582    164121 ns/op  |   7528    170787 ns/op  |   7676    157865 ns/op  |   7582    161672 ns/op |
+----------------------------------------+--------+-------------------------+-------------------------+-------------------------+------------------------+
```

Run with

```bash
for v in "v1" "v2" "v3" "v4"; do
    bazel test --config=x \
               --test_output=all \
               --platforms="//:amd64_$v" \
               --host_platform="//:amd64_$v" \
               "server/remote_cache/digest:digest_bench_test_$v"
done
```
