# TPC-H benchmarks

Measured by `benchmarks/tpch.py` and rendered by `benchmarks/tpch_report.py` (re-run both to update this
page; see [Reproduce](#reproduce)). TPC-H Q1–Q22, every execution materializing its full result; every
backend's results are hashed and must match native's.

- **baseline**: commit `340bafb869` (with uncommitted changes), DuckDB v1.5.6, macOS-15.7.7-x86_64-i386-64bit-Mach-O, 8 CPUs, 8 GiB RAM, measured 2026-10-20T11:13:49Z; extension test hooks absent.
- **after**: commit `8d62388bb7` (with uncommitted changes), DuckDB v2.0.0-alpha46057, macOS-15.8.1-x86_64-i386-64bit-Mach-O, 8 CPUs, 8 GiB RAM, measured 2026-10-20T20:23:45Z; extension test hooks absent.

Configuration: scale factor 1, `threads = 4`, `memory_limit` default, 5 measured warm runs after 1 warm-up, 3 cold-process runs, 3 write runs; OS page cache not dropped (warm for local files). S3 is a disk-backed MinIO in Docker on the same machine: no network latency, so S3 numbers show the extension's overhead, not AWS latency.

## Summary

Geometric mean over the queries of each backend's median time divided by the reference's (native file; for S3 reads, the native file read through httpfs). Below 1.00 is faster than the reference.

| Suite | Backend | Sum of medians ms (after) | Ratio (after) | Ratio (baseline) | S3 GET+HEAD requests (all queries) |
| --- | --- | --- | --- | --- | --- |
| read_local | Native file | 900 | 1.00× | 1.00× | — |
| read_local | Differential (default store) | 1,057 | 1.15× | 0.94× | — |
| read_local | Differential (local pool) | 1,036 | 1.13× | 1.06× | — |
| read_local | Differential S3 database (local copy) | 1,058 | 1.16× | 1.14× | — |
| read_local_cold | Native file | 1,229 | 1.00× | 1.00× | — |
| read_local_cold | Differential (default store) | 1,388 | 1.15× | 1.20× | — |
| read_local_cold | Differential (local pool) | 2,653 | 2.19× | 1.85× | — |
| read_local_cold | Differential S3 database (local copy) | 1,351 | 1.11× | 1.18× | — |
| read_s3_cold | Native file on S3 (httpfs) | 19,982 | 1.00× | 1.00× | — |
| read_s3_cold | Export from branch head (memory cache) | 21,411 | 1.06× | 1.39× | 3,895 |
| read_s3_cold | Export, warm disk cache | 2,677 | 0.14× | 0.17× | 3 |
| read_s3_cold | Follower (FOLLOW true) | 21,094 | 1.05× | 1.27× | 4,855 |
| read_s3_cold | Recovered branch (recover_branch) | 29,364 | 1.44× | 1.30× | 4,744 |
| read_s3_cold | Evicted snapshot (via export) | 21,990 | 1.11× | 1.16× | 3,887 |
| read_s3_cold | Shared database, joined | 29,233 | 1.46× | 1.28× | 4,723 |

## Significant changes

Queries whose median changed by more than 10% and more than twice the larger standard deviation.

| Suite | Backend | Query | Baseline ms | After ms | Change |
| --- | --- | --- | --- | --- | --- |
| read_local | Native file | Q1 | 62.5 | 41.8 | -33% |
| read_local | Native file | Q3 | 38.6 | 32.1 | -17% |
| read_local | Native file | Q4 | 38.0 | 26.5 | -30% |
| read_local | Native file | Q5 | 48.1 | 35.0 | -27% |
| read_local | Native file | Q6 | 16.8 | 11.5 | -32% |
| read_local | Native file | Q7 | 52.0 | 34.3 | -34% |
| read_local | Native file | Q9 | 140.2 | 110.7 | -21% |
| read_local | Native file | Q10 | 83.1 | 67.3 | -19% |
| read_local | Native file | Q11 | 7.7 | 9.4 | +22% |
| read_local | Native file | Q12 | 34.3 | 30.2 | -12% |
| read_local | Native file | Q13 | 121.3 | 91.6 | -25% |
| read_local | Native file | Q14 | 33.6 | 26.8 | -20% |
| read_local | Native file | Q15 | 25.0 | 17.3 | -31% |
| read_local | Native file | Q16 | 47.8 | 37.7 | -21% |
| read_local | Native file | Q19 | 78.2 | 35.1 | -55% |
| read_local | Native file | Q20 | 42.4 | 30.1 | -29% |
| read_local | Native file | Q21 | 161.5 | 65.0 | -60% |
| read_local_cold | Native file | Q4 | 68.9 | 34.5 | -50% |
| read_local_cold | Native file | Q7 | 69.8 | 49.5 | -29% |
| read_local_cold | Native file | Q13 | 149.2 | 97.1 | -35% |
| read_local_cold | Native file | Q14 | 46.3 | 40.6 | -12% |
| read_local_cold | Native file | Q16 | 41.0 | 50.3 | +23% |
| read_local_cold | Native file | Q19 | 206.2 | 55.4 | -73% |
| read_local_cold | Native file | Q20 | 56.1 | 44.7 | -20% |
| read_local_cold | Native file | Q21 | 139.8 | 82.8 | -41% |
| read_local_cold | Native file | Q22 | 32.7 | 38.8 | +19% |
| read_local | Differential (default store) | Q1 | 60.8 | 44.6 | -27% |
| read_local | Differential (default store) | Q3 | 46.9 | 32.5 | -31% |
| read_local | Differential (default store) | Q4 | 45.0 | 25.8 | -43% |
| read_local | Differential (default store) | Q5 | 41.6 | 36.4 | -12% |
| read_local | Differential (default store) | Q7 | 45.2 | 37.3 | -18% |
| read_local | Differential (default store) | Q11 | 8.3 | 13.7 | +65% |
| read_local | Differential (default store) | Q14 | 34.0 | 30.4 | -11% |
| read_local | Differential (default store) | Q18 | 122.6 | 103.1 | -16% |
| read_local | Differential (default store) | Q19 | 68.5 | 35.4 | -48% |
| read_local | Differential (default store) | Q20 | 40.4 | 35.5 | -12% |
| read_local | Differential (default store) | Q21 | 157.7 | 72.5 | -54% |
| read_local | Differential (default store) | Q22 | 33.0 | 38.3 | +16% |
| read_local_cold | Differential (default store) | Q1 | 89.4 | 61.1 | -32% |
| read_local_cold | Differential (default store) | Q2 | 23.7 | 19.4 | -18% |
| read_local_cold | Differential (default store) | Q3 | 65.2 | 54.9 | -16% |
| read_local_cold | Differential (default store) | Q4 | 51.8 | 42.3 | -18% |
| read_local_cold | Differential (default store) | Q7 | 77.4 | 61.7 | -20% |
| read_local_cold | Differential (default store) | Q12 | 71.8 | 46.0 | -36% |
| read_local_cold | Differential (default store) | Q15 | 77.6 | 38.5 | -50% |
| read_local_cold | Differential (default store) | Q16 | 65.5 | 46.1 | -30% |
| read_local_cold | Differential (default store) | Q18 | 189.0 | 113.7 | -40% |
| read_local_cold | Differential (default store) | Q19 | 119.3 | 61.7 | -48% |
| read_local_cold | Differential (default store) | Q20 | 84.8 | 57.0 | -33% |
| read_local_cold | Differential (default store) | Q21 | 172.9 | 86.9 | -50% |
| read_local_cold | Differential (default store) | Q22 | 34.2 | 41.5 | +21% |
| read_local | Differential (local pool) | Q2 | 17.6 | 13.8 | -22% |
| read_local | Differential (local pool) | Q3 | 45.2 | 34.0 | -25% |
| read_local | Differential (local pool) | Q4 | 43.7 | 26.7 | -39% |
| read_local | Differential (local pool) | Q5 | 44.9 | 37.4 | -17% |
| read_local | Differential (local pool) | Q7 | 46.5 | 40.8 | -12% |
| read_local | Differential (local pool) | Q9 | 162.7 | 124.3 | -24% |
| read_local | Differential (local pool) | Q11 | 8.4 | 10.6 | +26% |
| read_local | Differential (local pool) | Q18 | 151.4 | 110.0 | -27% |
| read_local | Differential (local pool) | Q19 | 95.6 | 35.4 | -63% |
| read_local | Differential (local pool) | Q20 | 55.4 | 37.3 | -33% |
| read_local | Differential (local pool) | Q21 | 219.0 | 77.7 | -65% |
| read_local_cold | Differential (local pool) | Q1 | 186.1 | 125.0 | -33% |
| read_local_cold | Differential (local pool) | Q4 | 119.7 | 88.5 | -26% |
| read_local_cold | Differential (local pool) | Q8 | 178.6 | 154.1 | -14% |
| read_local_cold | Differential (local pool) | Q9 | 308.8 | 258.9 | -16% |
| read_local_cold | Differential (local pool) | Q21 | 202.2 | 143.3 | -29% |
| read_local_cold | Differential (local pool) | Q22 | 43.6 | 51.0 | +17% |
| read_s3_cold | Native file on S3 (httpfs) | Q4 | 883.5 | 691.3 | -22% |
| read_s3_cold | Native file on S3 (httpfs) | Q10 | 1471.6 | 1248.2 | -15% |
| read_s3_cold | Native file on S3 (httpfs) | Q12 | 979.1 | 836.0 | -15% |
| read_s3_cold | Native file on S3 (httpfs) | Q13 | 811.7 | 538.1 | -34% |
| read_s3_cold | Native file on S3 (httpfs) | Q15 | 1051.4 | 866.6 | -18% |
| read_s3_cold | Native file on S3 (httpfs) | Q17 | 1075.1 | 859.2 | -20% |
| read_s3_cold | Native file on S3 (httpfs) | Q18 | 1009.8 | 792.2 | -22% |
| read_s3_cold | Native file on S3 (httpfs) | Q20 | 1726.7 | 1064.2 | -38% |
| read_s3_cold | Native file on S3 (httpfs) | Q21 | 1089.2 | 914.2 | -16% |
| read_local | Differential S3 database (local copy) | Q1 | 81.7 | 47.0 | -42% |
| read_local | Differential S3 database (local copy) | Q2 | 20.6 | 13.7 | -34% |
| read_local | Differential S3 database (local copy) | Q4 | 42.2 | 26.2 | -38% |
| read_local | Differential S3 database (local copy) | Q7 | 51.5 | 40.3 | -22% |
| read_local | Differential S3 database (local copy) | Q8 | 39.8 | 35.4 | -11% |
| read_local | Differential S3 database (local copy) | Q9 | 159.9 | 124.7 | -22% |
| read_local | Differential S3 database (local copy) | Q10 | 96.2 | 79.9 | -17% |
| read_local | Differential S3 database (local copy) | Q13 | 134.6 | 117.9 | -12% |
| read_local | Differential S3 database (local copy) | Q15 | 32.8 | 21.8 | -34% |
| read_local | Differential S3 database (local copy) | Q18 | 165.0 | 112.4 | -32% |
| read_local | Differential S3 database (local copy) | Q19 | 101.1 | 36.6 | -64% |
| read_local | Differential S3 database (local copy) | Q20 | 46.2 | 36.9 | -20% |
| read_local | Differential S3 database (local copy) | Q21 | 156.4 | 78.9 | -50% |
| read_local_cold | Differential S3 database (local copy) | Q1 | 166.5 | 61.7 | -63% |
| read_local_cold | Differential S3 database (local copy) | Q4 | 60.8 | 39.6 | -35% |
| read_local_cold | Differential S3 database (local copy) | Q7 | 70.8 | 59.8 | -16% |
| read_local_cold | Differential S3 database (local copy) | Q13 | 182.7 | 117.2 | -36% |
| read_local_cold | Differential S3 database (local copy) | Q14 | 77.0 | 50.5 | -34% |
| read_local_cold | Differential S3 database (local copy) | Q15 | 55.0 | 38.9 | -29% |
| read_local_cold | Differential S3 database (local copy) | Q16 | 56.7 | 44.4 | -22% |
| read_local_cold | Differential S3 database (local copy) | Q17 | 60.8 | 44.5 | -27% |
| read_local_cold | Differential S3 database (local copy) | Q19 | 107.6 | 59.8 | -44% |
| read_local_cold | Differential S3 database (local copy) | Q20 | 66.9 | 55.9 | -16% |
| read_local_cold | Differential S3 database (local copy) | Q21 | 150.0 | 87.0 | -42% |
| read_local_cold | Differential S3 database (local copy) | Q22 | 34.7 | 43.2 | +24% |
| read_s3_cold | Export from branch head (memory cache) | Q5 | 1648.9 | 1245.9 | -24% |
| read_s3_cold | Export from branch head (memory cache) | Q7 | 1846.0 | 1394.4 | -24% |
| read_s3_cold | Export from branch head (memory cache) | Q8 | 2228.7 | 1591.5 | -29% |
| read_s3_cold | Export from branch head (memory cache) | Q9 | 2411.2 | 1848.7 | -23% |
| read_s3_cold | Export from branch head (memory cache) | Q10 | 2202.3 | 1397.1 | -37% |
| read_s3_cold | Export from branch head (memory cache) | Q11 | 219.4 | 150.7 | -31% |
| read_s3_cold | Export from branch head (memory cache) | Q12 | 1429.4 | 891.6 | -38% |
| read_s3_cold | Export from branch head (memory cache) | Q14 | 1449.7 | 1258.1 | -13% |
| read_s3_cold | Export from branch head (memory cache) | Q15 | 1272.8 | 1045.5 | -18% |
| read_s3_cold | Export from branch head (memory cache) | Q16 | 188.9 | 133.2 | -29% |
| read_s3_cold | Export from branch head (memory cache) | Q17 | 1816.1 | 1006.2 | -45% |
| read_s3_cold | Export from branch head (memory cache) | Q18 | 1173.5 | 779.9 | -34% |
| read_s3_cold | Export from branch head (memory cache) | Q19 | 2928.3 | 1505.0 | -49% |
| read_s3_cold | Export from branch head (memory cache) | Q20 | 2980.9 | 1237.5 | -58% |
| read_s3_cold | Export from branch head (memory cache) | Q21 | 2775.4 | 968.2 | -65% |
| read_s3_cold | Export, warm disk cache | Q2 | 38.0 | 30.2 | -20% |
| read_s3_cold | Export, warm disk cache | Q4 | 219.7 | 81.2 | -63% |
| read_s3_cold | Export, warm disk cache | Q7 | 271.0 | 141.4 | -48% |
| read_s3_cold | Export, warm disk cache | Q8 | 229.9 | 158.7 | -31% |
| read_s3_cold | Export, warm disk cache | Q12 | 118.5 | 99.5 | -16% |
| read_s3_cold | Export, warm disk cache | Q18 | 240.7 | 149.5 | -38% |
| read_s3_cold | Export, warm disk cache | Q21 | 204.2 | 148.5 | -27% |
| read_s3_cold | Follower (FOLLOW true) | Q3 | 1975.6 | 1183.7 | -40% |
| read_s3_cold | Follower (FOLLOW true) | Q4 | 1046.5 | 711.1 | -32% |
| read_s3_cold | Follower (FOLLOW true) | Q5 | 2007.1 | 1186.9 | -41% |
| read_s3_cold | Follower (FOLLOW true) | Q6 | 1412.2 | 955.3 | -32% |
| read_s3_cold | Follower (FOLLOW true) | Q9 | 3219.0 | 1882.9 | -42% |
| read_s3_cold | Follower (FOLLOW true) | Q10 | 2248.9 | 1340.5 | -40% |
| read_s3_cold | Follower (FOLLOW true) | Q14 | 1863.7 | 1208.6 | -35% |
| read_s3_cold | Follower (FOLLOW true) | Q18 | 954.6 | 835.3 | -12% |
| read_s3_cold | Follower (FOLLOW true) | Q21 | 1214.0 | 954.5 | -21% |
| read_s3_cold | Recovered branch (recover_branch) | Q2 | 179.8 | 203.9 | +13% |
| read_s3_cold | Recovered branch (recover_branch) | Q11 | 268.8 | 186.4 | -31% |
| read_s3_cold | Recovered branch (recover_branch) | Q16 | 215.3 | 176.2 | -18% |
| read_s3_cold | Evicted snapshot (via export) | Q1 | 1159.2 | 993.1 | -14% |
| read_s3_cold | Evicted snapshot (via export) | Q2 | 165.3 | 212.2 | +28% |
| read_s3_cold | Evicted snapshot (via export) | Q3 | 1502.2 | 1180.5 | -21% |
| read_s3_cold | Evicted snapshot (via export) | Q4 | 919.2 | 720.4 | -22% |
| read_s3_cold | Evicted snapshot (via export) | Q6 | 1618.0 | 992.1 | -39% |
| read_s3_cold | Evicted snapshot (via export) | Q8 | 1896.4 | 1593.5 | -16% |
| read_s3_cold | Evicted snapshot (via export) | Q14 | 1528.1 | 1274.9 | -17% |
| read_s3_cold | Evicted snapshot (via export) | Q18 | 967.8 | 801.1 | -17% |
| read_s3_cold | Evicted snapshot (via export) | Q20 | 1359.3 | 1215.0 | -11% |
| read_s3_cold | Evicted snapshot (via export) | Q21 | 1304.1 | 998.4 | -23% |
| read_s3_cold | Evicted snapshot (via export) | Q22 | 235.9 | 195.9 | -17% |
| read_s3_cold | Shared database, joined | Q5 | 1479.5 | 1656.9 | +12% |
| read_s3_cold | Shared database, joined | Q8 | 1927.4 | 2159.0 | +12% |
| read_s3_cold | Shared database, joined | Q14 | 1499.6 | 1669.5 | +11% |
| read_s3_cold | Shared database, joined | Q16 | 247.4 | 170.0 | -31% |
| read_s3_cold | Shared database, joined | Q20 | 2336.6 | 1643.0 | -30% |
| read_s3_cold | Shared database, joined | Q22 | 216.9 | 263.0 | +21% |

## Local reads, warm (one process: 1 warm-up, then measured runs)

Median ms (± standard deviation); ratio to the reference in parentheses.

| Query | Native file | Differential (default store) | Differential (local pool) | Differential S3 database (local copy) |
| --- | --- | --- | --- | --- |
| Q1 | 41.8 ± 4.6 | 44.6 ± 2.4 (1.07×) | 53.1 ± 6.7 (1.27×) | 47.0 ± 1.3 (1.13×) |
| Q2 | 14.9 ± 2.5 | 13.9 ± 0.3 (0.93×) | 13.8 ± 0.4 (0.92×) | 13.7 ± 0.1 (0.92×) |
| Q3 | 32.1 ± 0.5 | 32.5 ± 1.0 (1.01×) | 34.0 ± 1.9 (1.06×) | 36.6 ± 3.4 (1.14×) |
| Q4 | 26.5 ± 4.2 | 25.8 ± 1.4 (0.97×) | 26.7 ± 1.8 (1.01×) | 26.2 ± 1.6 (0.99×) |
| Q5 | 35.0 ± 2.2 | 36.4 ± 0.7 (1.04×) | 37.4 ± 1.5 (1.07×) | 38.8 ± 1.2 (1.11×) |
| Q6 | 11.5 ± 0.9 | 14.3 ± 1.5 (1.24×) | 13.8 ± 1.9 (1.20×) | 14.7 ± 1.7 (1.29×) |
| Q7 | 34.3 ± 1.2 | 37.3 ± 1.1 (1.09×) | 40.8 ± 2.7 (1.19×) | 40.3 ± 1.3 (1.18×) |
| Q8 | 31.8 ± 2.1 | 39.4 ± 2.5 (1.24×) | 35.0 ± 0.5 (1.10×) | 35.4 ± 0.8 (1.11×) |
| Q9 | 110.7 ± 3.8 | 135.1 ± 28.0 (1.22×) | 124.3 ± 4.3 (1.12×) | 124.7 ± 3.9 (1.13×) |
| Q10 | 67.3 ± 1.6 | 100.1 ± 7.6 (1.49×) | 79.1 ± 3.1 (1.18×) | 79.9 ± 1.9 (1.19×) |
| Q11 | 9.4 ± 0.4 | 13.7 ± 1.3 (1.46×) | 10.6 ± 0.9 (1.13×) | 10.7 ± 0.4 (1.14×) |
| Q12 | 30.2 ± 1.3 | 37.4 ± 3.7 (1.24×) | 33.1 ± 2.3 (1.10×) | 32.4 ± 3.2 (1.07×) |
| Q13 | 91.6 ± 4.7 | 116.8 ± 4.9 (1.28×) | 115.6 ± 3.4 (1.26×) | 117.9 ± 3.1 (1.29×) |
| Q14 | 26.8 ± 1.2 | 30.4 ± 1.2 (1.13×) | 28.0 ± 1.5 (1.04×) | 32.2 ± 0.8 (1.20×) |
| Q15 | 17.3 ± 0.5 | 20.8 ± 0.7 (1.20×) | 19.9 ± 1.6 (1.15×) | 21.8 ± 0.8 (1.26×) |
| Q16 | 37.7 ± 1.5 | 44.3 ± 3.4 (1.18×) | 42.3 ± 5.2 (1.12×) | 47.8 ± 2.0 (1.27×) |
| Q17 | 25.4 ± 1.3 | 29.4 ± 1.5 (1.16×) | 32.0 ± 2.7 (1.26×) | 31.2 ± 1.2 (1.23×) |
| Q18 | 91.1 ± 2.6 | 103.1 ± 2.4 (1.13×) | 110.0 ± 5.1 (1.21×) | 112.4 ± 4.3 (1.23×) |
| Q19 | 35.1 ± 1.5 | 35.4 ± 0.7 (1.01×) | 35.4 ± 2.7 (1.01×) | 36.6 ± 2.3 (1.04×) |
| Q20 | 30.1 ± 0.8 | 35.5 ± 0.8 (1.18×) | 37.3 ± 2.7 (1.24×) | 36.9 ± 0.6 (1.23×) |
| Q21 | 65.0 ± 2.8 | 72.5 ± 2.3 (1.12×) | 77.7 ± 3.4 (1.19×) | 78.9 ± 3.6 (1.21×) |
| Q22 | 34.3 ± 1.0 | 38.3 ± 1.3 (1.12×) | 35.8 ± 0.9 (1.04×) | 41.6 ± 1.4 (1.21×) |

## Local reads, cold process (a new process per run)

Median ms (± standard deviation); ratio to the reference in parentheses.

| Query | Native file | Differential (default store) | Differential (local pool) | Differential S3 database (local copy) |
| --- | --- | --- | --- | --- |
| Q1 | 52.8 ± 1.9 | 61.1 ± 1.5 (1.16×) | 125.0 ± 3.8 (2.37×) | 61.7 ± 1.6 (1.17×) |
| Q2 | 16.7 ± 0.3 | 19.4 ± 0.7 (1.16×) | 28.6 ± 0.9 (1.72×) | 18.5 ± 0.2 (1.11×) |
| Q3 | 44.7 ± 0.5 | 54.9 ± 4.4 (1.23×) | 137.2 ± 43.5 (3.07×) | 53.6 ± 2.1 (1.20×) |
| Q4 | 34.5 ± 1.1 | 42.3 ± 0.4 (1.23×) | 88.5 ± 3.9 (2.57×) | 39.6 ± 1.2 (1.15×) |
| Q5 | 48.6 ± 0.3 | 60.6 ± 1.0 (1.25×) | 130.9 ± 1.9 (2.70×) | 58.3 ± 0.5 (1.20×) |
| Q6 | 21.1 ± 0.3 | 33.5 ± 3.2 (1.59×) | 92.0 ± 3.1 (4.36×) | 29.5 ± 0.2 (1.40×) |
| Q7 | 49.5 ± 1.6 | 61.7 ± 1.3 (1.25×) | 139.3 ± 4.2 (2.81×) | 59.8 ± 1.2 (1.21×) |
| Q8 | 49.7 ± 5.2 | 64.4 ± 1.7 (1.29×) | 154.1 ± 1.3 (3.10×) | 62.5 ± 2.1 (1.26×) |
| Q9 | 134.2 ± 20.2 | 152.1 ± 4.7 (1.13×) | 258.9 ± 3.3 (1.93×) | 145.1 ± 1.8 (1.08×) |
| Q10 | 89.8 ± 1.3 | 109.8 ± 1.9 (1.22×) | 202.9 ± 8.0 (2.26×) | 105.1 ± 2.3 (1.17×) |
| Q11 | 12.7 ± 0.1 | 15.3 ± 0.9 (1.20×) | 22.9 ± 0.6 (1.80×) | 14.7 ± 0.3 (1.15×) |
| Q12 | 42.7 ± 1.4 | 46.0 ± 1.0 (1.08×) | 105.1 ± 2.2 (2.46×) | 47.1 ± 1.1 (1.10×) |
| Q13 | 97.1 ± 0.8 | 123.5 ± 2.0 (1.27×) | 158.3 ± 5.7 (1.63×) | 117.2 ± 4.2 (1.21×) |
| Q14 | 40.6 ± 0.8 | 51.0 ± 2.1 (1.26×) | 121.5 ± 1.4 (2.99×) | 50.5 ± 2.0 (1.25×) |
| Q15 | 32.6 ± 2.5 | 38.5 ± 1.8 (1.18×) | 101.9 ± 1.0 (3.12×) | 38.9 ± 1.6 (1.19×) |
| Q16 | 50.3 ± 2.8 | 46.1 ± 3.1 (0.92×) | 47.0 ± 1.1 (0.93×) | 44.4 ± 1.5 (0.88×) |
| Q17 | 74.5 ± 19.8 | 46.8 ± 1.1 (0.63×) | 109.2 ± 6.2 (1.47×) | 44.5 ± 1.5 (0.60×) |
| Q18 | 115.5 ± 30.1 | 113.7 ± 0.7 (0.98×) | 162.3 ± 5.8 (1.41×) | 113.8 ± 1.7 (0.99×) |
| Q19 | 55.4 ± 2.9 | 61.7 ± 5.7 (1.12×) | 149.2 ± 2.6 (2.70×) | 59.8 ± 3.0 (1.08×) |
| Q20 | 44.7 ± 1.5 | 57.0 ± 1.2 (1.27×) | 123.8 ± 2.6 (2.77×) | 55.9 ± 2.4 (1.25×) |
| Q21 | 82.8 ± 8.7 | 86.9 ± 2.8 (1.05×) | 143.3 ± 0.9 (1.73×) | 87.0 ± 3.2 (1.05×) |
| Q22 | 38.8 ± 0.4 | 41.5 ± 0.5 (1.07×) | 51.0 ± 0.5 (1.31×) | 43.2 ± 2.8 (1.11×) |

## S3 reads, cold process (a new process per run, empty caches unless stated)

Median ms (± standard deviation); ratio to the reference in parentheses.

| Query | Native file on S3 (httpfs) | Export from branch head (memory cache) | Export, warm disk cache | Follower (FOLLOW true) | Recovered branch (recover_branch) | Evicted snapshot (via export) | Shared database, joined |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | 1283.6 ± 119.6 | 1008.0 ± 47.8 (0.79×) | 116.5 ± 6.6 (0.09×) | 1003.2 ± 46.2 (0.78×) | 1415.1 ± 83.9 (1.10×) | 993.1 ± 64.2 (0.77×) | 1401.5 ± 110.5 (1.09×) |
| Q2 | 188.6 ± 20.7 | 165.7 ± 3.3 (0.88×) | 30.2 ± 1.0 (0.16×) | 200.3 ± 19.9 (1.06×) | 203.9 ± 3.9 (1.08×) | 212.2 ± 15.3 (1.13×) | 328.7 ± 88.2 (1.74×) |
| Q3 | 1112.8 ± 149.8 | 1198.1 ± 15.0 (1.08×) | 135.1 ± 13.6 (0.12×) | 1183.7 ± 67.1 (1.06×) | 1561.2 ± 379.0 (1.40×) | 1180.5 ± 44.9 (1.06×) | 1633.5 ± 87.8 (1.47×) |
| Q4 | 691.3 ± 14.9 | 670.9 ± 134.6 (0.97×) | 81.2 ± 7.4 (0.12×) | 711.1 ± 56.0 (1.03×) | 1058.6 ± 47.4 (1.53×) | 720.4 ± 35.2 (1.04×) | 1029.9 ± 26.0 (1.49×) |
| Q5 | 1079.7 ± 42.9 | 1245.9 ± 42.9 (1.15×) | 206.1 ± 50.2 (0.19×) | 1186.9 ± 173.9 (1.10×) | 1570.8 ± 67.6 (1.45×) | 1244.3 ± 47.1 (1.15×) | 1656.9 ± 53.7 (1.53×) |
| Q6 | 864.5 ± 41.8 | 1024.8 ± 75.2 (1.19×) | 90.4 ± 3.3 (0.10×) | 955.3 ± 71.6 (1.11×) | 1366.3 ± 22.4 (1.58×) | 992.1 ± 34.8 (1.15×) | 1336.6 ± 63.6 (1.55×) |
| Q7 | 1249.6 ± 54.7 | 1394.4 ± 59.6 (1.12×) | 141.4 ± 59.1 (0.11×) | 1325.6 ± 32.6 (1.06×) | 1835.4 ± 70.7 (1.47×) | 1379.2 ± 52.8 (1.10×) | 1833.0 ± 40.8 (1.47×) |
| Q8 | 1575.8 ± 101.8 | 1591.5 ± 77.9 (1.01×) | 158.7 ± 3.5 (0.10×) | 1542.0 ± 47.8 (0.98×) | 2197.7 ± 86.7 (1.39×) | 1593.5 ± 30.5 (1.01×) | 2159.0 ± 52.9 (1.37×) |
| Q9 | 1692.4 ± 80.3 | 1848.7 ± 96.2 (1.09×) | 256.9 ± 8.6 (0.15×) | 1882.9 ± 97.9 (1.11×) | 2640.9 ± 133.4 (1.56×) | 1866.6 ± 40.3 (1.10×) | 2491.3 ± 45.2 (1.47×) |
| Q10 | 1248.2 ± 18.6 | 1397.1 ± 9.8 (1.12×) | 177.3 ± 3.5 (0.14×) | 1340.5 ± 21.6 (1.07×) | 2020.5 ± 36.8 (1.62×) | 1360.5 ± 52.2 (1.09×) | 2025.0 ± 59.8 (1.62×) |
| Q11 | 145.1 ± 23.1 | 150.7 ± 12.7 (1.04×) | 24.4 ± 0.8 (0.17×) | 132.2 ± 13.1 (0.91×) | 186.4 ± 8.3 (1.28×) | 150.4 ± 9.7 (1.04×) | 191.6 ± 24.4 (1.32×) |
| Q12 | 836.0 ± 28.3 | 891.6 ± 27.2 (1.07×) | 99.5 ± 8.3 (0.12×) | 914.2 ± 83.4 (1.09×) | 1344.5 ± 57.6 (1.61×) | 925.5 ± 80.2 (1.11×) | 1282.6 ± 39.9 (1.53×) |
| Q13 | 538.1 ± 12.4 | 686.4 ± 49.2 (1.28×) | 157.6 ± 3.5 (0.29×) | 674.8 ± 15.2 (1.25×) | 888.4 ± 13.8 (1.65×) | 676.4 ± 9.0 (1.26×) | 906.1 ± 32.8 (1.68×) |
| Q14 | 1057.9 ± 44.7 | 1258.1 ± 57.4 (1.19×) | 121.4 ± 3.9 (0.11×) | 1208.6 ± 42.2 (1.14×) | 1700.5 ± 48.0 (1.61×) | 1274.9 ± 65.5 (1.21×) | 1669.5 ± 52.9 (1.58×) |
| Q15 | 866.6 ± 31.9 | 1045.5 ± 35.9 (1.21×) | 101.5 ± 36.3 (0.12×) | 1043.2 ± 34.8 (1.20×) | 1500.5 ± 117.0 (1.73×) | 1017.2 ± 44.5 (1.17×) | 1401.2 ± 109.5 (1.62×) |
| Q16 | 141.3 ± 5.7 | 133.2 ± 3.0 (0.94×) | 51.8 ± 1.5 (0.37×) | 130.1 ± 18.9 (0.92×) | 176.2 ± 9.6 (1.25×) | 187.0 ± 23.3 (1.32×) | 170.0 ± 9.0 (1.20×) |
| Q17 | 859.2 ± 20.1 | 1006.2 ± 65.0 (1.17×) | 108.1 ± 1.5 (0.13×) | 983.6 ± 88.5 (1.14×) | 1456.8 ± 62.9 (1.70×) | 1475.2 ± 49.8 (1.72×) | 1514.0 ± 129.6 (1.76×) |
| Q18 | 792.2 ± 28.2 | 779.9 ± 22.1 (0.98×) | 149.5 ± 4.7 (0.19×) | 835.3 ± 8.5 (1.05×) | 1014.8 ± 85.8 (1.28×) | 801.1 ± 30.8 (1.01×) | 1009.6 ± 46.3 (1.27×) |
| Q19 | 1586.8 ± 123.2 | 1505.0 ± 85.4 (0.95×) | 146.0 ± 4.8 (0.09×) | 1480.8 ± 41.9 (0.93×) | 2030.3 ± 71.8 (1.28×) | 1531.0 ± 60.4 (0.96×) | 2016.3 ± 55.3 (1.27×) |
| Q20 | 1064.2 ± 45.0 | 1237.5 ± 37.9 (1.16×) | 122.4 ± 2.9 (0.12×) | 1199.3 ± 36.2 (1.13×) | 1673.9 ± 39.6 (1.57×) | 1215.0 ± 64.5 (1.14×) | 1643.0 ± 56.9 (1.54×) |
| Q21 | 914.2 ± 48.4 | 968.2 ± 76.3 (1.06×) | 148.5 ± 9.4 (0.16×) | 954.5 ± 15.8 (1.04×) | 1270.7 ± 95.4 (1.39×) | 998.4 ± 7.4 (1.09×) | 1270.8 ± 58.0 (1.39×) |
| Q22 | 194.1 ± 12.1 | 203.2 ± 8.5 (1.05×) | 52.6 ± 2.7 (0.27×) | 205.9 ± 6.8 (1.06×) | 250.2 ± 10.7 (1.29×) | 195.9 ± 11.6 (1.01×) | 263.0 ± 23.0 (1.35×) |

S3 requests per query (median of the cold runs): GET / HEAD / LIST, and MiB read through S3.

| Query | Native file on S3 (httpfs) | Export from branch head (memory cache) | Export, warm disk cache | Follower (FOLLOW true) | Recovered branch (recover_branch) | Evicted snapshot (via export) | Shared database, joined |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | 154 / 0 / 0, 66.2 | 0 / 0 / 0, 0.0 | 242 / 0 / 0, 60.5 | 238 / 0 / 0, 64.5 | 155 / 0 / 0, 66.5 | 237 / 0 / 0, 62.0 |
| Q2 | — | 25 / 0 / 0, 8.0 | 0 / 0 / 0, 0.0 | 28 / 0 / 0, 7.0 | 28 / 0 / 0, 8.0 | 24 / 0 / 0, 7.5 | 28 / 0 / 0, 7.5 |
| Q3 | — | 235 / 0 / 0, 74.0 | 2 / 0 / 0, 0.5 | 274 / 0 / 0, 68.5 | 273 / 0 / 0, 72.5 | 235 / 0 / 0, 74.5 | 269 / 0 / 0, 71.5 |
| Q4 | — | 141 / 0 / 0, 45.2 | 0 / 0 / 0, 0.0 | 171 / 0 / 0, 42.8 | 169 / 0 / 0, 50.5 | 141 / 0 / 0, 45.8 | 171 / 0 / 0, 50.5 |
| Q5 | — | 259 / 0 / 0, 73.5 | 0 / 0 / 0, 0.0 | 276 / 0 / 0, 69.0 | 274 / 0 / 0, 75.2 | 259 / 0 / 0, 73.0 | 271 / 0 / 0, 75.2 |
| Q6 | — | 155 / 0 / 0, 66.5 | 0 / 0 / 0, 0.0 | 242 / 0 / 0, 60.5 | 234 / 0 / 0, 66.2 | 155 / 0 / 0, 66.5 | 226 / 0 / 0, 63.8 |
| Q7 | — | 273 / 0 / 0, 84.8 | 0 / 0 / 0, 0.0 | 312 / 0 / 0, 78.0 | 310 / 0 / 0, 85.2 | 272 / 0 / 0, 84.5 | 306 / 0 / 0, 86.0 |
| Q8 | — | 282 / 0 / 0, 98.2 | 0 / 0 / 0, 0.0 | 375 / 0 / 0, 93.8 | 363 / 0 / 0, 102.2 | 282 / 0 / 0, 98.8 | 358 / 0 / 0, 103.5 |
| Q9 | — | 297 / 0 / 0, 111.8 | 1 / 0 / 0, 0.2 | 422 / 0 / 0, 105.5 | 382 / 0 / 0, 115.8 | 295 / 0 / 0, 111.8 | 395 / 0 / 0, 119.0 |
| Q10 | — | 239 / 0 / 0, 81.8 | 0 / 0 / 0, 0.0 | 308 / 0 / 0, 77.0 | 304 / 0 / 0, 88.0 | 241 / 0 / 0, 81.2 | 297 / 0 / 0, 85.8 |
| Q11 | — | 23 / 0 / 0, 7.5 | 0 / 0 / 0, 0.0 | 25 / 0 / 0, 6.2 | 25 / 0 / 0, 7.5 | 23 / 0 / 0, 7.5 | 26 / 0 / 0, 7.0 |
| Q12 | — | 180 / 0 / 0, 57.5 | 0 / 0 / 0, 0.0 | 219 / 0 / 0, 54.8 | 219 / 0 / 0, 61.8 | 181 / 0 / 0, 57.5 | 218 / 0 / 0, 60.2 |
| Q13 | — | 123 / 0 / 0, 34.0 | 0 / 0 / 0, 0.0 | 133 / 0 / 0, 33.2 | 129 / 0 / 0, 38.8 | 120 / 0 / 0, 34.0 | 129 / 0 / 0, 39.8 |
| Q14 | — | 231 / 0 / 0, 80.8 | 0 / 0 / 0, 0.0 | 292 / 0 / 0, 73.0 | 291 / 0 / 0, 78.5 | 229 / 0 / 0, 81.0 | 287 / 0 / 0, 77.0 |
| Q15 | — | 207 / 0 / 0, 66.8 | 0 / 0 / 0, 0.0 | 246 / 0 / 0, 61.5 | 243 / 0 / 0, 66.5 | 205 / 0 / 0, 66.8 | 241 / 0 / 0, 66.8 |
| Q16 | — | 15 / 0 / 0, 4.2 | 0 / 0 / 0, 0.0 | 17 / 0 / 0, 4.2 | 17 / 0 / 0, 5.2 | 16 / 0 / 0, 4.8 | 18 / 0 / 0, 5.0 |
| Q17 | — | 208 / 0 / 0, 66.2 | 0 / 0 / 0, 0.0 | 244 / 0 / 0, 61.0 | 244 / 0 / 0, 71.2 | 208 / 0 / 0, 66.0 | 244 / 0 / 0, 71.5 |
| Q18 | — | 150 / 0 / 0, 42.0 | 0 / 0 / 0, 0.0 | 163 / 0 / 0, 40.8 | 155 / 0 / 0, 43.5 | 151 / 0 / 0, 41.5 | 152 / 0 / 0, 44.0 |
| Q19 | — | 239 / 0 / 0, 96.0 | 0 / 0 / 0, 0.0 | 350 / 0 / 0, 87.5 | 336 / 0 / 0, 98.0 | 237 / 0 / 0, 96.0 | 340 / 0 / 0, 98.2 |
| Q20 | — | 247 / 0 / 0, 77.2 | 0 / 0 / 0, 0.0 | 273 / 0 / 0, 68.2 | 268 / 0 / 0, 78.0 | 246 / 0 / 0, 76.8 | 269 / 0 / 0, 74.2 |
| Q21 | — | 183 / 0 / 0, 56.0 | 0 / 0 / 0, 0.0 | 212 / 0 / 0, 53.0 | 212 / 0 / 0, 56.8 | 183 / 0 / 0, 56.2 | 211 / 0 / 0, 56.8 |
| Q22 | — | 29 / 0 / 0, 8.8 | 0 / 0 / 0, 0.0 | 31 / 0 / 0, 7.8 | 30 / 0 / 0, 9.0 | 29 / 0 / 0, 8.8 | 30 / 0 / 0, 9.5 |

## Loading (COPY FROM DATABASE of the generated data)

| Backend | Copy ms | Checkpoint ms | Snapshot (export) ms | Detach ms |
| --- | --- | --- | --- | --- |
| Differential (default store) | 4,118 | 41 | — | 28 |
| Differential (local pool) | 6,699 | 60 | — | 28 |
| Differential S3 + snapshot export | 6,189 | 66 | 5,756 | 82 |
| Shared database + snapshot | 6,270 | 61 | 6,497 | 59 |

## Writes: CREATE TABLE lineitem AS … and CHECKPOINT into a new database

Medians over the write runs. Rows per second count the create and the checkpoint; the export (a snapshot uploaded to S3) and the detach (which ships what is left with SHIP_WAL) are listed separately.

| Backend | Create ms | Checkpoint ms | Rows/s | Export ms | Detach ms | Upload ms | S3 PUTs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Native file | 2,893 | 5 | 2,070,491 | — | 21 | — | 0 |
| Differential (default store) | 3,111 | 17 | 1,919,043 | — | 22 | — | 0 |
| Differential (local pool) | 4,775 | 22 | 1,250,897 | — | 20 | — | 0 |
| Differential S3 + snapshot export | 3,183 | 14 | 1,876,944 | 4,010 | 35 | — | 27 |
| Differential S3, SHIP_WAL 'async' | 3,161 | 16 | 1,889,146 | 6,308 | 3,364 | — | 34 |
| Differential S3, SHIP_WAL 'sync' | 14,583 | 424 | 399,892 | 78 | 47 | — | 41 |
| Shared database + snapshot | 4,810 | 20 | 1,242,580 | 5,124 | 61 | — | 31 |
| Native file upload to S3 | — | — | — | — | — | 2,358 | — |

## What each S3 read path reads

- **Native file on S3 (httpfs):** the `.duckdb` file uploaded once, read with DuckDB's httpfs and its external file cache, empty in each new process.
- **Export from branch head:** a fresh repository attaches `heads/<database>/main`; DuckDB's external file cache is the only cache (memory, empty per process).
- **Export, warm disk cache:** the same with `differential_remote_cache_size` set, after one unmeasured process read the query: the measured process reads the repository disk cache.
- **Evicted snapshot:** the writer's own snapshot after `differential_evict_snapshots`, read through its export.
- **Follower:** `FOLLOW true` on the branch head of the shipping database: its chain's base export plus the epochs (in memory).
- **Shared database, joined:** another repository attaches the shared database writable; its branch reads its adopted base snapshot from S3 through the store.
- **Recovered branch:** `differential_recover_branch` imported the base without downloading; reads go to S3 through the store, except chunks the epochs carried (written locally at recovery).

## Compared with the objfs article

The [object storage article](https://dentiny.github.io/posts/duckdb-on-object-storage/) reports TPC-H with the same aggregation (median, ratio to native): local disk 1.08× native at SF10, S3 cold process 1.20× native httpfs at SF1 (over the internet, Taipei to ap-east-2), writing to S3 0.95× of native plus AWS CLI upload, local writes 0.19× native. Their S3 numbers include network latency that a local MinIO does not have, and their machine (Apple M4, 10 threads) differs; compare the ratios, not the times.

## Reproduce

```sh
make release integration   # the release extension, loaded into the integration build's DuckDB
make bench-tpch            # both steps below, with the release extension
DIFFERENTIAL_EXTENSION=build/release/extension/differential/differential.duckdb_extension \
  python3 benchmarks/tpch.py --scale-factor 1 --threads 4
python3 benchmarks/tpch_report.py build/evidence/tpch.json            # this page
python3 benchmarks/tpch_report.py baseline.json build/evidence/tpch.json  # before/after
```

Needs Docker (MinIO) and network access once (DuckDB's `tpch` and `httpfs` extensions, cached in `build/benchmarks/`). SF1 needs about 3 GB of free disk; `--queries`, `--suites`, `--runs` narrow a run.
