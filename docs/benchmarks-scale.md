# Scale benchmarks

Measured by `benchmarks/scale.py` (manual; `.github/workflows/Benchmarks.yml` runs it on a hosted Linux
runner) and rendered by `benchmarks/scale_report.py`. Hosted runners are noisy: compare runs of the
same scale, not absolute numbers.

- **baseline**: commit `80281620d0` (with uncommitted changes), macOS-15.7.7-x86_64-i386-64bit-Mach-O, 8 CPUs, 0.5 GiB (4473924 rows), measured 2026-10-18T16:20:29Z.
- **after**: commit `80281620d0` (with uncommitted changes), macOS-15.7.7-x86_64-i386-64bit-Mach-O, 8 CPUs, 0.5 GiB (4473924 rows), measured 2026-10-19T08:38:08Z.

### Single-row commits, databases attached in one process

| Mode | Databases | Run | p50 ms | p99 ms | Commits/s |
| --- | --- | --- | --- | --- | --- |
| native | 1 | baseline | 0.53 | 1.04 | 1683 |
| native | 1 | after | 0.48 | 1.56 | 1751 |
| native | 4 | baseline | 0.79 | 1.14 | 4925 |
| native | 4 | after | 0.62 | 0.85 | 6264 |
| native | 16 | baseline | 1.65 | 4.53 | 8273 |
| native | 16 | after | 1.20 | 7.45 | 9429 |
| native | 4 + loader | baseline | 0.78 | 1.09 | 4880 |
| native | 4 + loader | after | 0.80 | 1.22 | 4811 |
| differential | 1 | baseline | 23.89 | 43.50 | 42 |
| differential | 1 | after | 0.93 | 1.89 | 1020 |
| differential | 4 | baseline | 191.02 | 263.20 | 24 |
| differential | 4 | after | 1.11 | 2.91 | 3195 |
| differential | 16 | baseline | 862.11 | 1084.86 | 19 |
| differential | 16 | after | 3.80 | 6.41 | 4116 |
| differential | 4 + loader | baseline | 87.73 | 1901.73 | 31 |
| differential | 4 + loader | after | 1.26 | 2.92 | 2756 |

### Bulk load, checkpoint and cold scan

| Mode | Run | Load ms | Checkpoint ms | Stored bytes | Cold scan ms | Scan GiB/s |
| --- | --- | --- | --- | --- | --- | --- |
| native | baseline | 5383 | 4 | 153,628,672 | 90 | 1.58 |
| native | after | 4716 | 3 | 153,890,816 | 101 | 1.42 |
| differential | baseline | 9621 | 824 | 247,500,456 | 316 | 0.46 |
| differential | after | 5458 | 15 | 156,481,160 | 121 | 1.19 |
| differential_external_pool | baseline | 11316 | 194 | 157,216,696 | 289 | 0.50 |
| differential_external_pool | after | 7070 | 22 | 161,754,146 | 214 | 0.69 |

### S3 export (MinIO, disk-backed)

| Settings | Step | Run | ms | PUTs | GETs | PUT bytes | Verified GET bytes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| checksum+disk | initial | baseline | 4,800 | 24 | 0 | 158,042,859 | 0 |
| checksum+disk | initial | after | 2,655 | 24 | 0 | 154,176,235 | 0 |
| checksum+disk | incremental | baseline | 1,057 | 5 | 1 | 5,409,549 | 239 |
| checksum+disk | incremental | after | 512 | 5 | 1 | 5,802,765 | 239 |
| checksum+memory | initial | baseline | 4,796 | 24 | 0 | 158,042,859 | 0 |
| checksum+memory | initial | after | 2,229 | 24 | 0 | 154,176,235 | 0 |
| checksum+memory | incremental | baseline | 1,421 | 5 | 1 | 5,409,549 | 239 |
| checksum+memory | incremental | after | 334 | 5 | 1 | 5,802,765 | 239 |
| read_back+disk | initial | baseline | 6,329 | 24 | 23 | 158,042,859 | 158,042,620 |
| read_back+disk | initial | after | 6,525 | 24 | 23 | 154,176,235 | 154,175,996 |
| read_back+disk | incremental | baseline | 1,381 | 5 | 5 | 5,409,549 | 5,409,549 |
| read_back+disk | incremental | after | 904 | 6 | 6 | 9,210,733 | 9,210,733 |
| read_back+memory | initial | baseline | 6,187 | 24 | 23 | 159,681,259 | 159,681,020 |
| read_back+memory | initial | after | 5,546 | 24 | 23 | 154,176,235 | 154,175,996 |
| read_back+memory | incremental | baseline | 1,235 | 5 | 5 | 5,475,085 | 5,475,085 |
| read_back+memory | incremental | after | 482 | 5 | 5 | 6,064,909 | 6,064,909 |

### Remote garbage collection after an expiry (three consecutive runs)

| Run | GC run | ms | Deleted | GETs | LISTs | GET bytes |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 1 | 324 | 0 | 2 | 4 | 456 |
| baseline | 2 | 269 | 0 | 1 | 4 | 239 |
| baseline | 3 | 194 | 0 | 1 | 4 | 239 |
| after | 1 | 66 | 0 | 2 | 4 | 456 |
| after | 2 | 40 | 0 | 1 | 4 | 239 |
| after | 3 | 46 | 0 | 1 | 4 | 239 |

### Shared history listing (300 snapshots, second host)

| Run | Listing | Snapshots | ms | LISTs | GETs |
| --- | --- | --- | --- | --- | --- |
| baseline | 1 | 300 | 727 | 5 | 600 |
| baseline | 2 | 300 | 232 | 3 | 0 |
| after | 1 | 300 | 660 | 5 | 600 |
| after | 2 | 300 | 176 | 3 | 0 |
