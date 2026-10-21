# Vector-store validation and benchmarking

The vector-store migration is covered by correctness tests and an authentic
legacy-writer fixture. `tests/legacy_vector_import.rs` checks exact vector
bytes, metadata, namespaces, timestamps, import retry behavior, and search
results against vectors produced by the public writer from TinyCortex v0.1.3.
That small fixture validates compatibility; it is not a performance dataset.

There is no populated TinyCortex generic vector-store database available in
the checked workspace, so this change reports no production vector count,
latency, or throughput. The existing `benchmarks/effectiveness` harness
measures lexical retrieval quality over its labeled corpus; it does not measure
the generic vector store.

## Synthetic scan sample

The ignored unit benchmark
`synthetic_scan_benchmark_reports_exact_recall_and_latency` is a reproducible
smoke measurement, not a production workload. It writes 1,000 generated
records with 384-dimensional vectors to the in-memory TSD testkit backend,
then runs 100 top-1 searches using exact vectors from those records. Metadata
is a small JSON dataset label. On Linux x86_64 with 28 logical CPUs, one run
reported exact recall@1 of **100/100**, median latency **120.581 ms**, and
p95 latency **182.705 ms**. This measures the in-memory testkit and current
search implementation together; it does not model disk or server-backed
latency. Run it with:

```bash
cargo test -p tinycortex --lib \
  synthetic_scan_benchmark_reports_exact_recall_and_latency -- --ignored --nocapture
```

The sample is synthetic and should not be compared with the authentic fixture's
compatibility results or treated as a measurement of a populated user store.
