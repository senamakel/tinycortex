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

Any future performance measurement should label generated records as a
**synthetic workload**, state the vector count, dimension, metadata size, query
count, and hardware, and keep those results separate from the authentic
fixture's exact-recall compatibility checks. No synthetic benchmark results
are claimed here.
