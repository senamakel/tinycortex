use super::*;

#[test]
fn chunk_collections_declare_existing_query_indexes() {
    let specs = collections();

    let chunks = specs.iter().find(|spec| spec.name == CHUNKS).unwrap();
    assert_eq!(chunks.indexes.len(), 4);
    assert_eq!(
        chunks.indexes[0].fields,
        ["metadata.source_kind", "metadata.source_id"]
    );

    let state_versions = specs
        .iter()
        .find(|spec| spec.name == STATE_VERSIONS)
        .unwrap();
    assert_eq!(
        state_versions.indexes[0].fields,
        ["collection", "key", "revision"]
    );
    assert_eq!(chunks.indexes[1].fields, ["metadata.timestamp"]);
    assert_eq!(chunks.indexes[2].fields, ["metadata.owner"]);
    assert_eq!(
        chunks.indexes[3].fields,
        [
            "metadata.source_kind",
            "metadata.source_id",
            "seq_in_source"
        ]
    );

    let embeddings = specs
        .iter()
        .find(|spec| spec.name == CHUNK_EMBEDDINGS)
        .unwrap();
    assert!(embeddings.indexes[0].unique);
    assert_eq!(
        embeddings.indexes[0].fields,
        ["chunk_id", "model_signature"]
    );
    assert_eq!(embeddings.indexes[1].fields, ["model_signature"]);

    let skips = specs
        .iter()
        .find(|spec| spec.name == CHUNK_REEMBED_SKIPS)
        .unwrap();
    assert!(skips.indexes[0].unique);
    assert_eq!(skips.indexes[0].fields, ["chunk_id", "model_signature"]);

    let sources = specs
        .iter()
        .find(|spec| spec.name == INGESTED_SOURCES)
        .unwrap();
    assert!(sources.indexes[0].unique);
    assert_eq!(sources.indexes[0].fields, ["source_kind", "source_id"]);

    let raw_refs = specs.iter().find(|spec| spec.name == RAW_REFS).unwrap();
    assert!(raw_refs.indexes[0].unique);
    assert_eq!(raw_refs.indexes[0].fields, ["chunk_id"]);
}
