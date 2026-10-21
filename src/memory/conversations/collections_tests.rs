use super::*;

#[test]
fn conversation_collections_declare_thread_projection_indexes() {
    let specs = collections();
    assert_eq!(specs.len(), 1);
    let threads = specs.iter().find(|spec| spec.name == THREAD_INDEX).unwrap();
    assert_eq!(threads.indexes.len(), 2);
    assert_eq!(threads.indexes[0].fields, ["updated_at"]);
    assert_eq!(threads.indexes[1].fields, ["parent_thread_id"]);
}
