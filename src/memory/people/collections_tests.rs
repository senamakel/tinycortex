use super::*;

#[test]
fn declares_people_alias_and_interaction_collections() {
    let specs = collections();

    assert_eq!(
        specs
            .iter()
            .map(|spec| spec.name.as_str())
            .collect::<Vec<_>>(),
        [
            "memory_people",
            "memory_people_handles",
            "memory_people_interactions"
        ]
    );
    let handles = specs
        .iter()
        .find(|spec| spec.name == "memory_people_handles")
        .unwrap();
    assert!(handles
        .indexes
        .iter()
        .any(|index| { index.name == "by_handle" && index.unique }));
}
