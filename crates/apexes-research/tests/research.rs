use apexes_research::{Artifact, ArtifactKind, Experiment, ResearchError, ResearchStore};
use std::collections::BTreeMap;

fn artifact(kind: ArtifactKind, content: &[u8]) -> Artifact {
    Artifact::new(kind, "fixture", "1", BTreeMap::new(), content.to_vec()).unwrap()
}

fn store() -> (ResearchStore, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("apexes-research-{}", uuid::Uuid::new_v4()));
    (ResearchStore::new(&root).unwrap(), root)
}

fn inputs(store: &ResearchStore) -> Experiment {
    let strategy = store
        .put(&artifact(ArtifactKind::Strategy, b"buy"))
        .unwrap();
    let dataset = store
        .put(&artifact(ArtifactKind::Dataset, b"1,2,3"))
        .unwrap();
    let hypothesis = store
        .put(&artifact(ArtifactKind::Hypothesis, b"momentum"))
        .unwrap();
    let configuration = store
        .put(&artifact(ArtifactKind::Configuration, b"fee=0.001"))
        .unwrap();
    Experiment::new(
        strategy,
        dataset,
        hypothesis,
        configuration,
        "engine@revision;lock@sha256",
        42,
    )
    .unwrap()
}

#[test]
fn artifacts_round_trip_and_every_change_creates_a_new_identity() {
    let (store, root) = store();
    let original = artifact(ArtifactKind::Strategy, b"buy");
    let id = store.put(&original).unwrap();
    assert_eq!(store.put(&original).unwrap(), id);
    assert_eq!(store.get(&id).unwrap(), original);
    for changed in [
        artifact(ArtifactKind::Strategy, b"sell"),
        artifact(ArtifactKind::Dataset, b"buy"),
        Artifact::new(
            ArtifactKind::Strategy,
            "fixture",
            "2",
            BTreeMap::new(),
            b"buy".to_vec(),
        )
        .unwrap(),
        Artifact::new(
            ArtifactKind::Strategy,
            "fixture",
            "1",
            BTreeMap::from([("source".into(), "other".into())]),
            b"buy".to_vec(),
        )
        .unwrap(),
    ] {
        assert_ne!(store.put(&changed).unwrap(), id);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn metadata_order_does_not_change_identity() {
    let a = Artifact::new(
        ArtifactKind::Dataset,
        "data",
        "1",
        BTreeMap::from([("b".into(), "2".into()), ("a".into(), "1".into())]),
        vec![1],
    )
    .unwrap();
    let b = Artifact::new(
        ArtifactKind::Dataset,
        "data",
        "1",
        BTreeMap::from([("a".into(), "1".into()), ("b".into(), "2".into())]),
        vec![1],
    )
    .unwrap();
    assert_eq!(a.id().unwrap(), b.id().unwrap());
}

#[test]
fn experiment_replay_resolves_exact_inputs_and_detects_divergence() {
    let (store, root) = store();
    let experiment = inputs(&store);
    let run = store
        .run(&experiment, |resolved| {
            assert_eq!(resolved.strategy.content(), b"buy");
            assert_eq!(resolved.dataset.content(), b"1,2,3");
            assert_eq!(resolved.hypothesis.content(), b"momentum");
            assert_eq!(resolved.configuration.content(), b"fee=0.001");
            assert_eq!(resolved.seed, 42);
            Ok(b"net_pnl=1.25".to_vec())
        })
        .unwrap();
    assert_eq!(store.get(&run).unwrap().kind(), ArtifactKind::Result);
    store
        .verify(&run, |_| Ok(b"net_pnl=1.25".to_vec()))
        .unwrap();
    assert!(matches!(
        store.verify(&run, |_| Ok(b"net_pnl=1.24".to_vec())),
        Err(ResearchError::ReplayMismatch)
    ));
    assert_eq!(
        store
            .run(&experiment, |_| Ok(b"net_pnl=1.25".to_vec()))
            .unwrap(),
        run
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn experiment_identity_includes_seed_runtime_and_configuration() {
    let (store, root) = store();
    let original = inputs(&store);
    let base = store.run(&original, |_| Ok(vec![0])).unwrap();
    for (seed, runtime) in [
        (43, "engine@revision;lock@sha256"),
        (42, "different-engine"),
    ] {
        let changed = Experiment::new(
            original.strategy().to_owned(),
            original.dataset().to_owned(),
            original.hypothesis().to_owned(),
            original.configuration().to_owned(),
            runtime,
            seed,
        )
        .unwrap();
        assert_ne!(store.run(&changed, |_| Ok(vec![0])).unwrap(), base);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn wrong_kind_missing_inputs_and_failed_execution_are_rejected() {
    let (store, root) = store();
    let original = inputs(&store);
    let wrong = Experiment::new(
        original.dataset().to_owned(),
        original.dataset().to_owned(),
        original.hypothesis().to_owned(),
        original.configuration().to_owned(),
        "runtime",
        42,
    )
    .unwrap();
    assert!(matches!(
        store.run(&wrong, |_| panic!("must not execute")),
        Err(ResearchError::WrongKind { .. })
    ));
    let missing = Experiment::new(
        "0".repeat(64),
        original.dataset().to_owned(),
        original.hypothesis().to_owned(),
        original.configuration().to_owned(),
        "runtime",
        42,
    )
    .unwrap();
    assert!(store.run(&missing, |_| panic!("must not execute")).is_err());
    assert!(matches!(
        store.run(&original, |_| Err("backtest failed".into())),
        Err(ResearchError::Execution(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_ids_and_manifests_are_rejected() {
    let (store, root) = store();
    for id in [
        "../escape",
        "/tmp/file",
        "",
        &"A".repeat(64),
        &"g".repeat(64),
    ] {
        assert!(matches!(store.get(id), Err(ResearchError::Invalid(_))));
    }
    assert!(Artifact::new(ArtifactKind::Strategy, "", "1", BTreeMap::new(), vec![1]).is_err());
    assert!(Artifact::new(ArtifactKind::Strategy, "name", "", BTreeMap::new(), vec![1]).is_err());
    assert!(Artifact::new(ArtifactKind::Strategy, "name", "1", BTreeMap::new(), vec![]).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn corrupted_artifacts_cannot_be_read_or_silently_overwritten() {
    let (store, root) = store();
    let original = artifact(ArtifactKind::Strategy, b"buy");
    let id = store.put(&original).unwrap();
    std::fs::write(root.join(format!("{id}.json")), b"{}").unwrap();
    assert!(store.get(&id).is_err());
    assert!(store.put(&original).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn tampering_and_unsupported_schema_are_rejected() {
    let (store, root) = store();
    let original = artifact(ArtifactKind::Strategy, b"buy");
    let id = store.put(&original).unwrap();
    let path = root.join(format!("{id}.json"));
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let mut tampered = value.clone();
    tampered["name"] = "tampered".into();
    std::fs::write(&path, serde_json::to_vec(&tampered).unwrap()).unwrap();
    assert!(matches!(store.get(&id), Err(ResearchError::Integrity)));
    let mut unsupported = value;
    unsupported["schema_version"] = 99.into();
    std::fs::write(&path, serde_json::to_vec(&unsupported).unwrap()).unwrap();
    assert!(matches!(store.get(&id), Err(ResearchError::Invalid(_))));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn concurrent_publication_is_idempotent_and_leaves_no_temporary_files() {
    let (store, root) = store();
    let original = artifact(ArtifactKind::Dataset, &[0, 255, 128, 1]);
    let ids = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| store.put(&original).unwrap()))
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(ids.iter().all(|id| id == &ids[0]));
    assert_eq!(
        ResearchStore::new(&root).unwrap().get(&ids[0]).unwrap(),
        original
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn configuration_changes_pin_new_results_and_failed_runs_publish_nothing() {
    let (store, root) = store();
    let original = inputs(&store);
    let base = store.run(&original, |_| Ok(vec![0])).unwrap();
    let configuration = store
        .put(&artifact(ArtifactKind::Configuration, b"fee=0.002"))
        .unwrap();
    let changed = Experiment::new(
        original.strategy().to_owned(),
        original.dataset().to_owned(),
        original.hypothesis().to_owned(),
        configuration,
        original.runtime(),
        original.seed(),
    )
    .unwrap();
    assert_ne!(store.run(&changed, |_| Ok(vec![0])).unwrap(), base);
    let before = std::fs::read_dir(&root).unwrap().count();
    assert!(store.run(&original, |_| Ok(vec![])).is_err());
    assert!(store.run(&original, |_| Err("failed".into())).is_err());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), before);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn deserialization_cannot_bypass_validation_at_the_store_boundary() {
    let (store, root) = store();
    let experiment = inputs(&store);
    let mut manifest = serde_json::to_value(&experiment).unwrap();
    for (field, value) in [
        ("schema_version", serde_json::json!(99)),
        ("strategy", serde_json::json!("../escape")),
        ("runtime", serde_json::json!("")),
    ] {
        manifest[field] = value;
        let invalid: Experiment = serde_json::from_value(manifest.clone()).unwrap();
        assert!(matches!(
            store.run(&invalid, |_| panic!("must not execute")),
            Err(ResearchError::Invalid(_))
        ));
        manifest = serde_json::to_value(&experiment).unwrap();
    }
    let result = store
        .put(&artifact(ArtifactKind::Result, b"not a receipt"))
        .unwrap();
    assert!(
        store
            .verify(&result, |_| panic!("must not execute"))
            .is_err()
    );
    assert!(
        store
            .verify(experiment.strategy(), |_| panic!("must not execute"))
            .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn symlink_artifacts_are_not_followed_or_overwritten() {
    let (store, root) = store();
    let original = artifact(ArtifactKind::Dataset, b"private");
    let id = original.id().unwrap();
    let outside = root.with_extension("outside");
    std::fs::write(&outside, b"private").unwrap();
    std::os::unix::fs::symlink(&outside, root.join(format!("{id}.json"))).unwrap();
    assert!(store.get(&id).is_err());
    assert!(store.put(&original).is_err());
    assert_eq!(std::fs::read(&outside).unwrap(), b"private");
    std::fs::remove_file(outside).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn crash_leftovers_do_not_block_new_publication() {
    let (store, root) = store();
    for sequence in 0..1024 {
        std::fs::write(
            root.join(format!(".pending-{}-{sequence}", std::process::id())),
            b"abandoned",
        )
        .unwrap();
    }
    let original = artifact(ArtifactKind::Strategy, b"buy");
    let id = store.put(&original).unwrap();
    assert_eq!(store.get(&id).unwrap(), original);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn schema_one_identity_is_stable() {
    let original = artifact(ArtifactKind::Strategy, b"buy");
    assert_eq!(
        original.id().unwrap(),
        "ccab72795f7964bd5878f514fa48bc73b0792b16332309c54f7723409c0676b8"
    );
    assert_eq!(original.name(), "fixture");
    assert_eq!(original.revision(), "1");
    assert!(original.metadata().is_empty());
}

#[test]
fn text_and_metadata_bounds_are_enforced() {
    for name in [" ".to_owned(), "bad\nname".into(), "x".repeat(257)] {
        assert!(
            Artifact::new(ArtifactKind::Strategy, &name, "1", BTreeMap::new(), vec![1]).is_err()
        );
    }
    for metadata in [
        BTreeMap::from([("".into(), "value".into())]),
        BTreeMap::from([("key".into(), "\t".into())]),
        BTreeMap::from([("key".into(), "v".repeat(4097))]),
        (0..129).map(|i| (i.to_string(), "value".into())).collect(),
    ] {
        assert!(Artifact::new(ArtifactKind::Dataset, "fixture", "1", metadata, vec![1]).is_err());
    }
}

#[test]
fn oversized_files_and_non_directory_roots_fail_without_reading_payload() {
    let (store, root) = store();
    let path = root.join(format!("{}.json", "0".repeat(64)));
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(64 * 1024 * 1024 + 1).unwrap();
    assert!(matches!(
        store.get(&"0".repeat(64)),
        Err(ResearchError::Invalid(_))
    ));
    assert!(ResearchStore::new(&path).is_err());
    std::fs::remove_dir_all(root).unwrap();
}
