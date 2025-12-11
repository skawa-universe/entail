mod common;

use common::init_ring;
use std::{collections::HashSet, sync::Arc};

use entail::{
    Entail, EntailError, EntityModel,
    ds::{DatastoreShell, Entity, Key, Mutation, MutationBatch, Transaction, Value},
};

#[tokio::test]
pub async fn test_create_conflict() {
    let result = create_conflict().await;
    match result {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{:?}", err);
            assert!(false, "Failed with error");
        }
    }
}

fn new_test_key() -> Key {
    Key::new("Test")
}

pub async fn create_conflict() -> Result<(), EntailError> {
    init_ring();

    let ds = DatastoreShell::new("test-project", false, None)
        .await
        .map_err(|_| Default::default())?;
    let keys: Vec<Key> = ds
        .allocate_ids(&vec![new_test_key(), new_test_key()])
        .await?
        .into_iter()
        .map(|key| new_test_key().with_name(format!("one_{}", key.id().unwrap())))
        .collect();
    let key = &keys[0];
    println!("Keys: {}, {}", keys[0], keys[1]);
    ds.commit(MutationBatch::new().upsert({
        let mut e = Entity::new(key.clone());
        e.set_indexed("glank", Value::unicode_string("cluff"));
        e
    }))
    .await?;
    let mut tries_a = 0;
    let ta = Transaction::new(&ds).run(|ts| {
        tries_a += 1;
        let key1 = key.clone();
        async move {
            eprintln!("a1..");
            _ = ts.get_single(key1.clone()).await?;
            eprintln!("a2..");
            ts.commit(MutationBatch::new().add(Mutation::Upsert({
                let mut e = Entity::new(key1.clone());
                e.set_indexed("glank", Value::unicode_string("wibz"));
                e
            })))
            .await?;
            eprintln!("a3..");
            Ok(tries_a)
        }
    });
    let mut tries_b = 0;
    let t2 = Transaction::new(&ds).run(|ts| {
        tries_b += 1;
        let key2 = key.clone();
        async move {
            eprintln!("b1..");
            _ = ts.get_single(key2.clone()).await?;
            eprintln!("b2..");
            ts.commit(MutationBatch::new().add(Mutation::Upsert({
                let mut e = Entity::new(key2.clone());
                e.set_indexed("glank", Value::unicode_string("vrob"));
                e
            })))
            .await?;
            eprintln!("b3..");
            Ok(tries_b)
        }
    });
    let (a, b) = tokio::join!(ta, t2);
    println!("a tries: {}", a.unwrap());
    println!("b tries: {}", b.unwrap());
    ds.commit(MutationBatch::new().delete_all(keys)).await?;
    Ok(())
}

#[derive(Entail, Default, Debug)]
struct Sample {
    #[entail]
    key: String,
    #[entail]
    value: i32,
}

#[tokio::test]
pub async fn test_adapter() -> Result<(), EntailError> {
    init_ring();

    let ds = Arc::new(
        DatastoreShell::new("test-project", false, None)
            .await
            .map_err(|_| Default::default())?,
    );
    let s = Sample {
        key: "test".into(),
        value: 47,
    };
    ds.commit(MutationBatch::new().upsert(s.to_ds_entity()?))
        .await?;
    let a = Sample::adapter();
    let rs = a.fetch_single(&ds, a.create_named_key("test")).await?;
    assert_eq!(s.value, rs.value);
    // get_single would return with None successfully
    let non_existent = a
        .fetch_single(&ds, a.create_named_key("does_not_exist"))
        .await;
    assert!(non_existent.is_err());
    // the error is forwarded from get_single, because this is a bad request
    let incomplete = a.fetch_single(&ds, a.create_key()).await;
    assert!(incomplete.is_err());
    let key1 = a.create_named_key("does_not_exist");
    let key2 = a.create_named_key("test");
    let exotic: HashSet<&Key> = [&key1, &key2].into();
    let map = a.fetch_all(&ds, exotic).await?;
    assert_eq!(map.len(), 1);
    assert!(map.get(&key1).is_none());
    assert_eq!(map.get(&key2).unwrap().value, rs.value);

    let simple: Vec<Key> = vec![key1.clone(), key2.clone()];
    let map = a.fetch_all(&ds, &simple).await?;
    assert_eq!(map.len(), 1);
    assert_eq!(simple.len(), 2);
    assert!(map.get(&key1).is_none());
    assert_eq!(map.get(&key2).unwrap().value, rs.value);

    let missing_entity: Option<Entity> = None;
    a.required_from(missing_entity)
        .expect_err("This should have failed");
    let existing_entity = Some(s.to_ds_entity()?);
    assert_eq!(
        a.required_from(existing_entity)
            .expect("This should be successful")
            .value,
        47
    );
    let e = s.to_ds_entity()?;
    let ref_existing_entity = Some(&e);
    assert_eq!(
        a.required_from(ref_existing_entity)
            .expect("This should be successful")
            .value,
        47
    );
    Ok(())
}

#[derive(Entail, Default, Debug)]
struct OptionalKey {
    #[entail]
    key: Option<Key>,
    #[entail]
    value: i32,
}

#[tokio::test]
pub async fn test_optional_key() -> Result<(), EntailError> {
    let mut model = OptionalKey::default();
    let entity = model.to_ds_entity()?;
    assert_eq!(entity.key().kind(), OptionalKey::adapter().kind());
    assert!(!entity.key().is_complete());

    model.key = Some(OptionalKey::adapter().create_named_key("foo"));
    let entity = model.to_ds_entity()?;
    assert_eq!(entity.key().kind(), OptionalKey::adapter().kind());
    assert_eq!(entity.key().name(), Some("foo"));

    Ok(())
}
