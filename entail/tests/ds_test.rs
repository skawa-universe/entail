mod common;

use std::sync::Arc;
use common::init_ring;

use entail::{
    ds::{DatastoreShell, Entity, Key, Mutation, MutationBatch, Transaction, Value}, Entail, EntailError, EntityModel
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

pub async fn create_conflict() -> Result<(), EntailError> {
    init_ring();

    let ds = DatastoreShell::new("test-project", false, None)
        .await
        .map_err(|_| Default::default())?;
    let key = Key::new("Test").with_name("one");
    ds.commit(MutationBatch::new().add(Mutation::Upsert({
        let mut e = Entity::new(key.clone());
        e.set_indexed("glank", Value::unicode_string("cluff"));
        e
    })))
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
    Ok(())
}

#[derive(Entail, Default)]
struct Sample {
    #[entail]
    key: String,
    #[entail]
    value: i32,
}

#[tokio::test]
pub async fn test_adapter() -> Result<(), EntailError> {
    init_ring();

    let ds = Arc::new(DatastoreShell::new("test-project", false, None)
        .await
        .map_err(|_| Default::default())?);
    let s = Sample { key: "test".into(), value: 47 };
    ds.commit(MutationBatch::new().upsert(s.to_ds_entity()?)).await?;
    let a = Sample::adapter();
    let rs = a.fetch_single(&ds, a.create_named_key("test")).await?;
    assert_eq!(s.value, rs.value);
    // get_single would return with None successfully
    let non_existent = a.fetch_single(&ds, a.create_named_key("does_not_exist")).await;
    assert!(non_existent.is_err());
    // the error is forwarded from get_single, because this is a bad request
    let incomplete = a.fetch_single(&ds, a.create_key()).await;
    assert!(incomplete.is_err());
    Ok(())
}
