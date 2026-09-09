#![cfg(all(not(target_arch = "wasm32"), feature = "in_memory"))]

use std::time::{Duration, Instant};

use bellows::{
    PublishActivationStrategy, PublishTrigger, TaskDefinition, TaskExecutionBackend,
    TaskPublishingBackend,
    backends::{ClaimTaskError, PublishTaskError, PublishedTask, in_memory::InMemoryBackend},
};

struct EchoTask;

impl TaskDefinition for EchoTask {
    const NAME: &str = "publishing_echo";
    type Callback = String;
    type Trigger = PublishTrigger<String>;
}

// Deliberately implements only publication, not execution or subscriptions.
#[derive(Clone)]
struct PublishingOnly(InMemoryBackend);

impl TaskPublishingBackend for PublishingOnly {
    async fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.0.publish::<T>(payload).await
    }

    async fn publish_future<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.0.publish_future::<T>(payload, available_from).await
    }
}

async fn publish_tasks<B: TaskPublishingBackend>(
    backend: B,
    available_from: Instant,
) -> Result<(PublishedTask, PublishedTask), PublishTaskError> {
    let immediate = backend.publish::<EchoTask>("immediate".to_owned()).await?;
    let future = backend
        .publish_future::<EchoTask>("future".to_owned(), available_from)
        .await?;
    Ok((immediate, future))
}

#[tokio::test]
async fn generic_producer_only_requires_publishing_capability() {
    let inner = InMemoryBackend::new();
    let publisher = PublishingOnly(inner.clone());
    let available_from = Instant::now() + Duration::from_secs(60);

    // The generic producer and publication futures remain Send.
    let (immediate, future) = tokio::spawn(publish_tasks(publisher.clone(), available_from))
        .await
        .unwrap()
        .unwrap();
    drop(publisher);

    assert_ne!(immediate.task_id, future.task_id);
    let expiration = available_from + Duration::from_secs(60);
    let claimed = inner
        .claim_published::<EchoTask>(17, immediate.task_id, expiration)
        .await
        .unwrap();
    assert_eq!(claimed.task_id, immediate.task_id);
    assert_eq!(claimed.task_payload, "immediate");
    assert!(matches!(
        inner.claim_published::<EchoTask>(17, future.task_id, expiration).await,
        Err(ClaimTaskError::TaskUnavailable { available_from: Some(at) }) if at == available_from
    ));

    // A non-unit callback can be completed without an awaitable publication handle.
    let finished = inner
        .finish::<EchoTask>(17, immediate.task_id, "done".to_owned(), None)
        .await
        .unwrap();
    assert_eq!(finished.task_id, immediate.task_id);
    assert!(matches!(
        inner
            .claim_published::<EchoTask>(17, immediate.task_id, expiration)
            .await,
        Err(ClaimTaskError::TaskNotFound)
    ));
}
