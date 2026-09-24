#![cfg(all(feature = "cloudflare", not(target_arch = "wasm32")))]

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bellows::cloudflare::{
    BoxDispatchError, DispatchIntent, DispatchTask, DispatcherState, DispatcherStorage,
    DurableObjectNamespaceLike, ProcessorFetcher, RetainedTaskDispatcher, ScheduleUpdate,
    TaskIdentity, TextBody, dispatch_task, dispatch_tasks,
};
use http::{Request, Response};
use serde_json::{Value, json};
use tokio::sync::oneshot;

type IoResult<T> = Result<T, BoxDispatchError>;
const NOW: i64 = 1_700_000_000_000;

fn singleton(name: &str, intent: DispatchIntent) -> DispatchTask {
    DispatchTask {
        task: TaskIdentity::Singleton {
            task_name: name.into(),
        },
        intent,
    }
}

fn batch_request(tasks: Value) -> Request<TextBody> {
    Request::post("/dispatch")
        .header("content-type", "application/json")
        .body(text_body(json!({ "tasks": tasks }).to_string()))
        .unwrap()
}

fn singleton_record(
    storage: &FakeAlarmStorage,
    name: &str,
) -> Option<bellows::cloudflare::DispatcherTask> {
    storage
        .0
        .lock()
        .unwrap()
        .durable
        .tasks
        .get(&format!("singleton:{name}"))
        .cloned()
}

#[tokio::test]
async fn entire_batch_is_validated_before_any_side_effect() {
    let ensure = singleton("7", DispatchIntent::Ensure);
    for invalid in [
        json!({"task": {"kind": "published", "taskName": "7", "taskId": ""}, "intent": "run"}),
        json!({"task": {"kind": "published", "taskName": "7", "taskId": format!("{}a", "😀".repeat(100))}, "intent": "run"}),
        json!({"task": {"kind": "singleton", "taskName": null}, "intent": "ensure"}),
        json!({"task": {"kind": "singleton", "taskName": "7", "taskId": "7"}, "intent": "run"}),
        json!({"task": {"kind": "singleton", "taskName": "7", "extra": true}, "intent": "run"}),
        json!({"task": {"kind": "publish", "taskName": "7", "taskId": "7"}, "intent": "run"}),
        json!({"task": {"kind": "published", "taskName": "7"}, "intent": "run"}),
        json!({"task": {"kind": "published", "taskName": "7", "taskId": "7"}, "intent": "ensure"}),
        json!({"task": {"kind": "singleton", "taskName": ""}, "intent": "run"}),
        json!({"task": {"kind": "singleton", "taskName": "7"}, "intent": "unknown"}),
        json!({"task": {"kind": "singleton", "taskName": "7"}}),
        json!({"task": ensure.task, "intent": "ensure", "extra": true}),
        json!(singleton(&"😀".repeat(509), DispatchIntent::Ensure)),
    ] {
        let storage = FakeAlarmStorage::default();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
        assert_eq!(
            dispatcher
                .fetch(batch_request(json!([ensure, invalid])))
                .await
                .status(),
            400
        );
        assert_eq!(processor.count(), 0);
        let state = storage.0.lock().unwrap();
        assert_eq!(state.alarm_reads, 0);
        assert_eq!(state.transactions, 0);
        assert!(state.durable.tasks.is_empty());
    }
}

#[tokio::test]
async fn batch_helpers_validate_before_lookup_and_empty_batches_do_no_io() {
    let namespace = RecordingNamespace::default();
    dispatch_tasks(&namespace, &[]).await.unwrap();
    assert!(namespace.names.lock().unwrap().is_empty());
    assert!(
        dispatch_tasks(
            &namespace,
            &[
                singleton("7", DispatchIntent::Ensure),
                singleton(&"x".repeat(2034), DispatchIntent::Ensure),
            ]
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("byte limit")
    );
    assert!(namespace.names.lock().unwrap().is_empty());
    let tasks = [
        singleton(&format!("{}x", "😀".repeat(508)), DispatchIntent::Ensure),
        singleton(" published:7 雪\n", DispatchIntent::Run),
    ];
    namespace.stub.0.lock().unwrap().response =
        Some(Ok(response(200, text_body(r#"{"ok":true}"#))));
    dispatch_tasks(&namespace, &tasks).await.unwrap();
    assert_eq!(*namespace.names.lock().unwrap(), ["global"]);
    assert_eq!(
        serde_json::from_str::<Value>(namespace.stub.0.lock().unwrap().requests[0].body()).unwrap(),
        json!({"tasks": tasks})
    );
    let storage = FakeAlarmStorage::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), DeferredProcessor::default(), now);
    assert_json(
        dispatcher.fetch(batch_request(json!([]))).await,
        200,
        json!({"ok": true}),
    );
    assert_eq!(storage.0.lock().unwrap().alarm_reads, 0);
}

#[tokio::test]
async fn all_300_distinct_mixed_entries_launch_before_one_blocked_warming_check() {
    let storage = FakeAlarmStorage::default();
    let (release, gate) = oneshot::channel();
    storage.0.lock().unwrap().get_gate = Some(gate);
    let processor = DeferredProcessor::default();
    let dispatcher = Arc::new(RetainedTaskDispatcher::with_clock(
        storage.clone(),
        processor.clone(),
        now,
    ));
    let tasks: Vec<_> = (0..300)
        .map(|i| {
            if i % 2 == 0 {
                singleton(&(i / 2).to_string(), DispatchIntent::Ensure)
            } else {
                DispatchTask {
                    task: TaskIdentity::Published {
                        task_id: ((i - 1) / 2).to_string(),
                        task_name: "published".into(),
                    },
                    intent: DispatchIntent::Run,
                }
            }
        })
        .collect();
    let batch: Vec<_> = tasks.iter().chain(&tasks).collect();
    let request = batch_request(json!(batch));
    let acceptance = tokio::spawn(async move { dispatcher.fetch(request).await });
    wait_for(|| processor.count() == 300).await;
    {
        let state = storage.0.lock().unwrap();
        assert_eq!(state.alarm_reads, 1);
        assert_eq!(state.transactions, 0);
        assert_eq!(state.task_reads, 0);
        assert!(state.durable.tasks.is_empty());
    }
    release.send(()).unwrap();
    assert_json(acceptance.await.unwrap(), 200, json!({"ok": true}));
    assert_eq!(storage.0.lock().unwrap().scheduled.len(), 1);
    for i in 0..300 {
        finish(&processor, i).await;
    }
}

#[tokio::test]
async fn bootstrap_suppression_never_suppresses_run_and_current_done_forgets_it() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    let ensure = singleton("7", DispatchIntent::Ensure);
    let run = singleton("7", DispatchIntent::Run);
    dispatcher
        .fetch(batch_request(json!([ensure, ensure])))
        .await;
    wait_for(|| processor.count() == 1).await;
    dispatcher.fetch(batch_request(json!([ensure, run]))).await;
    assert_eq!(processor.count(), 1);
    reply(
        &processor,
        0,
        json!({"type": "retryAt", "atMs": NOW + 60_000}),
    )
    .await;
    let reads = storage.0.lock().unwrap().alarm_reads;
    dispatcher
        .fetch(batch_request(json!([ensure, ensure])))
        .await;
    assert_eq!(processor.count(), 1);
    assert_eq!(storage.0.lock().unwrap().alarm_reads, reads + 1);
    dispatcher.fetch(batch_request(json!([ensure, run]))).await;
    reply(&processor, 1, json!({"type": "done"})).await;
    assert!(singleton_record(&storage, "7").is_none());
    dispatcher.fetch(batch_request(json!([ensure]))).await;
    finish(&processor, 2).await;
    dispatcher.fetch(batch_request(json!([ensure]))).await;
    finish(&processor, 3).await;
}

#[tokio::test]
async fn singleton_chains_survive_reconstruction_and_alarm_launches_establish_suppression() {
    for intent in [DispatchIntent::Run, DispatchIntent::Ensure] {
        let storage = FakeAlarmStorage::default();
        let processor = DeferredProcessor::default();
        let make = || {
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now)
        };
        let mut dispatcher = make();
        let name = format!("singleton: published:7 雪🦀 {}", "x".repeat(300));
        let ensure = singleton(&name, DispatchIntent::Ensure);
        dispatcher
            .fetch(batch_request(json!([singleton(&name, intent)])))
            .await;
        let deadline = scheduler_now() + 10_000;
        reply(&processor, 0, json!({"type": "retryAt", "atMs": deadline})).await;
        assert_eq!(singleton_record(&storage, &name).unwrap().task, ensure.task);
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        assert_eq!(processor.count(), 1);
        dispatcher = make();
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        reply(
            &processor,
            1,
            json!({"type": "retryAt", "atMs": deadline + 1}),
        )
        .await;
        dispatcher = make();
        advance(10_001);
        dispatcher.alarm().await.unwrap();
        wait_for(|| processor.count() == 3).await;
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        assert_eq!(processor.count(), 3);
        reply(
            &processor,
            2,
            json!({"type": "retryAt", "atMs": scheduler_now() + 60_000}),
        )
        .await;
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        assert_eq!(processor.count(), 3);
    }
}

#[tokio::test]
async fn singleton_infrastructure_retries_keep_suppression_and_reset_on_past_hints() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let make =
        || RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    let mut dispatcher = make();
    let ensure = singleton("7", DispatchIntent::Ensure);
    dispatcher.fetch(batch_request(json!([ensure]))).await;
    wait_for(|| processor.count() == 1).await;
    processor.complete(0, Err("network".into()));
    wait_for(|| storage.0.lock().unwrap().task_reads == 1).await;
    dispatcher.fetch(batch_request(json!([ensure]))).await;
    assert_eq!(processor.count(), 1);
    assert!(storage.0.lock().unwrap().durable.tasks.is_empty());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while processor.count() != 2 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    reply(
        &processor,
        1,
        json!({"type": "retryAt", "atMs": scheduler_now()}),
    )
    .await;
    for failures in 1..=7 {
        dispatcher = make();
        dispatcher.alarm().await.unwrap();
        let count = failures as usize + 2;
        wait_for(|| processor.count() == count).await;
        processor.complete(count - 1, Err("network".into()));
        wait_for(|| {
            singleton_record(&storage, "7").unwrap().state
                == bellows::cloudflare::TaskSchedule::Pending
        })
        .await;
        let record = singleton_record(&storage, "7").unwrap();
        assert_eq!(record.infrastructure_failures, failures.min(6));
        let delay = (1_000_i64 << (failures - 1)).min(30_000);
        assert_eq!(record.next_attempt_at_ms, scheduler_now() + delay);
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        assert_eq!(processor.count(), count);
        advance(delay);
    }
    dispatcher.alarm().await.unwrap();
    reply(
        &processor,
        9,
        json!({"type": "retryAt", "atMs": scheduler_now() - 1}),
    )
    .await;
    assert_eq!(
        singleton_record(&storage, "7")
            .unwrap()
            .infrastructure_failures,
        0
    );
}

#[tokio::test]
async fn stale_singleton_outcomes_cannot_replace_or_forget_a_newer_chain() {
    for action in ["done", "retryAt", "error"] {
        let storage = FakeAlarmStorage::default();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
        let ensure = singleton("7", DispatchIntent::Ensure);
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        reply(
            &processor,
            0,
            json!({"type": "retryAt", "atMs": scheduler_now()}),
        )
        .await;
        dispatcher.alarm().await.unwrap();
        wait_for(|| processor.count() == 2).await;
        // Another delegate leaves the old future alive, exercising durable attempt fencing.
        let successor =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
        advance(60_000);
        successor.alarm().await.unwrap();
        advance(1_000);
        successor.alarm().await.unwrap();
        reply(
            &processor,
            2,
            json!({"type": "retryAt", "atMs": scheduler_now() + 50_000}),
        )
        .await;
        let saved = singleton_record(&storage, "7");
        if action == "error" {
            processor.complete(1, Err("late error".into()));
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        } else {
            reply(
                &processor,
                1,
                if action == "done" {
                    json!({"type": "done"})
                } else {
                    json!({"type": "retryAt", "atMs": 0})
                },
            )
            .await;
        }
        successor.fetch(batch_request(json!([ensure]))).await;
        assert_eq!(processor.count(), 3);
        assert_eq!(singleton_record(&storage, "7"), saved);
    }
}

#[tokio::test]
async fn a_singleton_record_under_another_identity_key_is_rejected() {
    let storage = FakeAlarmStorage::default();
    seed_schedule(&storage, "contract");
    storage
        .0
        .lock()
        .unwrap()
        .durable
        .tasks
        .get_mut("published:id")
        .unwrap()
        .task = singleton("7", DispatchIntent::Ensure).task;
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage, processor.clone(), now);
    assert!(
        dispatcher
            .alarm()
            .await
            .unwrap_err()
            .to_string()
            .contains("task record")
    );
    assert_eq!(processor.count(), 0);
}

fn now() -> i64 {
    NOW
}

#[tokio::test]
async fn singleton_persistence_failures_preserve_the_retry_chain() {
    for uncertain in [false, true] {
        let storage = FakeAlarmStorage::default();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
        let ensure = singleton("7", DispatchIntent::Ensure);
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        if uncertain {
            storage.0.lock().unwrap().uncertain_commit = true;
        } else {
            storage.0.lock().unwrap().set_failures = 1;
        }
        reply(&processor, 0, json!({"type": "retryAt", "atMs": NOW})).await;
        assert_eq!(singleton_record(&storage, "7").is_some(), uncertain);
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        assert_eq!(processor.count(), 1);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while processor.count() != 2 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        reply(
            &processor,
            1,
            json!({"type": "retryAt", "atMs": NOW + 60_000}),
        )
        .await;
        assert_eq!(
            singleton_record(&storage, "7")
                .unwrap()
                .infrastructure_failures,
            0
        );
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        assert_eq!(processor.count(), 2);
    }
}

#[tokio::test]
async fn mismatched_singleton_responses_cannot_end_the_chain() {
    for task in [
        json!({"kind": "singleton", "taskName": "7 "}),
        json!({"kind": "published", "taskName": "7", "taskId": "7"}),
        json!({"kind": "singleton", "taskName": "7", "taskId": "7"}),
    ] {
        let storage = FakeAlarmStorage::default();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
        let ensure = singleton("7", DispatchIntent::Ensure);
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        reply(&processor, 0, json!({"type": "retryAt", "atMs": NOW})).await;
        dispatcher.alarm().await.unwrap();
        wait_for(|| processor.count() == 2).await;
        processor.complete(
            1,
            Ok(response(
                200,
                text_body(json!({"task": task, "nextAction": {"type": "done"}}).to_string()),
            )),
        );
        wait_for(|| {
            singleton_record(&storage, "7")
                .unwrap()
                .infrastructure_failures
                == 1
        })
        .await;
        dispatcher.fetch(batch_request(json!([ensure]))).await;
        assert_eq!(processor.count(), 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immediate_completions_still_dispatch_each_batch_identity_only_once() {
    use std::sync::atomic::AtomicUsize;
    struct ImmediateProcessor(Arc<AtomicUsize>);
    impl ProcessorFetcher for ImmediateProcessor {
        async fn fetch(&self, request: Request<String>) -> IoResult<Response<TextBody>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let body: Value = serde_json::from_str(request.body())?;
            Ok(response(
                200,
                text_body(
                    json!({"task": body["task"], "nextAction": {"type": "done"}}).to_string(),
                ),
            ))
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let storage = FakeAlarmStorage::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), ImmediateProcessor(count.clone()), now);
    let entries: Vec<_> = (0..1000)
        .flat_map(|_| {
            [
                singleton("7", DispatchIntent::Ensure),
                DispatchTask {
                    task: TaskIdentity::Published {
                        task_id: "7".into(),
                        task_name: "published".into(),
                    },
                    intent: DispatchIntent::Run,
                },
            ]
        })
        .collect();
    assert_json(
        dispatcher.fetch(batch_request(json!(entries))).await,
        200,
        json!({"ok": true}),
    );
    wait_for(|| storage.0.lock().unwrap().task_reads >= 2).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn healthy_dispatch_and_unsaved_completion_do_not_write_or_scan_schedules() {
    let storage = FakeAlarmStorage::default();
    storage.0.lock().unwrap().alarm = Some(NOW + 10_000);
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    dispatcher.fetch(dispatch_request("id")).await;
    dispatcher.fetch(named_request("ignored", "id")).await;
    wait_for(|| processor.count() == 1).await;
    assert_eq!(storage.0.lock().unwrap().task_reads, 0);
    reply(&processor, 0, json!({"type": "done"})).await;
    let state = storage.0.lock().unwrap();
    assert_eq!(state.task_reads, 1);
    assert_eq!(state.transactions, 0);
    assert!(state.scheduled.is_empty());
    assert!(state.durable.tasks.is_empty());
    assert!(state.durable.metadata.is_none());
}

#[tokio::test]
async fn unsaved_uncertainty_retries_in_memory_until_a_valid_hint_starts_persistence() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    dispatcher.fetch(dispatch_request("id")).await;
    wait_for(|| processor.count() == 1).await;
    processor.complete(0, Err("network".into()));
    wait_for(|| storage.0.lock().unwrap().task_reads == 1).await;
    assert_eq!(storage.0.lock().unwrap().transactions, 0);
    assert_eq!(storage.0.lock().unwrap().scheduled.len(), 1);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while processor.count() != 2 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    reply(&processor, 1, json!({"type": "retryAt", "atMs": NOW - 1})).await;
    assert_eq!(
        stored_task(&storage, "id").unwrap().infrastructure_failures,
        0
    );
    dispatcher.alarm().await.unwrap();
    finish(&processor, 2).await;
}

#[tokio::test]
async fn explicit_redispatch_reconciles_saved_schedules_only_after_its_response() {
    let storage = FakeAlarmStorage::default();
    seed_schedule(&storage, "old");
    storage.0.lock().unwrap().alarm = Some(NOW - 1);
    let before = stored_task(&storage, "id");
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    dispatcher.fetch(named_request("corrected", "id")).await;
    dispatcher.fetch(named_request("ignored", "id")).await;
    assert_eq!(storage.0.lock().unwrap().transactions, 0);
    assert_eq!(stored_task(&storage, "id"), before);
    assert!(storage.0.lock().unwrap().scheduled.is_empty());
    dispatcher.alarm().await.unwrap();
    wait_for(|| processor.count() == 1).await;
    assert!(stored_task(&storage, "id").is_some());
    reply(
        &processor,
        0,
        json!({"type": "retryAt", "atMs": NOW + 10_000}),
    )
    .await;
    assert_eq!(
        stored_task(&storage, "id").unwrap().task.name(),
        "corrected"
    );
    dispatcher.fetch(dispatch_request("id")).await;
    reply(&processor, 1, json!({"type": "done"})).await;
    assert!(stored_task(&storage, "id").is_none());
}

#[tokio::test]
async fn first_delayed_hint_preserves_the_already_armed_warming_deadline() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    dispatcher.fetch(dispatch_request("id")).await;
    let heartbeat = storage.0.lock().unwrap().alarm;
    advance(10_000);
    reply(
        &processor,
        0,
        json!({"type": "retryAt", "atMs": scheduler_now() + 60_000}),
    )
    .await;
    assert_eq!(storage.0.lock().unwrap().alarm, heartbeat);
}

#[tokio::test]
async fn dispatch_launches_through_blocked_outcome_bookkeeping_and_preserves_its_earlier_alarm() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = Arc::new(RetainedTaskDispatcher::with_clock(
        storage.clone(),
        processor.clone(),
        now,
    ));
    dispatcher.fetch(dispatch_request("id")).await;
    let (release, gate) = oneshot::channel();
    storage.0.lock().unwrap().get_gate = Some(gate);
    reply(
        &processor,
        0,
        json!({"type": "retryAt", "atMs": NOW + 1_000}),
    )
    .await;
    wait_for(|| storage.0.lock().unwrap().get_gate.is_none()).await;
    let dispatch = tokio::spawn({
        let dispatcher = dispatcher.clone();
        async move { dispatcher.fetch(dispatch_request("other")).await }
    });
    wait_for(|| processor.count() == 2).await;
    assert!(storage.0.lock().unwrap().durable.tasks.is_empty());
    release.send(()).unwrap();
    assert_eq!(dispatch.await.unwrap().status(), 200);
    assert_eq!(storage.0.lock().unwrap().alarm, Some(NOW + 1_000));
    assert!(stored_task(&storage, "other").is_none());
    finish(&processor, 1).await;
}

fn seed_schedule(storage: &FakeAlarmStorage, name: &str) {
    use bellows::cloudflare::{DispatcherTask, SchedulerMetadata, TaskSchedule};
    let mut state = storage.0.lock().unwrap();
    state.durable.metadata = Some(SchedulerMetadata {
        next_heartbeat_at_ms: scheduler_now() + 30_000,
        next_attempt_id: 0,
    });
    state.durable.tasks.insert(
        "published:id".into(),
        DispatcherTask {
            task: TaskIdentity::Published {
                task_id: "id".into(),
                task_name: name.into(),
            },
            next_attempt_at_ms: scheduler_now(),
            infrastructure_failures: 0,
            state: TaskSchedule::Pending,
        },
    );
}

#[derive(Default)]
struct AlarmState {
    durable: DispatcherState,
    transactions: usize,
    task_reads: usize,
    alarm_reads: usize,
    alarm: Option<i64>,
    scheduled: Vec<i64>,
    get_error: Option<&'static str>,
    set_error: Option<&'static str>,
    set_failures: usize,
    uncertain_commit: bool,
    get_gate: Option<oneshot::Receiver<()>>,
}

#[derive(Clone, Default)]
struct FakeAlarmStorage(Arc<Mutex<AlarmState>>);

impl DispatcherStorage for FakeAlarmStorage {
    async fn get_alarm(&self) -> IoResult<Option<i64>> {
        let gate = {
            let mut state = self.0.lock().unwrap();
            state.alarm_reads += 1;
            state.get_gate.take()
        };
        if let Some(gate) = gate {
            gate.await?;
        }
        let state = self.0.lock().unwrap();
        if let Some(error) = state.get_error {
            return Err(error.into());
        }
        Ok(state.alarm)
    }

    async fn set_alarm(&self, at_ms: i64) -> IoResult<()> {
        let mut state = self.0.lock().unwrap();
        if let Some(error) = state.set_error {
            return Err(error.into());
        }
        state.alarm = Some(at_ms);
        state.scheduled.push(at_ms);
        Ok(())
    }

    async fn contains_task(&self, id: &str) -> IoResult<bool> {
        let mut state = self.0.lock().unwrap();
        state.task_reads += 1;
        Ok(state.durable.tasks.contains_key(id))
    }

    async fn transaction<T>(&self, update: ScheduleUpdate<T>) -> IoResult<T>
    where
        T: Send + 'static,
    {
        let gate = self.0.lock().unwrap().get_gate.take();
        if let Some(gate) = gate {
            gate.await?;
        }
        let mut state = self.0.lock().unwrap();
        state.transactions += 1;
        if let Some(error) = state.get_error {
            return Err(error.into());
        }
        let mut durable = state.durable.clone();
        durable.alarm = state.alarm;
        let result = update(&mut durable)?;
        if state.set_failures > 0 {
            state.set_failures -= 1;
            return Err("alarm storage failed".into());
        }
        if let Some(error) = state.set_error {
            return Err(error.into());
        }
        state.alarm = Some(durable.alarm_at_ms());
        state.scheduled.push(durable.alarm_at_ms());
        state.durable = durable;
        if state.uncertain_commit {
            state.uncertain_commit = false;
            return Err("commit acknowledgement lost".into());
        }
        Ok(result)
    }
}

struct ProcessorCall {
    request: Request<String>,
    response: Option<oneshot::Sender<IoResult<Response<TextBody>>>>,
}

#[derive(Clone, Default)]
struct DeferredProcessor(Arc<Mutex<Vec<ProcessorCall>>>);

impl DeferredProcessor {
    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    fn complete(&self, index: usize, response: IoResult<Response<TextBody>>) {
        let sender = self.0.lock().unwrap()[index].response.take().unwrap();
        assert!(sender.send(response).is_ok());
    }
}

impl ProcessorFetcher for DeferredProcessor {
    async fn fetch(&self, request: Request<String>) -> IoResult<Response<TextBody>> {
        let (sender, receiver) = oneshot::channel();
        self.0.lock().unwrap().push(ProcessorCall {
            request,
            response: Some(sender),
        });
        receiver.await?
    }
}

#[derive(Default)]
struct StubState {
    requests: Vec<Request<String>>,
    response: Option<IoResult<Response<TextBody>>>,
}

#[derive(Clone, Default)]
struct RecordingStub(Arc<Mutex<StubState>>);

impl ProcessorFetcher for RecordingStub {
    async fn fetch(&self, request: Request<String>) -> IoResult<Response<TextBody>> {
        let mut state = self.0.lock().unwrap();
        state.requests.push(request);
        state.response.take().unwrap()
    }
}

#[derive(Default)]
struct RecordingNamespace {
    stub: RecordingStub,
    names: Mutex<Vec<String>>,
    error: Option<&'static str>,
}

impl DurableObjectNamespaceLike for RecordingNamespace {
    type Stub = RecordingStub;

    fn get_by_name(&self, name: &str) -> IoResult<Self::Stub> {
        self.names.lock().unwrap().push(name.to_owned());
        if let Some(error) = self.error {
            return Err(error.into());
        }
        Ok(self.stub.clone())
    }
}

struct BodyControl {
    started: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    sender: oneshot::Sender<IoResult<String>>,
}

fn controlled_body() -> (TextBody, BodyControl) {
    let (sender, receiver) = oneshot::channel();
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let body_started = started.clone();
    let body_finished = finished.clone();
    let body = Box::pin(async move {
        body_started.store(true, Ordering::SeqCst);
        let result = receiver.await?;
        body_finished.store(true, Ordering::SeqCst);
        result
    });
    (
        body,
        BodyControl {
            started,
            finished,
            sender,
        },
    )
}

fn text_body(text: impl Into<String>) -> TextBody {
    let text = text.into();
    Box::pin(async { Ok(text) })
}

fn response(status: u16, body: TextBody) -> Response<TextBody> {
    Response::builder().status(status).body(body).unwrap()
}

fn dispatch_request(task_id: &str) -> Request<TextBody> {
    named_request("contract", task_id)
}

fn named_request(task_name: &str, task_id: &str) -> Request<TextBody> {
    Request::post("https://dispatcher/dispatch")
        .header("content-type", "application/json")
        .body(text_body(
            json!({ "tasks": [{ "task": { "kind": "published", "taskId": task_id, "taskName": task_name }, "intent": "run" }] }).to_string(),
        ))
        .unwrap()
}

fn assert_json(response: Response<String>, status: u16, body: Value) {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers().len(), 3);
    assert_eq!(
        response.headers()["content-type"],
        "application/json; charset=utf-8"
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        serde_json::from_str::<Value>(response.body()).unwrap(),
        body
    );
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    for _ in 0..100 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition was not met");
}

async fn finish(processor: &DeferredProcessor, index: usize) {
    wait_for(|| processor.count() > index).await;
    let (body, control) = controlled_body();
    processor.complete(index, Ok(response(200, body)));
    let id = serde_json::from_str::<Value>(processor.0.lock().unwrap()[index].request.body())
        .unwrap()["task"]
        .clone();
    control
        .sender
        .send(Ok(
            json!({"task": id, "nextAction": {"type": "done"}}).to_string()
        ))
        .unwrap();
    wait_for(|| control.finished.load(Ordering::SeqCst)).await;
}

// One subscriber avoids global callsite-cache races between concurrent test registrations.
// Each current-thread Tokio test still captures only its own actual library diagnostics.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<HashMap<String, String>>>>);

thread_local! {
    static ACTIVE_LOGS: std::cell::RefCell<Option<Logs>> = const { std::cell::RefCell::new(None) };
}

struct LogCapture;

impl Logs {
    fn capture(&self) -> LogCapture {
        static SUBSCRIBER: std::sync::Once = std::sync::Once::new();
        SUBSCRIBER.call_once(|| tracing::subscriber::set_global_default(LogCapture).unwrap());
        ACTIVE_LOGS.set(Some(self.clone()));
        LogCapture
    }
}

impl Drop for LogCapture {
    fn drop(&mut self) {
        ACTIVE_LOGS.set(None);
    }
}

struct Fields(HashMap<String, String>);

impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}

impl tracing::Subscriber for LogCapture {
    fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
        Some(tracing::metadata::LevelFilter::ERROR)
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::ERROR
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = Fields(HashMap::new());
        event.record(&mut fields);
        ACTIVE_LOGS.with_borrow(|logs| {
            if let Some(logs) = logs {
                logs.0.lock().unwrap().push(fields.0);
            }
        });
    }
}

#[tokio::test]
async fn dispatch_targets_global_and_fully_consumes_the_response() {
    let namespace = Arc::new(RecordingNamespace::default());
    let (body, control) = controlled_body();
    namespace.stub.0.lock().unwrap().response = Some(Ok(response(200, body)));
    let dispatch = tokio::spawn({
        let namespace = namespace.clone();
        async move { dispatch_task(namespace.as_ref(), "contract", "123").await }
    });
    wait_for(|| control.started.load(Ordering::SeqCst)).await;
    assert!(!dispatch.is_finished());
    assert!(!control.finished.load(Ordering::SeqCst));
    assert_eq!(*namespace.names.lock().unwrap(), ["global"]);
    {
        let stub = namespace.stub.0.lock().unwrap();
        assert_eq!(stub.requests.len(), 1);
        let request = &stub.requests[0];
        assert_eq!(request.method(), "POST");
        assert_eq!(request.uri(), "https://dispatcher/dispatch");
        assert_eq!(request.headers()["content-type"], "application/json");
        assert_eq!(
            serde_json::from_str::<Value>(request.body()).unwrap(),
            json!({ "tasks": [{ "task": { "kind": "published", "taskId": "123", "taskName": "contract" }, "intent": "run" }] })
        );
    }
    control.sender.send(Ok(r#"{"ok":true}"#.into())).unwrap();
    dispatch.await.unwrap().unwrap();
    assert!(control.finished.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dispatch_consumes_an_error_response_before_rejecting() {
    let namespace = Arc::new(RecordingNamespace::default());
    let (body, control) = controlled_body();
    namespace.stub.0.lock().unwrap().response = Some(Ok(response(503, body)));
    let dispatch = tokio::spawn({
        let namespace = namespace.clone();
        async move { dispatch_task(namespace.as_ref(), "contract", "123").await }
    });
    wait_for(|| control.started.load(Ordering::SeqCst)).await;
    assert!(!dispatch.is_finished());
    control
        .sender
        .send(Ok("dispatcher unavailable".into()))
        .unwrap();
    assert_eq!(
        dispatch.await.unwrap().unwrap_err().to_string(),
        "task dispatcher returned HTTP 503: dispatcher unavailable"
    );
    assert!(control.finished.load(Ordering::SeqCst));
}

#[tokio::test]
async fn retained_dispatch_accepts_early_and_suppresses_duplicates_through_body_consumption() {
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
    assert_json(
        dispatcher.fetch(dispatch_request("task-1")).await,
        200,
        json!({ "ok": true }),
    );
    wait_for(|| processor.count() == 1).await;
    {
        let calls = processor.0.lock().unwrap();
        let request = &calls[0].request;
        assert_eq!(request.method(), "POST");
        assert_eq!(request.uri(), "https://processor/process");
        assert_eq!(request.headers()["content-type"], "application/json");
        assert_eq!(
            serde_json::from_str::<Value>(request.body()).unwrap(),
            json!({ "task": { "kind": "published", "taskId": "task-1", "taskName": "contract" } })
        );
    }
    let duplicate = json!({ "ok": true });
    assert_json(
        dispatcher.fetch(named_request("other", "task-1")).await,
        200,
        duplicate.clone(),
    );
    let (body, control) = controlled_body();
    processor.complete(0, Ok(response(200, body)));
    wait_for(|| control.started.load(Ordering::SeqCst)).await;
    assert_json(
        dispatcher.fetch(dispatch_request("task-1")).await,
        200,
        duplicate,
    );
    assert_eq!(processor.count(), 1);
    control
        .sender
        .send(Ok(
            json!({"task": {"kind": "published", "taskId": "task-1", "taskName": "contract"}, "nextAction": {"type": "done"}}).to_string(),
        ))
        .unwrap();
    wait_for(|| control.finished.load(Ordering::SeqCst)).await;

    // Successful responses release IDs too: a later dispatch is a new attempt.
    assert_json(
        dispatcher.fetch(dispatch_request("task-1")).await,
        200,
        json!({ "ok": true }),
    );
    finish(&processor, 1).await;
}

#[tokio::test]
async fn different_task_ids_retain_concurrent_processor_requests() {
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
    let (first, second) = tokio::join!(
        dispatcher.fetch(dispatch_request("task-1")),
        dispatcher.fetch(named_request("other", "task-2")),
    );
    assert_json(first, 200, json!({ "ok": true }));
    assert_json(second, 200, json!({ "ok": true }));
    wait_for(|| processor.count() == 2).await;
    assert!(
        processor
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|call| call.request.uri() == "https://processor/process")
    );
    let bodies: Vec<Value> = processor
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|call| serde_json::from_str(call.request.body()).unwrap())
        .collect();
    assert!(bodies.contains(
        &json!({ "task": { "kind": "published", "taskId": "task-1", "taskName": "contract" } })
    ));
    assert!(bodies.contains(
        &json!({ "task": { "kind": "published", "taskId": "task-2", "taskName": "other" } })
    ));
    finish(&processor, 0).await;
    finish(&processor, 1).await;
}

#[tokio::test]
async fn dispatch_and_alarm_schedule_the_30_second_heartbeat_even_when_idle() {
    let idle_storage = FakeAlarmStorage::default();
    let idle =
        RetainedTaskDispatcher::with_clock(idle_storage.clone(), DeferredProcessor::default(), now);
    idle.alarm().await.unwrap();
    assert_eq!(idle_storage.0.lock().unwrap().scheduled, [NOW + 30_000]);

    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    dispatcher.fetch(dispatch_request("task-1")).await;
    assert_eq!(storage.0.lock().unwrap().scheduled, [NOW + 30_000]);
    dispatcher.alarm().await.unwrap();
    assert_eq!(
        storage.0.lock().unwrap().scheduled,
        [NOW + 30_000, NOW + 30_000]
    );
    finish(&processor, 0).await;
    dispatcher.alarm().await.unwrap();
    assert!(
        storage
            .0
            .lock()
            .unwrap()
            .scheduled
            .iter()
            .all(|time| *time == NOW + 30_000)
    );
}

#[tokio::test]
async fn processor_http_failures_are_consumed_and_permit_explicit_redispatch() {
    for (status, message) in [(500, "failed"), (404, r#"{"error":"unknown task name"}"#)] {
        let logs = Logs::default();
        let _subscriber = logs.capture();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        dispatcher.fetch(named_request("unknown", "task-1")).await;
        wait_for(|| processor.count() == 1).await;
        let (body, control) = controlled_body();
        processor.complete(0, Ok(response(status, body)));
        wait_for(|| control.started.load(Ordering::SeqCst)).await;
        assert!(logs.0.lock().unwrap().is_empty());
        assert_json(
            dispatcher.fetch(dispatch_request("task-1")).await,
            200,
            json!({ "ok": true }),
        );
        control.sender.send(Ok(message.into())).unwrap();
        wait_for(|| logs.0.lock().unwrap().len() == 1).await;
        {
            let logs = logs.0.lock().unwrap();
            assert_eq!(logs[0]["message"], "task processor failed");
            assert_eq!(logs[0]["kind"], "published");
            assert_eq!(
                logs[0]["error"],
                format!("task processor returned HTTP {status}: {message}")
            );
        }
        assert!(control.finished.load(Ordering::SeqCst));
        assert_json(
            dispatcher.fetch(dispatch_request("task-1")).await,
            200,
            json!({ "ok": true }),
        );
        finish(&processor, 1).await;
    }
}

#[tokio::test]
async fn generic_ids_use_utf16_limits_and_round_trip_without_numeric_parsing() {
    for id in ["".into(), "a".repeat(201), format!("{}a", "😀".repeat(100))] {
        let namespace = RecordingNamespace::default();
        assert_eq!(
            dispatch_task(&namespace, "contract", &id)
                .await
                .unwrap_err()
                .to_string(),
            "taskId must be a non-empty string no longer than 200 characters"
        );
        assert!(namespace.names.lock().unwrap().is_empty());
    }
    for id in [
        "0".into(),
        " ".into(),
        "opaque/\"\\\n雪".into(),
        "a".repeat(200),
        "😀".repeat(100),
        format!("{}😀", "a".repeat(198)),
    ] {
        let namespace = RecordingNamespace::default();
        namespace.stub.0.lock().unwrap().response =
            Some(Ok(response(200, text_body(r#"{"ok":true}"#))));
        dispatch_task(&namespace, "contract", &id).await.unwrap();
        let encoded = namespace.stub.0.lock().unwrap().requests[0].body().clone();
        assert_eq!(
            serde_json::from_str::<Value>(&encoded).unwrap(),
            json!({ "tasks": [{ "task": { "kind": "published", "taskId": id, "taskName": "contract" }, "intent": "run" }] })
        );
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        // Like TypeScript, routing ignores the query and media-type matching is case-insensitive.
        let request = Request::post("https://dispatcher/dispatch?ignored=true")
            .header("content-type", "Application/JSON; charset=utf-8")
            .body(text_body(encoded))
            .unwrap();
        assert_json(dispatcher.fetch(request).await, 200, json!({ "ok": true }));
        finish(&processor, 0).await;
    }
}

#[tokio::test]
async fn invalid_routes_and_inputs_do_not_launch_work_or_schedule_alarms() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    for (method, path) in [
        ("GET", "/dispatch"),
        ("POST", "/dispatch/"),
        ("POST", "/process"),
    ] {
        let unread: TextBody = Box::pin(async { panic!("invalid route must not read the body") });
        let request = Request::builder()
            .method(method)
            .uri(path)
            .body(unread)
            .unwrap();
        assert_json(
            dispatcher.fetch(request).await,
            404,
            json!({ "error": "not-found", "ok": false }),
        );
    }
    for content_type in [None, Some("text/plain")] {
        let mut request = Request::post("/dispatch");
        if let Some(value) = content_type {
            request = request.header("content-type", value);
        }
        let unread: TextBody =
            Box::pin(async { panic!("invalid media type must not read the body") });
        assert_json(
            dispatcher.fetch(request.body(unread).unwrap()).await,
            400,
            json!({ "error": "request content-type must be application/json", "ok": false }),
        );
    }
    for body in [
        "null".into(),
        "[]".into(),
        "[{\"taskId\":\"id\"}]".into(),
        "true".into(),
        "1".into(),
        "\"id\"".into(),
        "{}".into(),
        "{\"taskId\":null}".into(),
        "{\"taskId\":1}".into(),
        "{\"taskId\":false}".into(),
        "{\"taskId\":[]}".into(),
        "{\"taskId\":{}}".into(),
        "{\"taskId\":\"\"}".into(),
        json!({ "taskId": "a".repeat(201) }).to_string(),
        json!({ "taskId": format!("{}a", "😀".repeat(100)) }).to_string(),
    ] {
        let error = "invalid dispatch batch";
        let request = Request::post("/dispatch")
            .header("content-type", "application/json")
            .body(text_body(body))
            .unwrap();
        assert_json(
            dispatcher.fetch(request).await,
            400,
            json!({ "error": error, "ok": false }),
        );
    }
    let malformed = Request::post("/dispatch")
        .header("content-type", "application/json")
        .body(text_body("{"))
        .unwrap();
    let result = dispatcher.fetch(malformed).await;
    assert_eq!(result.status(), 400);
    let body: Value = serde_json::from_str(result.body()).unwrap();
    assert_eq!(body["ok"], false);
    assert!(!body["error"].as_str().unwrap().is_empty());
    assert_eq!(processor.count(), 0);
    assert!(storage.0.lock().unwrap().scheduled.is_empty());
}

#[tokio::test]
async fn invalid_names_reject_before_lookup_launch_or_alarm() {
    let namespace = RecordingNamespace::default();
    assert_eq!(
        dispatch_task(&namespace, "", "17")
            .await
            .unwrap_err()
            .to_string(),
        "taskName must be a non-empty string"
    );
    assert_eq!(
        dispatch_task(&namespace, "", "")
            .await
            .unwrap_err()
            .to_string(),
        "taskName must be a non-empty string"
    );
    assert!(namespace.names.lock().unwrap().is_empty());
    let processor = DeferredProcessor::default();
    let storage = FakeAlarmStorage::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    let mut bodies = vec![json!({ "taskId": "17" })];
    bodies.extend(
        [
            json!(""),
            json!(null),
            json!(17),
            json!([]),
            json!({}),
            json!(false),
        ]
        .map(|name| json!({ "taskId": "17", "taskName": name })),
    );
    for body in bodies {
        let request = Request::post("/dispatch")
            .header("content-type", "application/json")
            .body(text_body(body.to_string()))
            .unwrap();
        assert_json(
            dispatcher.fetch(request).await,
            400,
            json!({
                "error": "invalid dispatch batch", "ok": false
            }),
        );
    }
    assert_eq!(processor.count(), 0);
    assert!(storage.0.lock().unwrap().scheduled.is_empty());
}

#[tokio::test]
async fn exact_names_round_trip_both_hops_without_forwarding_payloads() {
    for name in [
        "contract".to_owned(),
        " ".to_owned(),
        "Name/\"\\\n雪🦀".to_owned(),
        "x".repeat(1000),
    ] {
        let namespace = RecordingNamespace::default();
        namespace.stub.0.lock().unwrap().response =
            Some(Ok(response(200, text_body(r#"{"ok":true}"#))));
        dispatch_task(&namespace, &name, "opaque").await.unwrap();
        let body: Value =
            serde_json::from_str(namespace.stub.0.lock().unwrap().requests[0].body()).unwrap();
        assert_eq!(
            body,
            json!({ "tasks": [{ "task": { "kind": "published", "taskId": "opaque", "taskName": name }, "intent": "run" }] })
        );
        let incoming = body.clone();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        let request = Request::post("/dispatch")
            .header("content-type", "application/json")
            .body(text_body(incoming.to_string()))
            .unwrap();
        assert_json(dispatcher.fetch(request).await, 200, json!({ "ok": true }));
        wait_for(|| processor.count() == 1).await;
        assert_eq!(
            serde_json::from_str::<Value>(processor.0.lock().unwrap()[0].request.body()).unwrap(),
            json!({"task": {"kind": "published", "taskId": "opaque", "taskName": name}})
        );
        finish(&processor, 0).await;
    }
}

#[tokio::test]
async fn dispatch_only_repairs_a_missing_or_too_late_heartbeat_alarm() {
    for alarm in [
        None,
        Some(NOW - 1),
        Some(NOW),
        Some(NOW + 1),
        Some(NOW + 30_000),
        Some(NOW + 30_001),
    ] {
        let storage = FakeAlarmStorage::default();
        storage.0.lock().unwrap().alarm = alarm;
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
        let expected = if alarm.is_none_or(|at| at > NOW + 30_000) {
            vec![NOW + 30_000]
        } else {
            vec![]
        };
        for _ in [false, true] {
            {
                let mut state = storage.0.lock().unwrap();
                state.alarm = alarm;
                state.scheduled.clear();
            }
            let result = dispatcher.fetch(dispatch_request("task-1")).await;
            let body: Value = serde_json::from_str(result.body()).unwrap();
            assert_eq!(body, json!({ "ok": true }));
            assert_eq!(storage.0.lock().unwrap().scheduled, expected);
        }
        finish(&processor, 0).await;
    }
}

#[tokio::test]
async fn rejected_processor_fetches_and_body_reads_permit_explicit_redispatch() {
    for status in [None, Some(200), Some(503)] {
        let logs = Logs::default();
        let _subscriber = logs.capture();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        dispatcher.fetch(dispatch_request("task-1")).await;
        wait_for(|| processor.count() == 1).await;
        if let Some(status) = status {
            let body: TextBody = Box::pin(async { Err("body read rejected".into()) });
            processor.complete(0, Ok(response(status, body)));
        } else {
            processor.complete(0, Err("fetch rejected".into()));
        }
        wait_for(|| logs.0.lock().unwrap().len() == 1).await;
        assert_eq!(
            logs.0.lock().unwrap()[0]["error"],
            if status.is_some() {
                "body read rejected"
            } else {
                "fetch rejected"
            }
        );
        assert_json(
            dispatcher.fetch(dispatch_request("task-1")).await,
            200,
            json!({ "ok": true }),
        );
        finish(&processor, 1).await;
    }
}

#[tokio::test]
async fn dispatch_propagates_namespace_fetch_and_body_read_errors() {
    let namespace = RecordingNamespace {
        error: Some("namespace unavailable"),
        ..Default::default()
    };
    assert_eq!(
        dispatch_task(&namespace, "contract", "id")
            .await
            .unwrap_err()
            .to_string(),
        "namespace unavailable"
    );
    for status in [None, Some(200), Some(503)] {
        let namespace = RecordingNamespace::default();
        let error: BoxDispatchError = std::io::Error::other("transport or body failed").into();
        namespace.stub.0.lock().unwrap().response = Some(if let Some(status) = status {
            Ok(response(status, Box::pin(async { Err(error) })))
        } else {
            Err(error)
        });
        let error = dispatch_task(&namespace, "contract", "id")
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "transport or body failed");
        assert!(error.downcast_ref::<std::io::Error>().is_some());
    }
}

#[tokio::test]
async fn diagnostic_truncation_is_unicode_safe_and_never_limits_body_consumption() {
    for (text, expected) in [
        ("a".repeat(600), "a".repeat(500)),
        ("雪".repeat(600), "雪".repeat(500)),
        (format!("{}😀tail", "a".repeat(499)), "a".repeat(499)),
        (format!("{}tail", "😀".repeat(250)), "😀".repeat(250)),
    ] {
        let namespace = RecordingNamespace::default();
        let (body, control) = controlled_body();
        namespace.stub.0.lock().unwrap().response = Some(Ok(response(503, body)));
        control.sender.send(Ok(text)).unwrap();
        assert_eq!(
            dispatch_task(&namespace, "contract", "id")
                .await
                .unwrap_err()
                .to_string(),
            format!("task dispatcher returned HTTP 503: {expected}")
        );
        assert!(control.finished.load(Ordering::SeqCst));
    }
    let prefix = "task processor returned HTTP 500: ";
    for status in [None, Some(500)] {
        let logs = Logs::default();
        let _subscriber = logs.capture();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        dispatcher.fetch(dispatch_request("id")).await;
        wait_for(|| processor.count() == 1).await;
        let prefix = if status.is_some() { prefix } else { "" };
        let text = format!("{}😀tail", "a".repeat(499 - prefix.len()));
        processor.complete(
            0,
            if let Some(status) = status {
                Ok(response(status, text_body(text)))
            } else {
                Err(text.into())
            },
        );
        wait_for(|| logs.0.lock().unwrap().len() == 1).await;
        assert_eq!(
            logs.0.lock().unwrap()[0]["error"],
            format!("{prefix}{}", "a".repeat(499 - prefix.len()))
        );
    }
}

#[tokio::test]
async fn request_read_errors_and_storage_errors_use_the_existing_error_envelope() {
    let processor = DeferredProcessor::default();
    let storage = FakeAlarmStorage::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    let body: TextBody = Box::pin(async { Err(format!("{}😀tail", "a".repeat(499)).into()) });
    let request = Request::post("/dispatch")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    assert_json(
        dispatcher.fetch(request).await,
        400,
        json!({ "error": "a".repeat(499), "ok": false }),
    );
    assert_eq!(processor.count(), 0);
    assert!(storage.0.lock().unwrap().scheduled.is_empty());
    for get in [true, false] {
        {
            let mut state = storage.0.lock().unwrap();
            state.get_error = get.then_some("storage failed");
            state.set_error = (!get).then_some("storage failed");
        }
        assert_json(
            dispatcher.fetch(dispatch_request("id")).await,
            400,
            json!({ "error": "storage failed", "ok": false }),
        );
    }
    assert_eq!(
        dispatcher.alarm().await.unwrap_err().to_string(),
        "storage failed"
    );
    {
        let mut state = storage.0.lock().unwrap();
        state.set_error = None;
        state.get_error = None;
    }
    assert_json(
        dispatcher.fetch(dispatch_request("id")).await,
        200,
        json!({ "ok": true }),
    );
    finish(&processor, 0).await;
}

#[tokio::test]
async fn launch_precedes_heartbeat_storage_and_never_persists_acceptance() {
    let storage = FakeAlarmStorage::default();
    let (release, gate) = oneshot::channel();
    storage.0.lock().unwrap().get_gate = Some(gate);
    let processor = DeferredProcessor::default();
    let dispatcher = Arc::new(RetainedTaskDispatcher::with_clock(
        storage.clone(),
        processor.clone(),
        now,
    ));
    let acceptance = tokio::spawn({
        let dispatcher = dispatcher.clone();
        async move { dispatcher.fetch(dispatch_request("id")).await }
    });
    wait_for(|| storage.0.lock().unwrap().get_gate.is_none()).await;
    assert!(!acceptance.is_finished());
    wait_for(|| processor.count() == 1).await;
    assert_eq!(storage.0.lock().unwrap().transactions, 0);
    release.send(()).unwrap();
    assert_json(acceptance.await.unwrap(), 200, json!({ "ok": true }));
    assert_json(
        dispatcher.fetch(dispatch_request("id")).await,
        200,
        json!({ "ok": true }),
    );
    assert_json(
        dispatcher.fetch(dispatch_request("other")).await,
        200,
        json!({ "ok": true }),
    );
    finish(&processor, 0).await;
    finish(&processor, 1).await;
    assert_eq!(storage.0.lock().unwrap().transactions, 0);
    assert!(storage.0.lock().unwrap().durable.metadata.is_none());
}

// Native-only coverage of the unwind guard, rather than a claim that wasm traps are recoverable.
#[tokio::test]
async fn native_processor_panics_are_logged_and_permit_explicit_redispatch() {
    struct PanickingProcessor;
    impl ProcessorFetcher for PanickingProcessor {
        async fn fetch(&self, _: Request<String>) -> IoResult<Response<TextBody>> {
            panic!("test transport panic");
        }
    }
    let logs = Logs::default();
    let _subscriber = logs.capture();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), PanickingProcessor, now);
    for count in 1..=2 {
        assert_json(
            dispatcher.fetch(dispatch_request("id")).await,
            200,
            json!({ "ok": true }),
        );
        wait_for(|| logs.0.lock().unwrap().len() == count).await;
        tokio::task::yield_now().await;
        assert_eq!(
            logs.0.lock().unwrap()[count - 1]["error"],
            "processor task exited without an observed response"
        );
    }
}

#[tokio::test]
async fn failed_alarm_bookkeeping_rolls_back_launches_and_rearms_from_current_records() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    seed_schedule(&storage, "contract");
    dispatcher.alarm().await.unwrap();
    reply(
        &processor,
        0,
        json!({"type": "retryAt", "atMs": NOW + 1_000}),
    )
    .await;
    advance(1_000);
    storage.0.lock().unwrap().set_failures = 1;
    assert_eq!(
        dispatcher.alarm().await.unwrap_err().to_string(),
        "alarm storage failed"
    );
    assert_eq!(processor.count(), 1);
    assert!(matches!(
        stored_task(&storage, "id").unwrap().state,
        bellows::cloudflare::TaskSchedule::Pending
    ));
    assert_eq!(storage.0.lock().unwrap().alarm, Some(scheduler_now()));
    dispatcher.alarm().await.unwrap();
    wait_for(|| processor.count() == 2).await;
    finish(&processor, 1).await;
}

#[tokio::test]
async fn result_persistence_is_awaited_before_retiring_local_tracking() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    seed_schedule(&storage, "contract");
    dispatcher.alarm().await.unwrap();
    let (release, gate) = oneshot::channel();
    storage.0.lock().unwrap().get_gate = Some(gate);
    reply(&processor, 0, json!({"type": "done"})).await;
    wait_for(|| storage.0.lock().unwrap().get_gate.is_none()).await;
    assert!(matches!(
        stored_task(&storage, "id").unwrap().state,
        bellows::cloudflare::TaskSchedule::Running { .. }
    ));
    assert_eq!(processor.count(), 1);
    release.send(()).unwrap();
    wait_for(|| stored_task(&storage, "id").is_none()).await;
}

#[tokio::test]
async fn stale_done_retry_and_body_errors_cannot_affect_an_id_accepted_again_after_completion() {
    for action in ["done", "retryAt", "body-error"] {
        let storage = FakeAlarmStorage::default();
        let processor = DeferredProcessor::default();
        let original = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
        seed_schedule(&storage, "contract");
        original.alarm().await.unwrap();
        wait_for(|| processor.count() == 1).await;
        let (body, control) = controlled_body();
        processor.complete(0, Ok(response(200, body)));
        wait_for(|| control.started.load(Ordering::SeqCst)).await;
        let successor = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
        successor.fetch(dispatch_request("id")).await;
        reply(&processor, 1, json!({"type": "done"})).await;
        assert!(stored_task(&storage, "id").is_none());
        successor.fetch(named_request("new", "id")).await;
        let current = stored_task(&storage, "id");
        control
            .sender
            .send(if action == "body-error" {
                Err("late body error".into())
            } else {
                Ok(json!({"task": {"kind": "published", "taskId": "id", "taskName": "contract"}, "nextAction": if action == "done" {
                    json!({"type": "done"})
                } else {
                    json!({"type": "retryAt", "atMs": 0})
                }})
                .to_string())
            })
            .unwrap();
        wait_for(|| control.finished.load(Ordering::SeqCst)).await;
        original.alarm().await.unwrap();
        assert_eq!(stored_task(&storage, "id"), current);
        assert_json(
            successor.fetch(named_request("wrong", "id")).await,
            200,
            json!({ "ok": true }),
        );
        wait_for(|| processor.count() == 3).await;
        finish(&processor, 2).await;
    }
}

#[tokio::test]
async fn later_results_and_duplicates_cannot_postpone_earlier_deadlines() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    for id in ["early", "late", "active"] {
        dispatcher.fetch(dispatch_request(id)).await;
    }
    reply(
        &processor,
        0,
        json!({"type": "retryAt", "atMs": NOW + 1_000}),
    )
    .await;
    reply(
        &processor,
        1,
        json!({"type": "retryAt", "atMs": NOW + 20_000}),
    )
    .await;
    dispatcher.fetch(dispatch_request("active")).await;
    dispatcher.alarm().await.unwrap();
    assert_eq!(storage.0.lock().unwrap().alarm, Some(NOW + 1_000));
    assert_eq!(processor.count(), 3);
}

thread_local! {
    static SCHEDULER_NOW: std::cell::Cell<i64> = const { std::cell::Cell::new(NOW) };
}

fn scheduler_now() -> i64 {
    SCHEDULER_NOW.get()
}

#[tokio::test]
async fn uncertain_scheduling_commit_retains_a_recoverable_schedule() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    assert_eq!(dispatcher.fetch(dispatch_request("id")).await.status(), 200);
    assert!(stored_task(&storage, "id").is_none());
    storage.0.lock().unwrap().uncertain_commit = true;
    reply(
        &processor,
        0,
        json!({"type": "retryAt", "atMs": NOW + 1_000}),
    )
    .await;
    assert!(matches!(
        stored_task(&storage, "id").unwrap().state,
        bellows::cloudflare::TaskSchedule::Pending
    ));
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    advance(1_000);
    dispatcher.alarm().await.unwrap();
    wait_for(|| processor.count() == 2).await;
    finish(&processor, 1).await;
}

fn advance(ms: i64) {
    SCHEDULER_NOW.set(scheduler_now() + ms);
}

fn stored_task(
    storage: &FakeAlarmStorage,
    id: &str,
) -> Option<bellows::cloudflare::DispatcherTask> {
    storage
        .0
        .lock()
        .unwrap()
        .durable
        .tasks
        .get(&format!("published:{id}"))
        .cloned()
}

async fn reply(processor: &DeferredProcessor, index: usize, action: Value) {
    wait_for(|| processor.count() > index).await;
    let id = serde_json::from_str::<Value>(processor.0.lock().unwrap()[index].request.body())
        .unwrap()["task"]
        .clone();
    processor.complete(
        index,
        Ok(response(
            200,
            text_body(json!({"task": id, "nextAction": action}).to_string()),
        )),
    );
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn future_hints_survive_reconstruction_and_preserve_independent_heartbeat() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    dispatcher.fetch(dispatch_request("id")).await;
    reply(
        &processor,
        0,
        json!({"type": "retryAt", "atMs": NOW + 10_000}),
    )
    .await;
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    dispatcher.alarm().await.unwrap();
    assert_eq!(storage.0.lock().unwrap().alarm, Some(NOW + 10_000));
    advance(9_999);
    dispatcher.alarm().await.unwrap();
    assert_eq!(processor.count(), 1);
    advance(1);
    dispatcher.alarm().await.unwrap();
    wait_for(|| processor.count() == 2).await;
    assert_eq!(storage.0.lock().unwrap().alarm, Some(NOW + 30_000));
    dispatcher.alarm().await.unwrap();
    assert_eq!(processor.count(), 2);
    reply(&processor, 1, json!({"type": "done"})).await;
    assert!(stored_task(&storage, "id").is_none());
    advance(25_000);
    dispatcher.alarm().await.unwrap();
    assert_eq!(
        storage.0.lock().unwrap().alarm,
        Some(scheduler_now() + 30_000)
    );
}

#[tokio::test]
async fn watchdog_recovers_hung_and_reconstructed_attempts_and_ignores_stale_results() {
    for reconstruct in [false, true] {
        SCHEDULER_NOW.set(NOW);
        let storage = FakeAlarmStorage::default();
        let processor = DeferredProcessor::default();
        let mut dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
        seed_schedule(&storage, "old");
        dispatcher.alarm().await.unwrap();
        wait_for(|| processor.count() == 1).await;
        let first = stored_task(&storage, "id").unwrap().state;
        if reconstruct {
            dispatcher = RetainedTaskDispatcher::with_clock(
                storage.clone(),
                processor.clone(),
                scheduler_now,
            );
        }
        advance(59_999);
        dispatcher.alarm().await.unwrap();
        assert_eq!(processor.count(), 1);
        advance(1);
        dispatcher.alarm().await.unwrap();
        assert_eq!(
            stored_task(&storage, "id").unwrap().next_attempt_at_ms,
            scheduler_now() + 1_000
        );
        advance(1_000);
        dispatcher.alarm().await.unwrap();
        wait_for(|| processor.count() == 2).await;
        assert_ne!(stored_task(&storage, "id").unwrap().state, first);
        reply(
            &processor,
            1,
            json!({"type": "retryAt", "atMs": scheduler_now() + 20_000}),
        )
        .await;
        if reconstruct {
            // The old delegate still has a response future; real eviction is not claimed here.
            reply(&processor, 0, json!({"type": "done"})).await;
        }
        assert_eq!(
            stored_task(&storage, "id").unwrap().next_attempt_at_ms,
            scheduler_now() + 20_000
        );
        dispatcher.fetch(named_request("corrected", "id")).await;
        dispatcher
            .fetch(named_request("must-not-replace", "id"))
            .await;
        wait_for(|| processor.count() == 3).await;
        assert_eq!(stored_task(&storage, "id").unwrap().task.name(), "old");
        assert_eq!(
            serde_json::from_str::<Value>(processor.0.lock().unwrap()[2].request.body()).unwrap()["task"]
                ["taskName"],
            "corrected"
        );
        reply(&processor, 2, json!({"type": "done"})).await;
        assert!(stored_task(&storage, "id").is_none());
        dispatcher.fetch(dispatch_request("id")).await;
        assert!(stored_task(&storage, "id").is_none());
        finish(&processor, 3).await;
    }
}

#[tokio::test]
async fn all_300_due_ids_launch_while_every_response_is_gated() {
    use bellows::cloudflare::{DispatcherTask, SchedulerMetadata, TaskSchedule};
    let storage = FakeAlarmStorage::default();
    {
        let mut state = storage.0.lock().unwrap();
        state.durable.metadata = Some(SchedulerMetadata {
            next_heartbeat_at_ms: NOW + 30_000,
            next_attempt_id: 0,
        });
        for id in 0..300 {
            state.durable.tasks.insert(
                format!("published:{id}"),
                DispatcherTask {
                    task: TaskIdentity::Published {
                        task_id: id.to_string(),
                        task_name: if id % 2 == 0 { "first" } else { "second" }.into(),
                    },
                    next_attempt_at_ms: NOW + 1_000,
                    infrastructure_failures: 0,
                    state: TaskSchedule::Pending,
                },
            );
        }
    }
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    advance(35_000);
    dispatcher.alarm().await.unwrap();
    wait_for(|| processor.count() == 300).await;
    assert_eq!(
        storage.0.lock().unwrap().alarm,
        Some(scheduler_now() + 30_000)
    );
    dispatcher.alarm().await.unwrap();
    assert_eq!(processor.count(), 300);
    assert!(
        storage
            .0
            .lock()
            .unwrap()
            .durable
            .tasks
            .values()
            .all(|task| matches!(task.state, TaskSchedule::Running { .. }))
    );
}

#[tokio::test]
async fn uncertain_envelopes_retain_durable_tracking_and_back_off() {
    let mut invalid = vec![
        "".to_owned(),
        "{}".to_owned(),
        "{".to_owned(),
        "null".to_owned(),
        r#"{"taskId":"id","attemptFinished":true}"#.to_owned(),
        r#"{"taskId":"other","nextAction":{"type":"done"}}"#.to_owned(),
        r#"{"taskId":"id","nextAction":{"type":"unknown"}}"#.to_owned(),
    ];
    for task in [
        json!({ "kind": "published", "taskId": "other", "taskName": "contract" }),
        json!({ "kind": "published", "taskId": "id", "taskName": "contract " }),
        json!({ "kind": "singleton", "taskName": "contract" }),
        json!({ "kind": "published", "taskId": "id", "taskName": "contract", "extra": true }),
        json!({ "kind": "publish", "taskId": "id", "taskName": "contract" }),
    ] {
        invalid.push(json!({ "task": task, "nextAction": { "type": "done" } }).to_string());
    }
    for at in [
        json!(-1),
        json!(0.5),
        json!(8_640_000_000_000_001_i64),
        json!(9_007_199_254_740_992_i64),
        json!("1"),
        Value::Null,
    ] {
        invalid.push(
            json!({"task": {"kind": "published", "taskId": "id", "taskName": "contract"}, "nextAction": {"type": "retryAt", "atMs": at}}).to_string(),
        );
    }
    for body in invalid {
        let storage = FakeAlarmStorage::default();
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
        seed_schedule(&storage, "contract");
        dispatcher.alarm().await.unwrap();
        wait_for(|| processor.count() == 1).await;
        processor.complete(0, Ok(response(200, text_body(body))));
        wait_for(|| stored_task(&storage, "id").unwrap().infrastructure_failures == 1).await;
        assert_eq!(
            stored_task(&storage, "id").unwrap().next_attempt_at_ms,
            NOW + 1_000
        );
    }
}

#[tokio::test]
async fn backoff_survives_reconstruction_saturates_and_resets_on_valid_past_hint() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let mut dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    seed_schedule(&storage, "unknown");
    dispatcher.alarm().await.unwrap();
    for index in 0..8 {
        wait_for(|| processor.count() == index + 1).await;
        processor.complete(index, Ok(response(404, text_body("unknown"))));
        wait_for(|| {
            matches!(
                stored_task(&storage, "id").unwrap().state,
                bellows::cloudflare::TaskSchedule::Pending
            )
        })
        .await;
        let delay = (1_000 << index).min(30_000);
        assert_eq!(
            stored_task(&storage, "id").unwrap().next_attempt_at_ms,
            scheduler_now() + delay
        );
        assert_eq!(
            stored_task(&storage, "id").unwrap().infrastructure_failures,
            (index as u32 + 1).min(6)
        );
        dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
        advance(delay);
        dispatcher.alarm().await.unwrap();
    }
    reply(
        &processor,
        8,
        json!({"type": "retryAt", "atMs": scheduler_now() - 1}),
    )
    .await;
    assert_eq!(
        stored_task(&storage, "id").unwrap().infrastructure_failures,
        0
    );
    dispatcher.alarm().await.unwrap();
    wait_for(|| processor.count() == 10).await;
    processor.complete(9, Err("network".into()));
    wait_for(|| stored_task(&storage, "id").unwrap().infrastructure_failures == 1).await;
    assert_eq!(
        stored_task(&storage, "id").unwrap().next_attempt_at_ms,
        scheduler_now() + 1_000
    );
}

#[tokio::test]
async fn failed_result_persistence_retains_a_scheduled_attempts_watchdog() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    seed_schedule(&storage, "contract");
    dispatcher.alarm().await.unwrap();
    storage.0.lock().unwrap().set_error = Some("commit failed");
    reply(&processor, 0, json!({"type": "done"})).await;
    wait_for(|| storage.0.lock().unwrap().transactions == 2).await;
    assert!(matches!(
        stored_task(&storage, "id").unwrap().state,
        bellows::cloudflare::TaskSchedule::Running { .. }
    ));
    assert_eq!(storage.0.lock().unwrap().alarm, Some(NOW + 30_000));
    storage.0.lock().unwrap().set_error = None;
    let dispatcher =
        RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
    advance(60_000);
    dispatcher.alarm().await.unwrap();
    advance(1_000);
    dispatcher.alarm().await.unwrap();
    wait_for(|| processor.count() == 2).await;
    finish(&processor, 1).await;
}

#[tokio::test]
async fn corrupt_current_records_never_become_an_empty_queue() {
    use bellows::cloudflare::SchedulerMetadata;
    for metadata in [
        SchedulerMetadata {
            next_heartbeat_at_ms: -1,
            next_attempt_id: 0,
        },
        SchedulerMetadata {
            next_heartbeat_at_ms: NOW,
            next_attempt_id: u64::MAX,
        },
    ] {
        let storage = FakeAlarmStorage::default();
        storage.0.lock().unwrap().durable.metadata = Some(metadata.clone());
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), scheduler_now);
        assert_eq!(dispatcher.fetch(dispatch_request("id")).await.status(), 200);
        assert!(dispatcher.alarm().await.is_err());
        wait_for(|| processor.count() == 1).await;
        assert_eq!(storage.0.lock().unwrap().durable.metadata, Some(metadata));
    }
}
