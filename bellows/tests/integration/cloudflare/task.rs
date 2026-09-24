use bellows::{PublishTrigger, SingletonTrigger, TaskDefinition};
use serde::{Deserialize, Serialize};

pub struct SingletonTask;
impl TaskDefinition for SingletonTask {
    const NAME: &'static str = "cloudflare_singleton";
    type Callback = ();
    type Trigger = SingletonTrigger;
}

pub struct GreetingTask;

// Registered only by processors, deliberately absent from producer bootstrap configuration.
#[allow(dead_code)]
pub struct UnconfiguredSingletonTask;
impl TaskDefinition for UnconfiguredSingletonTask {
    const NAME: &'static str = " singleton:7 🦀 ";
    type Callback = ();
    type Trigger = SingletonTrigger;
}

#[derive(Serialize, Deserialize)]
pub struct GreetingPayload {
    pub name: String,
}

impl TaskDefinition for GreetingTask {
    const NAME: &'static str = "cloudflare_greeting";
    type Callback = ();
    type Trigger = PublishTrigger<GreetingPayload>;
}

pub struct FullNameTask;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FullNamePayload {
    pub first_name: String,
    pub last_name: String,
}

impl TaskDefinition for FullNameTask {
    const NAME: &'static str = "cloudflare_full_name";
    type Callback = ();
    type Trigger = PublishTrigger<FullNamePayload>;
}

pub struct SchedulingTask;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchedulingPayload {
    pub name: String,
    pub mode: SchedulingMode,
    pub available_from_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SchedulingMode {
    Failure,
    Success,
    Immediate,
}

impl TaskDefinition for SchedulingTask {
    const NAME: &'static str = "cloudflare_scheduling";
    type Callback = ();
    type Trigger = PublishTrigger<SchedulingPayload>;
}

// Test producers and workers accept absolute dates; Bellows' Rust API takes Instant.
pub fn deadline(at_ms: u64) -> bellows::time::Instant {
    use std::time::Duration;
    let (now_ms, now) = loop {
        let before = worker::Date::now().as_millis();
        let now = bellows::time::Instant::now();
        if before == worker::Date::now().as_millis() {
            break (before, now);
        }
    };
    if at_ms >= now_ms {
        now + Duration::from_millis(at_ms - now_ms)
    } else {
        now - Duration::from_millis(now_ms - at_ms)
    }
}
