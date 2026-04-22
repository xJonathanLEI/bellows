use bellows::{PublishTrigger, TaskDefinition};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

pub struct InteropEchoTask;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropEchoPayload {
    pub name: String,
}

impl TaskDefinition for InteropEchoTask {
    const NAME: &'static str = "postgres_interop_echo";

    type Callback = String;
    type Trigger = PublishTrigger<InteropEchoPayload>;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum InteropProcessEvent {
    Ready,
    Published {
        #[serde(rename = "taskId")]
        task_id: u64,
        name: String,
    },
    Processed {
        #[serde(rename = "taskId")]
        task_id: u64,
        name: String,
    },
    Awaited {
        #[serde(rename = "taskId")]
        task_id: u64,
        name: String,
    },
}

pub fn emit_event(event: &InteropProcessEvent) {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, event).expect("failed to serialize interop process event");
    stdout
        .write_all(b"\n")
        .expect("failed to write interop process event newline");
    stdout
        .flush()
        .expect("failed to flush interop process event");
}
