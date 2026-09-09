use bellows::{PublishTrigger, TaskDefinition};
use serde::{Deserialize, Serialize};

pub struct GreetingTask;

#[derive(Serialize, Deserialize)]
pub struct GreetingPayload {
    pub name: String,
}

impl TaskDefinition for GreetingTask {
    const NAME: &'static str = "cloudflare_greeting";
    type Callback = ();
    type Trigger = PublishTrigger<GreetingPayload>;
}
