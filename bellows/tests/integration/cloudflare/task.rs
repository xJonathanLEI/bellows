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
