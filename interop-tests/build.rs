use std::path::Path;
use std::process::{Command, Stdio};

fn main() {
    println!("cargo:rerun-if-changed=../bellows-ts/package.json");
    println!("cargo:rerun-if-changed=../bellows-ts/tsconfig.json");
    println!("cargo:rerun-if-changed=../bellows-ts/src");
    println!("cargo:rerun-if-changed=../package.json");
    println!("cargo:rerun-if-changed=../pnpm-lock.yaml");
    println!("cargo:rerun-if-changed=../pnpm-workspace.yaml");

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("interop-tests manifest directory should have a workspace parent");

    let status = Command::new("pnpm")
        .args(["--filter", "@xjonathanlei/bellows", "build"])
        .current_dir(workspace_root)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("failed to run pnpm to build TypeScript interop targets");

    assert!(
        status.success(),
        "building TypeScript interop targets failed"
    );
}
