//! Smoke tests for steno.  These aren't close to exhaustive, but tests that
//! it's not completely broken.

use expectorate::assert_contents;
use std::env::current_exe;
use std::path::PathBuf;
use steno::SagaSerialized;
use subprocess::Exec;
use subprocess::Redirection;

fn example_bin() -> PathBuf {
    // This is unfortunate, but it's the best way I know to run one of the
    // examples out of our project.
    let mut my_path = current_exe().expect("failed to find test program");
    my_path.pop();
    assert_eq!(my_path.file_name().unwrap(), "deps");
    my_path.pop();
    my_path.push("examples");
    my_path.push("demo-provision");
    my_path
}

fn run_example(test_name: &str, config_fn: impl Fn(Exec) -> Exec) -> String {
    let config = config_fn(Exec::cmd(example_bin()).stdout(Redirection::Pipe));
    let cmdline = config.to_cmdline_lossy();
    eprintln!("test \"{}\": run: {}", test_name, cmdline);
    config.capture().expect("failed to execute command").stdout_str()
}

#[test]
fn no_args() {
    assert_contents(
        "tests/test_smoke_no_args.out",
        &run_example("no_args", |exec| exec.stderr(Redirection::Merge)),
    );
}

#[test]
fn cmd_info() {
    assert_contents(
        "tests/test_smoke_info.out",
        &run_example("info", |exec| {
            exec.stderr(Redirection::Merge).arg("info")
        }),
    );
}

#[test]
fn cmd_dot() {
    assert_contents(
        "tests/test_smoke_dot.out",
        &run_example("dot", |exec| exec.stderr(Redirection::Merge).arg("dot")),
    );
}

#[test]
fn cmd_run_basic() {
    assert_contents(
        "tests/test_smoke_run_basic.out",
        &run_example("run_basic", |exec| exec.arg("run")),
    );
}

#[test]
fn cmd_run_error() {
    assert_contents(
        "tests/test_smoke_run_error.out",
        &run_example("run_error", |exec| {
            exec.arg("run").arg("--inject-error=instance_boot")
        }),
    );
}

#[test]
fn cmd_run_stuck() {
    assert_contents(
        "tests/test_smoke_run_stuck.out",
        &run_example("run_stuck", |exec| {
            exec.arg("run")
                .arg("--inject-error=instance_boot")
                .arg("--inject-undo-error=instance_id")
        }),
    );
}

#[test]
fn cmd_run_recover() {
    // Do a normal run and save the log so we can try recovering from it.
    let log = run_example("recover1", |exec| {
        exec.arg("run").arg("--dump-to=-").arg("--quiet")
    });

    // First, try recovery without having changed anything.
    let recovery_done = run_example("recover2", |exec| {
        exec.arg("run").arg("--recover-from=-").stdin(log.as_str())
    });
    assert_contents("tests/test_smoke_run_recover_done.out", &recovery_done);

    // Now try lopping off the last handful of records so there's work to do.
    let mut log_parsed: SagaSerialized =
        serde_json::from_str(&log).expect("failed to parse generated log");
    log_parsed.events.truncate(
        (log_parsed.events.len() - 5).clamp(0, log_parsed.events.len()),
    );
    let log_shortened = serde_json::to_string(&log_parsed).unwrap();
    assert_contents(
        "tests/test_smoke_run_recover_some.out",
        &run_example("recover3", |exec| {
            exec.arg("run")
                .arg("--recover-from=-")
                .stdin(log_shortened.as_str())
        }),
    );
}

#[test]
fn cmd_run_recover_unwind() {
    // Do a failed run and save the log so we can try recovering from it.
    let log = run_example("recover_fail1", |exec| {
        exec.arg("run")
            .arg("--dump-to=-")
            .arg("--quiet")
            .arg("--inject-error=instance_boot")
    });

    // First, try recovery without having changed anything.
    let recovery_done = run_example("recover_fail2", |exec| {
        exec.arg("run").arg("--recover-from=-").stdin(log.as_str())
    });
    assert_contents(
        "tests/test_smoke_run_recover_fail_done.out",
        &recovery_done,
    );

    // Now try lopping off the last handful of records so there's work to do.
    let mut log_parsed: SagaSerialized =
        serde_json::from_str(&log).expect("failed to parse generated log");
    log_parsed.events.truncate(
        (log_parsed.events.len() - 3).clamp(0, log_parsed.events.len()),
    );
    let log_shortened = serde_json::to_string(&log_parsed).unwrap();
    assert_contents(
        "tests/test_smoke_run_recover_fail_some.out",
        &run_example("recover_fail3", |exec| {
            exec.arg("run")
                .arg("--recover-from=-")
                .stdin(log_shortened.as_str())
        }),
    );
}

#[test]
fn cmd_run_recover_stuck() {
    // Do a failed run and save the log so we can try recovering from it.
    let log = run_example("recover_stuck1", |exec| {
        exec.arg("run")
            .arg("--dump-to=-")
            .arg("--quiet")
            .arg("--inject-error=instance_boot")
            .arg("--inject-undo-error=instance_id")
    });

    // First, try recovery without having changed anything.
    let recovery_done = run_example("recover_stuck2", |exec| {
        exec.arg("run").arg("--recover-from=-").stdin(log.as_str())
    });
    assert_contents(
        "tests/test_smoke_run_recover_stuck_done.out",
        &recovery_done,
    );
}
