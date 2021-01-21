/*!
 * Command-line tool for demo'ing saga interfaces
 */

use anyhow::Context;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use steno::make_provision_saga;
use steno::SagaExecutor;
use steno::SagaLog;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subcmd = Demo::from_args();
    match subcmd {
        Demo::Dot => cmd_dot().await,
        Demo::Info => cmd_info().await,
        Demo::PrintLog { ref print_log_args } => {
            cmd_print_log(print_log_args).await
        }
        Demo::Run { ref run_args } => cmd_run(run_args).await,
    }
}

/// Demo saga implementation
#[derive(Debug, StructOpt)]
enum Demo {
    /// Dump a dot (graphviz) representation of the saga graph
    Dot,

    /// Dump information about the saga graph (not an execution)
    Info,

    /// Pretty-print the log from a previous execution
    PrintLog {
        #[structopt(flatten)]
        print_log_args: PrintLogArgs,
    },

    /// Execute the saga
    Run {
        #[structopt(flatten)]
        run_args: RunArgs,
    },
}

/*
 * "dot" subcommand
 */

async fn cmd_dot() -> Result<(), anyhow::Error> {
    let saga_template = make_provision_saga();
    println!("{}", saga_template.dot());
    Ok(())
}

/*
 * "info" subcommand
 */

async fn cmd_info() -> Result<(), anyhow::Error> {
    let saga_template = make_provision_saga();
    eprintln!("*** saga template definition ***");
    eprintln!("saga template graph: ");
    eprintln!("{}", saga_template.dot());

    eprintln!("*** initial state ***");
    let exec = SagaExecutor::new(saga_template, "provision-info");
    eprintln!("{}", exec.status().await);
    Ok(())
}

/*
 * "print-log" subcommand
 */

#[derive(Debug, StructOpt)]
struct PrintLogArgs {
    /// path to the saga log to pretty-print
    input_log_path: PathBuf,
}

async fn cmd_print_log(args: &PrintLogArgs) -> Result<(), anyhow::Error> {
    let input_log_path = &args.input_log_path;
    let file = fs::File::open(input_log_path).with_context(|| {
        format!("open recovery log \"{}\"", input_log_path.display())
    })?;
    let sglog = SagaLog::load("unused", file).with_context(|| {
        format!("load log \"{}\"", input_log_path.display())
    })?;
    eprintln!("{:?}", sglog);
    Ok(())
}

/*
 * "run" subcommand
 */

#[derive(Debug, StructOpt)]
struct RunArgs {
    /// simulate an error at the named saga node
    #[structopt(long)]
    inject_error: Vec<String>,

    /// upon completion, dump the workflog log to the named file
    #[structopt(long)]
    dump_to: Option<PathBuf>,

    /// recover the saga log from the named file and resume execution
    #[structopt(long)]
    recover_from: Option<PathBuf>,

    /// "creator" attribute used when creating new log entries
    #[structopt(long, default_value = "demo-provision")]
    creator: String,
}

async fn cmd_run(args: &RunArgs) -> Result<(), anyhow::Error> {
    let saga_template = make_provision_saga();
    let exec = if let Some(input_log_path) = &args.recover_from {
        eprintln!("recovering from log: {}", input_log_path.display());

        let file = fs::File::open(&input_log_path).with_context(|| {
            format!("open recovery log \"{}\"", input_log_path.display())
        })?;
        let sglog = SagaLog::load(&args.creator, file).with_context(|| {
            format!("load log \"{}\"", input_log_path.display())
        })?;
        let exec = SagaExecutor::new_recover(
            Arc::clone(&saga_template),
            sglog,
            &args.creator,
        )
        .with_context(|| {
            format!("recover log \"{}\"", input_log_path.display())
        })?;

        eprint!("recovered state\n");
        eprintln!("{}", exec.status().await);
        eprintln!("");
        exec
    } else {
        SagaExecutor::new(Arc::clone(&saga_template), &args.creator)
    };

    for node_name in &args.inject_error {
        let node_id =
            saga_template.node_for_name(&node_name).with_context(|| {
                format!("bad argument for --inject-error: {:?}", node_name)
            })?;
        exec.inject_error(node_id).await;
        eprintln!("will inject error at node \"{}\"", node_name);
    }

    eprintln!("*** running saga ***");
    exec.run().await;
    eprintln!("*** finished saga ***");

    eprintln!("\n*** final state ***");
    eprintln!("{}", exec.status().await);

    if let Some(output_log_path) = &args.dump_to {
        let result = exec.result();
        let log = result.sglog;
        let out = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_log_path)
            .with_context(|| {
                format!("open output log \"{}\"", output_log_path.display())
            })?;
        log.dump(out).with_context(|| {
            format!("save output log \"{}\"", output_log_path.display())
        })?;
        eprintln!("dumped log to \"{}\"", output_log_path.display());
    }

    Ok(())
}
