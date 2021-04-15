/*!
 * Command-line tool for demo'ing saga interfaces
 */

use anyhow::Context;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use steno::make_example_provision_saga;
use steno::ExampleContext;
use steno::ExampleParams;
use steno::SagaExecutor;
use steno::NullSink;
use steno::SagaId;
use steno::SagaLog;
use structopt::StructOpt;
use uuid::Uuid;

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
 * We use a hardcoded SagaId for ease of automated testing. See the note in
 * demo_prov_server_alloc().
 */
fn make_saga_id() -> SagaId {
    SagaId(Uuid::parse_str("049b2522-308d-442e-bc65-9bfaef863597").unwrap())
}

fn reader_for_log_input(
    path: &Path,
) -> Result<Box<dyn io::Read>, anyhow::Error> {
    if *path == PathBuf::from("-") {
        Ok(Box::new(io::stdin()))
    } else {
        Ok(Box::new(fs::File::open(&path).with_context(|| {
            format!("open recovery log \"{}\"", path.display())
        })?))
    }
}

/*
 * "dot" subcommand
 */

async fn cmd_dot() -> Result<(), anyhow::Error> {
    let saga_template = make_example_provision_saga();
    println!("{}", saga_template.metadata().dot());
    Ok(())
}

/*
 * "info" subcommand
 */

async fn cmd_info() -> Result<(), anyhow::Error> {
    let saga_template = make_example_provision_saga();
    println!("*** saga template definition ***");
    println!("saga template graph: ");
    println!("{}", saga_template.metadata().dot());

    println!("*** initial state ***");
    let exec = SagaExecutor::new(
        &make_saga_id(),
        saga_template,
        "provision-info",
        Arc::new(ExampleContext::default()),
        ExampleParams { instance_name: "fake-o instance".to_string() },
        Arc::new(NullSink),
    )
    .unwrap();
    println!("{}", exec.status().await);
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
    let file = reader_for_log_input(&input_log_path)?;
    let sglog = SagaLog::load("unused", file).with_context(|| {
        format!("load log \"{}\"", input_log_path.display())
    })?;
    println!("{:?}", sglog);
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

    /// do not print to stdout
    #[structopt(long)]
    quiet: bool,

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
    let saga_template = make_example_provision_saga();
    let exec = if let Some(input_log_path) = &args.recover_from {
        if !args.quiet {
            println!("recovering from log: {}", input_log_path.display());
        }

        let file = reader_for_log_input(input_log_path)?;
        let sglog = SagaLog::load(&args.creator, file).with_context(|| {
            format!("load log \"{}\"", input_log_path.display())
        })?;
        let exec = SagaExecutor::new_recover(
            Arc::clone(&saga_template),
            sglog,
            &args.creator,
            Arc::new(ExampleContext::default()),
            Arc::new(NullSink),
        )
        .with_context(|| {
            format!("recover log \"{}\"", input_log_path.display())
        })?;

        if !args.quiet {
            print!("recovered state\n");
            println!("{}", exec.status().await);
            println!("");
        }
        exec
    } else {
        SagaExecutor::new(
            &make_saga_id(),
            Arc::clone(&saga_template),
            &args.creator,
            Arc::new(ExampleContext::default()),
            ExampleParams { instance_name: "fake-o instance".to_string() },
            Arc::new(NullSink),
        )
        .unwrap()
    };

    for node_name in &args.inject_error {
        let node_id =
            saga_template.metadata().node_for_name(&node_name).with_context(
                || format!("bad argument for --inject-error: {:?}", node_name),
            )?;
        exec.inject_error(node_id).await;
        if !args.quiet {
            println!("will inject error at node \"{}\"", node_name);
        }
    }

    if !args.quiet {
        println!("*** running saga ***");
    }
    exec.run().await;
    if !args.quiet {
        println!("*** finished saga ***");
        println!("\n*** final state ***");
        println!("{}", exec.status().await);
    }

    if let Some(output_log_path) = &args.dump_to {
        let result = exec.result();
        let log = result.saga_log;
        let (mut stdout_holder, mut file_holder);
        let (label, out): (String, &mut dyn io::Write) = if *output_log_path
            == PathBuf::from("-")
        {
            stdout_holder = io::stdout();
            (String::from("stdout"), &mut stdout_holder)
        } else {
            file_holder = fs::OpenOptions::new()
                .write(true)
                .open(output_log_path)
                .with_context(|| {
                    format!("open output log \"{}\"", output_log_path.display())
                })?;
            (format!("\"{}\"", output_log_path.display()), &mut file_holder)
        };
        log.dump(out).with_context(|| format!("save output log {}", label))?;
        if !args.quiet {
            println!("dumped log to {}", label);
        }
    }

    Ok(())
}
