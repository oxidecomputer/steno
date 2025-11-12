//! Command-line tool for demo'ing saga interfaces

use anyhow::anyhow;
use anyhow::Context;
use slog::Drain;
use std::convert::TryFrom;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use steno::load_example_actions;
use steno::make_example_provision_dag;
use steno::ActionRegistry;
use steno::ExampleContext;
use steno::ExampleParams;
use steno::ExampleSagaType;
use steno::SagaDag;
use steno::SagaId;
use steno::SagaLog;
use steno::SagaResultErr;
use steno::SagaSerialized;
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
#[structopt(no_version)]
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

// We use a hardcoded SagaId for ease of automated testing. See the note in
// demo_prov_server_alloc().
fn make_saga_id() -> SagaId {
    SagaId(Uuid::parse_str("049b2522-308d-442e-bc65-9bfaef863597").unwrap())
}

fn make_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog::LevelFilter(drain, slog::Level::Warning).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

fn make_sec(log: &slog::Logger) -> steno::SecClient {
    steno::sec(log.new(slog::o!()), Arc::new(steno::InMemorySecStore::new()))
}

fn reader_for_log_input(
    path: &Path,
) -> Result<Box<dyn io::Read>, anyhow::Error> {
    if path == Path::new("-") {
        Ok(Box::new(io::stdin()))
    } else {
        Ok(Box::new(fs::File::open(&path).with_context(|| {
            format!("open recovery log \"{}\"", path.display())
        })?))
    }
}

fn read_saga_state<R: io::Read>(
    reader: R,
) -> Result<SagaSerialized, anyhow::Error> {
    serde_json::from_reader(reader).context("reading saga state")
}

fn make_example_action_registry() -> Arc<ActionRegistry<ExampleSagaType>> {
    let mut registry = ActionRegistry::new();
    load_example_actions(&mut registry);
    Arc::new(registry)
}

// "dot" subcommand

async fn cmd_dot() -> Result<(), anyhow::Error> {
    let params = ExampleParams {
        instance_name: "fake-o-instance".to_string(),
        number_of_instances: 1,
    };
    let dag = make_example_provision_dag(params);
    println!("{}", dag.dot());
    Ok(())
}

// "info" subcommand

async fn cmd_info() -> Result<(), anyhow::Error> {
    let log = make_log();
    let sec = make_sec(&log);

    let registry = make_example_action_registry();
    let params = ExampleParams {
        instance_name: "fake-o instance".to_string(),
        number_of_instances: 1,
    };
    let dag = make_example_provision_dag(params);
    println!("*** saga dag definition ***");
    println!("saga graph: ");
    println!("{}", dag.dot());

    println!("*** initial state ***");
    let saga_id = make_saga_id();
    let uctx = Arc::new(ExampleContext {});
    let _unused_future = sec.saga_create(saga_id, uctx, dag, registry).await?;

    let saga = sec.saga_get(saga_id).await.unwrap();
    let status = saga.state.status();
    println!("{}", status);

    Ok(())
}

// "print-log" subcommand

#[derive(Debug, StructOpt)]
struct PrintLogArgs {
    /// path to the saga log to pretty-print
    input_log_path: PathBuf,
}

async fn cmd_print_log(args: &PrintLogArgs) -> Result<(), anyhow::Error> {
    let input_log_path = &args.input_log_path;
    let file = reader_for_log_input(&input_log_path)?;
    let saga_serialized = read_saga_state(file)?;
    let saga_log = SagaLog::try_from(saga_serialized)?;
    println!("{:?}", saga_log.pretty());
    Ok(())
}

// "run" subcommand

#[derive(Debug, StructOpt)]
struct RunArgs {
    /// simulate an error at the named saga node
    #[structopt(long)]
    inject_error: Vec<String>,

    /// simulate an error at the named saga node's undo action
    #[structopt(long)]
    inject_undo_error: Vec<String>,

    /// do not print to stdout
    #[structopt(long)]
    quiet: bool,

    /// upon completion, dump the workflog log to the named file
    #[structopt(long)]
    dump_to: Option<PathBuf>,

    /// recover the saga log from the named file and resume execution
    #[structopt(long)]
    recover_from: Option<PathBuf>,
}

async fn cmd_run(args: &RunArgs) -> Result<(), anyhow::Error> {
    let log = make_log();
    let sec = make_sec(&log);
    let registry = make_example_action_registry();
    let uctx = Arc::new(ExampleContext {});
    let (saga_id, future, dag) =
        if let Some(input_log_path) = &args.recover_from {
            if !args.quiet {
                println!("recovering from log: {}", input_log_path.display());
            }

            let file = reader_for_log_input(input_log_path)?;
            let saga_recovered = read_saga_state(file)?;
            let saga_id = saga_recovered.saga_id;
            let future = sec
                .saga_resume(
                    saga_id,
                    uctx,
                    saga_recovered.dag.clone(),
                    registry,
                    saga_recovered.events,
                )
                .await
                .context("resuming saga")?;
            let saga = sec.saga_get(saga_id).await.map_err(|_: ()| {
                anyhow!("failed to fetch newly-created saga")
            })?;
            if !args.quiet {
                print!("recovered state\n");
                println!("{}", saga.state.status());
                println!("");
            }
            let dag: Arc<SagaDag> =
                Arc::new(serde_json::from_value(saga_recovered.dag)?);
            (saga_id, future, dag)
        } else {
            let params = ExampleParams {
                instance_name: "fake-o instance".to_string(),
                number_of_instances: 1,
            };
            let dag = make_example_provision_dag(params);
            let saga_id = make_saga_id();
            let future =
                sec.saga_create(saga_id, uctx, dag.clone(), registry).await?;
            (saga_id, future, dag)
        };

    for node_name in &args.inject_error {
        let node_id = dag.get_index(node_name).with_context(|| {
            format!("bad argument for --inject-error: {:?}", node_name)
        })?;
        sec.saga_inject_error(saga_id, node_id)
            .await
            .context("injecting error")?;
        if !args.quiet {
            println!("will inject error at node \"{}\"", node_name);
        }
    }

    for node_name in &args.inject_undo_error {
        let node_id = dag.get_index(node_name).with_context(|| {
            format!("bad argument for --inject-undo-error: {:?}", node_name)
        })?;
        sec.saga_inject_error_undo(saga_id, node_id)
            .await
            .context("injecting error")?;
        if !args.quiet {
            println!("will inject error at node \"{}\" undo action", node_name);
        }
    }

    if !args.quiet {
        println!("*** running saga ***");
    }

    sec.saga_start(saga_id).await.expect("failed to start saga");
    let result = future.await;
    assert_eq!(saga_id, result.saga_id);

    let saga = sec
        .saga_get(saga_id)
        .await
        .map_err(|_: ()| anyhow!("failed to fetch saga after running it"))?;
    if !args.quiet {
        println!("*** finished saga ***");
        println!("\n*** final state ***");
        println!("{}", saga.state.status());

        print!("result: ");
        match result.kind {
            Ok(success_case) => {
                println!("SUCCESS");
                println!(
                    "final output: {:?}",
                    success_case.saga_output::<String>().unwrap()
                );
            }
            Err(SagaResultErr {
                error_node_name,
                error_source,
                undo_failure,
            }) => {
                println!("ACTION FAILURE");
                println!("failed at node:    {:?}", error_node_name);
                println!("failed with error: {:#}", error_source);
                if let Some((undo_node_name, undo_error)) = undo_failure {
                    println!("FOLLOWED BY UNDO ACTION FAILURE");
                    println!("failed at node:    {:?}", undo_node_name);
                    println!("failed with error: {:#}", undo_error);
                }
            }
        }
    }

    if let Some(output_log_path) = &args.dump_to {
        let serialized = saga.serialized();
        let (mut stdout_holder, mut file_holder);
        let (label, out): (String, &mut dyn io::Write) = if *output_log_path
            == Path::new("-")
        {
            stdout_holder = io::stdout();
            (String::from("stdout"), &mut stdout_holder)
        } else {
            file_holder = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(output_log_path)
                .with_context(|| {
                    format!("open output log \"{}\"", output_log_path.display())
                })?;
            (format!("\"{}\"", output_log_path.display()), &mut file_holder)
        };
        serde_json::to_writer_pretty(out, &serialized)
            .with_context(|| format!("save output log {}", label))?;
        if !args.quiet {
            println!("dumped log to {}", label);
        }
    }

    Ok(())
}
