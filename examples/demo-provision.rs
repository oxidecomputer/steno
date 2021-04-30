/*!
 * Command-line tool for demo'ing saga interfaces
 */

use anyhow::bail;
use anyhow::Context;
use slog::Drain;
use slog::Logger;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use steno::make_example_provision_saga;
use steno::ExampleContext;
use steno::ExampleParams;
use steno::SagaId;
use steno::SagaStateView;
use steno::SagaTemplateGeneric;
use structopt::StructOpt;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subcmd = Demo::from_args();
    match subcmd {
        Demo::Dot => cmd_dot().await,
        Demo::Info => cmd_info().await,
        // XXX need to test print log but I can't do that until I fix Run
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

fn make_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog::LevelFilter(drain, slog::Level::Warning).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

fn make_sec(log: &slog::Logger) -> steno::Sec {
    steno::Sec::new(
        log.new(slog::o!()),
        Arc::new(steno::InMemorySecStore::new()),
    )
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
    let log = make_log();
    let mut sec = make_sec(&log);

    let saga_template = make_example_provision_saga();
    println!("*** saga template definition ***");
    println!("saga template graph: ");
    println!("{}", saga_template.metadata().dot());

    println!("*** initial state ***");
    let saga_id = make_saga_id();
    sec
        .saga_create(
            Arc::new(ExampleContext::default()),
            saga_id,
            saga_template,
            "demo-provision".to_string(),
            ExampleParams { instance_name: "fake-o instance".to_string() },
        )
        .await
        .unwrap();

    let saga = sec.saga_get_state(saga_id).await.unwrap();
    let status = saga.state.status();
    println!("{}", status);
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
    let saga_recovered =
        steno::SagaRecovered::read(file).context("reading log")?;
    println!("{:?}", saga_recovered.log);
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
}

async fn cmd_run(args: &RunArgs) -> Result<(), anyhow::Error> {
    let log = make_log();
    let mut sec = make_sec(&log);
    let saga_template = make_example_provision_saga();
    let template_name = "example-template".to_string();
    let uctx = Arc::new(ExampleContext::default());
    let saga_id = if let Some(input_log_path) = &args.recover_from {
        if !args.quiet {
            println!("recovering from log: {}", input_log_path.display());
        }

        let file = reader_for_log_input(input_log_path)?;
        let saga_recovered =
            steno::SagaRecovered::read(file).context("reading saga state")?;
        let saga_id = saga_recovered.saga_id;
        sec
            .saga_resume(
                uctx,
                saga_id,
                saga_template.clone() as Arc<dyn SagaTemplateGeneric<_>>,
                saga_recovered.params,
                saga_recovered.log,
            )
            .context("resuming saga");
        let saga = sec
            .saga_get_state(saga_id)
            .await
            .context("fetching newly-created saga")?;
        if !args.quiet {
            print!("recovered state\n");
            println!("{}", saga.state.status());
        }
        saga_id
    } else {
        let saga_id = make_saga_id();
        sec
            .saga_create(
                uctx,
                saga_id,
                saga_template.clone(),
                template_name,
                ExampleParams { instance_name: "fake-o instance".to_string() },
            )
            .await?;
        saga_id
    };

    for node_name in &args.inject_error {
        let node_id =
            saga_template.metadata().node_for_name(&node_name).with_context(
                || format!("bad argument for --inject-error: {:?}", node_name),
            )?;
        sec
            .saga_inject_error(saga_id, node_id)
            .await
            .context("injecting error");
        if !args.quiet {
            println!("will inject error at node \"{}\"", node_name);
        }
    }

    if !args.quiet {
        println!("*** running saga ***");
    }
    // XXX what's the equivalent here?  await on the sec?  Do we need to
    // "close" it first or something?
    // exec.run().await;

    let saga = sec
        .saga_get_state(saga_id)
        .await
        .context("fetching saga after running it")?;
    if !args.quiet {
        println!("*** finished saga ***");
        println!("\n*** final state ***");
        println!("{}", saga.state.status());
    }

    match &saga.state {
        SagaStateView::Done { result, .. } => &result,
        _ => bail!("saga's final state was not finished"),
    };

    // XXX use a file-based store instead?
    if let Some(output_log_path) = &args.dump_to {
        let serialized = saga.serialized();
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
        serde_json::to_writer_pretty(out, &serialized)
            .with_context(|| format!("save output log {}", label))?;
        if !args.quiet {
            println!("dumped log to {}", label);
        }
    }

    Ok(())
}
