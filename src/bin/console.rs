//! The console binary: the JSON facade and web UI for a running cluster.
//! Everything lives in `pipestream_search::console`; this is the entry
//! point.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!(
            "console --coordinator=host:port [--nodes=host:port,...] [--analysis=host:port] \
             [--listen=127.0.0.1:8600] [--allow-remote] [--ui-dir=<dir>] [--tls-ca=... \
             --tls-client-cert=... --tls-client-key=...] [--bearer-token-file=...]"
        );
        return Ok(());
    }
    let config = pipestream_search::console::ConsoleConfig::from_args(&args)?;
    let mut console = pipestream_search::console::Console::bind(config).await?;
    if let Some(dir) = pipestream_search::console::ui_dir_arg(&args) {
        console = console.with_ui_dir(&dir)?;
    }
    eprintln!("{}", console.describe());
    console.serve().await?;
    Ok(())
}
