use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

pub fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_line_number(true)
        .with_file(true) // Enable line numbers
        .with_span_events(FmtSpan::CLOSE)
        .init();
}
