use opentelemetry::sdk::trace::Tracer;
use opentelemetry::{global, trace::{TraceError, TracerProvider}};
use opentelemetry::sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::Layered;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Registry;

pub fn init_tracer(service_name: String) -> Result<(), TraceError> {
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(service_name)
        //.with_agent_endpoint("127.0.0.1:6831")
        .install_simple()?;
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(telemetry);
    global::set_text_map_propagator(TraceContextPropagator::new());
    tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");

    Ok(())
}
