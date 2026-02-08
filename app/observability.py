import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

socket_connections_active = Gauge(
    "sockethub_connections_active", "Active Socket.IO connections"
)
socket_join_auth_failures_total = Counter(
    "sockethub_join_auth_failures_total", "Join room auth failures"
)
socket_worker_events_forwarded_total = Counter(
    "sockethub_worker_events_forwarded_total",
    "Worker updates forwarded by event type",
    ["event_type"],
)
socket_duplicate_completed_dropped_total = Counter(
    "sockethub_duplicate_completed_dropped_total",
    "Duplicate completed events dropped",
)
socket_kafka_processing_errors_total = Counter(
    "sockethub_kafka_processing_errors_total",
    "Kafka listener/processing errors",
)
socket_posts_published_total = Counter(
    "sockethub_posts_published_total", "Posts published to Kafka"
)
socket_post_publish_failures_total = Counter(
    "sockethub_post_publish_failures_total",
    "Post publish failures",
)

_TRACING_INITIALIZED = False


def setup_tracing(app) -> None:
    global _TRACING_INITIALIZED
    if _TRACING_INITIALIZED:
        return
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    service_name = os.getenv("OTEL_SERVICE_NAME", "socket-hub")
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
    )
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
    _TRACING_INITIALIZED = True


def get_trace_context() -> dict[str, str | None]:
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if not span_context or not span_context.is_valid:
        return {"trace_id": None, "span_id": None, "parent_span_id": None}
    parent = span.parent
    return {
        "trace_id": f"{span_context.trace_id:032x}",
        "span_id": f"{span_context.span_id:016x}",
        "parent_span_id": f"{parent.span_id:016x}" if parent else None,
    }


def metrics_payload() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
