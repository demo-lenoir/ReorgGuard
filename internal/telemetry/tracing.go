package telemetry

import (
	"context"
	"errors"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Init supports an explicit off mode (the default) or local JSON span output.
// No endpoint URL, DSN, response body or raw error is ever added to a span.
func Init(mode string) (func(context.Context) error, error) {
	if mode == "" || mode == "off" {
		return func(context.Context) error { return nil }, nil
	}
	if mode != "stdout" {
		return nil, errors.New("invalid tracing mode")
	}
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(os.Stdout))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(resource.NewWithAttributes("", attribute.String("service.name", "reorgguard"))))
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer("reorgguard").Start(ctx, name, trace.WithAttributes(attrs...))
}

func Failure(span trace.Span, category string) {
	span.SetStatus(codes.Error, category)
	span.RecordError(errors.New(category))
}
