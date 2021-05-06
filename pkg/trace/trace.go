package trace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jenkins-x/lighthouse-telemetry-plugin/internal/otelcarrier"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	TraceIDAnnotationKey = "lighthouse.jenkins-x.io/traceID"
	SpanIDAnnotationKey  = "lighthouse.jenkins-x.io/spanID"
)

var (
	ErrTraceNotFound = errors.New("The Trace parent/state could not be found in the annotations")
)

type EventTrace struct {
	EventGUID  string
	RootSpan   trace.Span
	spans      map[trace.SpanID]SpanHolder
	spansMutex sync.RWMutex
}

func NewEventTrace(eventGUID string, rootSpan trace.Span) *EventTrace {
	return &EventTrace{
		EventGUID: eventGUID,
		RootSpan:  rootSpan,
		spans:     make(map[trace.SpanID]SpanHolder),
	}
}

func (t EventTrace) GetSpan(spanID trace.SpanID) (SpanHolder, bool) {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	span, found := t.spans[spanID]
	return span, found
}

func (t EventTrace) FindSpanFor(entityType EntityType, entityName string) (SpanHolder, bool) {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	for _, span := range t.spans {
		if span.Entity.Type == entityType && span.Entity.Name == entityName {
			return span, true
		}
	}
	return SpanHolder{}, false
}

func (t EventTrace) AddSpan(span SpanHolder) {
	t.spansMutex.Lock()
	defer t.spansMutex.Unlock()
	t.spans[span.SpanContext().SpanID()] = span
}

func (t EventTrace) EndRootSpanIfNeeded(options ...trace.SpanOption) {
	if !t.RootSpan.IsRecording() {
		return
	}

	var recordingLighthouseJobSpans int
	t.spansMutex.RLock()
	for _, span := range t.spans {
		switch span.Entity.Type {
		case EntityTypeLighthouseJob:
			if span.IsRecording() {
				recordingLighthouseJobSpans++
			}
		default:
			continue
		}
	}
	defer t.spansMutex.RUnlock()

	if recordingLighthouseJobSpans == 0 {
		t.RootSpan.End(options...)
	}
}

type SpanHolder struct {
	trace.Span
	Entity Entity
}

func extractTraceFrom(annotations map[string]string) (context.Context, trace.SpanContext, error) {
	var (
		ctx        = context.Background()
		propagator = propagation.TraceContext{}
	)
	ctx = propagator.Extract(context.Background(), otelcarrier.AnnotationsCarrier(annotations))
	sc := trace.RemoteSpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ctx, sc, ErrTraceNotFound
	}
	return ctx, sc, nil
}

func injectTraceInto(ctx context.Context, annotations map[string]string) (string, error) {
	var (
		propagator = propagation.TraceContext{}
		carrier    = otelcarrier.AnnotationsCarrier(annotations)
	)
	propagator.Inject(ctx, carrier)

	// for conveniance, return a json patch ready to be applied
	changedAnnotations := make(map[string]string, len(propagator.Fields()))
	for _, key := range propagator.Fields() {
		changedAnnotations[otelcarrier.AnnotationsKeyPrefix+key] = carrier.Get(key)
	}

	if len(changedAnnotations) == 0 {
		return "", nil
	}

	var builder strings.Builder
	err := json.NewEncoder(&builder).Encode(changedAnnotations)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`{"metadata": {"annotations": %s}}`, builder.String()), nil
}
