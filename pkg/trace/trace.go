package trace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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
	spans      map[trace.SpanID]EventSpan
	spansMutex sync.RWMutex
}

func NewEventTrace(eventGUID string, rootSpan trace.Span) *EventTrace {
	return &EventTrace{
		EventGUID: eventGUID,
		RootSpan:  rootSpan,
		spans:     make(map[trace.SpanID]EventSpan),
	}
}

func (t EventTrace) GetSpan(spanID trace.SpanID) (EventSpan, bool) {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	span, found := t.spans[spanID]
	return span, found
}

func (t EventTrace) FindSpanFor(entityType EntityType, entityName string) (EventSpan, bool) {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	for _, span := range t.spans {
		if span.Entity.Type == entityType && span.Entity.Name == entityName {
			return span, true
		}
	}
	return EventSpan{}, false
}

func (t *EventTrace) AddSpan(span EventSpan) {
	t.spansMutex.Lock()
	defer t.spansMutex.Unlock()
	t.spans[span.SpanContext().SpanID()] = span
}

func (t EventTrace) EndRootSpanIfNeeded(options ...trace.SpanEndOption) {
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

type EventSpan struct {
	trace.Span
	Entity Entity
}

type GitopsTrace struct {
	rootSpanID trace.SpanID
	spans      map[trace.SpanID]*GitopsSpan
	spansMutex sync.RWMutex
}

func NewGitopsTrace(rootSpan GitopsSpan) *GitopsTrace {
	return &GitopsTrace{
		rootSpanID: rootSpan.SpanContext().SpanID(),
		spans: map[trace.SpanID]*GitopsSpan{
			rootSpan.SpanContext().SpanID(): &rootSpan,
		},
	}
}

func (t GitopsTrace) RootSpan() *GitopsSpan {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	return t.spans[t.rootSpanID]
}

func (t GitopsTrace) GetSpan(spanID trace.SpanID) (*GitopsSpan, bool) {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	span, found := t.spans[spanID]
	if found {
		return span, true
	}
	return nil, false
}

func (t GitopsTrace) GetSpanFor(pr PullRequest) (*GitopsSpan, bool) {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	for i := range t.spans {
		if t.spans[i].PullRequest == pr {
			return t.spans[i], true
		}
	}
	return nil, false
}

func (t *GitopsTrace) AddSpan(span GitopsSpan) {
	t.spansMutex.Lock()
	defer t.spansMutex.Unlock()
	t.spans[span.SpanContext().SpanID()] = &span
}

func (t *GitopsTrace) SpanChildren(spanID trace.SpanID) []*GitopsSpan {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	var spans []*GitopsSpan
	for i := range t.spans {
		if t.spans[i].ParentSpanID == spanID {
			spans = append(spans, t.spans[i])
		}
	}
	return spans
}

func (t *GitopsTrace) IterateOnGitopsSpans(callback func(gitopsSpan *GitopsSpan)) {
	t.spansMutex.RLock()
	defer t.spansMutex.RUnlock()
	for i := range t.spans {
		callback(t.spans[i])
	}
}

type GitopsSpan struct {
	trace.Span
	PullRequest       PullRequest
	PullRequestClosed *time.Time
	ParentSpanID      trace.SpanID
}

func extractTraceFrom(annotations map[string]string) (context.Context, trace.SpanContext, error) {
	var (
		propagator = propagation.TraceContext{}
	)
	ctx := propagator.Extract(context.Background(), otelcarrier.AnnotationsCarrier(annotations))
	sc := trace.SpanContextFromContext(ctx).WithRemote(true)
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
