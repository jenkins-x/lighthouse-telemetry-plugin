package trace

import (
	"context"
	"time"

	"github.com/jenkins-x/go-scm/scm"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type LighthouseEventHandler struct {
	BaseResourceEventHandler
	Tracer trace.Tracer
}

func (h *LighthouseEventHandler) handleWebhook(webhook scm.Webhook) error {
	log := h.Logger.WithField("repo", webhook.Repository().FullName)

	var err error
	switch event := webhook.(type) {
	case *scm.IssueCommentHook:
		log.
			WithField("guid", event.GUID).
			WithField("comment", event.Comment.Body).
			Debug("Handling issue comment hook event")
		err = h.handleEvent(event.GUID, event.Comment.Created)
	case *scm.PullRequestCommentHook:
		log.
			WithField("guid", event.GUID).
			WithField("comment", event.Comment.Body).
			Debug("Handling pullrequest comment hook event")
		err = h.handleEvent(event.GUID, event.Comment.Created)
	case *scm.PushHook:
		log.
			WithField("guid", event.GUID).
			WithField("commit", event.Commit.Sha).
			Debug("Handling push hook event")
		err = h.handleEvent(event.GUID, time.Now())
	default:
		log.Trace("Ignoring non push or comment hook event")
	}

	return err
}

func (h *LighthouseEventHandler) handleEvent(eventGUID string, timestamp time.Time) error {
	eventTrace, err := h.Store.FindEventTraceByEventGUID(eventGUID)
	if eventTrace != nil {
		// we already have a trace for this event, no need to create a new one
		return nil
	}
	if err != nil && err != ErrEventTraceNotFound {
		return err
	}

	// let's create a new trace for this event
	_, rootSpan := h.Tracer.Start(context.Background(), "LighthouseEvent",
		trace.WithNewRoot(),
		trace.WithTimestamp(timestamp),
		trace.WithAttributes(
			attribute.Key("event.guid").String(eventGUID),
		),
	)
	h.Logger.
		WithField("traceID", rootSpan.SpanContext().TraceID()).
		WithField("spanID", rootSpan.SpanContext().SpanID()).
		Info("Creating new EventTrace")
	eventTrace = NewEventTrace(eventGUID, rootSpan)
	h.Store.AddEventTrace(eventTrace)

	return nil
}
