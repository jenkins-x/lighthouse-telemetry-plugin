package trace

import (
	"context"
	"errors"
	"fmt"
	"time"

	lhv1alpha1 "github.com/jenkins-x/lighthouse/pkg/apis/lighthouse/v1alpha1"
	lighthousev1alpha1 "github.com/jenkins-x/lighthouse/pkg/client/clientset/versioned/typed/lighthouse/v1alpha1"
	lhutil "github.com/jenkins-x/lighthouse/pkg/util"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type LighthouseJobHandler struct {
	BaseResourceEventHandler
	Tracer              trace.Tracer
	LighthouseJobClient lighthousev1alpha1.LighthouseJobInterface
}

func (h *LighthouseJobHandler) OnAdd(obj interface{}) {
	job, ok := obj.(*lhv1alpha1.LighthouseJob)
	if !ok {
		h.Logger.Warningf("LighthouseJobHandler called with non LighthouseJob object: %T", obj)
		return
	}

	log := h.Logger.WithField("LighthouseJob", job.Name)
	log.Trace("Handling LighthouseJob Added Event")

	h.Store.AddLighthouseJob(job)

	err := h.handleLighthouseJob(job)
	if err != nil {
		log.WithError(err).Error("failed to handle LighthouseJob")
		return
	}
}

func (h *LighthouseJobHandler) OnUpdate(oldObj, newObj interface{}) {
	newJob, ok := newObj.(*lhv1alpha1.LighthouseJob)
	if !ok {
		h.Logger.Warningf("LighthouseJobHandler called with non LighthouseJob object: %T", newObj)
		return
	}

	log := h.Logger.WithField("LighthouseJob", newJob.Name)
	log.Trace("Handling LighthouseJob Updated Event")

	h.Store.AddLighthouseJob(newJob)

	err := h.handleLighthouseJob(newJob)
	if err != nil {
		log.WithError(err).Error("failed to handle LighthouseJob")
		return
	}
}

func (h *LighthouseJobHandler) OnDelete(obj interface{}) {
	job, ok := obj.(*lhv1alpha1.LighthouseJob)
	if !ok {
		h.Logger.Warningf("LighthouseJobHandler called with non LighthouseJob object: %T", obj)
		return
	}

	log := h.Logger.WithField("LighthouseJob", job.Name)
	log.Trace("Handling LighthouseJob Deleted Event")

	h.Store.DeleteLighthouseJob(job.Name)
}

func (h *LighthouseJobHandler) handleLighthouseJob(job *lhv1alpha1.LighthouseJob) error {
	log := h.Logger.WithField("LighthouseJob", job.Name)
	if job.Annotations == nil {
		job.Annotations = make(map[string]string)
	}

	span, eventTrace, err := h.getSpanFor(job)
	if errors.Is(err, ErrEventTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrTraceNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	if job.Complete() && span.IsRecording() {
		log.WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("Lighthouse job complete - ending span")
		switch job.Status.State {
		case lhv1alpha1.FailureState, lhv1alpha1.AbortedState, lhv1alpha1.ErrorState:
			span.SetStatus(codes.Error, job.Status.Description)
		default:
			span.SetStatus(codes.Ok, job.Status.Description)
		}
		span.End(
			trace.WithTimestamp(job.Status.CompletionTime.Time),
		)
		eventTrace.EndRootSpanIfNeeded(
			trace.WithTimestamp(job.Status.CompletionTime.Time),
		)
	}

	return nil
}

func (h *LighthouseJobHandler) getSpanFor(job *lhv1alpha1.LighthouseJob) (*SpanHolder, *EventTrace, error) {
	_, spanContext, err := extractTraceFrom(job.Annotations)
	if errors.Is(err, ErrTraceNotFound) {
		err := h.createSpanFor(job)
		if err != nil {
			return nil, nil, err
		}
		_, spanContext, err = extractTraceFrom(job.Annotations)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to find or create a span for LighthouseJob %s", job.Name)
		}
	} else if err != nil {
		return nil, nil, err
	}

	eventTrace, err := h.Store.GetEventTrace(spanContext.TraceID())
	if err != nil {
		return nil, nil, err
	}

	span, ok := eventTrace.GetSpan(spanContext.SpanID())
	if !ok {
		return nil, nil, fmt.Errorf("no span found in event trace for spanID %q", spanContext.SpanID())
	}

	return &span, eventTrace, nil
}

func (h *LighthouseJobHandler) createSpanFor(job *lhv1alpha1.LighthouseJob) error {
	eventGUID := job.Labels["event-GUID"]
	eventTrace, _ := h.Store.FindEventTraceByEventGUID(eventGUID)
	if eventTrace == nil {
		h.Logger.WithField("LighthouseJob", job.Name).Info("Creating new EventTrace")
		eventTrace = h.createEventTrace(eventGUID, job.CreationTimestamp.Time)
	}

	if span, ok := eventTrace.FindSpanFor(EntityTypeLighthouseJob, job.Name); ok {
		injectTraceInto(trace.ContextWithSpan(context.Background(), span), job.Annotations)
		return nil
	}

	ctx := trace.ContextWithRemoteSpanContext(context.Background(), eventTrace.RootSpan.SpanContext())
	ctx, span := h.Tracer.Start(ctx, job.Spec.Job,
		trace.WithTimestamp(job.Status.StartTime.Time),
		trace.WithAttributes(
			attribute.Key("pipeline.owner").String(job.Labels[lhutil.OrgLabel]),
			attribute.Key("pipeline.repository").String(job.Labels[lhutil.RepoLabel]),
			attribute.Key("pipeline.branch").String(job.Labels[lhutil.BranchLabel]),
			attribute.Key("pipeline.build").String(job.Labels[lhutil.BuildNumLabel]),
			attribute.Key("pipeline.context").String(job.Labels[lhutil.ContextLabel]),
		),
	)

	patch, err := injectTraceInto(ctx, job.Annotations)
	if err != nil {
		return fmt.Errorf("failed to inject SpanContext into LighthouseJob %q: %w", job.Name, err)
	}
	_, err = h.LighthouseJobClient.Patch(ctx, job.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch LighthouseJob %q with patch %q: %w", job.Name, patch, err)
	}
	h.Store.AddLighthouseJob(job)

	eventTrace.AddSpan(SpanHolder{
		Span: span,
		Entity: Entity{
			Type: EntityTypeLighthouseJob,
			Name: job.Name,
		},
	})
	h.Logger.WithField("LighthouseJob", job.Name).WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("Creating new Span")

	return nil
}

func (h *LighthouseJobHandler) createEventTrace(eventGUID string, timestamp time.Time) *EventTrace {
	_, rootSpan := h.Tracer.Start(context.Background(), "LighthouseEvent",
		trace.WithNewRoot(),
		trace.WithTimestamp(timestamp),
		trace.WithAttributes(
			attribute.Key("event.guid").String(eventGUID),
		),
	)
	h.Logger.WithField("traceID", rootSpan.SpanContext().TraceID()).WithField("spanID", rootSpan.SpanContext().SpanID()).Info("Creating new EventTrace")
	eventTrace := NewEventTrace(eventGUID, rootSpan)
	h.Store.AddEventTrace(eventTrace)
	return eventTrace
}
