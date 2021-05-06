package trace

import (
	"context"
	"errors"
	"fmt"

	tknv1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	tektonv1beta1 "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/typed/pipeline/v1beta1"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type TektonTaskRunHandler struct {
	BaseResourceEventHandler
	Tracer        trace.Tracer
	TaskRunClient tektonv1beta1.TaskRunInterface
}

func (h *TektonTaskRunHandler) OnAdd(obj interface{}) {
	tr, ok := obj.(*tknv1beta1.TaskRun)
	if !ok {
		h.Logger.Warningf("TektonTaskRunHandler called with non TaskRun object: %T", obj)
		return
	}

	log := h.Logger.WithField("TaskRun", tr.Name)
	log.Trace("Handling TaskRun Added Event")

	h.Store.AddTknTaskRun(tr)

	err := h.handleTaskRun(tr)
	if err != nil {
		log.WithError(err).Error("failed to handle TaskRun")
		return
	}
}

func (h *TektonTaskRunHandler) OnUpdate(oldObj, newObj interface{}) {
	newTR, ok := newObj.(*tknv1beta1.TaskRun)
	if !ok {
		h.Logger.Warningf("TektonTaskRunHandler called with non TaskRun object: %T", newObj)
		return
	}

	log := h.Logger.WithField("TaskRun", newTR.Name)
	log.Trace("Handling TaskRun Updated Event")

	h.Store.AddTknTaskRun(newTR)

	err := h.handleTaskRun(newTR)
	if err != nil {
		log.WithError(err).Error("failed to handle TaskRun")
		return
	}
}

func (h *TektonTaskRunHandler) OnDelete(obj interface{}) {
	tr, ok := obj.(*tknv1beta1.TaskRun)
	if !ok {
		h.Logger.Warningf("TektonTaskRunHandler called with non TaskRun object: %T", obj)
		return
	}

	log := h.Logger.WithField("TaskRun", tr.Name)
	log.Trace("Handling TaskRun Deleted Event")

	h.Store.DeleteTknTaskRun(tr.Name)
}

func (h *TektonTaskRunHandler) handleTaskRun(tr *tknv1beta1.TaskRun) error {
	log := h.Logger.WithField("TaskRun", tr.Name)
	if tr.Annotations == nil {
		tr.Annotations = make(map[string]string)
	}

	span, err := h.getSpanFor(tr)
	if errors.Is(err, ErrEventTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrParentEntityNotFound) && tr.Status.CompletionTime != nil {
		// task is finished and its parent may have been garbage-collected, ignore it
		return nil
	}
	if err != nil {
		return err
	}

	if !span.IsRecording() {
		return nil
	}
	if tr.Status.CompletionTime == nil {
		return nil
	}

	log.WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("TaskRun complete - ending span")
	span.AddEvent("Start",
		trace.WithTimestamp(tr.Status.StartTime.Time),
	)
	span.End(
		trace.WithTimestamp(tr.Status.CompletionTime.Time),
	)

	return nil
}

func (h *TektonTaskRunHandler) getSpanFor(tr *tknv1beta1.TaskRun) (*SpanHolder, error) {
	_, spanContext, err := extractTraceFrom(tr.Annotations)
	if errors.Is(err, ErrTraceNotFound) {
		err := h.createSpanFor(tr)
		if err != nil {
			return nil, err
		}
		_, spanContext, err = extractTraceFrom(tr.Annotations)
		if err != nil {
			return nil, fmt.Errorf("failed to find or create a span for TaskRun %s", tr.Name)
		}
	} else if err != nil {
		return nil, err
	}

	eventTrace, err := h.Store.GetEventTrace(spanContext.TraceID())
	if err != nil {
		return nil, err
	}

	span, ok := eventTrace.GetSpan(spanContext.SpanID())
	if !ok {
		return nil, fmt.Errorf("no span found in event trace for spanID %q", spanContext.SpanID())
	}

	return &span, nil
}

func (h *TektonTaskRunHandler) createSpanFor(tr *tknv1beta1.TaskRun) error {
	pr := h.Store.GetTknPipelineRun(tr.Labels["tekton.dev/pipelineRun"])
	if pr == nil {
		return fmt.Errorf("failed to find the parent PipelineRun for Tekton TaskRun %s: %w", tr.Name, ErrParentEntityNotFound)
	}

	ctx, parentSpanContext, err := extractTraceFrom(pr.Annotations)
	if err != nil {
		return fmt.Errorf("failed to extract the trace from the PipelineRun %s: %w", pr.Name, err)
	}

	eventTrace, err := h.Store.GetEventTrace(parentSpanContext.TraceID())
	if err != nil {
		return err
	}

	if span, ok := eventTrace.FindSpanFor(EntityTypeTknTaskRun, tr.Name); ok {
		injectTraceInto(trace.ContextWithSpan(context.Background(), span), tr.Annotations)
		return nil
	}

	ctx, span := h.Tracer.Start(ctx, tr.Name,
		trace.WithTimestamp(tr.CreationTimestamp.Time),
	)

	patch, err := injectTraceInto(ctx, tr.Annotations)
	if err != nil {
		return fmt.Errorf("failed to inject SpanContext into TaskRun %q: %w", tr.Name, err)
	}
	_, err = h.TaskRunClient.Patch(ctx, tr.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch TaskRun %q with patch %q: %w", tr.Name, patch, err)
	}
	h.Store.AddTknTaskRun(tr)

	eventTrace.AddSpan(SpanHolder{
		Span: span,
		Entity: Entity{
			Type: EntityTypeTknTaskRun,
			Name: tr.Name,
		},
	})
	h.Logger.WithField("TaskRun", tr.Name).WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("Creating new Span")

	return nil
}
