package trace

import (
	"context"
	"errors"
	"fmt"

	lhv1alpha1 "github.com/jenkins-x/lighthouse/pkg/apis/lighthouse/v1alpha1"
	lhutil "github.com/jenkins-x/lighthouse/pkg/util"
	tknv1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	tektonv1beta1 "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/typed/pipeline/v1beta1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type TektonPipelineRunHandler struct {
	BaseResourceEventHandler
	Tracer            trace.Tracer
	PipelineRunClient tektonv1beta1.PipelineRunInterface
}

func (h *TektonPipelineRunHandler) OnAdd(obj interface{}) {
	pr, ok := obj.(*tknv1beta1.PipelineRun)
	if !ok {
		h.Logger.Warningf("TektonPipelineRunHandler called with non PipelineRun object: %T", obj)
		return
	}

	log := h.Logger.WithField("PipelineRun", pr.Name)
	log.Trace("Handling PipelineRun Added Event")

	h.Store.AddTknPipelineRun(pr)

	err := h.handlePipelineRun(pr)
	if err != nil {
		log.WithError(err).Error("failed to handle PipelineRun")
		return
	}
}

func (h *TektonPipelineRunHandler) OnUpdate(oldObj, newObj interface{}) {
	newPR, ok := newObj.(*tknv1beta1.PipelineRun)
	if !ok {
		h.Logger.Warningf("TektonPipelineRunHandler called with non PipelineRun object: %T", newObj)
		return
	}

	log := h.Logger.WithField("PipelineRun", newPR.Name)
	log.Trace("Handling PipelineRun Updated Event")

	h.Store.AddTknPipelineRun(newPR)

	err := h.handlePipelineRun(newPR)
	if err != nil {
		log.WithError(err).Error("failed to handle PipelineRun")
		return
	}
}

func (h *TektonPipelineRunHandler) OnDelete(obj interface{}) {
	pr, ok := obj.(*tknv1beta1.PipelineRun)
	if !ok {
		h.Logger.Warningf("TektonPipelineRunHandler called with non PipelineRun object: %T", obj)
		return
	}

	log := h.Logger.WithField("PipelineRun", pr.Name)
	log.Trace("Handling PipelineRun Deleted Event")

	h.Store.DeleteTknPipelineRun(pr.Name)
}

func (h *TektonPipelineRunHandler) handlePipelineRun(pr *tknv1beta1.PipelineRun) error {
	log := h.Logger.WithField("PipelineRun", pr.Name)
	if pr.Annotations == nil {
		pr.Annotations = make(map[string]string)
	}

	span, err := h.getSpanFor(pr)
	if errors.Is(err, ErrEventTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrParentEntityNotFound) && pr.Status.CompletionTime != nil {
		// pipelinerun is finished and its parent job may have been garbage-collected, ignore it
		return nil
	}
	if err != nil {
		return err
	}

	if !span.IsRecording() {
		return nil
	}
	if pr.Status.CompletionTime == nil {
		return nil
	}

	log.WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("PipelineRun complete - ending span")
	span.AddEvent("Start",
		trace.WithTimestamp(pr.Status.StartTime.Time),
	)
	span.End(
		trace.WithTimestamp(pr.Status.CompletionTime.Time),
	)

	return nil
}

func (h *TektonPipelineRunHandler) getSpanFor(pr *tknv1beta1.PipelineRun) (*SpanHolder, error) {
	_, spanContext, err := extractTraceFrom(pr.Annotations)
	if errors.Is(err, ErrTraceNotFound) {
		err := h.createSpanFor(pr)
		if err != nil {
			return nil, err
		}
		_, spanContext, err = extractTraceFrom(pr.Annotations)
		if err != nil {
			return nil, fmt.Errorf("failed to find or create a span for PipelineRun %s", pr.Name)
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

func (h *TektonPipelineRunHandler) createSpanFor(pr *tknv1beta1.PipelineRun) error {
	job, err := h.getParentLighthouseJob(pr)
	if err != nil {
		return fmt.Errorf("failed to find the parent LighthouseJob for Tekton PipelineRun %s: %w", pr.Name, err)
	}

	ctx, parentSpanContext, err := extractTraceFrom(job.Annotations)
	if err != nil {
		return fmt.Errorf("failed to extract the trace from the LighthouseJob %s: %w", job.Name, err)
	}

	eventTrace, err := h.Store.GetEventTrace(parentSpanContext.TraceID())
	if err != nil {
		return err
	}

	if span, ok := eventTrace.FindSpanFor(EntityTypeTknPipelineRun, pr.Name); ok {
		injectTraceInto(trace.ContextWithSpan(context.Background(), span), pr.Annotations)
		return nil
	}

	ctx, span := h.Tracer.Start(ctx, pr.Name,
		trace.WithTimestamp(pr.CreationTimestamp.Time),
		trace.WithAttributes(
			attribute.Key("pipeline.owner").String(labelValue(pr.Labels, lhutil.OrgLabel, "owner")),
			attribute.Key("pipeline.repository").String(labelValue(pr.Labels, lhutil.RepoLabel, "repository")),
			attribute.Key("pipeline.branch").String(labelValue(pr.Labels, lhutil.BranchLabel, "branch")),
			attribute.Key("pipeline.build").String(labelValue(pr.Labels, lhutil.BuildNumLabel, "build")),
			attribute.Key("pipeline.context").String(labelValue(pr.Labels, lhutil.ContextLabel, "context")),
		),
	)

	patch, err := injectTraceInto(ctx, pr.Annotations)
	if err != nil {
		return fmt.Errorf("failed to inject SpanContext into PipelineRun %q: %w", pr.Name, err)
	}
	_, err = h.PipelineRunClient.Patch(ctx, pr.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch PipelineRun %q with patch %q: %w", pr.Name, patch, err)
	}
	h.Store.AddTknPipelineRun(pr)

	eventTrace.AddSpan(SpanHolder{
		Span: span,
		Entity: Entity{
			Type: EntityTypeTknPipelineRun,
			Name: pr.Name,
		},
	})
	h.Logger.WithField("PipelineRun", pr.Name).WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().TraceID()).Info("Creating new Span")

	return nil
}

func (h *TektonPipelineRunHandler) getParentLighthouseJob(pr *tknv1beta1.PipelineRun) (*lhv1alpha1.LighthouseJob, error) {
	job := h.Store.FindLighthouseJob(map[string]string{
		lhutil.OrgLabel:      labelValue(pr.Labels, lhutil.OrgLabel, "owner"),
		lhutil.RepoLabel:     labelValue(pr.Labels, lhutil.RepoLabel, "repository"),
		lhutil.BranchLabel:   labelValue(pr.Labels, lhutil.BranchLabel, "branch"),
		lhutil.BuildNumLabel: labelValue(pr.Labels, lhutil.BuildNumLabel, "build"),
	})
	if job == nil {
		return nil, ErrParentEntityNotFound
	}
	return job, nil
}
