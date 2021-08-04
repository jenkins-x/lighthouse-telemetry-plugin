package trace

import (
	"context"
	"errors"
	"fmt"
	"strings"

	jxv1 "github.com/jenkins-x/jx-api/v4/pkg/apis/jenkins.io/v1"
	jenkinsiov1 "github.com/jenkins-x/jx-api/v4/pkg/client/clientset/versioned/typed/jenkins.io/v1"
	lhv1alpha1 "github.com/jenkins-x/lighthouse/pkg/apis/lighthouse/v1alpha1"
	lhutil "github.com/jenkins-x/lighthouse/pkg/util"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type JenkinsXPipelineActivityHandler struct {
	BaseResourceEventHandler
	Tracer                 trace.Tracer
	PipelineActivityClient jenkinsiov1.PipelineActivityInterface
}

func (h *JenkinsXPipelineActivityHandler) OnAdd(obj interface{}) {
	pa, ok := obj.(*jxv1.PipelineActivity)
	if !ok {
		h.Logger.Warningf("JenkinsXPipelineActivityHandler called with non PipelineActivity object: %T", obj)
		return
	}

	if h.isJenkinsPipelineActivity(pa) {
		return
	}

	log := h.Logger.WithField("PipelineActivity", pa.Name)
	log.Trace("Handling PipelineActivity Added Event")

	h.Store.AddJxPipelineActivity(pa)

	err := h.handlePipelineActivity(pa)
	if err != nil {
		log.WithError(err).Error("failed to handle PipelineActivity")
		return
	}
}

func (h *JenkinsXPipelineActivityHandler) OnUpdate(oldObj, newObj interface{}) {
	_, ok := oldObj.(*jxv1.PipelineActivity)
	if !ok {
		h.Logger.Warningf("JenkinsXPipelineActivityHandler called with non PipelineActivity object: %T", oldObj)
		return
	}
	newPA, ok := newObj.(*jxv1.PipelineActivity)
	if !ok {
		h.Logger.Warningf("JenkinsXPipelineActivityHandler called with non PipelineActivity object: %T", newObj)
		return
	}

	if h.isJenkinsPipelineActivity(newPA) {
		return
	}

	log := h.Logger.WithField("PipelineActivity", newPA.Name)
	log.Trace("Handling PipelineActivity Updated Event")

	h.Store.AddJxPipelineActivity(newPA)

	err := h.handlePipelineActivity(newPA)
	if err != nil {
		log.WithError(err).Error("failed to handle PipelineActivity")
		return
	}
}

func (h *JenkinsXPipelineActivityHandler) OnDelete(obj interface{}) {
	pa, ok := obj.(*jxv1.PipelineActivity)
	if !ok {
		h.Logger.Warningf("JenkinsXPipelineActivityHandler called with non PipelineActivity object: %T", obj)
		return
	}

	if h.isJenkinsPipelineActivity(pa) {
		return
	}

	log := h.Logger.WithField("PipelineActivity", pa.Name)
	log.Trace("Handling PipelineActivity Deleted Event")

	h.Store.DeleteJxPipelineActivity(pa.Name)
}

func (h *JenkinsXPipelineActivityHandler) handlePipelineActivity(pa *jxv1.PipelineActivity) error {
	log := h.Logger.WithField("PipelineActivity", pa.Name)
	if pa.Annotations == nil {
		pa.Annotations = make(map[string]string)
	}

	if pa.Spec.StartedTimestamp == nil {
		return nil
	}

	span, err := h.getSpanFor(pa)
	if errors.Is(err, ErrEventTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrParentEntityNotFound) && pa.Spec.Status.IsTerminated() {
		// PA is finished and its parent job may have been garbage-collected, ignore it
		return nil
	}
	if err != nil {
		return err
	}

	if !span.IsRecording() {
		// span has already been ended, there's nothing more to do
		return nil
	}
	if pa.Spec.CompletedTimestamp == nil {
		// PA is not finished, let's wait until it's finished
		return nil
	}

	log.WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("PipelineActivity complete - ending span")
	ctx := trace.ContextWithSpan(context.Background(), span)
	for i := range pa.Spec.Steps {
		stageStep := pa.Spec.Steps[i]
		if stageStep.Stage == nil {
			continue
		}
		stage := stageStep.Stage

		// ignore pending/running stages, which don't have started/completed timestamps
		if !stage.Status.IsTerminated() {
			continue
		}
		if stage.StartedTimestamp == nil {
			continue
		}

		stageCtx, stageSpan := h.Tracer.Start(ctx, stage.Name,
			trace.WithTimestamp(stage.StartedTimestamp.Time),
		)

		for j := range stage.Steps {
			step := stage.Steps[j]

			// ignore pending/running steps, which don't have started/completed timestamps
			if !step.Status.IsTerminated() {
				continue
			}
			if step.StartedTimestamp == nil {
				continue
			}

			_, stepSpan := h.Tracer.Start(stageCtx, step.Name,
				trace.WithTimestamp(step.StartedTimestamp.Time),
			)
			switch step.Status {
			case jxv1.ActivityStatusTypeFailed:
				stepSpan.SetStatus(codes.Error, stage.Description)
				stepSpan.RecordError(errors.New(stage.Description))
			default:
				stepSpan.SetStatus(codes.Ok, step.Description)
			}
			if step.CompletedTimestamp == nil {
				step.CompletedTimestamp = stage.CompletedTimestamp
			}
			if step.CompletedTimestamp == nil {
				step.CompletedTimestamp = pa.Spec.CompletedTimestamp
			}
			stepSpan.End(
				trace.WithTimestamp(step.CompletedTimestamp.Time),
			)
		}

		switch stage.Status {
		case jxv1.ActivityStatusTypeFailed:
			stageSpan.SetStatus(codes.Error, stage.Description)
			stageSpan.RecordError(errors.New(stage.Description))
		default:
			stageSpan.SetStatus(codes.Ok, stage.Description)
		}
		if stage.CompletedTimestamp == nil {
			stage.CompletedTimestamp = pa.Spec.CompletedTimestamp
		}
		stageSpan.End(
			trace.WithTimestamp(stage.CompletedTimestamp.Time),
		)
	}

	switch pa.Spec.Status {
	case jxv1.ActivityStatusTypeFailed:
		span.SetStatus(codes.Error, "pipeline failed")
	default:
		span.SetStatus(codes.Ok, "")
	}

	span.End(
		trace.WithTimestamp(pa.Spec.CompletedTimestamp.Time),
	)

	return nil
}

func (h *JenkinsXPipelineActivityHandler) getSpanFor(pa *jxv1.PipelineActivity) (*EventSpan, error) {
	_, spanContext, err := extractTraceFrom(pa.Annotations)
	if errors.Is(err, ErrTraceNotFound) {
		err := h.createSpanFor(pa)
		if err != nil {
			return nil, err
		}
		_, spanContext, err = extractTraceFrom(pa.Annotations)
		if err != nil {
			return nil, fmt.Errorf("failed to find or create a span for PipelineActivity %s", pa.Name)
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

func (h *JenkinsXPipelineActivityHandler) createSpanFor(pa *jxv1.PipelineActivity) error {
	job, err := h.getParentLighthouseJob(pa)
	if err != nil {
		return fmt.Errorf("failed to find the parent LighthouseJob for Jenkins X PipelineActivity %s: %w", pa.Name, err)
	}

	ctx, parentSpanContext, err := extractTraceFrom(job.Annotations)
	if err != nil {
		return fmt.Errorf("failed to extract the trace from the LighthouseJob %s: %w", job.Name, err)
	}

	eventTrace, err := h.Store.GetEventTrace(parentSpanContext.TraceID())
	if err != nil {
		return err
	}

	if span, ok := eventTrace.FindSpanFor(EntityTypeJxPipelineActivity, pa.Name); ok {
		injectTraceInto(trace.ContextWithSpan(context.Background(), span), pa.Annotations)
		return nil
	}

	ctx, span := h.Tracer.Start(ctx, pa.Name,
		trace.WithTimestamp(pa.Spec.StartedTimestamp.Time),
		trace.WithAttributes(
			attribute.Key("pipeline.owner").String(labelValue(pa.Labels, lhutil.OrgLabel, "owner")),
			attribute.Key("pipeline.repository").String(labelValue(pa.Labels, lhutil.RepoLabel, "repository")),
			attribute.Key("pipeline.branch").String(labelValue(pa.Labels, lhutil.BranchLabel, "branch")),
			attribute.Key("pipeline.build").String(labelValue(pa.Labels, lhutil.BuildNumLabel, "build")),
			attribute.Key("pipeline.context").String(labelValue(pa.Labels, lhutil.ContextLabel, "context")),
		),
	)

	patch, err := injectTraceInto(ctx, pa.Annotations)
	if err != nil {
		return fmt.Errorf("failed to inject SpanContext into PipelineActivity %q: %w", pa.Name, err)
	}
	_, err = h.PipelineActivityClient.Patch(ctx, pa.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		// TODO reset local PA instance (remove annotations?)
		return fmt.Errorf("failed to patch PipelineActivity %q with patch %q: %w", pa.Name, patch, err)
	}

	// ensure the activity has a traceID annotation that can be used directly from the web UI
	patch = fmt.Sprintf(`{"metadata": {"annotations": {"%s": "%s"}}}`, "lighthouse.jenkins-x.io/traceID", span.SpanContext().TraceID())
	_, err = h.PipelineActivityClient.Patch(ctx, pa.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch PipelineActivity %q with patch %q: %w", pa.Name, patch, err)
	}

	h.Store.AddJxPipelineActivity(pa)

	eventTrace.AddSpan(EventSpan{
		Span: span,
		Entity: Entity{
			Type: EntityTypeJxPipelineActivity,
			Name: pa.Name,
		},
	})
	h.Logger.WithField("PipelineActivity", pa.Name).WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("Creating new Span")

	return nil
}

func (h *JenkinsXPipelineActivityHandler) getParentLighthouseJob(pa *jxv1.PipelineActivity) (*lhv1alpha1.LighthouseJob, error) {
	job := h.Store.FindLighthouseJob(map[string]string{
		lhutil.OrgLabel:      labelValue(pa.Labels, lhutil.OrgLabel, "owner"),
		lhutil.RepoLabel:     labelValue(pa.Labels, lhutil.RepoLabel, "repository"),
		lhutil.BranchLabel:   labelValue(pa.Labels, lhutil.BranchLabel, "branch"),
		lhutil.BuildNumLabel: labelValue(pa.Labels, lhutil.BuildNumLabel, "build"),
	})
	if job == nil {
		return nil, ErrParentEntityNotFound
	}
	return job, nil
}

// isJenkinsPipelineActivity returns true if the given PipelineActivity has been created by Jenkins
// see https://github.com/jenkinsci/jx-resources-plugin/blob/master/src/main/java/org/jenkinsci/plugins/jx/resources/BuildSyncRunListener.java#L106
func (h *JenkinsXPipelineActivityHandler) isJenkinsPipelineActivity(pa *jxv1.PipelineActivity) bool {
	if strings.Contains(pa.Spec.BuildURL, "/blue/organizations/jenkins/") {
		return true
	}
	if strings.Contains(pa.Spec.BuildLogsURL, "/job/") && strings.HasSuffix(pa.Spec.BuildLogsURL, "/console") {
		return true
	}
	return false
}
