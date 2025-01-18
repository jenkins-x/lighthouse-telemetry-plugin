package trace

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/scylladb/go-set/strset"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kv1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

type KubernetesPodHandler struct {
	BaseResourceEventHandler
	Tracer    trace.Tracer
	PodClient kv1.PodInterface
}

func (h *KubernetesPodHandler) OnAdd(obj interface{}, isInInitialList bool) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		h.Logger.Warningf("KubernetesPodHandler called with non Pod object: %T", obj)
		return
	}

	if pod.Labels["app.kubernetes.io/managed-by"] != "tekton-pipelines" {
		return
	}

	log := h.Logger.WithField("Pod", pod.Name)
	log.Trace("Handling Pod Added Event")

	h.Store.AddKubePod(pod)

	err := h.handlePod(pod)
	if err != nil {
		log.WithError(err).Error("failed to handle Pod")
		return
	}
}

func (h *KubernetesPodHandler) OnUpdate(oldObj, newObj interface{}) {
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		h.Logger.Warningf("KubernetesPodHandler called with non Pod object: %T", newObj)
		return
	}

	if newPod.Labels["app.kubernetes.io/managed-by"] != "tekton-pipelines" {
		return
	}

	log := h.Logger.WithField("Pod", newPod.Name)
	log.Trace("Handling Pod Updated Event")

	h.Store.AddKubePod(newPod)

	err := h.handlePod(newPod)
	if err != nil {
		log.WithError(err).Error("failed to handle Pod")
		return
	}
}

func (h *KubernetesPodHandler) OnDelete(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		h.Logger.Warningf("KubernetesPodHandler called with non Pod object: %T", obj)
		return
	}

	if pod.Labels["app.kubernetes.io/managed-by"] != "tekton-pipelines" {
		return
	}

	log := h.Logger.WithField("Pod", pod.Name)
	log.Trace("Handling Pod Deleted Event")

	h.Store.DeleteKubePod(pod.Name)
}

func (h *KubernetesPodHandler) handlePod(pod *v1.Pod) error {
	log := h.Logger.WithField("Pod", pod.Name)
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	span, err := h.getSpanFor(pod)
	if errors.Is(err, ErrEventTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrTraceNotFound) {
		return nil
	}
	if errors.Is(err, ErrParentEntityNotFound) && (pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed) {
		// pod is terminated and its parent may have been garbage-collected, ignore it
		return nil
	}
	if err != nil {
		return err
	}

	if !span.IsRecording() {
		return nil
	}

	switch pod.Status.Phase {
	case v1.PodSucceeded, v1.PodFailed:
	default:
		return nil
	}

	var podCompletionTime time.Time
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Terminated != nil {
			if containerStatus.State.Terminated.FinishedAt.Time.After(podCompletionTime) {
				podCompletionTime = containerStatus.State.Terminated.FinishedAt.Time
			}
		}
	}

	var (
		ctx              = trace.ContextWithSpan(context.Background(), span)
		events           = h.Store.GetKubeEventsFor(Entity{Type: EntityTypeKubePod, Name: pod.Name})
		pulledContainers = strset.New()
	)
	for i := range events {
		var (
			previousEvent *v1.Event
			event         = events[i]
		)
		if i > 0 {
			previousEvent = events[i-1]
		}
		span.AddEvent(event.Message,
			trace.WithTimestamp(event.CreationTimestamp.Time),
			trace.WithAttributes(
				attribute.Key("reason").String(event.Reason),
				attribute.Key("fieldPath").String(event.InvolvedObject.FieldPath),
				attribute.Key("component").String(event.Source.Component),
				attribute.Key("host").String(event.Source.Host),
			),
		)

		switch event.Reason {
		case "Scheduled":
			if event.CreationTimestamp.Sub(pod.CreationTimestamp.Time) < time.Millisecond {
				continue
			}
			_, eventSpan := h.Tracer.Start(ctx, strings.ReplaceAll(event.Message, "Scheduled", "Scheduling"),
				trace.WithTimestamp(pod.CreationTimestamp.Time),
				trace.WithAttributes(
					attribute.Key("reason").String(event.Reason),
					attribute.Key("fieldPath").String(event.InvolvedObject.FieldPath),
					attribute.Key("component").String(event.Source.Component),
					attribute.Key("host").String(event.Source.Host),
				),
			)
			eventSpan.End(
				trace.WithTimestamp(event.CreationTimestamp.Time),
			)
		case "SuccessfulAttachVolume":
			scheduledEvent := Events(events).FirstMatchingEvent("Scheduled")
			if scheduledEvent == nil || scheduledEvent.CreationTimestamp.Sub(event.CreationTimestamp.Time) < time.Millisecond {
				continue
			}
			_, eventSpan := h.Tracer.Start(ctx, event.Message,
				trace.WithTimestamp(scheduledEvent.CreationTimestamp.Time),
				trace.WithAttributes(
					attribute.Key("reason").String(event.Reason),
					attribute.Key("component").String(event.Source.Component),
					attribute.Key("host").String(event.Source.Host),
				),
			)
			eventSpan.End(
				trace.WithTimestamp(event.CreationTimestamp.Time),
			)
		case "Pulling":
			pulledEvent := Events(events).FirstMatchingEvent("Pulled", event.InvolvedObject.FieldPath)
			if pulledEvent == nil || pulledEvent.CreationTimestamp.Sub(event.CreationTimestamp.Time) < time.Millisecond {
				continue
			}
			if pulledContainers.Has(event.InvolvedObject.FieldPath) {
				continue
			}
			pulledContainers.Add(event.InvolvedObject.FieldPath)
			_, eventSpan := h.Tracer.Start(ctx, event.Message,
				trace.WithTimestamp(event.CreationTimestamp.Time),
				trace.WithAttributes(
					attribute.Key("reason").String(event.Reason),
					attribute.Key("fieldPath").String(event.InvolvedObject.FieldPath),
					attribute.Key("component").String(event.Source.Component),
					attribute.Key("host").String(event.Source.Host),
				),
			)
			eventSpan.End(
				trace.WithTimestamp(pulledEvent.CreationTimestamp.Time),
			)
		case "Pulled":
			pullingEvent := Events(events).FirstMatchingEvent("Pulling", event.InvolvedObject.FieldPath)
			if pullingEvent == nil {
				pullingEvent = previousEvent
			}
			if pullingEvent == nil || event.CreationTimestamp.Sub(pullingEvent.CreationTimestamp.Time) < time.Millisecond {
				continue
			}
			if pulledContainers.Has(event.InvolvedObject.FieldPath) {
				continue
			}
			pulledContainers.Add(event.InvolvedObject.FieldPath)
			_, eventSpan := h.Tracer.Start(ctx, strings.ReplaceAll(event.Message, "Pulled", "Pulling"),
				trace.WithTimestamp(pullingEvent.CreationTimestamp.Time),
				trace.WithAttributes(
					attribute.Key("reason").String(event.Reason),
					attribute.Key("fieldPath").String(event.InvolvedObject.FieldPath),
					attribute.Key("component").String(event.Source.Component),
					attribute.Key("host").String(event.Source.Host),
				),
			)
			eventSpan.End(
				trace.WithTimestamp(event.CreationTimestamp.Time),
			)
		case "Created":
			pulledEvent := Events(events).FirstMatchingEvent("Pulled", event.InvolvedObject.FieldPath)
			if pulledEvent == nil || event.CreationTimestamp.Sub(pulledEvent.CreationTimestamp.Time) < time.Millisecond {
				continue
			}
			_, eventSpan := h.Tracer.Start(ctx, strings.ReplaceAll(event.Message, "Created", "Creating"),
				trace.WithTimestamp(pulledEvent.CreationTimestamp.Time),
				trace.WithAttributes(
					attribute.Key("reason").String(event.Reason),
					attribute.Key("fieldPath").String(event.InvolvedObject.FieldPath),
					attribute.Key("component").String(event.Source.Component),
					attribute.Key("host").String(event.Source.Host),
				),
			)
			eventSpan.End(
				trace.WithTimestamp(event.CreationTimestamp.Time),
			)
		case "Started":
			createdEvent := Events(events).FirstMatchingEvent("Created", event.InvolvedObject.FieldPath)
			if createdEvent == nil || event.CreationTimestamp.Sub(createdEvent.CreationTimestamp.Time) < time.Millisecond {
				continue
			}
			_, eventSpan := h.Tracer.Start(ctx, strings.ReplaceAll(event.Message, "Started", "Starting"),
				trace.WithTimestamp(createdEvent.CreationTimestamp.Time),
				trace.WithAttributes(
					attribute.Key("reason").String(event.Reason),
					attribute.Key("fieldPath").String(event.InvolvedObject.FieldPath),
					attribute.Key("component").String(event.Source.Component),
					attribute.Key("host").String(event.Source.Host),
				),
			)
			eventSpan.End(
				trace.WithTimestamp(event.CreationTimestamp.Time),
			)
		}
	}

	containerStatuses := make([]v1.ContainerStatus, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)
	containerStatuses = append(containerStatuses, pod.Status.ContainerStatuses...)
	sort.Slice(containerStatuses, func(i, j int) bool {
		return containerStatuses[i].State.Terminated.FinishedAt.Before(&containerStatuses[j].State.Terminated.FinishedAt)
	})
	for _, container := range containerStatuses {
		span.AddEvent(fmt.Sprintf("Container %s terminated", container.Name),
			trace.WithTimestamp(container.State.Terminated.FinishedAt.Time),
			trace.WithAttributes(
				attribute.Key("exit-code").Int(int(container.State.Terminated.ExitCode)),
				attribute.Key("reason").String(container.State.Terminated.Reason),
			),
		)
	}

	for _, condition := range pod.Status.Conditions {
		span.AddEvent(fmt.Sprintf("Condition %s (%s): %s", condition.Type, condition.Reason, condition.Status),
			trace.WithTimestamp(condition.LastTransitionTime.Time),
			trace.WithAttributes(
				attribute.Key("type").String(string(condition.Type)),
				attribute.Key("reason").String(condition.Reason),
				attribute.Key("status").String(string(condition.Status)),
			),
		)
	}

	log.WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("Pod complete - ending span")
	span.End(
		trace.WithTimestamp(podCompletionTime),
	)

	return nil
}

func (h *KubernetesPodHandler) getSpanFor(pod *v1.Pod) (*EventSpan, error) {
	_, spanContext, err := extractTraceFrom(pod.Annotations)
	if errors.Is(err, ErrTraceNotFound) {
		err := h.createSpanFor(pod)
		if err != nil {
			return nil, err
		}
		_, spanContext, err = extractTraceFrom(pod.Annotations)
		if err != nil {
			return nil, fmt.Errorf("failed to find or create a span for Pod %s", pod.Name)
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

func (h *KubernetesPodHandler) createSpanFor(pod *v1.Pod) error {
	tr := h.Store.GetTknTaskRun(pod.Labels["tekton.dev/taskRun"])
	if tr == nil {
		return fmt.Errorf("failed to find the parent TaskRun for Kubernetes Pod %s: %w", pod.Name, ErrParentEntityNotFound)
	}

	ctx, parentSpanContext, err := extractTraceFrom(tr.Annotations)
	if err != nil {
		return fmt.Errorf("failed to extract the trace from the TaskRun %s: %w", tr.Name, err)
	}

	eventTrace, err := h.Store.GetEventTrace(parentSpanContext.TraceID())
	if err != nil {
		return err
	}

	if span, ok := eventTrace.FindSpanFor(EntityTypeKubePod, pod.Name); ok {
		injectTraceInto(trace.ContextWithSpan(context.Background(), span), pod.Annotations)
		return nil
	}

	ctx, span := h.Tracer.Start(ctx, pod.Name,
		trace.WithTimestamp(pod.CreationTimestamp.Time),
	)

	patch, err := injectTraceInto(ctx, pod.Annotations)
	if err != nil {
		return fmt.Errorf("failed to inject SpanContext into Pod %q: %w", pod.Name, err)
	}
	_, err = h.PodClient.Patch(ctx, pod.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch Pod %q with patch %q: %w", pod.Name, patch, err)
	}
	h.Store.AddKubePod(pod)

	eventTrace.AddSpan(EventSpan{
		Span: span,
		Entity: Entity{
			Type: EntityTypeKubePod,
			Name: pod.Name,
		},
	})
	h.Logger.WithField("Pod", pod.Name).WithField("traceID", span.SpanContext().TraceID()).WithField("spanID", span.SpanContext().SpanID()).Info("Creating new Span")

	return nil
}
