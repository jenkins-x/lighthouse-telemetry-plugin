package trace

import (
	"context"
	"fmt"
	"time"

	"github.com/jenkins-x/go-scm/scm"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type LighthousePullRequestHandler struct {
	BaseResourceEventHandler
	Tracer                trace.Tracer
	ChildPullRequestDelay time.Duration
}

func (h *LighthousePullRequestHandler) Start(stopCh <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				h.endSpansForClosedPullRequests()
			}
		}
	}()
}

func (h *LighthousePullRequestHandler) handleWebhook(webhook scm.Webhook) error {
	log := h.Logger.WithField("repo", webhook.Repository().FullName)

	switch event := webhook.(type) {
	case *scm.PullRequestHook:
		log.
			WithField("pr", event.PullRequest.Number).
			WithField("action", event.Action).
			Debug("Handling pullrequest hook event")
		return h.handlePullRequestEvent(event)
	default:
		log.Trace("Ignoring non pullrequest hook event")
		return nil
	}
}

func (h *LighthousePullRequestHandler) handlePullRequestEvent(event *scm.PullRequestHook) error {
	pr := PullRequest{
		Owner:  event.Repo.Namespace,
		Repo:   event.Repo.Name,
		Number: fmt.Sprint(event.PullRequest.Number),
	}

	switch event.Action {
	case scm.ActionOpen, scm.ActionReopen:
		return h.handlePullRequestCreated(event, pr)
	case scm.ActionClose, scm.ActionMerge:
		return h.handlePullRequestClosed(event, pr)
	default:
		_, span, err := h.Store.FindGitopsTraceAndSpan(pr)
		if err != nil && err != ErrGitopsTraceNotFound {
			return err
		}
		if err == ErrGitopsTraceNotFound {
			return nil
		}
		if !span.IsRecording() {
			return nil
		}
		span.AddEvent(event.Action.String(),
			trace.WithTimestamp(event.PullRequest.Updated),
			trace.WithAttributes(attributesFromEvent(event)...),
		)
		return nil
	}
}

func (h *LighthousePullRequestHandler) handlePullRequestCreated(event *scm.PullRequestHook, pr PullRequest) error {
	// start by extracting the (parent) PRs referenced in this PR's release notes
	parentPullRequests := extractPullRequestReferences(event.PullRequest.Body)

	// we have a "root" application PR, so let's start a new trace for it
	if len(parentPullRequests) == 0 {
		h.startNewTrace(pr, event.PullRequest)
		return nil
	}

	// we have a "child" PR, we need to find its parent PR, trace and parent span
	parentPR := parentPullRequests[0]
	gitopsTrace, parentSpan, err := h.Store.FindGitopsTraceAndSpan(parentPR)
	if err != nil && err != ErrGitopsTraceNotFound {
		return err
	}
	if err == ErrGitopsTraceNotFound {
		h.Logger.
			WithField("parentPR", parentPR.String()).
			WithField("pr", pr.String()).
			Warning("Could not find an existing trace for parent PR - will start a new trace")
		h.startNewTrace(pr, event.PullRequest)
		return nil
	}

	// ensure that we don't duplicate the span if one already exists
	// (in case of duplicated webhook event)
	if _, found := gitopsTrace.GetSpanFor(pr); found {
		return nil
	}

	ctx := trace.ContextWithSpan(context.Background(), parentSpan)
	ctx, span := h.Tracer.Start(ctx, pr.Repo,
		trace.WithTimestamp(event.PullRequest.Created),
		trace.WithAttributes(
			attribute.Key("pr.owner").String(pr.Owner),
			attribute.Key("pr.repo").String(pr.Repo),
			attribute.Key("pr.number").String(pr.Number),
			attribute.Key("pr.title").String(event.PullRequest.Title),
			attribute.Key("pr.author").String(event.PullRequest.Author.Login),
			attribute.Key("pr.link").String(event.PullRequest.Link),
			attribute.Key("pr.parent").String(parentPR.String()),
		),
	)
	h.Logger.
		WithField("traceID", span.SpanContext().TraceID()).
		WithField("spanID", span.SpanContext().SpanID()).
		WithField("parentSpanID", parentSpan.SpanContext().SpanID()).
		WithField("pr", pr.String()).
		Info("Starting new span")
	gitopsTrace.AddSpan(GitopsSpan{
		Span:         span,
		PullRequest:  pr,
		ParentSpanID: parentSpan.SpanContext().SpanID(),
	})

	return nil
}

func (h *LighthousePullRequestHandler) handlePullRequestClosed(event *scm.PullRequestHook, pr PullRequest) error {
	gitopsTrace, span, err := h.Store.FindGitopsTraceAndSpan(pr)
	if err != nil && err != ErrGitopsTraceNotFound {
		return err
	}
	if err == ErrGitopsTraceNotFound {
		h.Logger.
			WithField("pr", pr.String()).
			Debug("Trace/Span not found for closed PR - ignoring event")
		return nil
	}

	if gitopsTrace.RootSpan().PullRequest == pr {
		h.Logger.
			WithField("pr", pr.String()).
			Debug("Marking root PR as closed")
		gitopsTrace.RootSpan().PullRequestClosed = &event.PullRequest.Updated
	} else {
		h.Logger.
			WithField("pr", pr.String()).
			Debug("Marking child PR as closed")
		span.PullRequestClosed = &event.PullRequest.Updated
	}

	span.AddEvent(event.Action.String(),
		trace.WithTimestamp(event.PullRequest.Updated),
		trace.WithAttributes(
			attribute.Key("merged").Bool(event.PullRequest.Merged),
		),
	)

	if event.PullRequest.Merged {
		span.SetStatus(codes.Ok, "Pull Request merged")
	} else {
		span.SetStatus(codes.Error, "Pull Request closed but not merged")
		span.End()
	}

	return nil
}

func (h *LighthousePullRequestHandler) startNewTrace(pr PullRequest, pullRequest scm.PullRequest) {
	_, rootSpan := h.Tracer.Start(context.Background(), pr.Repo,
		trace.WithNewRoot(),
		trace.WithTimestamp(pullRequest.Created),
		trace.WithAttributes(
			attribute.Key("pr.owner").String(pr.Owner),
			attribute.Key("pr.repo").String(pr.Repo),
			attribute.Key("pr.number").String(pr.Number),
			attribute.Key("pr.title").String(pullRequest.Title),
			attribute.Key("pr.author").String(pullRequest.Author.Login),
			attribute.Key("pr.link").String(pullRequest.Link),
		),
	)
	h.Logger.
		WithField("traceID", rootSpan.SpanContext().TraceID()).
		WithField("spanID", rootSpan.SpanContext().SpanID()).
		WithField("pr", pr.String()).
		Info("Creating new GitopsTrace")
	gitopsTrace := NewGitopsTrace(GitopsSpan{
		Span:        rootSpan,
		PullRequest: pr,
	})
	h.Store.AddGitopsTrace(*gitopsTrace)
}

func (h *LighthousePullRequestHandler) endSpansForClosedPullRequests() {
	h.Store.IterateOnGitopsTraces(func(gitopsTrace *GitopsTrace) {
		gitopsTrace.IterateOnGitopsSpans(func(span *GitopsSpan) {
			if !span.IsRecording() { // already merged and finished
				h.Logger.
					WithField("traceID", span.SpanContext().TraceID()).
					WithField("spanID", span.SpanContext().SpanID()).
					WithField("pr", span.PullRequest.String()).
					WithField("prClosed", span.PullRequestClosed).
					Debug("span already finished")
				return
			}
			if span.PullRequestClosed == nil { // not merged yet
				h.Logger.
					WithField("traceID", span.SpanContext().TraceID()).
					WithField("spanID", span.SpanContext().SpanID()).
					WithField("pr", span.PullRequest.String()).
					WithField("prClosed", span.PullRequestClosed).
					Debug("PR not closed - nothing to do")
				return
			}

			spanChildren := gitopsTrace.SpanChildren(span.SpanContext().SpanID())
			if len(spanChildren) == 0 {
				if time.Since(*span.PullRequestClosed) > h.ChildPullRequestDelay {
					h.Logger.
						WithField("traceID", span.SpanContext().TraceID()).
						WithField("spanID", span.SpanContext().SpanID()).
						WithField("pr", span.PullRequest.String()).
						WithField("prClosed", span.PullRequestClosed).
						Infof("Ending childless span (PR closed more than %s ago)", h.ChildPullRequestDelay)
					span.End(
						trace.WithTimestamp(*span.PullRequestClosed),
					)
				} else {
					h.Logger.
						WithField("traceID", span.SpanContext().TraceID()).
						WithField("spanID", span.SpanContext().SpanID()).
						WithField("pr", span.PullRequest.String()).
						WithField("prClosed", span.PullRequestClosed).
						Debugf("NOT ending childless span yet (PR closed less than %s ago, waiting for possible children)", h.ChildPullRequestDelay)
				}
				return
			}

			var lastPRClosed time.Time
			for _, spanChild := range spanChildren {
				if spanChild.IsRecording() {
					h.Logger.
						WithField("traceID", span.SpanContext().TraceID()).
						WithField("spanID", span.SpanContext().SpanID()).
						WithField("pr", span.PullRequest.String()).
						WithField("prClosed", span.PullRequestClosed).
						WithField("prChildren", len(spanChildren)).
						WithField("prChildRecording", spanChild.PullRequest.String()).
						Debug("At least one child span is still recording...")
					return
				}
				if lastPRClosed.IsZero() || spanChild.PullRequestClosed.After(lastPRClosed) {
					lastPRClosed = *spanChild.PullRequestClosed
				}
			}
			// at this point, we know that we have no open child spans
			// so we can safely end the span
			h.Logger.
				WithField("traceID", span.SpanContext().TraceID()).
				WithField("spanID", span.SpanContext().SpanID()).
				WithField("pr", span.PullRequest.String()).
				WithField("prClosed", span.PullRequestClosed).
				WithField("prChildren", len(spanChildren)).
				Info("Ending span (because all children are already finished)")
			span.End(
				trace.WithTimestamp(lastPRClosed),
			)
		})
	})
}

func attributesFromEvent(event *scm.PullRequestHook) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	switch event.Action {
	case scm.ActionLabel, scm.ActionUnlabel:
		attrs = append(attrs, attribute.Key("label").String(event.Label.Name))
	case scm.ActionAssigned, scm.ActionUnassigned:
		var assignees []string
		for _, assignee := range event.PullRequest.Assignees {
			assignees = append(assignees, assignee.Login)
		}
		attrs = append(attrs, attribute.Key("assignees").StringSlice(assignees))
	}
	return attrs
}
