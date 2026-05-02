package main

// providerRouter dispatches each incoming workflow to whichever
// configured Provider matches the workflow's YAML body. Selection
// happens per-workflow: we decode the top-level `tack:` map and pick
// the first child key that names one of the registered providers.
// This keeps the trigger plumbing oblivious to which backend any
// given workflow will run on, and lets a single tack instance host
// multiple providers concurrently — the workflow YAML is the source
// of truth for routing, not the operator's env.
//
// Providers are registered as a (key → Provider) map. When a
// workflow names more than one provider key under `tack:` (a config
// mistake in practice) the router walks the YAML's child keys in
// document order and picks the first one that has a registered
// provider — Go's map iteration randomness never enters into it,
// because the YAML's MapSlice preserves order.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.yaml.in/yaml/v2"
	"tangled.org/core/api/tangled"
)

// providerRouter is itself a Provider so the rest of tack (knot
// consumer, HTTP handlers) keeps talking to a single Provider value
// regardless of how many real backends are wired in.
type providerRouter struct {
	log       *slog.Logger
	providers map[string]Provider
}

// Compile-time interface conformance check.
var _ Provider = (*providerRouter)(nil)

// newProviderRouter wires a router from a (key → Provider) map.
func newProviderRouter(
	log *slog.Logger,
	providers map[string]Provider,
) *providerRouter {
	return &providerRouter{
		log:       log.With("component", "provider", "kind", "router"),
		providers: providers,
	}
}

// Spawn satisfies Provider. Each workflow is routed independently —
// a single pipeline can mix workflows that target different
// providers. Workflows whose YAML doesn't name a known provider are
// logged loudly and skipped, since silently dropping them would
// hide a config error from the operator.
func (r *providerRouter) Spawn(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	actor string,
	trigger *tangled.Pipeline_TriggerMetadata,
	workflows []*tangled.Pipeline_Workflow,
) {
	for _, wf := range workflows {
		// Defensive: the lexicon allows nil entries and doesn't
		// require a name. We can't route a workflow that has no
		// body to inspect.
		if wf == nil || wf.Name == "" {
			continue
		}
		p, err := r.pick(wf.Raw)
		if err != nil {
			r.log.Error("route workflow",
				"err", err,
				"knot", knot,
				"pipeline_rkey", pipelineRkey,
				"workflow", wf.Name,
			)
			continue
		}
		// Hand the workflow off as a single-element slice so the
		// downstream provider's existing Spawn loop runs unchanged.
		// actor passes through verbatim; the router doesn't
		// authorize, it just dispatches.
		p.Spawn(ctx, knot, pipelineRkey, actor, trigger,
			[]*tangled.Pipeline_Workflow{wf},
		)
	}
}

// Logs satisfies Provider. The (knot, rkey, workflow) tuple alone
// doesn't tell us which backend ran the workflow — the YAML body
// isn't carried on the request — so we ask each provider and
// surface the first one that has a stream. ErrLogsNotFound from a
// provider just means "not mine"; we keep walking. Any other error
// is the answer (a real backend failure should surface to the HTTP
// caller, not be masked by the next provider).
//
// Map iteration order is undefined, but in practice exactly one
// provider should know about any given (knot, rkey, workflow) so the
// order is moot.
func (r *providerRouter) Logs(
	ctx context.Context,
	knot string,
	pipelineRkey string,
	workflow string,
) (<-chan LogLine, error) {
	for _, p := range r.providers {
		ch, err := p.Logs(ctx, knot, pipelineRkey, workflow)
		if errors.Is(err, ErrLogsNotFound) {
			continue
		}
		return ch, err
	}
	return nil, ErrLogsNotFound
}

// pick decodes raw and returns the first registered provider whose
// key appears as a child of the top-level `tack:` map, walking the
// YAML's children in document order. An empty body or YAML with no
// `tack:` block — or one whose children name no registered provider
// — is a structural error; no routing decision is possible.
func (r *providerRouter) pick(raw string) (Provider, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("workflow body is empty")
	}
	// MapSlice preserves the on-the-wire ordering of the children
	// of `tack:` so "first match" is deterministic w.r.t. the YAML
	// document, not Go's randomised map iteration.
	var doc struct {
		Tack yaml.MapSlice `yaml:"tack"`
	}
	if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, fmt.Errorf("parse workflow yaml: %w", err)
	}
	for _, item := range doc.Tack {
		// YAML map keys are usually strings, but the lexicon
		// doesn't enforce that — guard so a stray int/bool key
		// doesn't panic the type assertion.
		key, ok := item.Key.(string)
		if !ok {
			continue
		}
		if p, ok := r.providers[key]; ok {
			return p, nil
		}
	}
	return nil, fmt.Errorf(
		"workflow yaml has no `tack:` key matching a registered provider",
	)
}
