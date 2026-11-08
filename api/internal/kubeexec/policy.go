package kubeexec

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"dataslate.local/api/internal/platform"
)

// GuardedBackend is the mandatory production controller boundary. It reloads
// administrator grants for every reconciliation and immediately before POST.
// Reads/cancellation remain possible after revocation so existing work can stop.
type GuardedBackend struct {
	*Client
	bindingsFile string
}

func NewGuarded(client *Client, bindingsFile string) (*GuardedBackend, error) {
	if client == nil || strings.TrimSpace(bindingsFile) == "" {
		return nil, errors.New("controller requires an execution bindings file")
	}
	bindings, err := platform.LoadExecutionBindings(bindingsFile)
	if err != nil {
		return nil, errors.New("controller execution bindings cannot be loaded")
	}
	if err = client.ValidateBindings(bindings); err != nil {
		return nil, err
	}
	return &GuardedBackend{Client: client, bindingsFile: bindingsFile}, nil
}

func (b *GuardedBackend) Authorize(ctx context.Context, r platform.Run) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	bindings, err := platform.LoadExecutionBindings(b.bindingsFile)
	if err != nil {
		return false, errors.New("current execution policy is unavailable")
	}
	if err = b.Client.ValidateBindings(bindings); err != nil {
		return false, errors.New("current execution policy is invalid")
	}
	for _, binding := range bindings {
		if binding.ID == r.BindingID && binding.WorkspaceID == r.WorkspaceID && reflect.DeepEqual(binding.ExecutionProfile, r.Profile) && binding.Allows(r.SubmittedBy) {
			return true, nil
		}
	}
	return false, nil
}

func (b *GuardedBackend) Submit(ctx context.Context, r platform.Run) (platform.BackendObservation, error) {
	allowed, err := b.Authorize(ctx, r)
	if err != nil {
		return platform.BackendObservation{}, err
	}
	if !allowed {
		return platform.BackendObservation{}, errors.New("execution authorization was revoked")
	}
	return b.Client.submit(ctx, r, b.Authorize)
}

func (b *GuardedBackend) PrepareSource(ctx context.Context, r platform.Run) (platform.SourceArtifact, error) {
	allowed, err := b.Authorize(ctx, r)
	if err != nil {
		return platform.SourceArtifact{}, err
	}
	if !allowed {
		return platform.SourceArtifact{}, errors.New("source preparation authorization revoked")
	}
	return b.Client.prepareSource(ctx, r, b.Authorize)
}
