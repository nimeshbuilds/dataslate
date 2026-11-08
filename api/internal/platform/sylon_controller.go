package platform

import (
	"context"
	"errors"
	"time"

	"dataslate.local/api/internal/llm"
)

type SylonController struct {
	Config         Config
	Store          Store
	Generator      SylonGenerator
	Embedder       SearchEmbedder
	CatalogBackend CatalogBackend
}

func (c *SylonController) server() *Server {
	return &Server{Config: c.Config, Store: c.Store, SearchEmbedder: c.Embedder, CatalogBackend: c.CatalogBackend}
}
func (c *SylonController) ReconcileOnce(ctx context.Context, owner string) (bool, error) {
	work, lease, err := c.Store.AcquireSylonJob(ctx, owner, 120*time.Second)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	finish := func(state, message string, prepared *SylonPreparedContext, result *llm.Result) (bool, error) {
		finishCtx, done := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer done()
		var resultJob *SylonJob
		if result != nil {
			resultJob = &SylonJob{Result: result}
		}
		return true, c.Store.FinishSylonJob(finishCtx, lease, state, message, prepared, resultJob)
	}
	if work.Job.CancelRequested {
		return finish("cancelled", "Output discarded; a prior provider request may still complete or be billed.", nil, nil)
	}
	if work.Job.DispatchStarted {
		return finish("unknown", "A previous provider dispatch has an uncertain outcome. No request was retried; submit a new explicit message to try again.", nil, nil)
	}
	server := c.server()
	binding, err := server.authorizedSylonBinding(work.Job.WorkspaceID, work.Job.BindingID, work.Job.SubmittedBy)
	if err != nil || binding.Hash() != work.PolicyHash {
		return finish("failed", "Current assistant authority is unavailable or changed; no provider request was made.", nil, nil)
	}
	prepCtx, prepDone := context.WithTimeout(ctx, 30*time.Second)
	history, err := c.Store.SylonHistory(prepCtx, work.Job.WorkspaceID, work.Job.ConversationID, work.Job.SubmittedBy)
	if err != nil {
		prepDone()
		return finish("failed", "Conversation context is unavailable.", nil, nil)
	}
	prior := []SylonWork{}
	for _, old := range history {
		if old.Job.ID != work.Job.ID {
			prior = append(prior, old)
		}
	}
	beforeExternal := func() error {
		if err := server.authorizeSylonWork(prepCtx, work, true); err != nil {
			return err
		}
		if err := c.Store.MarkSylonDispatch(prepCtx, lease, "preparing"); err != nil {
			return err
		}
		return server.authorizeSylonWork(prepCtx, work, true)
	}
	prepared, err := server.PrepareSylonContext(prepCtx, binding, work, prior, beforeExternal)
	prepDone()
	if err != nil {
		if errors.Is(err, ErrSylonCancelled) {
			return finish("cancelled", "Cancelled before generation; no new generation request was made.", nil, nil)
		}
		latest, lookupErr := c.Store.SylonWork(ctx, work.Job.WorkspaceID, work.Job.ID, work.Job.SubmittedBy)
		if lookupErr != nil {
			return true, lookupErr
		}
		if latest.Job.DispatchStarted {
			return finish("unknown", "Context preparation stopped after a possible provider request. No automatic retry was made.", nil, nil)
		}
		return finish("failed", "Context could not be authorized or fit within the configured limits. No provider request was made.", nil, nil)
	}
	saveCtx, saveDone := context.WithTimeout(ctx, 5*time.Second)
	err = c.Store.SaveSylonContext(saveCtx, lease, prepared)
	saveDone()
	if errors.Is(err, ErrSylonCancelled) {
		return finish("cancelled", "Output discarded before generation.", &prepared, nil)
	}
	if err != nil {
		return true, err
	}
	work.Context = prepared
	generationCtx, generationDone := context.WithTimeout(ctx, 60*time.Second)
	defer generationDone()
	if err = server.authorizeSylonWork(generationCtx, work, true); err != nil {
		return finish("failed", "Current context grants or source revisions changed before generation.", &prepared, nil)
	}
	if err = c.Store.MarkSylonDispatch(generationCtx, lease, "generating"); err != nil {
		if errors.Is(err, ErrSylonCancelled) {
			return finish("cancelled", "Cancelled before generation.", &prepared, nil)
		}
		return true, err
	}
	if err = server.authorizeSylonWork(generationCtx, work, true); err != nil {
		return finish("failed", "Context authority changed before provider dispatch.", &prepared, nil)
	}
	ids := []string{}
	for _, citation := range prepared.Citations {
		ids = append(ids, citation.ID)
	}
	result, err := c.Generator.Generate(generationCtx, binding.Provider, llm.Request{Instructions: prepared.Instructions, Input: prepared.Input, AllowedCitationIDs: ids})
	if err != nil {
		return finish("unknown", "Provider generation failed or its outcome is uncertain. No automatic retry was made.", &prepared, nil)
	}
	verifyCtx, verifyDone := context.WithTimeout(ctx, 5*time.Second)
	defer verifyDone()
	if err = server.authorizeSylonWork(verifyCtx, work, true); err != nil {
		return finish("failed", "Context authority or source revision changed; the generated output was discarded.", &prepared, nil)
	}
	if result.ProposedCode != nil {
		if work.Job.Notebook == nil {
			return finish("failed", "An unbound code proposal was rejected; select an exact saved cell for code assistance.", &prepared, nil)
		}
		n, err := c.Store.Notebook(verifyCtx, work.Job.WorkspaceID, work.Job.Notebook.ID)
		if err != nil {
			return finish("failed", "The target notebook is unavailable; output was discarded.", &prepared, nil)
		}
		matches := false
		for _, cell := range n.Cells {
			if cell.ID == work.Job.Notebook.CellID && cell.Language == result.ProposedCode.Language {
				matches = true
			}
		}
		if !matches {
			return finish("failed", "The code proposal language does not match the selected saved cell.", &prepared, nil)
		}
	}
	return finish("succeeded", "Assistant response completed. Code proposals require explicit apply and separate execution.", &prepared, &result)
}
