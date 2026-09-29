package service

import (
	"net/http"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
)

const rerunReviewActionPath = "/workflows/actions/rerun_review"

// WorkflowLinkHandler returns the handler for user-initiated workflows linked
// from external services such as GitHub.
func (ws *workflowService) WorkflowLinkHandler() http.Handler {
	return http.HandlerFunc(ws.handleWorkflowLink)
}

func (ws *workflowService) handleWorkflowLink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if _, err := ws.env.GetAuthenticator().AuthenticatedUser(r.Context()); err != nil {
		if authutil.IsAnonymousUserError(err) {
			loginURL := "/?redirect_url=" + url.QueryEscape(r.URL.RequestURI())
			http.Redirect(w, r, loginURL, http.StatusTemporaryRedirect)
			return
		}
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return
	}

	switch r.URL.Path {
	case rerunReviewActionPath:
		// TODO: Implement: rerun the original workflow action with AGENT_REVIEW_FORCE=1.
		http.Error(w, "rerun review action is not implemented", http.StatusNotImplemented)
	default:
		http.NotFound(w, r)
	}
}
