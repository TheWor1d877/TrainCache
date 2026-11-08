// resolve_http.go:按节点名查 Agent 地址的只读端点,供 CLI 拨号
// (docs/7.1-design.md《地址解析端点》)。
package agentclient

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// ResolvePath is the HTTP path of the node address lookup endpoint. It is
// served by the controller-manager metrics server.
const ResolvePath = "/resolve"

// ResolveHandler serves GET /resolve?node=<node>.
//
// The response body is the JSON array of candidate Agent addresses, in
// preference order. Status codes:
//
//	200 - candidates found;
//	400 - the node query parameter is missing;
//	404 - the node has no Agent address;
//	405 - the method is not GET;
//	500 - the lookup failed for another reason.
//
// Failures carry a {"error":"<message>"} body.
func ResolveHandler(resolver CandidateResolver) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeResolveError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		node := strings.TrimSpace(r.URL.Query().Get("node"))
		if node == "" {
			writeResolveError(w, http.StatusBadRequest, "node query parameter is required")
			return
		}
		addrs, err := resolver.ResolveCandidates(r.Context(), node)
		if err != nil {
			if errors.Is(err, ErrNoAgentAddress) {
				writeResolveError(w, http.StatusNotFound, err.Error())
				return
			}
			writeResolveError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if addrs == nil {
			addrs = []string{}
		}
		writeResolveJSON(w, http.StatusOK, addrs)
	})
}

func writeResolveJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeResolveError(w http.ResponseWriter, code int, message string) {
	writeResolveJSON(w, code, map[string]string{"error": message})
}
