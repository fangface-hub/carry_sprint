package httptransport

import (
	"net/http"
	"strings"

	"carry_sprint/p1/transport/http/handler"
	"carry_sprint/p1/transport/http/middleware"
	"carry_sprint/p1/transport/http/presenter"
)

func NewRouter(h *handler.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "method not allowed", http.StatusNotFound)
			return
		}
		h.HandleListProjects(w, r)
	})

	mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.HandleListUsers(w, r)
		case http.MethodPost:
			h.HandleRegisterUser(w, r)
		default:
			presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "method not allowed", http.StatusNotFound)
		}
	})

	mux.HandleFunc("/api/locales/default", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "method not allowed", http.StatusNotFound)
			return
		}
		h.HandleGetDefaultLocale(w, r)
	})

	mux.HandleFunc("/api/projects/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/projects/")
		segs := splitPath(path)
		if len(segs) == 2 && segs[1] == "summary" && r.Method == http.MethodGet {
			h.HandleGetProjectSummary(w, r, segs[0])
			return
		}
		if len(segs) == 4 && segs[1] == "sprints" && segs[3] == "workspace" && r.Method == http.MethodGet {
			h.HandleGetSprintWorkspace(w, r, segs[0], segs[2])
			return
		}
		if len(segs) == 3 && segs[1] == "tasks" && r.Method == http.MethodPatch {
			h.HandleUpdateTask(w, r, segs[0], segs[2])
			return
		}
		if len(segs) == 2 && segs[1] == "resources" {
			if r.Method == http.MethodGet {
				h.HandleListResources(w, r, segs[0])
				return
			}
			if r.Method == http.MethodPut {
				h.HandleSaveResources(w, r, segs[0])
				return
			}
		}
		if len(segs) == 2 && segs[1] == "calendar" {
			if r.Method == http.MethodGet {
				h.HandleGetCalendar(w, r, segs[0])
				return
			}
			if r.Method == http.MethodPut {
				h.HandleSaveCalendar(w, r, segs[0])
				return
			}
		}
		if len(segs) == 5 && segs[1] == "sprints" && segs[3] == "carryover" && segs[4] == "apply" && r.Method == http.MethodPost {
			h.HandleApplyCarryover(w, r, segs[0], segs[2])
			return
		}
		if len(segs) == 2 && segs[1] == "roles" {
			if r.Method == http.MethodGet {
				h.HandleGetProjectRoles(w, r, segs[0])
				return
			}
			if r.Method == http.MethodPut {
				h.HandleSaveProjectRoles(w, r, segs[0])
				return
			}
		}
		presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "route not found", http.StatusNotFound)
	})

	mux.HandleFunc("/api/users/", func(w http.ResponseWriter, r *http.Request) {
		uid := strings.TrimPrefix(r.URL.Path, "/api/users/")
		if strings.TrimSpace(uid) == "" || strings.Contains(uid, "/") {
			presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "route not found", http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodPatch:
			h.HandleUpdateUser(w, r, uid)
		case http.MethodDelete:
			h.HandleDeleteUser(w, r, uid)
		default:
			presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "method not allowed", http.StatusNotFound)
		}
	})

	root := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			mux.ServeHTTP(w, r)
			return
		}
		presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "route not found", http.StatusNotFound)
	})
	return middleware.RequireRequestID(root)
}

func splitPath(v string) []string {
	parts := strings.Split(v, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return out
}
