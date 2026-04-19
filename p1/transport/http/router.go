package httptransport

import (
	_ "embed"
	"html/template"
	"net/http"
	"strings"

	"carry_sprint/p1/transport/http/handler"
	"carry_sprint/p1/transport/http/middleware"
	"carry_sprint/p1/transport/http/presenter"
)

//go:embed ui_shell.tmpl
var browserUIShellTemplateText string

//go:embed ui_app.js
var browserUIScreenScriptText string

var browserUIShellTemplate = template.Must(template.New("browser_ui_shell").Parse(browserUIShellTemplateText))

func NewRouter(h *handler.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.HandleListProjects(w, r)
		case http.MethodPost:
			h.HandleCreateProject(w, r)
		default:
			presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "method not allowed", http.StatusNotFound)
		}
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

	mux.HandleFunc("/api/top/menu", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "method not allowed", http.StatusNotFound)
			return
		}
		h.HandleGetTopMenu(w, r)
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
		path := strings.TrimPrefix(r.URL.Path, "/api/users/")
		segs := splitPath(path)
		if len(segs) == 0 || strings.TrimSpace(segs[0]) == "" {
			presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "route not found", http.StatusNotFound)
			return
		}
		uid := segs[0]
		if len(segs) == 2 && segs[1] == "menu-visibility" {
			switch r.Method {
			case http.MethodGet:
				h.HandleGetUserMenuVisibility(w, r, uid)
			case http.MethodPut:
				h.HandleSaveUserMenuVisibility(w, r, uid)
			default:
				presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "method not allowed", http.StatusNotFound)
			}
			return
		}
		if len(segs) == 2 && segs[1] == "locale" {
			switch r.Method {
			case http.MethodGet:
				h.HandleGetUserLocaleSetting(w, r, uid)
			case http.MethodPut:
				h.HandleSaveUserLocaleSetting(w, r, uid)
			default:
				presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "method not allowed", http.StatusNotFound)
			}
			return
		}
		if len(segs) != 1 {
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

	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		presenter.WriteError(w, r.Header.Get("X-Request-Id"), "ROUTE_NOT_FOUND", "route not found", http.StatusNotFound)
	})

	apiHandler := middleware.RequireRequestID(mux)

	root := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/") {
			apiHandler.ServeHTTP(w, r)
			return
		}
		if isBrowserUIRoute(r.URL.Path) {
			writeBrowserUIShell(w, r)
			return
		}
		presenter.WriteError(w, "", "ROUTE_NOT_FOUND", "route not found", http.StatusNotFound)
	})
	return root
}

func isBrowserUIRoute(path string) bool {
	if path == "/" || path == "/signin" || path == "/projects" || path == "/projects/new" || path == "/users" {
		return true
	}

	segs := splitPath(path)
	if len(segs) == 3 && segs[0] == "projects" {
		if segs[2] == "resources" || segs[2] == "calendar" {
			return true
		}
	}
	if len(segs) == 5 && segs[0] == "projects" && segs[2] == "sprints" && segs[4] == "workspace" {
		return true
	}
	return false
}

func writeBrowserUIShell(w http.ResponseWriter, r *http.Request) {
	title, desc, heading := resolveBrowserScreen(r.URL.Path, r.URL.Query().Get("dialog"))
	path := r.URL.RequestURI()
	view := struct {
		Title         string
		Desc          string
		Route         string
		Path          string
		ScreenHeading string
		Script        template.JS
	}{
		Title:         title,
		Desc:          desc,
		Route:         path,
		Path:          path,
		ScreenHeading: heading,
		Script:        template.JS(browserUIScreenScript()),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := browserUIShellTemplate.Execute(w, view); err != nil {
		http.Error(w, "failed to render browser ui", http.StatusInternalServerError)
	}
}

func resolveBrowserScreen(path string, dialog string) (string, string, string) {
	segs := splitPath(path)
	if path == "/signin" {
		return "Sign-In Screen", "Sign in to open browser UI screens.", "Sign In"
	}
	if path == "/" {
		return "Top Page", "Top menu, menu visibility settings, and locale settings screen.", "Top Menu"
	}
	if path == "/projects" {
		return "Project Select Screen", "Select a project to open project-specific screens.", "Projects"
	}
	if path == "/projects/new" {
		return "Project Register Screen", "Register a new project with an initial sprint and administrator assignment.", "Register Project"
	}
	if path == "/users" {
		return "User Management Screen", "Manage users and project role assignments.", "User Management"
	}
	if len(segs) == 3 && segs[0] == "projects" && segs[2] == "resources" {
		return "Resource Settings Screen", "Edit resource capacity for the selected project.", "Resource Settings"
	}
	if len(segs) == 3 && segs[0] == "projects" && segs[2] == "calendar" {
		return "Working-Day Calendar Screen", "Edit working-day calendar settings for the selected project.", "Working-Day Calendar"
	}
	if len(segs) == 5 && segs[0] == "projects" && segs[2] == "sprints" && segs[4] == "workspace" {
		if dialog == "carryover" {
			return "Carry-Over Review Dialog", "Review carry-over decisions in sprint workspace dialog mode.", "Carry-Over Review"
		}
		return "Sprint Workspace Screen", "Review sprint tasks and budget status for one sprint.", "Sprint Workspace"
	}
	return "Browser UI", "Browser UI shell is active.", "Screen"
}

func browserUIScreenScript() string {
return browserUIScreenScriptText
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
