package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	_ "modernc.org/sqlite"
)

type app struct {
	systemDB   *sql.DB
	dataDir    string
	projectDBs map[string]*sql.DB
	transport  p2Transport
	mu         sync.Mutex
}

type p2Transport interface {
	Send(zmqRequest) (zmqResponse, error)
	Close() error
}

type zmqClientTransport struct {
	ctx    context.Context
	socket zmq4.Socket
	mu     sync.Mutex
}

func (t *zmqClientTransport) Send(req zmqRequest) (zmqResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, err := json.Marshal(req)
	if err != nil {
		return zmqResponse{}, err
	}
	if err := t.socket.Send(zmq4.NewMsg(b)); err != nil {
		return zmqResponse{}, err
	}
	msg, err := t.socket.Recv()
	if err != nil {
		return zmqResponse{}, err
	}
	if len(msg.Frames) == 0 {
		return zmqResponse{}, errors.New("empty zmq response")
	}
	var resp zmqResponse
	if err := json.Unmarshal(msg.Frames[0], &resp); err != nil {
		return zmqResponse{}, err
	}
	return resp, nil
}

func (t *zmqClientTransport) Close() error {
	return t.socket.Close()
}

type apiError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *apiError) Error() string {
	return e.Code + ": " + e.Message
}

type responseBody struct {
	RequestID string         `json:"request_id"`
	Result    string         `json:"result"`
	Data      any            `json:"data,omitempty"`
	Error     *responseError `json:"error,omitempty"`
}

type responseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type zmqRequest struct {
	RequestID   string            `json:"request_id"`
	Command     string            `json:"command"`
	ProjectID   string            `json:"project_id"`
	PathParams  map[string]string `json:"path_params"`
	QueryParams map[string]string `json:"query_params"`
	Payload     json.RawMessage   `json:"payload"`
}

type zmqResponse struct {
	RequestID string         `json:"request_id"`
	Status    string         `json:"status"`
	Data      any            `json:"data,omitempty"`
	Error     *responseError `json:"error,omitempty"`
}

//go:embed ui_shell.tmpl
var browserUIShellTemplateText string

//go:embed ui_app.js
var browserUIScreenScriptText string

var browserUIShellTemplate = template.Must(template.New("browser_ui_shell").Parse(browserUIShellTemplateText))

func main() {
	dataDir := envOr("CARRY_SPRINT_DATA_DIR", "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	systemPath := filepath.Join(dataDir, "system.sqlite")
	systemDB, err := sql.Open("sqlite", systemPath)
	if err != nil {
		log.Fatalf("failed to open system db: %v", err)
	}
	defer systemDB.Close()

	if err := initSystemSchema(systemDB); err != nil {
		log.Fatalf("failed to init system schema: %v", err)
	}
	if err := seedSystemData(systemDB); err != nil {
		log.Fatalf("failed to seed system data: %v", err)
	}

	a := &app{
		systemDB:   systemDB,
		dataDir:    dataDir,
		projectDBs: map[string]*sql.DB{},
	}
	if err := a.ensureProjectSchema("demo"); err != nil {
		log.Fatalf("failed to init demo project db: %v", err)
	}
	if err := a.seedProjectData("demo"); err != nil {
		log.Fatalf("failed to seed demo project db: %v", err)
	}

	endpoint := envOr("CARRY_SPRINT_ZMQ_ENDPOINT", "tcp://127.0.0.1:5557")
	stopServer, clientTransport, err := a.startZMQBridge(endpoint)
	if err != nil {
		log.Fatalf("failed to start zmq bridge: %v", err)
	}
	defer stopServer()
	defer clientTransport.Close()
	a.transport = clientTransport
	log.Printf("CarrySprint uses ZeroMQ transport at %s", endpoint)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/projects", a.handleProjects)
	mux.HandleFunc("/api/projects/", a.handleProjectSubRoutes)
	mux.HandleFunc("/api/users", a.handleUsers)
	mux.HandleFunc("/api/users/", a.handleUserByID)
	mux.HandleFunc("/api/locales/default", a.handleDefaultLocale)
	mux.HandleFunc("/api/top/menu", a.handleTopMenu)
	mux.HandleFunc("/api/", a.handleNotFound)
	mux.HandleFunc("/", a.handleNotFound)

	addr := envOr("CARRY_SPRINT_ADDR", ":8080")
	log.Printf("CarrySprint server listening on %s", addr)
	if err := http.ListenAndServe(addr, loggingMiddleware(mux)); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s (%s)", r.Method, r.URL.Path, time.Since(start))
	})
}

func envOr(key, defaultValue string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultValue
	}
	return v
}

func (a *app) handleNotFound(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/api") {
		if isBrowserUIRoute(r.URL.Path) {
			writeBrowserUIShell(w, r)
			return
		}
		writeErr(w, "", &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
		return
	}

	rid, err := requireRequestID(r)
	if err != nil {
		writeErr(w, "", err)
		return
	}
	writeErr(w, rid, &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
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

func (a *app) handleProjects(w http.ResponseWriter, r *http.Request) {
	rid, reqErr := requireRequestID(r)
	if reqErr != nil {
		writeErr(w, "", reqErr)
		return
	}
	if r.URL.Path != "/api/projects" {
		writeErr(w, rid, &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
		return
	}

switch r.Method {
		case http.MethodGet:
			resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "list_projects"})
			if resp.Status == "error" {
				writeErr(w, rid, mapP2Error(resp.Error))
				return
			}
			writeOK(w, rid, resp.Data)
		case http.MethodPost:
			if err := requireJSONContentType(r); err != nil {
				writeErr(w, rid, err)
				return
			}
			var p map[string]any
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				writeErr(w, rid, &apiError{Code: "INVALID_JSON", Message: "invalid json payload", HTTPStatus: http.StatusBadRequest})
				return
			}
			payload, _ := json.Marshal(p)
			resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "create_project", Payload: payload})
			if resp.Status == "error" {
				writeErr(w, rid, mapP2Error(resp.Error))
				return
			}
			writeOK(w, rid, resp.Data)
		default:
			writeErr(w, rid, methodNotAllowed())
		}
}

func (a *app) handleProjectSubRoutes(w http.ResponseWriter, r *http.Request) {
	rid, err := requireRequestID(r)
	if err != nil {
		writeErr(w, "", err)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/projects/")
	segs := splitPath(path)
	if len(segs) == 0 {
		writeErr(w, rid, &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
		return
	}
	projectID := segs[0]

	if len(segs) == 2 && segs[1] == "summary" && r.Method == http.MethodGet {
		a.handleGetProjectSummary(w, rid, projectID)
		return
	}
	if len(segs) == 2 && segs[1] == "resources" {
		a.handleResources(w, r, rid, projectID)
		return
	}
	if len(segs) == 2 && segs[1] == "calendar" {
		a.handleCalendar(w, r, rid, projectID)
		return
	}
	if len(segs) == 2 && segs[1] == "roles" {
		a.handleRoles(w, r, rid, projectID)
		return
	}
	if len(segs) == 3 && segs[1] == "tasks" && r.Method == http.MethodPatch {
		a.handleUpdateTask(w, r, rid, projectID, segs[2])
		return
	}
	if len(segs) == 4 && segs[1] == "sprints" && segs[3] == "workspace" && r.Method == http.MethodGet {
		a.handleSprintWorkspace(w, rid, projectID, segs[2])
		return
	}
	if len(segs) == 5 && segs[1] == "sprints" && segs[3] == "carryover" && segs[4] == "apply" && r.Method == http.MethodPost {
		a.handleApplyCarryover(w, r, rid, projectID, segs[2])
		return
	}

	writeErr(w, rid, &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
}

func (a *app) handleGetProjectSummary(w http.ResponseWriter, rid, projectID string) {
	resp := a.sendToP2(zmqRequest{
		RequestID: rid,
		Command:   "get_project_summary",
		PathParams: map[string]string{
			"project_id": projectID,
		},
	})
	if resp.Status == "error" {
		writeErr(w, rid, mapP2Error(resp.Error))
		return
	}
	writeOK(w, rid, resp.Data)
}

func (a *app) handleSprintWorkspace(w http.ResponseWriter, rid, projectID, sprintID string) {
	resp := a.sendToP2(zmqRequest{
		RequestID: rid,
		Command:   "get_sprint_workspace",
		PathParams: map[string]string{
			"project_id": projectID,
			"sprint_id":  sprintID,
		},
	})
	if resp.Status == "error" {
		writeErr(w, rid, mapP2Error(resp.Error))
		return
	}
	writeOK(w, rid, resp.Data)
}

func (a *app) handleUpdateTask(w http.ResponseWriter, r *http.Request, rid, projectID, taskID string) {
	if err := requireJSONContentType(r); err != nil {
		writeErr(w, rid, err)
		return
	}

	type payload struct {
		EstimateHours *float64 `json:"estimate_hours"`
		Impact        *string  `json:"impact"`
		Status        *string  `json:"status"`
	}
	var p payload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeErr(w, rid, invalidJSON())
		return
	}
	bodyBytes, _ := json.Marshal(p)
	resp := a.sendToP2(zmqRequest{
		RequestID: rid,
		Command:   "update_task",
		PathParams: map[string]string{
			"project_id": projectID,
			"task_id":    taskID,
		},
		Payload: bodyBytes,
	})
	if resp.Status == "error" {
		writeErr(w, rid, mapP2Error(resp.Error))
		return
	}
	writeOK(w, rid, resp.Data)
}

func (a *app) handleResources(w http.ResponseWriter, r *http.Request, rid, projectID string) {
	switch r.Method {
	case http.MethodGet:
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "list_resources", PathParams: map[string]string{"project_id": projectID}})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeErr(w, rid, err)
			return
		}
		type resource struct {
			ResourceID          string  `json:"resource_id"`
			Name                string  `json:"name"`
			CapacityHoursPerDay float64 `json:"capacity_hours_per_day"`
		}
		var p struct {
			Resources []resource `json:"resources"`
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeErr(w, rid, invalidJSON())
			return
		}
		payload, _ := json.Marshal(p)
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "save_resources", PathParams: map[string]string{"project_id": projectID}, Payload: payload})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	default:
		writeErr(w, rid, methodNotAllowed())
	}
}

func (a *app) handleCalendar(w http.ResponseWriter, r *http.Request, rid, projectID string) {
	switch r.Method {
	case http.MethodGet:
		month := r.URL.Query().Get("month")
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "get_calendar", PathParams: map[string]string{"project_id": projectID}, QueryParams: map[string]string{"month": month}})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeErr(w, rid, err)
			return
		}
		type day struct {
			Date      string `json:"date"`
			IsWorking bool   `json:"is_working"`
		}
		var p struct {
			Days []day `json:"days"`
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeErr(w, rid, invalidJSON())
			return
		}
		payload, _ := json.Marshal(p)
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "save_calendar", PathParams: map[string]string{"project_id": projectID}, Payload: payload})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	default:
		writeErr(w, rid, methodNotAllowed())
	}
}

func (a *app) handleApplyCarryover(w http.ResponseWriter, r *http.Request, rid, projectID, sprintID string) {
	if err := requireJSONContentType(r); err != nil {
		writeErr(w, rid, err)
		return
	}
	type decision struct {
		TaskID         string  `json:"task_id"`
		Action         string  `json:"action"`
		TargetSprintID *string `json:"target_sprint_id"`
	}
	var p struct {
		Decisions []decision `json:"decisions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeErr(w, rid, invalidJSON())
		return
	}

	payload, _ := json.Marshal(p)
	resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "apply_carryover", PathParams: map[string]string{"project_id": projectID, "sprint_id": sprintID}, Payload: payload})
	if resp.Status == "error" {
		writeErr(w, rid, mapP2Error(resp.Error))
		return
	}
	writeOK(w, rid, resp.Data)
}

func (a *app) handleUsers(w http.ResponseWriter, r *http.Request) {
	rid, err := requireRequestID(r)
	if err != nil {
		writeErr(w, "", err)
		return
	}
	if r.URL.Path != "/api/users" {
		writeErr(w, rid, &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
		return
	}

	switch r.Method {
	case http.MethodGet:
		resp := a.sendToP2(zmqRequest{
			RequestID: rid,
			Command:   "list_users",
		})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	case http.MethodPost:
		if err := requireJSONContentType(r); err != nil {
			writeErr(w, rid, err)
			return
		}
		var p struct {
			UserID string `json:"user_id"`
			Name   string `json:"name"`
			Email  string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeErr(w, rid, invalidJSON())
			return
		}
		payload, _ := json.Marshal(p)
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "register_user", Payload: payload})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	default:
		writeErr(w, rid, methodNotAllowed())
	}
}

func (a *app) handleUserByID(w http.ResponseWriter, r *http.Request) {
	rid, err := requireRequestID(r)
	if err != nil {
		writeErr(w, "", err)
		return
	}

	segs := splitPath(strings.TrimPrefix(r.URL.Path, "/api/users/"))
	if len(segs) == 2 && segs[1] == "menu-visibility" {
		a.handleUserMenuVisibility(w, r, rid, segs[0])
		return
	}
	if len(segs) == 2 && segs[1] == "locale" {
		a.handleUserLocaleSetting(w, r, rid, segs[0])
		return
	}
	if len(segs) != 1 {
		writeErr(w, rid, &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
		return
	}
	userID := segs[0]

	switch r.Method {
	case http.MethodPatch:
		if err := requireJSONContentType(r); err != nil {
			writeErr(w, rid, err)
			return
		}
		var p struct {
			Name  *string `json:"name"`
			Email *string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeErr(w, rid, invalidJSON())
			return
		}
		if p.Name == nil && p.Email == nil {
			writeErr(w, rid, invalidJSON())
			return
		}
		payload, _ := json.Marshal(p)
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "update_user", PathParams: map[string]string{"user_id": userID}, Payload: payload})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	case http.MethodDelete:
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "delete_user", PathParams: map[string]string{"user_id": userID}})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeJSON(w, http.StatusOK, responseBody{RequestID: rid, Result: "ok"})
	default:
		writeErr(w, rid, methodNotAllowed())
	}
}

func (a *app) handleUserMenuVisibility(w http.ResponseWriter, r *http.Request, rid, userID string) {
	switch r.Method {
	case http.MethodGet:
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "get_user_menu_visibility", PathParams: map[string]string{"user_id": userID}})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeErr(w, rid, err)
			return
		}
		var p struct {
			MenuVisibility []struct {
				MenuKey   string `json:"menu_key"`
				IsEnabled bool   `json:"is_enabled"`
			} `json:"menu_visibility"`
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeErr(w, rid, invalidJSON())
			return
		}
		payload, _ := json.Marshal(p)
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "save_user_menu_visibility", PathParams: map[string]string{"user_id": userID}, Payload: payload})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	default:
		writeErr(w, rid, methodNotAllowed())
	}
}

func (a *app) handleUserLocaleSetting(w http.ResponseWriter, r *http.Request, rid, userID string) {
	switch r.Method {
	case http.MethodGet:
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "get_user_locale_setting", PathParams: map[string]string{"user_id": userID}})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeErr(w, rid, err)
			return
		}
		var p struct {
			Locale string `json:"locale"`
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeErr(w, rid, invalidJSON())
			return
		}
		payload, _ := json.Marshal(p)
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "save_user_locale_setting", PathParams: map[string]string{"user_id": userID}, Payload: payload})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	default:
		writeErr(w, rid, methodNotAllowed())
	}
}

func (a *app) handleRoles(w http.ResponseWriter, r *http.Request, rid, projectID string) {
	switch r.Method {
	case http.MethodGet:
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "get_project_roles", PathParams: map[string]string{"project_id": projectID}})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeErr(w, rid, err)
			return
		}
		type roleItem struct {
			UserID string `json:"user_id"`
			Role   string `json:"role"`
		}
		var p struct {
			Roles []roleItem `json:"roles"`
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeErr(w, rid, invalidJSON())
			return
		}
		payload, _ := json.Marshal(p)
		resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "save_project_roles", PathParams: map[string]string{"project_id": projectID}, Payload: payload})
		if resp.Status == "error" {
			writeErr(w, rid, mapP2Error(resp.Error))
			return
		}
		writeOK(w, rid, resp.Data)
	default:
		writeErr(w, rid, methodNotAllowed())
	}
}

func (a *app) handleDefaultLocale(w http.ResponseWriter, r *http.Request) {
	rid, reqErr := requireRequestID(r)
	if reqErr != nil {
		writeErr(w, "", reqErr)
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, rid, methodNotAllowed())
		return
	}
	if r.URL.Path != "/api/locales/default" {
		writeErr(w, rid, &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
		return
	}

	queryParams := map[string]string{
		"accept_language": r.Header.Get("Accept-Language"),
	}
	if userID := strings.TrimSpace(r.Header.Get("X-User-Id")); userID != "" {
		queryParams["user_id"] = userID
	}
	resp := a.sendToP2(zmqRequest{RequestID: rid, Command: "resolve_default_locale", QueryParams: queryParams})
	if resp.Status == "error" {
		writeErr(w, rid, mapP2Error(resp.Error))
		return
	}
	writeOK(w, rid, resp.Data)
}

func (a *app) handleTopMenu(w http.ResponseWriter, r *http.Request) {
	rid, reqErr := requireRequestID(r)
	if reqErr != nil {
		writeErr(w, "", reqErr)
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, rid, methodNotAllowed())
		return
	}
	if r.URL.Path != "/api/top/menu" {
		writeErr(w, rid, &apiError{Code: "ROUTE_NOT_FOUND", Message: "route not found", HTTPStatus: http.StatusNotFound})
		return
	}
	userID := strings.TrimSpace(r.Header.Get("X-User-Id"))
	if userID == "" {
		writeErr(w, rid, &apiError{Code: "INVALID_PATH_PARAM", Message: "X-User-Id is required", HTTPStatus: http.StatusBadRequest})
		return
	}

	resp := a.sendToP2(zmqRequest{
		RequestID: rid,
		Command:   "get_top_menu",
		QueryParams: map[string]string{
			"user_id": userID,
		},
	})
	if resp.Status == "error" {
		writeErr(w, rid, mapP2Error(resp.Error))
		return
	}
	writeOK(w, rid, resp.Data)
}

func (a *app) sendToP2(req zmqRequest) zmqResponse {
	if a.transport == nil {
		return a.dispatchP2(req)
	}
	resp, err := a.transport.Send(req)
	if err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "UPSTREAM_UNAVAILABLE", Message: err.Error()}}
	}
	return resp
}

func (a *app) startZMQBridge(endpoint string) (func(), *zmqClientTransport, error) {
	serverCtx, cancelServer := context.WithCancel(context.Background())
	server := zmq4.NewRep(serverCtx)
	if err := server.Listen(endpoint); err != nil {
		cancelServer()
		return nil, nil, err
	}

	go func() {
		defer server.Close()
		for {
			msg, err := server.Recv()
			if err != nil {
				return
			}
			if len(msg.Frames) == 0 {
				_ = server.Send(zmq4.NewMsg([]byte(`{"status":"error","error":{"code":"INVALID_JSON","message":"empty payload"}}`)))
				continue
			}
			var req zmqRequest
			if err := json.Unmarshal(msg.Frames[0], &req); err != nil {
				_ = server.Send(zmq4.NewMsg([]byte(`{"status":"error","error":{"code":"INVALID_JSON","message":"invalid payload"}}`)))
				continue
			}
			resp := a.dispatchP2(req)
			b, err := json.Marshal(resp)
			if err != nil {
				_ = server.Send(zmq4.NewMsg([]byte(`{"status":"error","error":{"code":"PERSISTENCE_ERROR","message":"marshal failed"}}`)))
				continue
			}
			if err := server.Send(zmq4.NewMsg(b)); err != nil {
				return
			}
		}
	}()

	clientCtx, _ := context.WithCancel(context.Background())
	clientSocket := zmq4.NewReq(clientCtx)
	if err := clientSocket.Dial(endpoint); err != nil {
		cancelServer()
		_ = clientSocket.Close()
		return nil, nil, err
	}

	stop := func() {
		cancelServer()
		_ = clientSocket.Close()
	}
	return stop, &zmqClientTransport{ctx: clientCtx, socket: clientSocket}, nil
}

func (a *app) dispatchP2(req zmqRequest) zmqResponse {
	switch req.Command {
	case "list_projects":
		return a.p2ListProjects(req)
	case "get_project_summary":
		return a.p2GetProjectSummary(req)
	case "create_project":
		return a.p2CreateProject(req)
	case "get_sprint_workspace":
		return a.p2GetSprintWorkspace(req)
	case "update_task":
		return a.p2UpdateTask(req)
	case "list_resources":
		return a.p2ListResources(req)
	case "save_resources":
		return a.p2SaveResources(req)
	case "get_calendar":
		return a.p2GetCalendar(req)
	case "save_calendar":
		return a.p2SaveCalendar(req)
	case "apply_carryover":
		return a.p2ApplyCarryover(req)
	case "list_users":
		return a.p2ListUsers(req)
	case "register_user":
		return a.p2RegisterUser(req)
	case "update_user":
		return a.p2UpdateUser(req)
	case "delete_user":
		return a.p2DeleteUser(req)
	case "get_project_roles":
		return a.p2GetProjectRoles(req)
	case "save_project_roles":
		return a.p2SaveProjectRoles(req)
	case "resolve_default_locale":
		return a.p2ResolveDefaultLocale(req)
	case "get_user_locale_setting":
		return a.p2GetUserLocaleSetting(req)
	case "save_user_locale_setting":
		return a.p2SaveUserLocaleSetting(req)
	case "get_top_menu":
		return a.p2GetTopMenu(req)
	case "get_user_menu_visibility":
		return a.p2GetUserMenuVisibility(req)
	case "save_user_menu_visibility":
		return a.p2SaveUserMenuVisibility(req)
	default:
		return zmqResponse{
			RequestID: req.RequestID,
			Status:    "error",
			Error:     &responseError{Code: "UNKNOWN_COMMAND", Message: "unknown command"},
		}
	}
}

func (a *app) p2ListProjects(req zmqRequest) zmqResponse {
	rows, err := a.systemDB.Query(`SELECT project_id, name, description, updated_at FROM projects ORDER BY name ASC`)
	if err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}
	defer rows.Close()

	type item struct {
		ProjectID   string `json:"project_id"`
		Name        string `json:"name"`
		Description string `json:"description"`
		UpdatedAt   string `json:"updated_at"`
	}
	list := []item{}
	for rows.Next() {
		var i item
		if err := rows.Scan(&i.ProjectID, &i.Name, &i.Description, &i.UpdatedAt); err != nil {
			return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
		}
		list = append(list, i)
	}

	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"projects": list}}
}

func (a *app) p2GetProjectSummary(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	var projectIDOut, name, description, updatedAt string
	if err := a.systemDB.QueryRow(
		`SELECT project_id, name, description, updated_at FROM projects WHERE project_id = ?`,
		projectID,
	).Scan(&projectIDOut, &name, &description, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p2NotFound(req.RequestID, "PROJECT_NOT_FOUND", "project not found")
		}
		return p2Persistence(req.RequestID, err)
	}
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	var sprintCount, taskCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sprints`).Scan(&sprintCount); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM tasks`).Scan(&taskCount); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{
		"project_id":   projectIDOut,
		"name":         name,
		"description":  description,
		"sprint_count": sprintCount,
		"task_count":   taskCount,
		"updated_at":   updatedAt,
	}}
}

func (a *app) p2CreateProject(req zmqRequest) zmqResponse {
	type initialSprintPayload struct {
		SprintID  string `json:"sprint_id"`
		Name      string `json:"name"`
		StartDate string `json:"start_date"`
		EndDate   string `json:"end_date"`
	}
	type payload struct {
		ProjectID      string               `json:"project_id"`
		Name           string               `json:"name"`
		Description    string               `json:"description"`
		InitialSprint  initialSprintPayload `json:"initial_sprint"`
		InitialAdminID string               `json:"initial_admin_user_id"`
	}
	var p payload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "INVALID_JSON", Message: "invalid json payload"}}
	}

	// Q1 - Check project_id uniqueness
	var existing string
	err := a.systemDB.QueryRow(`SELECT project_id FROM projects WHERE project_id = ?`, p.ProjectID).Scan(&existing)
	if err == nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "DUPLICATE_PROJECT_ID", Message: "project_id already exists"}}
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}

	// Q2 - Check initial_admin_user_id exists
	var adminUID string
	err = a.systemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, p.InitialAdminID).Scan(&adminUID)
	if errors.Is(err, sql.ErrNoRows) {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "USER_NOT_FOUND", Message: "initial_admin_user_id not found"}}
	}
	if err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}

	// Validate sprint date range
	start, err1 := time.Parse("2006-01-02", p.InitialSprint.StartDate)
	end, err2 := time.Parse("2006-01-02", p.InitialSprint.EndDate)
	if err1 != nil || err2 != nil || start.After(end) {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "INVALID_SPRINT_DATE_RANGE", Message: "initial_sprint.start_date must not be after end_date"}}
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// Q3 - Insert project (system.sqlite)
	tx, err := a.systemDB.Begin()
	if err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}
	_, err = tx.Exec(`INSERT INTO projects (project_id, name, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		p.ProjectID, p.Name, p.Description, now, now)
	if err != nil {
		_ = tx.Rollback()
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}
	if err := tx.Commit(); err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}

	// Q4 + Q5 - Open project DB, insert sprint and role
	pdb, err := a.openProjectDB(p.ProjectID)
	if err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}
	ptx, err := pdb.Begin()
	if err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}
	_, err = ptx.Exec(
		`INSERT INTO sprints (sprint_id, project_id, name, start_date, end_date, available_hours, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?)`,
		p.InitialSprint.SprintID, p.ProjectID, p.InitialSprint.Name, p.InitialSprint.StartDate, p.InitialSprint.EndDate, now, now)
	if err != nil {
		_ = ptx.Rollback()
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}
	_, err = ptx.Exec(
		`INSERT INTO project_roles (project_id, user_id, role) VALUES (?, ?, 'administrator')`,
		p.ProjectID, p.InitialAdminID)
	if err != nil {
		_ = ptx.Rollback()
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}
	if err := ptx.Commit(); err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}

	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{
		"project_id":            p.ProjectID,
		"name":                  p.Name,
		"description":           p.Description,
		"initial_admin_user_id": p.InitialAdminID,
		"initial_sprint": map[string]any{
			"sprint_id":  p.InitialSprint.SprintID,
			"name":       p.InitialSprint.Name,
			"start_date": p.InitialSprint.StartDate,
			"end_date":   p.InitialSprint.EndDate,
		},
		"created_at": now,
	}}
}

func (a *app) p2GetSprintWorkspace(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	sprintID := req.PathParams["sprint_id"]
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	var sprintName string
	var available float64
	if err := db.QueryRow(
		`SELECT name, available_hours FROM sprints WHERE sprint_id = ? AND project_id = ?`,
		sprintID, projectID,
	).Scan(&sprintName, &available); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p2NotFound(req.RequestID, "SPRINT_NOT_FOUND", "sprint not found")
		}
		return p2Persistence(req.RequestID, err)
	}
	rows, err := db.Query(`SELECT task_id, title, estimate_hours, impact, status FROM tasks WHERE sprint_id = ? ORDER BY task_id ASC`, sprintID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	defer rows.Close()
	type item struct {
		TaskID       string   `json:"task_id"`
		Title        string   `json:"title"`
		EstimateHour *float64 `json:"estimate_hours"`
		Impact       *string  `json:"impact"`
		Status       string   `json:"status"`
		NeedsInput   bool     `json:"needs_input"`
	}
	budgetIn := []item{}
	budgetOut := []item{}
	cumulative := 0.0
	inTotal := 0.0
	outTotal := 0.0
	for rows.Next() {
		var t item
		var est sql.NullFloat64
		var impact sql.NullString
		if err := rows.Scan(&t.TaskID, &t.Title, &est, &impact, &t.Status); err != nil {
			return p2Persistence(req.RequestID, err)
		}
		if est.Valid {
			e := est.Float64
			t.EstimateHour = &e
		}
		if impact.Valid {
			v := impact.String
			t.Impact = &v
		}
		t.NeedsInput = !est.Valid || !impact.Valid
		if t.NeedsInput {
			budgetOut = append(budgetOut, t)
			if est.Valid {
				outTotal += est.Float64
			}
			continue
		}
		next := cumulative + est.Float64
		if next <= available {
			budgetIn = append(budgetIn, t)
			cumulative = next
			inTotal += est.Float64
			continue
		}
		budgetOut = append(budgetOut, t)
		outTotal += est.Float64
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{
		"sprint_id":       sprintID,
		"sprint_name":     sprintName,
		"available_hours": available,
		"budget_in":       budgetIn,
		"budget_out":      budgetOut,
		"totals":          map[string]any{"budget_in_hours": inTotal, "budget_out_hours": outTotal},
	}}
}

func (a *app) p2UpdateTask(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	taskID := req.PathParams["task_id"]
	var p struct {
		EstimateHours *float64 `json:"estimate_hours"`
		Impact        *string  `json:"impact"`
		Status        *string  `json:"status"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}
	if p.EstimateHours == nil && p.Impact == nil && p.Status == nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "at least one field is required")
	}
	if p.EstimateHours != nil && *p.EstimateHours < 0 {
		return p2Validation(req.RequestID, "INVALID_ESTIMATE", "estimate_hours must be >= 0")
	}
	if p.Impact != nil && *p.Impact != "high" && *p.Impact != "medium" && *p.Impact != "low" {
		return p2Validation(req.RequestID, "INVALID_IMPACT", "impact must be high, medium, or low")
	}
	if p.Status != nil && *p.Status != "todo" && *p.Status != "in_progress" && *p.Status != "done" {
		return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "status must be todo, in_progress, or done")
	}
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	var exists string
	if err := db.QueryRow(`SELECT task_id FROM tasks WHERE task_id = ? AND project_id = ?`, taskID, projectID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p2NotFound(req.RequestID, "SPRINT_NOT_FOUND", "task not found")
		}
		return p2Persistence(req.RequestID, err)
	}
	sets := []string{}
	args := []any{}
	if p.EstimateHours != nil {
		sets = append(sets, "estimate_hours = ?")
		args = append(args, *p.EstimateHours)
	}
	if p.Impact != nil {
		sets = append(sets, "impact = ?")
		args = append(args, *p.Impact)
	}
	if p.Status != nil {
		sets = append(sets, "status = ?")
		args = append(args, *p.Status)
	}
	updatedAt := time.Now().UTC().Format(time.RFC3339)
	sets = append(sets, "updated_at = ?")
	args = append(args, updatedAt, taskID)
	q := "UPDATE tasks SET " + strings.Join(sets, ", ") + " WHERE task_id = ?"
	if _, err := db.Exec(q, args...); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	var est sql.NullFloat64
	var impact sql.NullString
	var status string
	var outUpdated string
	if err := db.QueryRow(`SELECT estimate_hours, impact, status, updated_at FROM tasks WHERE task_id = ?`, taskID).Scan(&est, &impact, &status, &outUpdated); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	resp := map[string]any{"task_id": taskID, "status": status, "updated_at": outUpdated, "estimate_hours": nil, "impact": nil}
	if est.Valid {
		resp["estimate_hours"] = est.Float64
	}
	if impact.Valid {
		resp["impact"] = impact.String
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: resp}
}

func (a *app) p2ListResources(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	rows, err := db.Query(`SELECT resource_id, name, capacity_hours_per_day FROM resources ORDER BY resource_id ASC`)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	defer rows.Close()
	resources := []map[string]any{}
	for rows.Next() {
		var id, name string
		var cap float64
		if err := rows.Scan(&id, &name, &cap); err != nil {
			return p2Persistence(req.RequestID, err)
		}
		resources = append(resources, map[string]any{"resource_id": id, "name": name, "capacity_hours_per_day": cap})
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"resources": resources}}
}

func (a *app) p2SaveResources(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	var p struct {
		Resources []struct {
			ResourceID          string  `json:"resource_id"`
			Name                string  `json:"name"`
			CapacityHoursPerDay float64 `json:"capacity_hours_per_day"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}
	seen := map[string]struct{}{}
	for _, item := range p.Resources {
		if _, ok := seen[item.ResourceID]; ok {
			return p2Validation(req.RequestID, "DUPLICATE_RESOURCE_ID", "duplicate resource_id")
		}
		seen[item.ResourceID] = struct{}{}
		if item.CapacityHoursPerDay <= 0 {
			return p2Validation(req.RequestID, "INVALID_RESOURCE_CAPACITY", "capacity_hours_per_day must be > 0")
		}
	}
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	tx, err := db.Begin()
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	if _, err := tx.Exec(`DELETE FROM resources`); err != nil {
		_ = tx.Rollback()
		return p2Persistence(req.RequestID, err)
	}
	for _, item := range p.Resources {
		if _, err := tx.Exec(`INSERT INTO resources(resource_id, name, capacity_hours_per_day) VALUES(?, ?, ?)`, item.ResourceID, item.Name, item.CapacityHoursPerDay); err != nil {
			_ = tx.Rollback()
			return p2Persistence(req.RequestID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"resources": p.Resources}}
}

func (a *app) p2GetCalendar(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	month := req.QueryParams["month"]
	if month == "" {
		month = time.Now().Format("2006-01")
	}
	start, end, err := monthRange(month)
	if err != nil {
		return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "month must be YYYY-MM")
	}
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	rows, err := db.Query(`SELECT date, is_working FROM working_day_calendar WHERE date >= ? AND date <= ? ORDER BY date ASC`, start, end)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	defer rows.Close()
	days := []map[string]any{}
	for rows.Next() {
		var d string
		var working int
		if err := rows.Scan(&d, &working); err != nil {
			return p2Persistence(req.RequestID, err)
		}
		days = append(days, map[string]any{"date": d, "is_working": working == 1})
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"month": month, "days": days}}
}

func (a *app) p2SaveCalendar(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	var p struct {
		Days []struct {
			Date      string `json:"date"`
			IsWorking bool   `json:"is_working"`
		} `json:"days"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}
	seen := map[string]struct{}{}
	for _, d := range p.Days {
		if _, err := time.Parse("2006-01-02", d.Date); err != nil {
			return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "date must be YYYY-MM-DD")
		}
		if _, ok := seen[d.Date]; ok {
			return p2Validation(req.RequestID, "DUPLICATE_CALENDAR_DATE", "duplicate date")
		}
		seen[d.Date] = struct{}{}
	}
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	tx, err := db.Begin()
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	for _, d := range p.Days {
		working := 0
		if d.IsWorking {
			working = 1
		}
		if _, err := tx.Exec(`INSERT INTO working_day_calendar(date, is_working) VALUES(?, ?) ON CONFLICT(date) DO UPDATE SET is_working = excluded.is_working`, d.Date, working); err != nil {
			_ = tx.Rollback()
			return p2Persistence(req.RequestID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"days": p.Days}}
}

func (a *app) p2ApplyCarryover(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	sprintID := req.PathParams["sprint_id"]
	var p struct {
		Decisions []struct {
			TaskID         string  `json:"task_id"`
			Action         string  `json:"action"`
			TargetSprintID *string `json:"target_sprint_id"`
		} `json:"decisions"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	for _, d := range p.Decisions {
		if d.Action != "carryover" && d.Action != "keep" {
			return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "action must be carryover or keep")
		}
		if d.Action == "carryover" && d.TargetSprintID != nil {
			var target string
			if err := db.QueryRow(`SELECT sprint_id FROM sprints WHERE sprint_id = ? AND project_id = ?`, *d.TargetSprintID, projectID).Scan(&target); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return p2NotFound(req.RequestID, "TARGET_SPRINT_NOT_FOUND", "target sprint not found")
				}
				return p2Persistence(req.RequestID, err)
			}
		}
	}
	tx, err := db.Begin()
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	updatedAt := time.Now().UTC().Format(time.RFC3339)
	applied := []map[string]any{}
	for _, d := range p.Decisions {
		if d.Action == "keep" {
			var current sql.NullString
			if err := tx.QueryRow(`SELECT sprint_id FROM tasks WHERE task_id = ?`, d.TaskID).Scan(&current); err != nil {
				_ = tx.Rollback()
				return p2Persistence(req.RequestID, err)
			}
			entry := map[string]any{"task_id": d.TaskID, "action": d.Action, "sprint_id": nil}
			if current.Valid {
				entry["sprint_id"] = current.String
			}
			applied = append(applied, entry)
			continue
		}
		var target any = nil
		if d.TargetSprintID != nil {
			target = *d.TargetSprintID
		}
		if _, err := tx.Exec(`UPDATE tasks SET sprint_id = ?, updated_at = ? WHERE task_id = ? AND sprint_id = ?`, target, updatedAt, d.TaskID, sprintID); err != nil {
			_ = tx.Rollback()
			return p2Persistence(req.RequestID, err)
		}
		applied = append(applied, map[string]any{"task_id": d.TaskID, "action": d.Action, "sprint_id": target})
	}
	if err := tx.Commit(); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"applied": applied}}
}

func (a *app) p2ListUsers(req zmqRequest) zmqResponse {
	rows, err := a.systemDB.Query(`SELECT user_id, name, email FROM users ORDER BY user_id ASC`)
	if err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}
	defer rows.Close()

	users := []map[string]any{}
	for rows.Next() {
		var id, name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
		}
		users = append(users, map[string]any{"user_id": id, "name": name, "email": email})
	}

	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"users": users}}
}

func (a *app) p2RegisterUser(req zmqRequest) zmqResponse {
	var p struct {
		UserID string `json:"user_id"`
		Name   string `json:"name"`
		Email  string `json:"email"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}
	var existing string
	err := a.systemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, p.UserID).Scan(&existing)
	if err == nil {
		return p2Validation(req.RequestID, "DUPLICATE_USER_ID", "user_id already exists")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return p2Persistence(req.RequestID, err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := a.systemDB.Exec(`INSERT INTO users(user_id, name, email, created_at, updated_at) VALUES(?, ?, ?, ?, ?)`, p.UserID, p.Name, p.Email, now, now); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": p.UserID, "name": p.Name, "email": p.Email, "created_at": now}}
}

func (a *app) p2UpdateUser(req zmqRequest) zmqResponse {
	userID := req.PathParams["user_id"]
	var p struct {
		Name  *string `json:"name"`
		Email *string `json:"email"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}
	if p.Name == nil && p.Email == nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "at least one field is required")
	}
	if err := ensureUserExists(a.systemDB, userID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}
	sets := []string{}
	args := []any{}
	if p.Name != nil {
		sets = append(sets, "name = ?")
		args = append(args, *p.Name)
	}
	if p.Email != nil {
		sets = append(sets, "email = ?")
		args = append(args, *p.Email)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	sets = append(sets, "updated_at = ?")
	args = append(args, now, userID)
	q := "UPDATE users SET " + strings.Join(sets, ", ") + " WHERE user_id = ?"
	if _, err := a.systemDB.Exec(q, args...); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	var name, email, updated string
	if err := a.systemDB.QueryRow(`SELECT name, email, updated_at FROM users WHERE user_id = ?`, userID).Scan(&name, &email, &updated); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": userID, "name": name, "email": email, "updated_at": updated}}
}

func (a *app) p2DeleteUser(req zmqRequest) zmqResponse {
	userID := req.PathParams["user_id"]
	if err := ensureUserExists(a.systemDB, userID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}
	if _, err := a.systemDB.Exec(`DELETE FROM users WHERE user_id = ?`, userID); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	rows, err := a.systemDB.Query(`SELECT project_id FROM projects`)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	defer rows.Close()
	for rows.Next() {
		var projectID string
		if err := rows.Scan(&projectID); err != nil {
			return p2Persistence(req.RequestID, err)
		}
		db, err := a.openProjectDB(projectID)
		if err != nil {
			return p2Persistence(req.RequestID, err)
		}
		if _, err := db.Exec(`DELETE FROM project_roles WHERE user_id = ?`, userID); err != nil {
			return p2Persistence(req.RequestID, err)
		}
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok"}
}

func (a *app) p2GetProjectRoles(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	if err := ensureProjectExists(a.systemDB, projectID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	rows, err := db.Query(`SELECT user_id, role FROM project_roles WHERE project_id = ? ORDER BY user_id ASC`, projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	defer rows.Close()
	roles := []map[string]any{}
	for rows.Next() {
		var userID, role string
		if err := rows.Scan(&userID, &role); err != nil {
			return p2Persistence(req.RequestID, err)
		}
		if err := ensureUserExists(a.systemDB, userID); err != nil {
			continue
		}
		roles = append(roles, map[string]any{"user_id": userID, "role": role})
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"roles": roles}}
}

func (a *app) p2SaveProjectRoles(req zmqRequest) zmqResponse {
	projectID := req.PathParams["project_id"]
	if err := ensureProjectExists(a.systemDB, projectID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}
	var p struct {
		Roles []struct {
			UserID string `json:"user_id"`
			Role   string `json:"role"`
		} `json:"roles"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}
	for _, item := range p.Roles {
		if item.Role != "administrator" && item.Role != "assignee" && item.Role != "viewer" {
			return p2Validation(req.RequestID, "INVALID_ROLE", "role must be administrator, assignee, or viewer")
		}
		if err := ensureUserExists(a.systemDB, item.UserID); err != nil {
			return p2FromAPIError(req.RequestID, err)
		}
	}
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	tx, err := db.Begin()
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	if _, err := tx.Exec(`DELETE FROM project_roles WHERE project_id = ?`, projectID); err != nil {
		_ = tx.Rollback()
		return p2Persistence(req.RequestID, err)
	}
	for _, item := range p.Roles {
		if _, err := tx.Exec(`INSERT INTO project_roles(project_id, user_id, role) VALUES(?, ?, ?)`, projectID, item.UserID, item.Role); err != nil {
			_ = tx.Rollback()
			return p2Persistence(req.RequestID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"roles": p.Roles}}
}

func (a *app) p2ResolveDefaultLocale(req zmqRequest) zmqResponse {
	uid := strings.TrimSpace(req.QueryParams["user_id"])
	if uid != "" {
		if err := ensureUserExists(a.systemDB, uid); err != nil {
			return p2FromAPIError(req.RequestID, err)
		}
		var locale string
		err := a.systemDB.QueryRow(`SELECT locale FROM user_locale_settings WHERE user_id = ?`, uid).Scan(&locale)
		if err == nil {
			return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"locale": locale, "source": "user_setting"}}
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return p2Persistence(req.RequestID, err)
		}
	}

	candidates := parseAcceptLanguage(req.QueryParams["accept_language"])
	if locale, source, err := resolveLocaleFromCandidates(a.systemDB, candidates); err != nil {
		return p2Persistence(req.RequestID, err)
	} else if locale != "" {
		return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"locale": locale, "source": source}}
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"locale": "en", "source": "fallback"}}
}

func (a *app) p2GetUserLocaleSetting(req zmqRequest) zmqResponse {
	userID := strings.TrimSpace(req.PathParams["user_id"])
	if userID == "" {
		return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "user_id is required")
	}
	if err := ensureUserExists(a.systemDB, userID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}

	var locale string
	err := a.systemDB.QueryRow(`SELECT locale FROM user_locale_settings WHERE user_id = ?`, userID).Scan(&locale)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return p2Persistence(req.RequestID, err)
	}
	if errors.Is(err, sql.ErrNoRows) {
		locale = ""
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": userID, "locale": locale, "locale_options": supportedLocales()}}
}

func (a *app) p2SaveUserLocaleSetting(req zmqRequest) zmqResponse {
	userID := strings.TrimSpace(req.PathParams["user_id"])
	if userID == "" {
		return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "user_id is required")
	}
	if err := ensureUserExists(a.systemDB, userID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}

	var p struct {
		Locale string `json:"locale"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}
	p.Locale = strings.TrimSpace(strings.ToLower(p.Locale))
	if p.Locale != "" && !isSupportedLocale(p.Locale) {
		return p2Validation(req.RequestID, "INVALID_LOCALE", "locale is not allowed")
	}
	if p.Locale == "" {
		if _, err := a.systemDB.Exec(`DELETE FROM user_locale_settings WHERE user_id = ?`, userID); err != nil {
			return p2Persistence(req.RequestID, err)
		}
		return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": userID, "locale": "", "locale_options": supportedLocales()}}
	}
	if _, err := a.systemDB.Exec(`INSERT INTO user_locale_settings(user_id, locale) VALUES(?, ?) ON CONFLICT(user_id) DO UPDATE SET locale = excluded.locale`, userID, p.Locale); err != nil {
		return p2Persistence(req.RequestID, err)
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": userID, "locale": p.Locale, "locale_options": supportedLocales()}}
}

var topMenuDefaultKeys = []string{"project_select", "sprint_workspace", "resource_settings", "calendar_settings"}

var menuVisibilityAllKeys = []string{"project_select", "sprint_workspace", "resource_settings", "calendar_settings", "user_management"}

var menuLabels = map[string]string{
	"project_select":    "Project Select",
	"sprint_workspace":  "Sprint Workspace",
	"resource_settings": "Resource Settings",
	"calendar_settings": "Working-Day Calendar",
	"user_management":   "User Management",
}

func (a *app) p2GetTopMenu(req zmqRequest) zmqResponse {
	userID := strings.TrimSpace(req.QueryParams["user_id"])
	if userID == "" {
		return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "user_id is required")
	}
	if err := ensureUserExists(a.systemDB, userID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}

	rows, err := a.systemDB.Query(`SELECT menu_key, is_enabled FROM user_menu_visibility WHERE user_id = ? ORDER BY menu_key ASC`, userID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	defer rows.Close()

	enabled := map[string]bool{}
	hasAny := false
	for rows.Next() {
		var key string
		var flag int
		if err := rows.Scan(&key, &flag); err != nil {
			return p2Persistence(req.RequestID, err)
		}
		hasAny = true
		enabled[key] = flag == 1
	}

	buttons := make([]map[string]any, 0)
	if !hasAny {
		for _, key := range topMenuDefaultKeys {
			buttons = append(buttons, map[string]any{"menu_key": key, "label": menuLabels[key]})
		}
		return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": userID, "menu_buttons": buttons}}
	}

	for _, key := range menuVisibilityAllKeys {
		if enabled[key] {
			buttons = append(buttons, map[string]any{"menu_key": key, "label": menuLabels[key]})
		}
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": userID, "menu_buttons": buttons}}
}

func (a *app) p2GetUserMenuVisibility(req zmqRequest) zmqResponse {
	userID := strings.TrimSpace(req.PathParams["user_id"])
	if userID == "" {
		return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "user_id is required")
	}
	if err := ensureUserExists(a.systemDB, userID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}

	rows, err := a.systemDB.Query(`SELECT menu_key, is_enabled FROM user_menu_visibility WHERE user_id = ? ORDER BY menu_key ASC`, userID)
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	defer rows.Close()

	visibility := make([]map[string]any, 0)
	hasAny := false
	for rows.Next() {
		var key string
		var flag int
		if err := rows.Scan(&key, &flag); err != nil {
			return p2Persistence(req.RequestID, err)
		}
		hasAny = true
		visibility = append(visibility, map[string]any{"menu_key": key, "is_enabled": flag == 1})
	}
	if !hasAny {
		for _, key := range menuVisibilityAllKeys {
			visibility = append(visibility, map[string]any{"menu_key": key, "is_enabled": true})
		}
	}

	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": userID, "menu_visibility": visibility}}
}

func (a *app) p2SaveUserMenuVisibility(req zmqRequest) zmqResponse {
	userID := strings.TrimSpace(req.PathParams["user_id"])
	if userID == "" {
		return p2BadRequest(req.RequestID, "INVALID_PATH_PARAM", "user_id is required")
	}
	if err := ensureUserExists(a.systemDB, userID); err != nil {
		return p2FromAPIError(req.RequestID, err)
	}

	var p struct {
		MenuVisibility []struct {
			MenuKey   string `json:"menu_key"`
			IsEnabled bool   `json:"is_enabled"`
		} `json:"menu_visibility"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return p2BadRequest(req.RequestID, "INVALID_JSON", "invalid JSON payload")
	}

	allowed := map[string]struct{}{}
	for _, key := range menuVisibilityAllKeys {
		allowed[key] = struct{}{}
	}
	seen := map[string]struct{}{}
	for _, item := range p.MenuVisibility {
		if _, ok := allowed[item.MenuKey]; !ok {
			return p2Validation(req.RequestID, "INVALID_MENU_KEY", "menu_key is not allowed")
		}
		if _, ok := seen[item.MenuKey]; ok {
			return p2Validation(req.RequestID, "DUPLICATE_MENU_KEY", "duplicate menu_key")
		}
		seen[item.MenuKey] = struct{}{}
	}

	tx, err := a.systemDB.Begin()
	if err != nil {
		return p2Persistence(req.RequestID, err)
	}
	if _, err := tx.Exec(`DELETE FROM user_menu_visibility WHERE user_id = ?`, userID); err != nil {
		_ = tx.Rollback()
		return p2Persistence(req.RequestID, err)
	}
	for _, item := range p.MenuVisibility {
		flag := 0
		if item.IsEnabled {
			flag = 1
		}
		if _, err := tx.Exec(`INSERT INTO user_menu_visibility(user_id, menu_key, is_enabled) VALUES(?, ?, ?)`, userID, item.MenuKey, flag); err != nil {
			_ = tx.Rollback()
			return p2Persistence(req.RequestID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return p2Persistence(req.RequestID, err)
	}

	visibility := make([]map[string]any, 0, len(p.MenuVisibility))
	for _, item := range p.MenuVisibility {
		visibility = append(visibility, map[string]any{"menu_key": item.MenuKey, "is_enabled": item.IsEnabled})
	}
	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"user_id": userID, "menu_visibility": visibility}}
}

func p2BadRequest(requestID, code, message string) zmqResponse {
	return zmqResponse{RequestID: requestID, Status: "error", Error: &responseError{Code: code, Message: message}}
}

func p2Validation(requestID, code, message string) zmqResponse {
	return zmqResponse{RequestID: requestID, Status: "error", Error: &responseError{Code: code, Message: message}}
}

func p2NotFound(requestID, code, message string) zmqResponse {
	return zmqResponse{RequestID: requestID, Status: "error", Error: &responseError{Code: code, Message: message}}
}

func p2Persistence(requestID string, err error) zmqResponse {
	return zmqResponse{RequestID: requestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
}

func p2FromAPIError(requestID string, err *apiError) zmqResponse {
	if err == nil {
		return zmqResponse{RequestID: requestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: "unknown error"}}
	}
	return zmqResponse{RequestID: requestID, Status: "error", Error: &responseError{Code: err.Code, Message: err.Message}}
}

func mapP2Error(err *responseError) *apiError {
	if err == nil {
		return &apiError{Code: "UPSTREAM_UNAVAILABLE", Message: "empty upstream error", HTTPStatus: http.StatusBadGateway}
	}
	switch err.Code {
	case "UNKNOWN_COMMAND", "INVALID_PATH_PARAM", "INVALID_JSON":
		return &apiError{Code: err.Code, Message: err.Message, HTTPStatus: http.StatusBadRequest}
	case "PROJECT_NOT_FOUND", "SPRINT_NOT_FOUND", "TARGET_SPRINT_NOT_FOUND", "USER_NOT_FOUND":
		return &apiError{Code: err.Code, Message: err.Message, HTTPStatus: http.StatusNotFound}
	case "INVALID_ESTIMATE", "INVALID_IMPACT", "DUPLICATE_RESOURCE_ID", "INVALID_RESOURCE_CAPACITY", "DUPLICATE_CALENDAR_DATE", "DUPLICATE_USER_ID", "INVALID_ROLE", "INVALID_LOCALE", "INVALID_MENU_KEY", "DUPLICATE_MENU_KEY", "DUPLICATE_PROJECT_ID", "INVALID_SPRINT_DATE_RANGE":
		return &apiError{Code: err.Code, Message: err.Message, HTTPStatus: http.StatusUnprocessableEntity}
	default:
		return &apiError{Code: err.Code, Message: err.Message, HTTPStatus: http.StatusInternalServerError}
	}
}

func (a *app) openProjectDB(projectID string) (*sql.DB, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if db, ok := a.projectDBs[projectID]; ok {
		return db, nil
	}
	path := filepath.Join(a.dataDir, fmt.Sprintf("project_%s.sqlite", projectID))
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := initProjectSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	a.projectDBs[projectID] = db
	return db, nil
}

func (a *app) ensureProjectSchema(projectID string) error {
	_, err := a.openProjectDB(projectID)
	return err
}

func (a *app) seedProjectData(projectID string) error {
	db, err := a.openProjectDB(projectID)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)

	if _, err := db.Exec(
		`INSERT OR IGNORE INTO sprints(sprint_id, project_id, name, start_date, end_date, available_hours, created_at, updated_at)
		 VALUES('sp-001', ?, 'Sprint 1', '2026-04-01', '2026-04-14', 80, ?, ?)`,
		projectID, now, now,
	); err != nil {
		return err
	}

	tasks := []struct {
		ID       string
		Title    string
		Estimate any
		Impact   any
		Status   string
	}{
		{ID: "task-001", Title: "Set up API skeleton", Estimate: 12.0, Impact: "high", Status: "todo"},
		{ID: "task-002", Title: "Implement workspace classification", Estimate: 16.0, Impact: "high", Status: "in_progress"},
		{ID: "task-003", Title: "Refine role management", Estimate: nil, Impact: nil, Status: "todo"},
	}
	for _, t := range tasks {
		if _, err := db.Exec(
			`INSERT OR IGNORE INTO tasks(task_id, project_id, sprint_id, title, estimate_hours, impact, status, created_at, updated_at)
			 VALUES(?, ?, 'sp-001', ?, ?, ?, ?, ?, ?)`,
			t.ID, projectID, t.Title, t.Estimate, t.Impact, t.Status, now, now,
		); err != nil {
			return err
		}
	}
	return nil
}

func initSystemSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			user_id    TEXT NOT NULL PRIMARY KEY,
			name       TEXT NOT NULL,
			email      TEXT NOT NULL UNIQUE,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS projects (
			project_id  TEXT NOT NULL PRIMARY KEY,
			name        TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL,
			updated_at  TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS user_credentials (
			user_id       TEXT NOT NULL PRIMARY KEY,
			password_hash TEXT NOT NULL,
			created_at    TEXT NOT NULL,
			updated_at    TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS user_menu_visibility (
			user_id    TEXT NOT NULL,
			menu_key   TEXT NOT NULL,
			is_enabled INTEGER NOT NULL,
			PRIMARY KEY(user_id, menu_key)
		)`,
		`CREATE TABLE IF NOT EXISTS user_locale_settings (
			user_id TEXT NOT NULL PRIMARY KEY,
			locale  TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS locale_config (
			language TEXT NOT NULL,
			region   TEXT NOT NULL,
			locale   TEXT NOT NULL,
			PRIMARY KEY(language, region)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func initProjectSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS sprints (
			sprint_id       TEXT NOT NULL PRIMARY KEY,
			project_id      TEXT NOT NULL,
			name            TEXT NOT NULL,
			start_date      TEXT NOT NULL,
			end_date        TEXT NOT NULL,
			available_hours REAL NOT NULL DEFAULT 0,
			created_at      TEXT NOT NULL,
			updated_at      TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS tasks (
			task_id        TEXT NOT NULL PRIMARY KEY,
			project_id     TEXT NOT NULL,
			sprint_id      TEXT,
			title          TEXT NOT NULL,
			estimate_hours REAL,
			impact         TEXT,
			status         TEXT NOT NULL DEFAULT 'todo',
			created_at     TEXT NOT NULL,
			updated_at     TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_sprint_id ON tasks(sprint_id)`,
		`CREATE TABLE IF NOT EXISTS resources (
			resource_id            TEXT NOT NULL PRIMARY KEY,
			name                   TEXT NOT NULL,
			capacity_hours_per_day REAL NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS working_day_calendar (
			date       TEXT    NOT NULL PRIMARY KEY,
			is_working INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE IF NOT EXISTS task_resource_allocations (
			task_id     TEXT NOT NULL,
			resource_id TEXT NOT NULL,
			hours       REAL NOT NULL,
			PRIMARY KEY (task_id, resource_id)
		)`,
		`CREATE TABLE IF NOT EXISTS project_roles (
			project_id TEXT NOT NULL,
			user_id    TEXT NOT NULL,
			role       TEXT NOT NULL,
			PRIMARY KEY (project_id, user_id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func seedSystemData(db *sql.DB) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO projects(project_id, name, description, created_at, updated_at)
		 VALUES('demo', 'Demo Project', 'Seeded project for CarrySprint', ?, ?)`,
		now, now,
	); err != nil {
		return err
	}
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO users(user_id, name, email, created_at, updated_at)
		 VALUES('u001', 'Demo User', 'demo@example.com', ?, ?)`,
		now, now,
	); err != nil {
		return err
	}
	localeSeeds := []struct {
		language string
		region   string
		locale   string
	}{
		{language: "ja", region: "JP", locale: "ja"},
		{language: "de", region: "DE", locale: "de"},
		{language: "zh", region: "CN", locale: "zh"},
		{language: "it", region: "IT", locale: "it"},
		{language: "fr", region: "FR", locale: "fr"},
	}
	for _, seed := range localeSeeds {
		if _, err := db.Exec(`INSERT OR IGNORE INTO locale_config(language, region, locale) VALUES(?, ?, ?)`, seed.language, seed.region, seed.locale); err != nil {
			return err
		}
	}
	if err := initializeAdminUser(db); err != nil {
		return err
	}
	return nil
}

func initializeAdminUser(db *sql.DB) error {
	var userID string
	err := db.QueryRow(`SELECT user_id FROM users WHERE user_id = 'admin'`).Scan(&userID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	h := sha256.Sum256([]byte("admin"))
	passwordHash := hex.EncodeToString(h[:])

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO users(user_id, name, email, created_at, updated_at) VALUES('admin', 'Administrator', 'admin@local', ?, ?)`, now, now); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec(`INSERT INTO user_credentials(user_id, password_hash, created_at, updated_at) VALUES('admin', ?, ?, ?)`, passwordHash, now, now); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func requireRequestID(r *http.Request) (string, *apiError) {
	requestID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
	if requestID == "" {
		return "", &apiError{Code: "INVALID_PATH_PARAM", Message: "X-Request-Id is required", HTTPStatus: http.StatusBadRequest}
	}
	return requestID, nil
}

func requireJSONContentType(r *http.Request) *apiError {
	ct := r.Header.Get("Content-Type")
	if ct == "" || !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		return &apiError{Code: "INVALID_JSON", Message: "Content-Type must be application/json", HTTPStatus: http.StatusBadRequest}
	}
	return nil
}

func writeOK(w http.ResponseWriter, rid string, data any) {
	writeJSON(w, http.StatusOK, responseBody{RequestID: rid, Result: "ok", Data: data})
}

func writeErr(w http.ResponseWriter, rid string, err *apiError) {
	if rid == "" {
		rid = "missing-request-id"
	}
	writeJSON(w, err.HTTPStatus, responseBody{
		RequestID: rid,
		Result:    "error",
		Error: &responseError{
			Code:    err.Code,
			Message: err.Message,
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, body responseBody) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func methodNotAllowed() *apiError {
	return &apiError{Code: "ROUTE_NOT_FOUND", Message: "method not allowed", HTTPStatus: http.StatusNotFound}
}

func invalidJSON() *apiError {
	return &apiError{Code: "INVALID_JSON", Message: "invalid JSON payload", HTTPStatus: http.StatusBadRequest}
}

func validationErr(code, msg string) *apiError {
	return &apiError{Code: code, Message: msg, HTTPStatus: http.StatusUnprocessableEntity}
}

func notFoundErr(code, msg string) *apiError {
	return &apiError{Code: code, Message: msg, HTTPStatus: http.StatusNotFound}
}

func persistenceErr(err error) *apiError {
	return &apiError{Code: "PERSISTENCE_ERROR", Message: err.Error(), HTTPStatus: http.StatusInternalServerError}
}

func ensureProjectExists(db *sql.DB, projectID string) *apiError {
	var id string
	err := db.QueryRow(`SELECT project_id FROM projects WHERE project_id = ?`, projectID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return notFoundErr("PROJECT_NOT_FOUND", "project not found")
	}
	if err != nil {
		return persistenceErr(err)
	}
	return nil
}

func ensureUserExists(db *sql.DB, userID string) *apiError {
	var id string
	err := db.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, userID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return notFoundErr("USER_NOT_FOUND", "user not found")
	}
	if err != nil {
		return persistenceErr(err)
	}
	return nil
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

func monthRange(month string) (string, string, error) {
	t, err := time.Parse("2006-01", month)
	if err != nil {
		return "", "", err
	}
	start := t.Format("2006-01-02")
	end := t.AddDate(0, 1, -1).Format("2006-01-02")
	return start, end, nil
}

type localeCandidate struct {
	language string
	region   string
	q        float64
}

// parseAcceptLanguage parses an Accept-Language header value and returns
// locale candidates sorted by q value descending.
func parseAcceptLanguage(raw string) []localeCandidate {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var entries []localeCandidate
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		segments := strings.SplitN(part, ";", 2)
		tag := strings.TrimSpace(segments[0])
		q := 1.0
		if len(segments) == 2 {
			qs := strings.TrimSpace(segments[1])
			if strings.HasPrefix(qs, "q=") {
				if v, err := strconv.ParseFloat(qs[2:], 64); err == nil {
					q = v
				}
			}
		}
		tag = strings.ReplaceAll(tag, "_", "-")
		chunks := strings.Split(tag, "-")
		if len(chunks) == 0 || chunks[0] == "*" {
			continue
		}
		entries = append(entries, localeCandidate{
			language: strings.ToLower(chunks[0]),
			region:   detectRegion(chunks[1:]),
			q:      q,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].q > entries[j].q })
	return entries
}

func detectRegion(parts []string) string {
	for i := len(parts) - 1; i >= 0; i-- {
		part := strings.TrimSpace(parts[i])
		if len(part) == 2 || len(part) == 3 {
			return strings.ToUpper(part)
		}
	}
	return ""
}

func resolveLocaleFromCandidates(db *sql.DB, candidates []localeCandidate) (string, string, error) {
	for _, candidate := range candidates {
		if candidate.language == "" || candidate.region == "" {
			continue
		}
		var locale string
		err := db.QueryRow(`SELECT locale FROM locale_config WHERE language = ? AND region = ? LIMIT 1`, candidate.language, candidate.region).Scan(&locale)
		if err == nil {
			return locale, "matched", nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", "", err
		}
	}
	for _, candidate := range candidates {
		if candidate.region == "" {
			continue
		}
		var locale string
		err := db.QueryRow(`SELECT locale FROM locale_config WHERE region = ? ORDER BY language ASC LIMIT 1`, candidate.region).Scan(&locale)
		if err == nil {
			return locale, "region_matched", nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", "", err
		}
	}
	for _, candidate := range candidates {
		if candidate.language == "" {
			continue
		}
		var locale string
		err := db.QueryRow(`SELECT locale FROM locale_config WHERE language = ? ORDER BY region ASC LIMIT 1`, candidate.language).Scan(&locale)
		if err == nil {
			return locale, "language_matched", nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", "", err
		}
	}
	return "", "", nil
}

func supportedLocales() []string {
	return []string{"en", "de", "fr", "it", "ja", "zh"}
}

func isSupportedLocale(locale string) bool {
	for _, candidate := range supportedLocales() {
		if locale == candidate {
			return true
		}
	}
	return false
}
