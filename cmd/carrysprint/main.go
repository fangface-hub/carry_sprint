package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	if path == "/" || path == "/projects" || path == "/users" {
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
	title, desc, content := resolveBrowserScreen(r.URL.Path, r.URL.Query().Get("dialog"))
	escPath := html.EscapeString(r.URL.RequestURI())
	escTitle := html.EscapeString(title)
	escDesc := html.EscapeString(desc)
	script := browserUIScreenScript()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>CarrySprint</title><style>body{font-family:Segoe UI,Helvetica,Arial,sans-serif;background:#f5f7fb;color:#1f2937;margin:0}main{max-width:980px;margin:28px auto;padding:24px;background:#fff;border:1px solid #e5e7eb;border-radius:12px;box-shadow:0 8px 24px rgba(15,23,42,.06)}h1{margin:0 0 8px;font-size:28px}.screen{display:inline-block;margin:0 0 12px;padding:6px 10px;background:#dbeafe;color:#1e3a8a;border-radius:999px;font-size:13px;font-weight:600}p{margin:8px 0 16px;line-height:1.6}.route{padding:10px 12px;background:#eef2ff;border-radius:8px;font-family:Consolas,monospace}.layout{display:grid;grid-template-columns:230px 1fr;gap:18px}.nav{padding:14px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px}.nav ul{margin:0;padding-left:18px}.nav li{margin:8px 0}.panel{padding:16px;border:1px solid #e5e7eb;border-radius:10px;background:#fff}.muted{color:#64748b}.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}.toolbar input,.toolbar select{padding:8px;border:1px solid #cbd5e1;border-radius:8px}.toolbar button{padding:8px 12px;border:1px solid #cbd5e1;background:#fff;border-radius:8px;cursor:pointer}.toolbar button:hover{background:#eff6ff}table{width:100%%;border-collapse:collapse}th,td{border-bottom:1px solid #e5e7eb;padding:8px;text-align:left}.error{margin-top:10px;padding:10px;border-radius:8px;background:#fee2e2;color:#991b1b;display:none}.small{font-size:12px;color:#64748b}</style></head><body><main><h1>CarrySprint</h1><div class=\"screen\">%s</div><p>%s</p><p class=\"route\">Current route: %s</p><div class=\"layout\"><nav class=\"nav\"><strong>Screen Links</strong><ul><li><a href=\"/\">Top Page</a></li><li><a href=\"/projects\">Project Select</a></li><li><a href=\"/projects/demo/sprints/sp-001/workspace\">Sprint Workspace</a></li><li><a href=\"/projects/demo/sprints/sp-001/workspace?dialog=carryover\">Carry-Over Dialog</a></li><li><a href=\"/projects/demo/resources\">Resource Settings</a></li><li><a href=\"/projects/demo/calendar\">Calendar Settings</a></li><li><a href=\"/users\">User Management</a></li></ul></nav><section class=\"panel\" id=\"app\" data-route=\"%s\">%s</section></div></main><script>%s</script></body></html>", escTitle, escDesc, escPath, escPath, content, script)
}

func resolveBrowserScreen(path string, dialog string) (string, string, string) {
	segs := splitPath(path)
	if path == "/" {
		return "Top Page", "Top menu and menu visibility settings screen.", "<h2>Top Menu</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
	}
	if path == "/projects" {
		return "Project Select Screen", "Select a project to open project-specific screens.", "<h2>Projects</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
	}
	if path == "/users" {
		return "User Management Screen", "Manage users and project role assignments.", "<h2>User Management</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
	}
	if len(segs) == 3 && segs[0] == "projects" && segs[2] == "resources" {
		return "Resource Settings Screen", "Edit resource capacity for the selected project.", "<h2>Resource Settings</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
	}
	if len(segs) == 3 && segs[0] == "projects" && segs[2] == "calendar" {
		return "Working-Day Calendar Screen", "Edit working-day calendar settings for the selected project.", "<h2>Working-Day Calendar</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
	}
	if len(segs) == 5 && segs[0] == "projects" && segs[2] == "sprints" && segs[4] == "workspace" {
		if dialog == "carryover" {
			return "Carry-Over Review Dialog", "Review carry-over decisions in sprint workspace dialog mode.", "<h2>Carry-Over Review</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
		}
		return "Sprint Workspace Screen", "Review sprint tasks and budget status for one sprint.", "<h2>Sprint Workspace</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
	}
	return "Browser UI", "Browser UI shell is active.", "<h2>Screen</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
}

func browserUIScreenScript() string {
	return `(function () {
	const root = document.getElementById('screen-root');
	const errorBox = document.getElementById('screen-error');
	if (!root) return;

	const esc = (v) => String(v ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
	const rid = () => (globalThis.crypto && crypto.randomUUID ? crypto.randomUUID() : ('req-' + Date.now() + '-' + Math.floor(Math.random() * 1000000)));
	const showError = (msg) => {
		if (!errorBox) return;
		errorBox.style.display = 'block';
		errorBox.textContent = msg;
	};
	const setHTML = (html) => {
		root.innerHTML = html;
		if (errorBox) {
			errorBox.style.display = 'none';
			errorBox.textContent = '';
		}
	};

	const api = async (path, opt = {}) => {
		const headers = Object.assign({ 'X-Request-Id': rid() }, opt.headers || {});
		if (opt.body !== undefined && !headers['Content-Type']) {
			headers['Content-Type'] = 'application/json';
		}
		const resp = await fetch(path, {
			method: opt.method || 'GET',
			headers,
			body: opt.body !== undefined ? JSON.stringify(opt.body) : undefined
		});
		const json = await resp.json().catch(() => ({}));
		if (!resp.ok || json.result !== 'ok') {
			const code = json && json.error ? json.error.code : ('HTTP_' + resp.status);
			const msg = json && json.error ? json.error.message : 'request failed';
			throw new Error(code + ': ' + msg);
		}
		return json.data || {};
	};

	const route = location.pathname;
	const segs = route.split('/').filter(Boolean);
	const query = new URLSearchParams(location.search);

	const renderTop = async () => {
		const userResp = await api('/api/users');
		const users = userResp.users || [];
		const selectedUser = users.length > 0 ? users[0].user_id : 'u001';

		const drawForUser = async (uid) => {
			const menu = await api('/api/top/menu', { headers: { 'X-User-Id': uid } });
			const vis = await api('/api/users/' + encodeURIComponent(uid) + '/menu-visibility');
			const visList = vis.menu_visibility || [];
			setHTML(
				'<div class="toolbar"><label>User</label><select id="menu-user">' +
					users.map(u => '<option value="' + esc(u.user_id) + '">' + esc(u.user_id) + ' - ' + esc(u.name) + '</option>').join('') +
				'</select><button id="menu-load">Load</button><button id="menu-save">Save Visibility</button></div>' +
				'<h3>Top Menu Buttons</h3><ul>' + (menu.menu_buttons || []).map(m => '<li>' + esc(m.menu_key) + ' (' + esc(m.label) + ')</li>').join('') + '</ul>' +
				'<h3>Visibility</h3><div>' + visList.map(v => '<label style="display:block"><input type="checkbox" data-menu-key="' + esc(v.menu_key) + '" ' + (v.is_enabled ? 'checked' : '') + '> ' + esc(v.menu_key) + '</label>').join('') + '</div>'
			);
			const userSelect = document.getElementById('menu-user');
			if (userSelect) userSelect.value = uid;
			document.getElementById('menu-load').onclick = async () => {
				try { await drawForUser(document.getElementById('menu-user').value); } catch (e) { showError(e.message); }
			};
			document.getElementById('menu-save').onclick = async () => {
				try {
					const rows = Array.from(root.querySelectorAll('input[data-menu-key]')).map(el => ({ menu_key: el.getAttribute('data-menu-key'), is_enabled: el.checked }));
					await api('/api/users/' + encodeURIComponent(document.getElementById('menu-user').value) + '/menu-visibility', { method: 'PUT', body: { menu_visibility: rows } });
					await drawForUser(document.getElementById('menu-user').value);
				} catch (e) { showError(e.message); }
			};
		};

		await drawForUser(selectedUser);
	};

	const renderProjects = async () => {
		const data = await api('/api/projects');
		const projects = data.projects || [];
		const first = projects[0];
		let summary = null;
		if (first) {
			summary = await api('/api/projects/' + encodeURIComponent(first.project_id) + '/summary');
		}
		setHTML(
			'<table><thead><tr><th>ID</th><th>Name</th><th>Description</th></tr></thead><tbody>' +
			projects.map(p => '<tr><td>' + esc(p.project_id) + '</td><td>' + esc(p.name) + '</td><td>' + esc(p.description) + '</td></tr>').join('') +
			'</tbody></table>' +
			(summary ? ('<p class="small">First project summary: sprint_count=' + esc(summary.sprint_count) + ', task_count=' + esc(summary.task_count) + '</p>') : '')
		);
	};

	const renderUsers = async () => {
		const usersData = await api('/api/users');
		const users = usersData.users || [];
		const rolesData = await api('/api/projects/demo/roles');
		const roleMap = new Map((rolesData.roles || []).map(r => [r.user_id, r.role]));
		setHTML(
			'<div class="toolbar"><input id="new-user-id" placeholder="user_id"><input id="new-user-name" placeholder="name"><input id="new-user-email" placeholder="email"><button id="create-user">Create</button></div>' +
			'<table><thead><tr><th>User ID</th><th>Name</th><th>Email</th><th>Role(demo)</th><th>Action</th></tr></thead><tbody>' +
			users.map(u => '<tr><td>' + esc(u.user_id) + '</td><td><input data-name-for="' + esc(u.user_id) + '" value="' + esc(u.name) + '"></td><td><input data-email-for="' + esc(u.user_id) + '" value="' + esc(u.email) + '"></td><td><select data-role-for="' + esc(u.user_id) + '"><option value="administrator" ' + (roleMap.get(u.user_id) === 'administrator' ? 'selected' : '') + '>administrator</option><option value="assignee" ' + (roleMap.get(u.user_id) === 'assignee' ? 'selected' : '') + '>assignee</option></select></td><td><button data-update-user="' + esc(u.user_id) + '">Update</button> <button data-delete-user="' + esc(u.user_id) + '">Delete</button></td></tr>').join('') +
			'</tbody></table><div class="toolbar"><button id="save-roles">Save Roles For demo</button></div>'
		);

		document.getElementById('create-user').onclick = async () => {
			try {
				await api('/api/users', { method: 'POST', body: { user_id: document.getElementById('new-user-id').value, name: document.getElementById('new-user-name').value, email: document.getElementById('new-user-email').value } });
				await renderUsers();
			} catch (e) { showError(e.message); }
		};
		Array.from(root.querySelectorAll('button[data-update-user]')).forEach(btn => {
			btn.onclick = async () => {
				const uid = btn.getAttribute('data-update-user');
				try {
					await api('/api/users/' + encodeURIComponent(uid), { method: 'PATCH', body: { name: root.querySelector('input[data-name-for="' + uid + '"]').value, email: root.querySelector('input[data-email-for="' + uid + '"]').value } });
					await renderUsers();
				} catch (e) { showError(e.message); }
			};
		});
		Array.from(root.querySelectorAll('button[data-delete-user]')).forEach(btn => {
			btn.onclick = async () => {
				const uid = btn.getAttribute('data-delete-user');
				try {
					await api('/api/users/' + encodeURIComponent(uid), { method: 'DELETE' });
					await renderUsers();
				} catch (e) { showError(e.message); }
			};
		});
		document.getElementById('save-roles').onclick = async () => {
			try {
				const roles = Array.from(root.querySelectorAll('select[data-role-for]')).map(sel => ({ user_id: sel.getAttribute('data-role-for'), role: sel.value }));
				await api('/api/projects/demo/roles', { method: 'PUT', body: { roles } });
				await renderUsers();
			} catch (e) { showError(e.message); }
		};
	};

	const renderResources = async (projectID) => {
		const data = await api('/api/projects/' + encodeURIComponent(projectID) + '/resources');
		const rows = data.resources || [];
		setHTML('<p>Project: <strong>' + esc(projectID) + '</strong></p><table><thead><tr><th>ID</th><th>Name</th><th>Capacity</th></tr></thead><tbody>' + rows.map(r => '<tr><td>' + esc(r.resource_id) + '</td><td><input data-res-name="' + esc(r.resource_id) + '" value="' + esc(r.name) + '"></td><td><input data-res-cap="' + esc(r.resource_id) + '" value="' + esc(r.capacity_hours_per_day) + '"></td></tr>').join('') + '</tbody></table><div class="toolbar"><button id="save-resources">Save Resources</button></div>');
		document.getElementById('save-resources').onclick = async () => {
			try {
				const payload = {
					resources: rows.map(r => ({
						resource_id: r.resource_id,
						name: root.querySelector('input[data-res-name="' + r.resource_id + '"]').value,
						capacity_hours_per_day: Number(root.querySelector('input[data-res-cap="' + r.resource_id + '"]').value)
					}))
				};
				await api('/api/projects/' + encodeURIComponent(projectID) + '/resources', { method: 'PUT', body: payload });
				await renderResources(projectID);
			} catch (e) { showError(e.message); }
		};
	};

	const renderCalendar = async (projectID) => {
		const month = query.get('month') || new Date().toISOString().slice(0, 7);
		const data = await api('/api/projects/' + encodeURIComponent(projectID) + '/calendar?month=' + encodeURIComponent(month));
		const rows = data.days || [];
		setHTML('<p>Project: <strong>' + esc(projectID) + '</strong> / Month: <strong>' + esc(month) + '</strong></p><table><thead><tr><th>Date</th><th>Working</th></tr></thead><tbody>' + rows.map(d => '<tr><td>' + esc(d.date) + '</td><td><input type="checkbox" data-cal-date="' + esc(d.date) + '" ' + (d.is_working ? 'checked' : '') + '></td></tr>').join('') + '</tbody></table><div class="toolbar"><button id="save-calendar">Save Calendar</button></div>');
		document.getElementById('save-calendar').onclick = async () => {
			try {
				const days = rows.map(d => ({
					date: d.date,
					is_working: root.querySelector('input[data-cal-date="' + d.date + '"]').checked
				}));
				await api('/api/projects/' + encodeURIComponent(projectID) + '/calendar', { method: 'PUT', body: { days } });
				await renderCalendar(projectID);
			} catch (e) { showError(e.message); }
		};
	};

	const renderWorkspace = async (projectID, sprintID, withDialog) => {
		const data = await api('/api/projects/' + encodeURIComponent(projectID) + '/sprints/' + encodeURIComponent(sprintID) + '/workspace');
		const budgetIn = data.budget_in || [];
		const budgetOut = data.budget_out || [];
		const taskRows = (arr) => arr.map(t => '<tr><td>' + esc(t.task_id) + '</td><td>' + esc(t.title) + '</td><td>' + esc(t.status) + '</td><td><select data-task-status="' + esc(t.task_id) + '"><option value="todo" ' + (t.status === 'todo' ? 'selected' : '') + '>todo</option><option value="in_progress" ' + (t.status === 'in_progress' ? 'selected' : '') + '>in_progress</option><option value="done" ' + (t.status === 'done' ? 'selected' : '') + '>done</option></select></td><td><button data-task-update="' + esc(t.task_id) + '">Update</button></td></tr>').join('');
		setHTML('<p>Project: <strong>' + esc(projectID) + '</strong> / Sprint: <strong>' + esc(sprintID) + '</strong></p><p class="small">available_hours=' + esc(data.available_hours) + '</p><h3>Budget In</h3><table><thead><tr><th>Task</th><th>Title</th><th>Status</th><th>New Status</th><th></th></tr></thead><tbody>' + taskRows(budgetIn) + '</tbody></table><h3>Budget Out</h3><table><thead><tr><th>Task</th><th>Title</th><th>Status</th><th>New Status</th><th></th></tr></thead><tbody>' + taskRows(budgetOut) + '</tbody></table>' + (withDialog ? '<div class="toolbar"><button id="apply-carryover">Apply Carry-Over</button></div>' : ''));
		Array.from(root.querySelectorAll('button[data-task-update]')).forEach(btn => {
			btn.onclick = async () => {
				const taskID = btn.getAttribute('data-task-update');
				try {
					const status = root.querySelector('select[data-task-status="' + taskID + '"]').value;
					await api('/api/projects/' + encodeURIComponent(projectID) + '/tasks/' + encodeURIComponent(taskID), { method: 'PATCH', body: { status } });
					await renderWorkspace(projectID, sprintID, withDialog);
				} catch (e) { showError(e.message); }
			};
		});
		if (withDialog) {
			const btn = document.getElementById('apply-carryover');
			if (btn) {
				btn.onclick = async () => {
					try {
						const decisions = budgetOut.map(t => ({ task_id: t.task_id, action: 'keep' }));
						await api('/api/projects/' + encodeURIComponent(projectID) + '/sprints/' + encodeURIComponent(sprintID) + '/carryover/apply', { method: 'POST', body: { decisions } });
						await renderWorkspace(projectID, sprintID, true);
					} catch (e) { showError(e.message); }
				};
			}
		}
	};

	(async () => {
		try {
			if (route === '/') return await renderTop();
			if (route === '/projects') return await renderProjects();
			if (route === '/users') return await renderUsers();
			if (segs.length === 3 && segs[0] === 'projects' && segs[2] === 'resources') return await renderResources(segs[1]);
			if (segs.length === 3 && segs[0] === 'projects' && segs[2] === 'calendar') return await renderCalendar(segs[1]);
			if (segs.length === 5 && segs[0] === 'projects' && segs[2] === 'sprints' && segs[4] === 'workspace') return await renderWorkspace(segs[1], segs[3], query.get('dialog') === 'carryover');
			setHTML('<p>Unsupported route</p>');
		} catch (e) {
			showError(e.message || String(e));
		}
	})();
})();`
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

	if r.Method != http.MethodGet {
		writeErr(w, rid, methodNotAllowed())
		return
	}

	resp := a.sendToP2(zmqRequest{
		RequestID: rid,
		Command:   "list_projects",
	})
	if resp.Status == "error" {
		writeErr(w, rid, mapP2Error(resp.Error))
		return
	}
	writeOK(w, rid, resp.Data)
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

	resp := a.sendToP2(zmqRequest{
		RequestID: rid,
		Command:   "resolve_default_locale",
		QueryParams: map[string]string{
			"accept_language": r.Header.Get("Accept-Language"),
		},
	})
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
		if item.Role != "administrator" && item.Role != "assignee" {
			return p2Validation(req.RequestID, "INVALID_ROLE", "role must be administrator or assignee")
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
	lang, region := parseAcceptLanguage(req.QueryParams["accept_language"])
	if lang == "" || region == "" {
		return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"locale": "en", "source": "fallback"}}
	}

	var locale string
	err := a.systemDB.QueryRow(
		`SELECT locale FROM locale_config WHERE language = ? AND region = ? LIMIT 1`,
		lang, region,
	).Scan(&locale)
	if errors.Is(err, sql.ErrNoRows) {
		return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"locale": "en", "source": "fallback"}}
	}
	if err != nil {
		return zmqResponse{RequestID: req.RequestID, Status: "error", Error: &responseError{Code: "PERSISTENCE_ERROR", Message: err.Error()}}
	}

	return zmqResponse{RequestID: req.RequestID, Status: "ok", Data: map[string]any{"locale": locale, "source": "matched"}}
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
	case "INVALID_ESTIMATE", "INVALID_IMPACT", "DUPLICATE_RESOURCE_ID", "INVALID_RESOURCE_CAPACITY", "DUPLICATE_CALENDAR_DATE", "DUPLICATE_USER_ID", "INVALID_ROLE", "INVALID_MENU_KEY", "DUPLICATE_MENU_KEY":
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
	if _, err := db.Exec(`INSERT OR IGNORE INTO locale_config(language, region, locale) VALUES('ja', 'JP', 'ja')`); err != nil {
		return err
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

func parseAcceptLanguage(raw string) (string, string) {
	if strings.TrimSpace(raw) == "" {
		return "", ""
	}
	parts := strings.Split(raw, ",")
	tokens := []string{}
	for _, p := range parts {
		t := strings.TrimSpace(strings.SplitN(p, ";", 2)[0])
		if t == "" {
			continue
		}
		tokens = append(tokens, t)
	}
	for _, token := range tokens {
		token = strings.ReplaceAll(token, "_", "-")
		if strings.Count(token, "-") < 1 {
			continue
		}
		chunks := strings.Split(token, "-")
		lang := strings.ToLower(chunks[0])
		region := strings.ToUpper(chunks[1])
		return lang, region
	}
	return "", ""
}
