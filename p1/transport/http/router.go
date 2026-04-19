package httptransport

import (
	"fmt"
	"html"
	"net/http"
	"strings"

	"carry_sprint/p1/transport/http/handler"
	"carry_sprint/p1/transport/http/middleware"
	"carry_sprint/p1/transport/http/presenter"
)

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
	if path == "/" || path == "/projects" || path == "/projects/new" || path == "/users" {
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
	_, _ = fmt.Fprintf(w, "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>CarrySprint</title><style>body{font-family:Segoe UI,Helvetica,Arial,sans-serif;background:#f5f7fb;color:#1f2937;margin:0}main{max-width:980px;margin:28px auto;padding:24px;background:#fff;border:1px solid #e5e7eb;border-radius:12px;box-shadow:0 8px 24px rgba(15,23,42,.06)}h1{margin:0 0 8px;font-size:28px}.screen{display:inline-block;margin:0 0 12px;padding:6px 10px;background:#dbeafe;color:#1e3a8a;border-radius:999px;font-size:13px;font-weight:600}p{margin:8px 0 16px;line-height:1.6}.route{padding:10px 12px;background:#eef2ff;border-radius:8px;font-family:Consolas,monospace}.layout{display:grid;grid-template-columns:230px 1fr;gap:18px}.nav{padding:14px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px}.nav ul{margin:0;padding-left:18px}.nav li{margin:8px 0}.panel{padding:16px;border:1px solid #e5e7eb;border-radius:10px;background:#fff}.muted{color:#64748b}.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}.toolbar input,.toolbar select{padding:8px;border:1px solid #cbd5e1;border-radius:8px}.toolbar button{padding:8px 12px;border:1px solid #cbd5e1;background:#fff;border-radius:8px;cursor:pointer}.toolbar button:hover{background:#eff6ff}table{width:100%%;border-collapse:collapse}th,td{border-bottom:1px solid #e5e7eb;padding:8px;text-align:left}.error{margin-top:10px;padding:10px;border-radius:8px;background:#fee2e2;color:#991b1b;display:none}.small{font-size:12px;color:#64748b}</style></head><body><main><h1>CarrySprint</h1><div class=\"screen\">%s</div><p>%s</p><p class=\"route\">Current route: %s</p><div class=\"layout\"><nav class=\"nav\"><strong>Screen Links</strong><ul><li><a href=\"/\">Top Page</a></li><li><a href=\"/projects\">Project Select</a></li><li><a href=\"/projects/new\">Project Register</a></li><li><a href=\"/projects/demo/sprints/sp-001/workspace\">Sprint Workspace</a></li><li><a href=\"/projects/demo/sprints/sp-001/workspace?dialog=carryover\">Carry-Over Dialog</a></li><li><a href=\"/projects/demo/resources\">Resource Settings</a></li><li><a href=\"/projects/demo/calendar\">Calendar Settings</a></li><li><a href=\"/users\">User Management</a></li></ul></nav><section class=\"panel\" id=\"app\" data-route=\"%s\">%s</section></div></main><script>%s</script></body></html>", escTitle, escDesc, escPath, escPath, content, script)
}

func resolveBrowserScreen(path string, dialog string) (string, string, string) {
	segs := splitPath(path)
	if path == "/" {
		return "Top Page", "Top menu, menu visibility settings, and locale settings screen.", "<h2>Top Menu</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
	}
	if path == "/projects" {
		return "Project Select Screen", "Select a project to open project-specific screens.", "<h2>Projects</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
	}
	if path == "/projects/new" {
		return "Project Register Screen", "Register a new project with an initial sprint and administrator assignment.", "<h2>Register Project</h2><div id=\"screen-root\" class=\"muted\">Loading...</div><div id=\"screen-error\" class=\"error\"></div>"
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
			const localeSetting = await api('/api/users/' + encodeURIComponent(uid) + '/locale');
			const effectiveLocale = await api('/api/locales/default', { headers: { 'X-User-Id': uid } });
			const visList = vis.menu_visibility || [];
			const localeOptions = [''].concat(localeSetting.locale_options || []);
			setHTML(
				'<div class="toolbar"><label>User</label><select id="menu-user">' +
					users.map(u => '<option value="' + esc(u.user_id) + '">' + esc(u.user_id) + ' - ' + esc(u.name) + '</option>').join('') +
				'</select><button id="menu-load">Load</button><button id="menu-save">Save Visibility</button><button id="locale-save">Save Locale</button></div>' +
				'<h3>Locale</h3><div class="toolbar"><label>Preferred Locale <select id="locale-select">' +
					localeOptions.map(locale => '<option value="' + esc(locale) + '">' + (locale === '' ? 'Automatic' : esc(locale)) + '</option>').join('') +
				'</select></label><span class="small">Effective locale: <strong>' + esc(effectiveLocale.locale || '') + '</strong> (' + esc(effectiveLocale.source || '') + ')</span></div>' +
				'<h3>Top Menu Buttons</h3><ul>' + (menu.menu_buttons || []).map(m => '<li>' + esc(m.menu_key) + ' (' + esc(m.label) + ')</li>').join('') + '</ul>' +
				'<h3>Visibility</h3><div>' + visList.map(v => '<label style="display:block"><input type="checkbox" data-menu-key="' + esc(v.menu_key) + '" ' + (v.is_enabled ? 'checked' : '') + '> ' + esc(v.menu_key) + '</label>').join('') + '</div>'
			);
			const userSelect = document.getElementById('menu-user');
			if (userSelect) userSelect.value = uid;
			const localeSelect = document.getElementById('locale-select');
			if (localeSelect) localeSelect.value = localeSetting.locale || '';
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
			document.getElementById('locale-save').onclick = async () => {
				try {
					await api('/api/users/' + encodeURIComponent(document.getElementById('menu-user').value) + '/locale', { method: 'PUT', body: { locale: document.getElementById('locale-select').value } });
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
			(summary ? ('<p class="small">First project summary: sprint_count=' + esc(summary.sprint_count) + ', task_count=' + esc(summary.task_count) + '</p>') : '') +
			'<div class="toolbar"><button id="open-project-register" onclick="location.href=\'/projects/new\'">Register Project</button></div>'
		);
	};

	const renderProjectRegister = async () => {
		const usersData = await api('/api/users');
		const users = usersData.users || [];
		setHTML(
			'<div class="toolbar">' +
			'<label>Project ID <input id="reg-project-id" placeholder="project_id"></label>' +
			'<label>Name <input id="reg-name" placeholder="name"></label>' +
			'</div>' +
			'<div class="toolbar">' +
			'<label>Description <input id="reg-desc" placeholder="description"></label>' +
			'</div>' +
			'<h3>Initial Sprint</h3>' +
			'<div class="toolbar">' +
			'<label>Sprint ID <input id="reg-sprint-id" placeholder="sprint_id"></label>' +
			'<label>Sprint Name <input id="reg-sprint-name" placeholder="name"></label>' +
			'</div>' +
			'<div class="toolbar">' +
			'<label>Start Date <input id="reg-start-date" placeholder="YYYY-MM-DD"></label>' +
			'<label>End Date <input id="reg-end-date" placeholder="YYYY-MM-DD"></label>' +
			'</div>' +
			'<h3>Project Administrator</h3>' +
			'<div class="toolbar"><label>Admin User <select id="reg-admin">' +
			users.map(u => '<option value="' + esc(u.user_id) + '">' + esc(u.user_id) + ' - ' + esc(u.name) + '</option>').join('') +
			'</select></label></div>' +
			'<div class="toolbar"><button id="reg-submit">Register Project</button><button onclick="location.href=\'/projects\'">Cancel</button></div>'
		);
		document.getElementById('reg-submit').onclick = async () => {
			try {
				const body = {
					project_id: document.getElementById('reg-project-id').value,
					name: document.getElementById('reg-name').value,
					description: document.getElementById('reg-desc').value,
					initial_sprint: {
						sprint_id: document.getElementById('reg-sprint-id').value,
						name: document.getElementById('reg-sprint-name').value,
						start_date: document.getElementById('reg-start-date').value,
						end_date: document.getElementById('reg-end-date').value
					},
					initial_admin_user_id: document.getElementById('reg-admin').value
				};
				await api('/api/projects', { method: 'POST', body });
				location.href = '/projects';
			} catch (e) { showError(e.message); }
		};
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
			if (route === '/projects/new') return await renderProjectRegister();
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
