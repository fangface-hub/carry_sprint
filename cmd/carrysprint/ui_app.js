(function () {
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
		applyLocaleToDocument();
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
	const authKey = 'carrysprint.signedin.user';
	let uiLocale = 'en';

const jaTextMap = {
'Screen Links': '画面リンク',
'Current route:': '現在のルート:',
'Sign-In Screen': 'サインイン画面',
'Sign in to open browser UI screens.': 'ブラウザUIを開くにはサインインしてください。',
'Top Page': 'トップページ',
'Top menu, menu visibility settings, and locale settings screen.': 'トップメニュー・メニュー表示設定・ロケール設定の画面です。',
'Project Select Screen': 'プロジェクト選択画面',
'Select a project to open project-specific screens.': 'プロジェクトを選択してプロジェクト別画面を開きます。',
'Project Register Screen': 'プロジェクト登録画面',
'User Management Screen': 'ユーザー管理画面',
'Resource Settings Screen': 'リソース設定画面',
'Working-Day Calendar Screen': '稼働日カレンダー画面',
'Sprint Workspace Screen': 'スプリント作業画面',
'Carry-Over Review Dialog': '繰越レビュー画面',
'Top Menu': 'トップメニュー',
'Projects': 'プロジェクト',
'Register Project': 'プロジェクト登録',
'User Management': 'ユーザー管理',
'Resource Settings': 'リソース設定',
'Calendar Settings': 'カレンダー設定',
'Sprint Workspace': 'スプリント作業',
'Carry-Over Review': '繰越レビュー',
'Sign In': 'サインイン',
'Forgot Password': 'パスワードを忘れた場合',
'User ID': 'ユーザーID',
'Password': 'パスワード',
'Sign Out': 'サインアウト',
'User': 'ユーザー',
'Load': '読込',
'Save Visibility': '表示設定を保存',
'Save Locale': 'ロケールを保存',
'Preferred Locale': '優先ロケール',
'Locale Options:': 'ロケール選択肢:',
'Effective locale:': '有効ロケール:',
'Automatic': '自動',
'Top Menu Buttons': 'トップメニューボタン',
'Visibility': '表示設定',
'Save Resources': 'リソースを保存',
'Save Calendar': 'カレンダーを保存',
'Apply Carry-Over': '繰越を適用',
'Unsupported route': '未対応のルートです',
'Please enter both user ID and password.': 'ユーザーIDとパスワードを入力してください。',
'Invalid user ID or password.': 'ユーザーIDまたはパスワードが正しくありません。',
'Password reset is not available in this local demo.': 'このローカルデモではパスワード再発行は利用できません。'
};

	const applyLocaleToDocument = () => {
		if (uiLocale !== 'ja') {
			document.documentElement.lang = 'en';
			return;
		}
		document.documentElement.lang = 'ja';
		const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
		let node = walker.nextNode();
		while (node) {
			let text = node.nodeValue;
			for (const [enText, jaText] of Object.entries(jaTextMap)) {
				if (text.includes(enText)) {
					text = text.split(enText).join(jaText);
				}
			}
			node.nodeValue = text;
			node = walker.nextNode();
		}
	};

	const currentSignedInUser = () => {
		const v = sessionStorage.getItem(authKey);
		return v ? String(v) : '';
	};

	const resolveUILocale = async () => {
		let locale = '';
		const uid = currentSignedInUser();
		if (uid) {
			try {
				const data = await api('/api/locales/default', { headers: { 'X-User-Id': uid } });
				locale = String(data.locale || '').toLowerCase();
			} catch (_) {
				locale = '';
			}
		}
		if (!locale) {
			locale = String(navigator.language || 'en').toLowerCase();
		}
		uiLocale = locale.startsWith('ja') ? 'ja' : 'en';
	};

	const gotoSignIn = () => {
		const next = location.pathname + location.search;
		location.replace('/signin?next=' + encodeURIComponent(next));
	};

	const renderSignIn = async () => {
		setHTML(
			'<div class="toolbar"><label>User ID <input id="signin-user-id" placeholder="user id"></label></div>' +
			'<div class="toolbar"><label>Password <input id="signin-password" type="password" placeholder="password"></label></div>' +
			'<div class="toolbar"><button id="signin-submit">Sign In</button><button id="signin-forgot">Forgot Password</button></div>'
		);

		document.getElementById('signin-forgot').onclick = () => {
			showError('Password reset is not available in this local demo.');
		};

		document.getElementById('signin-submit').onclick = async () => {
			try {
				const userID = (document.getElementById('signin-user-id').value || '').trim();
				const password = document.getElementById('signin-password').value || '';
				if (!userID || !password) {
					throw new Error('Please enter both user ID and password.');
				}

				const userResp = await api('/api/users');
				const users = userResp.users || [];
				if (!users.some(u => u.user_id === userID)) {
					throw new Error('Invalid user ID or password.');
				}

				sessionStorage.setItem(authKey, userID);
				const next = query.get('next');
				if (next && next.startsWith('/')) {
					location.replace(next);
					return;
				}
				location.replace('/');
			} catch (e) {
				showError(e.message || String(e));
			}
		};
	};

	const renderTop = async () => {
		const userResp = await api('/api/users');
		const users = userResp.users || [];
		const signedInUser = currentSignedInUser();
		const selectedUser = users.some(u => u.user_id === signedInUser) ? signedInUser : (users.length > 0 ? users[0].user_id : 'u001');

		const drawForUser = async (uid) => {
			const menu = await api('/api/top/menu', { headers: { 'X-User-Id': uid } });
			const vis = await api('/api/users/' + encodeURIComponent(uid) + '/menu-visibility');
			const localeSetting = await api('/api/users/' + encodeURIComponent(uid) + '/locale');
			const effectiveLocale = await api('/api/locales/default', { headers: { 'X-User-Id': uid } });
			const visList = vis.menu_visibility || [];
			const localeOptions = [''].concat(localeSetting.locale_options || []);
			const localeOptionsLabel = localeOptions.map(locale => (locale === '' ? 'Automatic' : locale)).join(', ');
			setHTML(
				'<div class="toolbar"><button id="signout-btn">Sign Out</button><label>User</label><select id="menu-user">' +
					users.map(u => '<option value="' + esc(u.user_id) + '">' + esc(u.user_id) + ' - ' + esc(u.name) + '</option>').join('') +
				'</select><button id="menu-load">Load</button><button id="menu-save">Save Visibility</button><button id="locale-save">Save Locale</button></div>' +
				'<h3>Locale</h3><div class="toolbar"><label>Preferred Locale <select id="locale-select">' +
					localeOptions.map(locale => '<option value="' + esc(locale) + '">' + (locale === '' ? 'Automatic' : esc(locale)) + '</option>').join('') +
				'</select></label><span class="small">Locale Options: <strong>' + esc(localeOptionsLabel) + '</strong></span><span class="small">Effective locale: <strong>' + esc(effectiveLocale.locale || '') + '</strong> (' + esc(effectiveLocale.source || '') + ')</span></div>' +
				'<h3>Top Menu Buttons</h3><ul>' + (menu.menu_buttons || []).map(m => '<li>' + esc(m.menu_key) + ' (' + esc(m.label) + ')</li>').join('') + '</ul>' +
				'<h3>Visibility</h3><div>' + visList.map(v => '<label style="display:block"><input type="checkbox" data-menu-key="' + esc(v.menu_key) + '" ' + (v.is_enabled ? 'checked' : '') + '> ' + esc(v.menu_key) + '</label>').join('') + '</div>'
			);
			const userSelect = document.getElementById('menu-user');
			if (userSelect) userSelect.value = uid;
			const localeSelect = document.getElementById('locale-select');
			if (localeSelect) localeSelect.value = localeSetting.locale || '';
			document.getElementById('signout-btn').onclick = () => {
				sessionStorage.removeItem(authKey);
				location.replace('/signin');
			};
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
			'<table><thead><tr><th>ID</th><th>Name</th><th>Description</th><th>Action</th></tr></thead><tbody>' +
			projects.map(p => '<tr><td>' + esc(p.project_id) + '</td><td>' + esc(p.name) + '</td><td>' + esc(p.description) + '</td><td><button data-open-project="' + esc(p.project_id) + '">Open</button></td></tr>').join('') +
			'</tbody></table>' +
			(summary ? ('<p class="small">First project summary: sprint_count=' + esc(summary.sprint_count) + ', task_count=' + esc(summary.task_count) + '</p>') : '') +
		'<div class="toolbar"><button id="open-project-register" onclick="location.href=\'/projects/new\'">Register Project</button></div>'
		);

		Array.from(root.querySelectorAll('button[data-open-project]')).forEach(btn => {
			btn.onclick = () => {
				const projectID = btn.getAttribute('data-open-project');
				location.href = '/projects/' + encodeURIComponent(projectID) + '/resources';
			};
		});
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
			users.map(u => '<tr><td>' + esc(u.user_id) + '</td><td><input data-name-for="' + esc(u.user_id) + '" value="' + esc(u.name) + '"></td><td><input data-email-for="' + esc(u.user_id) + '" value="' + esc(u.email) + '"></td><td><select data-role-for="' + esc(u.user_id) + '"><option value="administrator" ' + (roleMap.get(u.user_id) === 'administrator' ? 'selected' : '') + '>administrator</option><option value="assignee" ' + (roleMap.get(u.user_id) === 'assignee' ? 'selected' : '') + '>assignee</option><option value="viewer" ' + (roleMap.get(u.user_id) === 'viewer' ? 'selected' : '') + '>viewer</option></select></td><td><button data-update-user="' + esc(u.user_id) + '">Update</button> <button data-delete-user="' + esc(u.user_id) + '">Delete</button></td></tr>').join('') +
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
			await resolveUILocale();
			applyLocaleToDocument();
			if (route === '/signin') return await renderSignIn();
			if (!currentSignedInUser()) {
				gotoSignIn();
				return;
			}
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
})();