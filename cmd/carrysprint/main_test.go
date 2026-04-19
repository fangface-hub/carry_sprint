package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestGetProjects(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	req := httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	req.Header.Set("X-Request-Id", "req-1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if body["result"] != "ok" {
		t.Fatalf("expected result ok, got %v", body["result"])
	}
}

func TestGetUsers(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	req.Header.Set("X-Request-Id", "req-2")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if body["result"] != "ok" {
		t.Fatalf("expected result ok, got %v", body["result"])
	}
}

func TestGetProjectSummaryAndWorkspace(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	reqSummary := httptest.NewRequest(http.MethodGet, "/api/projects/demo/summary", nil)
	reqSummary.Header.Set("X-Request-Id", "req-s1")
	wSummary := httptest.NewRecorder()
	mux.ServeHTTP(wSummary, reqSummary)
	if wSummary.Code != http.StatusOK {
		t.Fatalf("summary expected 200, got %d", wSummary.Code)
	}

	reqWs := httptest.NewRequest(http.MethodGet, "/api/projects/demo/sprints/sp-001/workspace", nil)
	reqWs.Header.Set("X-Request-Id", "req-s2")
	wWs := httptest.NewRecorder()
	mux.ServeHTTP(wWs, reqWs)
	if wWs.Code != http.StatusOK {
		t.Fatalf("workspace expected 200, got %d", wWs.Code)
	}
	if !strings.Contains(wWs.Body.String(), "budget_in") {
		t.Fatalf("workspace response missing budget_in")
	}
}

func TestUpdateTaskAndResourceCalendarFlow(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	updatePayload := []byte(`{"estimate_hours":20,"impact":"medium","status":"in_progress"}`)
	reqTask := httptest.NewRequest(http.MethodPatch, "/api/projects/demo/tasks/task-001", bytes.NewReader(updatePayload))
	reqTask.Header.Set("X-Request-Id", "req-u1")
	reqTask.Header.Set("Content-Type", "application/json")
	wTask := httptest.NewRecorder()
	mux.ServeHTTP(wTask, reqTask)
	if wTask.Code != http.StatusOK {
		t.Fatalf("task update expected 200, got %d", wTask.Code)
	}

	resPayload := []byte(`{"resources":[{"resource_id":"r1","name":"Res1","capacity_hours_per_day":6}]}`)
	reqResPut := httptest.NewRequest(http.MethodPut, "/api/projects/demo/resources", bytes.NewReader(resPayload))
	reqResPut.Header.Set("X-Request-Id", "req-u2")
	reqResPut.Header.Set("Content-Type", "application/json")
	wResPut := httptest.NewRecorder()
	mux.ServeHTTP(wResPut, reqResPut)
	if wResPut.Code != http.StatusOK {
		t.Fatalf("resource put expected 200, got %d", wResPut.Code)
	}

	reqResGet := httptest.NewRequest(http.MethodGet, "/api/projects/demo/resources", nil)
	reqResGet.Header.Set("X-Request-Id", "req-u3")
	wResGet := httptest.NewRecorder()
	mux.ServeHTTP(wResGet, reqResGet)
	if wResGet.Code != http.StatusOK {
		t.Fatalf("resource get expected 200, got %d", wResGet.Code)
	}

	calPayload := []byte(`{"days":[{"date":"2026-04-02","is_working":false}]}`)
	reqCalPut := httptest.NewRequest(http.MethodPut, "/api/projects/demo/calendar", bytes.NewReader(calPayload))
	reqCalPut.Header.Set("X-Request-Id", "req-u4")
	reqCalPut.Header.Set("Content-Type", "application/json")
	wCalPut := httptest.NewRecorder()
	mux.ServeHTTP(wCalPut, reqCalPut)
	if wCalPut.Code != http.StatusOK {
		t.Fatalf("calendar put expected 200, got %d", wCalPut.Code)
	}

	reqCalGet := httptest.NewRequest(http.MethodGet, "/api/projects/demo/calendar?month=2026-04", nil)
	reqCalGet.Header.Set("X-Request-Id", "req-u5")
	wCalGet := httptest.NewRecorder()
	mux.ServeHTTP(wCalGet, reqCalGet)
	if wCalGet.Code != http.StatusOK {
		t.Fatalf("calendar get expected 200, got %d", wCalGet.Code)
	}
}

func TestRolesAndUserLifecycle(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	newUser := []byte(`{"user_id":"u100","name":"User100","email":"u100@example.com"}`)
	reqUserCreate := httptest.NewRequest(http.MethodPost, "/api/users", bytes.NewReader(newUser))
	reqUserCreate.Header.Set("X-Request-Id", "req-r1")
	reqUserCreate.Header.Set("Content-Type", "application/json")
	wUserCreate := httptest.NewRecorder()
	mux.ServeHTTP(wUserCreate, reqUserCreate)
	if wUserCreate.Code != http.StatusOK {
		t.Fatalf("user create expected 200, got %d", wUserCreate.Code)
	}

	roles := []byte(`{"roles":[{"user_id":"u100","role":"assignee"}]}`)
	reqRolesPut := httptest.NewRequest(http.MethodPut, "/api/projects/demo/roles", bytes.NewReader(roles))
	reqRolesPut.Header.Set("X-Request-Id", "req-r2")
	reqRolesPut.Header.Set("Content-Type", "application/json")
	wRolesPut := httptest.NewRecorder()
	mux.ServeHTTP(wRolesPut, reqRolesPut)
	if wRolesPut.Code != http.StatusOK {
		t.Fatalf("roles put expected 200, got %d", wRolesPut.Code)
	}

	reqRolesGet := httptest.NewRequest(http.MethodGet, "/api/projects/demo/roles", nil)
	reqRolesGet.Header.Set("X-Request-Id", "req-r3")
	wRolesGet := httptest.NewRecorder()
	mux.ServeHTTP(wRolesGet, reqRolesGet)
	if wRolesGet.Code != http.StatusOK {
		t.Fatalf("roles get expected 200, got %d", wRolesGet.Code)
	}

	userPatch := []byte(`{"name":"User101"}`)
	reqUserPatch := httptest.NewRequest(http.MethodPatch, "/api/users/u100", bytes.NewReader(userPatch))
	reqUserPatch.Header.Set("X-Request-Id", "req-r4")
	reqUserPatch.Header.Set("Content-Type", "application/json")
	wUserPatch := httptest.NewRecorder()
	mux.ServeHTTP(wUserPatch, reqUserPatch)
	if wUserPatch.Code != http.StatusOK {
		t.Fatalf("user patch expected 200, got %d", wUserPatch.Code)
	}

	reqUserDelete := httptest.NewRequest(http.MethodDelete, "/api/users/u100", nil)
	reqUserDelete.Header.Set("X-Request-Id", "req-r5")
	wUserDelete := httptest.NewRecorder()
	mux.ServeHTTP(wUserDelete, reqUserDelete)
	if wUserDelete.Code != http.StatusOK {
		t.Fatalf("user delete expected 200, got %d", wUserDelete.Code)
	}
}

func TestGetDefaultLocale(t *testing.T) {
	tests := []struct {
		name           string
		acceptLanguage string
		wantLocale     string
		wantSource     string
	}{
		{name: "japanese", acceptLanguage: "ja-JP,ja;q=0.9,en-US;q=0.8", wantLocale: "ja", wantSource: "matched"},
		{name: "region fallback for japan", acceptLanguage: "en-JP,en;q=0.9", wantLocale: "ja", wantSource: "region_matched"},
		{name: "language fallback for japanese", acceptLanguage: "ja,en-US;q=0.8", wantLocale: "ja", wantSource: "language_matched"},
		{name: "script tag chinese", acceptLanguage: "zh-Hans-CN,zh;q=0.9,en-US;q=0.8", wantLocale: "zh", wantSource: "matched"},
		{name: "german", acceptLanguage: "de-DE,de;q=0.9,en-US;q=0.8", wantLocale: "de", wantSource: "matched"},
		{name: "chinese", acceptLanguage: "zh-CN,zh;q=0.9,en-US;q=0.8", wantLocale: "zh", wantSource: "matched"},
		{name: "italian", acceptLanguage: "it-IT,it;q=0.9,en-US;q=0.8", wantLocale: "it", wantSource: "matched"},
		{name: "french", acceptLanguage: "fr-FR,fr;q=0.9,en-US;q=0.8", wantLocale: "fr", wantSource: "matched"},
		{name: "fallback", acceptLanguage: "en-US,en;q=0.9", wantLocale: "en", wantSource: "fallback"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newTestApp(t)
			mux := newTestMux(a)

			req := httptest.NewRequest(http.MethodGet, "/api/locales/default", nil)
			req.Header.Set("X-Request-Id", "req-3")
			req.Header.Set("Accept-Language", tt.acceptLanguage)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d", w.Code)
			}

			var body struct {
				Result string `json:"result"`
				Data   struct {
					Locale string `json:"locale"`
					Source string `json:"source"`
				} `json:"data"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("failed to parse response: %v", err)
			}
			if body.Result != "ok" {
				t.Fatalf("expected result ok, got %s", body.Result)
			}
			if body.Data.Locale != tt.wantLocale {
				t.Fatalf("expected locale %s, got %s", tt.wantLocale, body.Data.Locale)
			}
			if body.Data.Source != tt.wantSource {
				t.Fatalf("expected source %s, got %s", tt.wantSource, body.Data.Source)
			}
		})
	}
}

func TestUserLocaleSettingOverridesBrowserLocale(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	putPayload := []byte(`{"locale":"fr"}`)
	putReq := httptest.NewRequest(http.MethodPut, "/api/users/u001/locale", bytes.NewReader(putPayload))
	putReq.Header.Set("X-Request-Id", "req-locale-save")
	putReq.Header.Set("Content-Type", "application/json")
	putW := httptest.NewRecorder()
	mux.ServeHTTP(putW, putReq)
	if putW.Code != http.StatusOK {
		t.Fatalf("locale setting put expected 200, got %d", putW.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/users/u001/locale", nil)
	getReq.Header.Set("X-Request-Id", "req-locale-get")
	getW := httptest.NewRecorder()
	mux.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("locale setting get expected 200, got %d", getW.Code)
	}
	if !strings.Contains(getW.Body.String(), `"locale":"fr"`) {
		t.Fatalf("expected saved locale fr, got %s", getW.Body.String())
	}

	defaultReq := httptest.NewRequest(http.MethodGet, "/api/locales/default", nil)
	defaultReq.Header.Set("X-Request-Id", "req-locale-default")
	defaultReq.Header.Set("X-User-Id", "u001")
	defaultReq.Header.Set("Accept-Language", "ja-JP,ja;q=0.9")
	defaultW := httptest.NewRecorder()
	mux.ServeHTTP(defaultW, defaultReq)
	if defaultW.Code != http.StatusOK {
		t.Fatalf("default locale expected 200, got %d", defaultW.Code)
	}
	if !strings.Contains(defaultW.Body.String(), `"locale":"fr"`) {
		t.Fatalf("expected explicit locale fr, got %s", defaultW.Body.String())
	}
	if !strings.Contains(defaultW.Body.String(), `"source":"user_setting"`) {
		t.Fatalf("expected user_setting source, got %s", defaultW.Body.String())
	}

	clearPayload := []byte(`{"locale":""}`)
	clearReq := httptest.NewRequest(http.MethodPut, "/api/users/u001/locale", bytes.NewReader(clearPayload))
	clearReq.Header.Set("X-Request-Id", "req-locale-clear")
	clearReq.Header.Set("Content-Type", "application/json")
	clearW := httptest.NewRecorder()
	mux.ServeHTTP(clearW, clearReq)
	if clearW.Code != http.StatusOK {
		t.Fatalf("locale setting clear expected 200, got %d", clearW.Code)
	}

	defaultReq2 := httptest.NewRequest(http.MethodGet, "/api/locales/default", nil)
	defaultReq2.Header.Set("X-Request-Id", "req-locale-default2")
	defaultReq2.Header.Set("X-User-Id", "u001")
	defaultReq2.Header.Set("Accept-Language", "en-JP,en;q=0.9")
	defaultW2 := httptest.NewRecorder()
	mux.ServeHTTP(defaultW2, defaultReq2)
	if defaultW2.Code != http.StatusOK {
		t.Fatalf("default locale after clear expected 200, got %d", defaultW2.Code)
	}
	if !strings.Contains(defaultW2.Body.String(), `"locale":"ja"`) {
		t.Fatalf("expected browser-resolved locale ja, got %s", defaultW2.Body.String())
	}
	if !strings.Contains(defaultW2.Body.String(), `"source":"region_matched"`) {
		t.Fatalf("expected region_matched source, got %s", defaultW2.Body.String())
	}
}

func TestUserLocaleSettingRejectsUnsupportedLocale(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	req := httptest.NewRequest(http.MethodPut, "/api/users/u001/locale", bytes.NewReader([]byte(`{"locale":"en"}`)))
	req.Header.Set("X-Request-Id", "req-invalid-locale")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid locale expected 422, got %d", w.Code)
	}
}

func TestTopMenuAndVisibilityFlow(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	getTop := httptest.NewRequest(http.MethodGet, "/api/top/menu", nil)
	getTop.Header.Set("X-Request-Id", "req-t1")
	getTop.Header.Set("X-User-Id", "u001")
	wGetTop := httptest.NewRecorder()
	mux.ServeHTTP(wGetTop, getTop)
	if wGetTop.Code != http.StatusOK {
		t.Fatalf("top menu expected 200, got %d", wGetTop.Code)
	}

	getVis := httptest.NewRequest(http.MethodGet, "/api/users/u001/menu-visibility", nil)
	getVis.Header.Set("X-Request-Id", "req-t2")
	wGetVis := httptest.NewRecorder()
	mux.ServeHTTP(wGetVis, getVis)
	if wGetVis.Code != http.StatusOK {
		t.Fatalf("menu visibility get expected 200, got %d", wGetVis.Code)
	}

	putPayload := []byte(`{"menu_visibility":[{"menu_key":"project_select","is_enabled":true},{"menu_key":"sprint_workspace","is_enabled":false},{"menu_key":"resource_settings","is_enabled":true},{"menu_key":"calendar_settings","is_enabled":false},{"menu_key":"user_management","is_enabled":true}]}`)
	putVis := httptest.NewRequest(http.MethodPut, "/api/users/u001/menu-visibility", bytes.NewReader(putPayload))
	putVis.Header.Set("X-Request-Id", "req-t3")
	putVis.Header.Set("Content-Type", "application/json")
	wPutVis := httptest.NewRecorder()
	mux.ServeHTTP(wPutVis, putVis)
	if wPutVis.Code != http.StatusOK {
		t.Fatalf("menu visibility put expected 200, got %d", wPutVis.Code)
	}

	getTop2 := httptest.NewRequest(http.MethodGet, "/api/top/menu", nil)
	getTop2.Header.Set("X-Request-Id", "req-t4")
	getTop2.Header.Set("X-User-Id", "u001")
	wGetTop2 := httptest.NewRecorder()
	mux.ServeHTTP(wGetTop2, getTop2)
	if wGetTop2.Code != http.StatusOK {
		t.Fatalf("top menu after update expected 200, got %d", wGetTop2.Code)
	}
	if strings.Contains(wGetTop2.Body.String(), "sprint_workspace") {
		t.Fatalf("top menu should not include disabled sprint_workspace")
	}
}

func TestTopMenuWithoutUserID(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	req := httptest.NewRequest(http.MethodGet, "/api/top/menu", nil)
	req.Header.Set("X-Request-Id", "req-top-missing-user")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("top menu without user id expected 400, got %d", w.Code)
	}
}

func TestAdminBootstrap(t *testing.T) {
	a := newTestApp(t)

	var userID string
	err := a.systemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = 'admin'`).Scan(&userID)
	if err != nil {
		t.Fatalf("expected admin user bootstrap, got error: %v", err)
	}

	var hash string
	err = a.systemDB.QueryRow(`SELECT password_hash FROM user_credentials WHERE user_id = 'admin'`).Scan(&hash)
	if err != nil {
		t.Fatalf("expected admin credential bootstrap, got error: %v", err)
	}
	if hash == "" {
		t.Fatalf("expected non-empty password hash")
	}
}

func TestMissingRequestID(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	req := httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", w.Code)
	}
}

func TestBrowserUIRoutes(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	routes := []string{
		"/",
		"/projects",
		"/projects/demo/sprints/sp-001/workspace",
		"/projects/demo/resources",
		"/projects/demo/calendar",
		"/projects/demo/sprints/sp-001/workspace?dialog=carryover",
		"/users",
	}

	for _, route := range routes {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("browser route %s expected 200, got %d", route, w.Code)
		}
		if !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
			t.Fatalf("browser route %s expected text/html, got %s", route, w.Header().Get("Content-Type"))
		}
	}
}

func TestBrowserUIRouteShowsDifferentScreenTitle(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	topReq := httptest.NewRequest(http.MethodGet, "/", nil)
	topW := httptest.NewRecorder()
	mux.ServeHTTP(topW, topReq)
	if topW.Code != http.StatusOK {
		t.Fatalf("top route expected 200, got %d", topW.Code)
	}
	if !strings.Contains(topW.Body.String(), "Top Page") {
		t.Fatalf("top route should contain Top Page title")
	}

	usersReq := httptest.NewRequest(http.MethodGet, "/users", nil)
	usersW := httptest.NewRecorder()
	mux.ServeHTTP(usersW, usersReq)
	if usersW.Code != http.StatusOK {
		t.Fatalf("users route expected 200, got %d", usersW.Code)
	}
	if !strings.Contains(usersW.Body.String(), "User Management Screen") {
		t.Fatalf("users route should contain User Management Screen title")
	}
}

func TestUndefinedBrowserRouteReturnsNotFound(t *testing.T) {
	a := newTestApp(t)
	mux := newTestMux(a)

	req := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("undefined browser route expected 404, got %d", w.Code)
	}
}

func newTestMux(a *app) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/projects", a.handleProjects)
	mux.HandleFunc("/api/projects/", a.handleProjectSubRoutes)
	mux.HandleFunc("/api/users", a.handleUsers)
	mux.HandleFunc("/api/users/", a.handleUserByID)
	mux.HandleFunc("/api/locales/default", a.handleDefaultLocale)
	mux.HandleFunc("/api/top/menu", a.handleTopMenu)
	mux.HandleFunc("/api/", a.handleNotFound)
	mux.HandleFunc("/", a.handleNotFound)
	return mux
}

func newTestApp(t *testing.T) *app {
	t.Helper()
	var a *app
	dataDir := t.TempDir()
	systemPath := filepath.Join(dataDir, "system.sqlite")
	systemDB, err := sql.Open("sqlite", systemPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	t.Cleanup(func() {
		for _, db := range a.projectDBs {
			_ = db.Close()
		}
		_ = systemDB.Close()
	})

	if err := initSystemSchema(systemDB); err != nil {
		t.Fatalf("failed to init system schema: %v", err)
	}
	if err := seedSystemData(systemDB); err != nil {
		t.Fatalf("failed to seed system data: %v", err)
	}

	a = &app{systemDB: systemDB, dataDir: dataDir, projectDBs: map[string]*sql.DB{}}
	if err := a.ensureProjectSchema("demo"); err != nil {
		t.Fatalf("failed to ensure project schema: %v", err)
	}
	if err := a.seedProjectData("demo"); err != nil {
		t.Fatalf("failed to seed project data: %v", err)
	}
	return a
}
