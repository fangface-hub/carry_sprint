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
	a := newTestApp(t)
	mux := newTestMux(a)

	req := httptest.NewRequest(http.MethodGet, "/api/locales/default", nil)
	req.Header.Set("X-Request-Id", "req-3")
	req.Header.Set("Accept-Language", "ja-JP,ja;q=0.9,en-US;q=0.8")
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
	if body.Data.Locale != "ja" {
		t.Fatalf("expected locale ja, got %s", body.Data.Locale)
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

func newTestMux(a *app) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/projects", a.handleProjects)
	mux.HandleFunc("/api/projects/", a.handleProjectSubRoutes)
	mux.HandleFunc("/api/users", a.handleUsers)
	mux.HandleFunc("/api/users/", a.handleUserByID)
	mux.HandleFunc("/api/locales/default", a.handleDefaultLocale)
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
