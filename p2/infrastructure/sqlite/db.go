package sqlite

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Manager struct {
	DataDir    string
	SystemDB   *sql.DB
	projectDBs map[string]*sql.DB
	mu         sync.Mutex
}

func NewManager(dataDir string) (*Manager, error) {
	systemPath := filepath.Join(dataDir, "system.sqlite")
	sdb, err := sql.Open("sqlite", systemPath)
	if err != nil {
		return nil, err
	}
	m := &Manager{DataDir: dataDir, SystemDB: sdb, projectDBs: map[string]*sql.DB{}}
	if err := m.initSystemSchema(); err != nil {
		return nil, err
	}
	if err := m.seedSystemData(); err != nil {
		return nil, err
	}
	if err := m.InitializeAdminUser(); err != nil {
		return nil, err
	}
	if _, err := m.ProjectDB("demo"); err != nil {
		return nil, err
	}
	if err := m.seedProjectData("demo"); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, db := range m.projectDBs {
		_ = db.Close()
	}
	if m.SystemDB != nil {
		_ = m.SystemDB.Close()
	}
}

func (m *Manager) ProjectDB(projectID string) (*sql.DB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if db, ok := m.projectDBs[projectID]; ok {
		return db, nil
	}
	path := filepath.Join(m.DataDir, fmt.Sprintf("project_%s.sqlite", projectID))
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := initProjectSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	m.projectDBs[projectID] = db
	return db, nil
}

func (m *Manager) initSystemSchema() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			user_id TEXT NOT NULL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL UNIQUE,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS projects (
			project_id TEXT NOT NULL PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS user_credentials (
			user_id TEXT NOT NULL PRIMARY KEY,
			password_hash TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS user_menu_visibility (
			user_id TEXT NOT NULL,
			menu_key TEXT NOT NULL,
			is_enabled INTEGER NOT NULL,
			PRIMARY KEY (user_id, menu_key)
		)`,
		`CREATE TABLE IF NOT EXISTS user_locale_settings (
			user_id TEXT NOT NULL PRIMARY KEY,
			locale TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS locale_config (
			language TEXT NOT NULL,
			region TEXT NOT NULL,
			locale TEXT NOT NULL,
			PRIMARY KEY(language, region)
		)`,
	}
	for _, s := range stmts {
		if _, err := m.SystemDB.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

func initProjectSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS sprints (
			sprint_id TEXT NOT NULL PRIMARY KEY,
			project_id TEXT NOT NULL,
			name TEXT NOT NULL,
			start_date TEXT NOT NULL,
			end_date TEXT NOT NULL,
			available_hours REAL NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS tasks (
			task_id TEXT NOT NULL PRIMARY KEY,
			project_id TEXT NOT NULL,
			sprint_id TEXT,
			title TEXT NOT NULL,
			estimate_hours REAL,
			impact TEXT,
			status TEXT NOT NULL DEFAULT 'todo',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_sprint_id ON tasks(sprint_id)`,
		`CREATE TABLE IF NOT EXISTS resources (
			resource_id TEXT NOT NULL PRIMARY KEY,
			name TEXT NOT NULL,
			capacity_hours_per_day REAL NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS working_day_calendar (
			date TEXT NOT NULL PRIMARY KEY,
			is_working INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE IF NOT EXISTS task_resource_allocations (
			task_id TEXT NOT NULL,
			resource_id TEXT NOT NULL,
			hours REAL NOT NULL,
			PRIMARY KEY (task_id, resource_id)
		)`,
		`CREATE TABLE IF NOT EXISTS project_roles (
			project_id TEXT NOT NULL,
			user_id TEXT NOT NULL,
			role TEXT NOT NULL,
			PRIMARY KEY (project_id, user_id)
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) seedSystemData() error {
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := m.SystemDB.Exec(`INSERT OR IGNORE INTO projects(project_id, name, description, created_at, updated_at) VALUES('demo','Demo Project','Seeded project',?,?)`, now, now); err != nil {
		return err
	}
	if _, err := m.SystemDB.Exec(`INSERT OR IGNORE INTO users(user_id, name, email, created_at, updated_at) VALUES('u001','Demo User','demo@example.com',?,?)`, now, now); err != nil {
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
		if _, err := m.SystemDB.Exec(`INSERT OR IGNORE INTO locale_config(language, region, locale) VALUES(?, ?, ?)`, seed.language, seed.region, seed.locale); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) seedProjectData(projectID string) error {
	db, err := m.ProjectDB(projectID)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT OR IGNORE INTO sprints(sprint_id, project_id, name, start_date, end_date, available_hours, created_at, updated_at) VALUES('sp-001',?, 'Sprint 1', '2026-04-01','2026-04-14',80,?,?)`, projectID, now, now); err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO tasks(task_id, project_id, sprint_id, title, estimate_hours, impact, status, created_at, updated_at) VALUES('task-001',?, 'sp-001','Set up API skeleton',12,'high','todo',?,?)`, projectID, now, now); err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO tasks(task_id, project_id, sprint_id, title, estimate_hours, impact, status, created_at, updated_at) VALUES('task-002',?, 'sp-001','Implement workspace classification',16,'high','in_progress',?,?)`, projectID, now, now); err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO tasks(task_id, project_id, sprint_id, title, estimate_hours, impact, status, created_at, updated_at) VALUES('task-003',?, 'sp-001','Refine role management',NULL,NULL,'todo',?,?)`, projectID, now, now); err != nil {
		return err
	}
	return nil
}

func (m *Manager) InitializeAdminUser() error {
	var existing string
	err := m.SystemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = 'admin'`).Scan(&existing)
	if err == nil {
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	hash := sha256.Sum256([]byte("admin"))
	passwordHash := hex.EncodeToString(hash[:])

	tx, err := m.SystemDB.Begin()
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
