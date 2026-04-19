package usecase

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"carry_sprint/p2/application/presenter"
	"carry_sprint/p2/application/validator"
	"carry_sprint/p2/domain/model"
	"carry_sprint/p2/shared/apperror"
)

// ListProjects and GetProjectSummary are implemented in service.go.

type createProjectPayload struct {
	ProjectID      string         `json:"project_id"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	InitialSprint  initialSprint  `json:"initial_sprint"`
	InitialAdminID string         `json:"initial_admin_user_id"`
}

type initialSprint struct {
	SprintID  string `json:"sprint_id"`
	Name      string `json:"name"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
}

func (s *Service) createProject(req model.ZMQRequest) model.ZMQResponse {
	var p createProjectPayload
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}

	// Q1 - Check project_id uniqueness
	var existing string
	err := s.DB.SystemDB.QueryRow(`SELECT project_id FROM projects WHERE project_id = ?`, p.ProjectID).Scan(&existing)
	if err == nil {
		return presenter.Error(req.RequestID, apperror.DuplicateProjectID, "project_id already exists")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}

	// Q2 - Check initial_admin_user_id exists
	var adminUID string
	err = s.DB.SystemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, p.InitialAdminID).Scan(&adminUID)
	if errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.UserNotFound, "initial_admin_user_id not found")
	}
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}

	// Validate sprint date range
	if !validator.ValidateSprintDateRange(p.InitialSprint.StartDate, p.InitialSprint.EndDate) {
		return presenter.Error(req.RequestID, apperror.InvalidSprintDateRange, "initial_sprint.start_date must not be after end_date")
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// Q3 - Insert project row (system.sqlite)
	tx, err := s.DB.SystemDB.Begin()
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	_, err = tx.Exec(`INSERT INTO projects (project_id, name, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		p.ProjectID, p.Name, p.Description, now, now)
	if err != nil {
		_ = tx.Rollback()
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	if err := tx.Commit(); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}

	// Open project_{id}.sqlite (creates file + schema)
	pdb, err := s.DB.ProjectDB(p.ProjectID)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}

	// Q4 + Q5 in a single transaction on project DB
	ptx, err := pdb.Begin()
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	_, err = ptx.Exec(
		`INSERT INTO sprints (sprint_id, project_id, name, start_date, end_date, available_hours, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?)`,
		p.InitialSprint.SprintID, p.ProjectID, p.InitialSprint.Name, p.InitialSprint.StartDate, p.InitialSprint.EndDate, now, now)
	if err != nil {
		_ = ptx.Rollback()
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	_, err = ptx.Exec(
		`INSERT INTO project_roles (project_id, user_id, role) VALUES (?, ?, 'administrator')`,
		p.ProjectID, p.InitialAdminID)
	if err != nil {
		_ = ptx.Rollback()
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	if err := ptx.Commit(); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}

	return presenter.OK(req.RequestID, map[string]any{
		"project_id":           p.ProjectID,
		"name":                 p.Name,
		"description":          p.Description,
		"initial_admin_user_id": p.InitialAdminID,
		"initial_sprint": map[string]any{
			"sprint_id":  p.InitialSprint.SprintID,
			"name":       p.InitialSprint.Name,
			"start_date": p.InitialSprint.StartDate,
			"end_date":   p.InitialSprint.EndDate,
		},
		"created_at": now,
	})
}
