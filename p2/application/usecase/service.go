package usecase

import (
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"carry_sprint/p2/application/presenter"
	"carry_sprint/p2/application/validator"
	"carry_sprint/p2/domain/model"
	"carry_sprint/p2/infrastructure/sqlite"
	"carry_sprint/p2/shared/apperror"
)

type Service struct {
	DB *sqlite.Manager
}

func (s *Service) Execute(req model.ZMQRequest) model.ZMQResponse {
	switch req.Command {
	case "list_projects":
		return s.listProjects(req)
	case "get_project_summary":
		return s.getProjectSummary(req)
	case "create_project":
		return s.createProject(req)
	case "get_sprint_workspace":
		return s.getSprintWorkspace(req)
	case "update_task":
		return s.updateTask(req)
	case "list_resources":
		return s.listResources(req)
	case "save_resources":
		return s.saveResources(req)
	case "get_calendar":
		return s.getCalendar(req)
	case "save_calendar":
		return s.saveCalendar(req)
	case "apply_carryover":
		return s.applyCarryover(req)
	case "list_users":
		return s.listUsers(req)
	case "register_user":
		return s.registerUser(req)
	case "update_user":
		return s.updateUser(req)
	case "delete_user":
		return s.deleteUser(req)
	case "get_project_roles":
		return s.getProjectRoles(req)
	case "save_project_roles":
		return s.saveProjectRoles(req)
	case "resolve_default_locale":
		return s.resolveDefaultLocale(req)
	case "get_user_locale_setting":
		return s.getUserLocaleSetting(req)
	case "save_user_locale_setting":
		return s.saveUserLocaleSetting(req)
	case "get_top_menu":
		return s.getTopMenu(req)
	case "get_user_menu_visibility":
		return s.getUserMenuVisibility(req)
	case "save_user_menu_visibility":
		return s.saveUserMenuVisibility(req)
	default:
		return presenter.Error(req.RequestID, apperror.UnknownCommand, "unknown command")
	}
}

func (s *Service) listProjects(req model.ZMQRequest) model.ZMQResponse {
	rows, err := s.DB.SystemDB.Query(`SELECT project_id, name, description, updated_at FROM projects ORDER BY name ASC`)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	defer rows.Close()
	projects := []map[string]any{}
	for rows.Next() {
		var id, name, desc, updated string
		if err := rows.Scan(&id, &name, &desc, &updated); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		projects = append(projects, map[string]any{"project_id": id, "name": name, "description": desc, "updated_at": updated})
	}
	return presenter.OK(req.RequestID, map[string]any{"projects": projects})
}

func (s *Service) getProjectSummary(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	var id, name, desc, updated string
	err := s.DB.SystemDB.QueryRow(`SELECT project_id, name, description, updated_at FROM projects WHERE project_id = ?`, pid).Scan(&id, &name, &desc, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.ProjectNotFound, "project not found")
	}
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	var sc, tc int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sprints`).Scan(&sc); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM tasks`).Scan(&tc); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	return presenter.OK(req.RequestID, map[string]any{"project_id": id, "name": name, "description": desc, "sprint_count": sc, "task_count": tc, "updated_at": updated})
}

func (s *Service) getSprintWorkspace(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	sid := req.PathParams["sprint_id"]
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	var sprintName string
	var available float64
	err = db.QueryRow(`SELECT name, available_hours FROM sprints WHERE sprint_id = ? AND project_id = ?`, sid, pid).Scan(&sprintName, &available)
	if errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.SprintNotFound, "sprint not found")
	}
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	rows, err := db.Query(`SELECT task_id, title, estimate_hours, impact, status FROM tasks WHERE sprint_id = ? ORDER BY task_id ASC`, sid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
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
	inTotal := 0.0
	outTotal := 0.0
	cum := 0.0
	for rows.Next() {
		var t item
		var est sql.NullFloat64
		var impact sql.NullString
		if err := rows.Scan(&t.TaskID, &t.Title, &est, &impact, &t.Status); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		if est.Valid {
			v := est.Float64
			t.EstimateHour = &v
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
		if cum+est.Float64 <= available {
			budgetIn = append(budgetIn, t)
			cum += est.Float64
			inTotal += est.Float64
			continue
		}
		budgetOut = append(budgetOut, t)
		outTotal += est.Float64
	}
	return presenter.OK(req.RequestID, map[string]any{"sprint_id": sid, "sprint_name": sprintName, "available_hours": available, "budget_in": budgetIn, "budget_out": budgetOut, "totals": map[string]any{"budget_in_hours": inTotal, "budget_out_hours": outTotal}})
}

func (s *Service) updateTask(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	tid := req.PathParams["task_id"]
	var p struct {
		EstimateHours *float64 `json:"estimate_hours"`
		Impact        *string  `json:"impact"`
		Status        *string  `json:"status"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}
	if p.EstimateHours == nil && p.Impact == nil && p.Status == nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "at least one field is required")
	}
	if p.EstimateHours != nil && *p.EstimateHours < 0 {
		return presenter.Error(req.RequestID, apperror.InvalidEstimate, "estimate_hours must be >= 0")
	}
	if p.Impact != nil && !validator.ValidateImpact(*p.Impact) {
		return presenter.Error(req.RequestID, apperror.InvalidImpact, "impact must be high, medium, or low")
	}
	if p.Status != nil && !validator.ValidateStatus(*p.Status) {
		return presenter.Error(req.RequestID, apperror.InvalidPathParam, "status must be todo, in_progress, or done")
	}
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	var exists string
	err = db.QueryRow(`SELECT task_id FROM tasks WHERE task_id = ? AND project_id = ?`, tid, pid).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.SprintNotFound, "task not found")
	}
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
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
	now := time.Now().UTC().Format(time.RFC3339)
	sets = append(sets, "updated_at = ?")
	args = append(args, now, tid)
	q := "UPDATE tasks SET " + strings.Join(sets, ", ") + " WHERE task_id = ?"
	if _, err := db.Exec(q, args...); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	var est sql.NullFloat64
	var impact sql.NullString
	var status, updated string
	if err := db.QueryRow(`SELECT estimate_hours, impact, status, updated_at FROM tasks WHERE task_id = ?`, tid).Scan(&est, &impact, &status, &updated); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	resp := map[string]any{"task_id": tid, "estimate_hours": nil, "impact": nil, "status": status, "updated_at": updated}
	if est.Valid {
		resp["estimate_hours"] = est.Float64
	}
	if impact.Valid {
		resp["impact"] = impact.String
	}
	return presenter.OK(req.RequestID, resp)
}

func (s *Service) listResources(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	rows, err := db.Query(`SELECT resource_id, name, capacity_hours_per_day FROM resources ORDER BY resource_id ASC`)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	defer rows.Close()
	resources := []map[string]any{}
	for rows.Next() {
		var id, name string
		var cap float64
		if err := rows.Scan(&id, &name, &cap); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		resources = append(resources, map[string]any{"resource_id": id, "name": name, "capacity_hours_per_day": cap})
	}
	return presenter.OK(req.RequestID, map[string]any{"resources": resources})
}

func (s *Service) saveResources(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	var p struct {
		Resources []struct {
			ResourceID          string  `json:"resource_id"`
			Name                string  `json:"name"`
			CapacityHoursPerDay float64 `json:"capacity_hours_per_day"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}
	ids := make([]string, 0, len(p.Resources))
	for _, r := range p.Resources {
		ids = append(ids, r.ResourceID)
		if r.CapacityHoursPerDay <= 0 {
			return presenter.Error(req.RequestID, apperror.InvalidResourceCapacity, "capacity_hours_per_day must be > 0")
		}
	}
	if validator.HasDuplicateResourceID(ids) {
		return presenter.Error(req.RequestID, apperror.DuplicateResourceID, "duplicate resource_id")
	}
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	tx, err := db.Begin()
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	if _, err := tx.Exec(`DELETE FROM resources`); err != nil {
		_ = tx.Rollback()
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	for _, r := range p.Resources {
		if _, err := tx.Exec(`INSERT INTO resources(resource_id, name, capacity_hours_per_day) VALUES(?, ?, ?)`, r.ResourceID, r.Name, r.CapacityHoursPerDay); err != nil {
			_ = tx.Rollback()
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
	}
	if err := tx.Commit(); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	return presenter.OK(req.RequestID, map[string]any{"resources": p.Resources})
}

func (s *Service) getCalendar(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	month := req.QueryParams["month"]
	if month == "" {
		month = time.Now().Format("2006-01")
	}
	start, end, err := monthRange(month)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidPathParam, "month must be YYYY-MM")
	}
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	rows, err := db.Query(`SELECT date, is_working FROM working_day_calendar WHERE date >= ? AND date <= ? ORDER BY date ASC`, start, end)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	defer rows.Close()
	days := []map[string]any{}
	for rows.Next() {
		var date string
		var work int
		if err := rows.Scan(&date, &work); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		days = append(days, map[string]any{"date": date, "is_working": work == 1})
	}
	return presenter.OK(req.RequestID, map[string]any{"month": month, "days": days})
}

func (s *Service) saveCalendar(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	var p struct {
		Days []struct {
			Date      string `json:"date"`
			IsWorking bool   `json:"is_working"`
		} `json:"days"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}
	dates := make([]string, 0, len(p.Days))
	for _, d := range p.Days {
		dates = append(dates, d.Date)
		if !validator.IsISODate(d.Date) {
			return presenter.Error(req.RequestID, apperror.InvalidPathParam, "date must be YYYY-MM-DD")
		}
	}
	if validator.HasDuplicateDate(dates) {
		return presenter.Error(req.RequestID, apperror.DuplicateCalendarDate, "duplicate date")
	}
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	tx, err := db.Begin()
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	for _, d := range p.Days {
		w := 0
		if d.IsWorking {
			w = 1
		}
		if _, err := tx.Exec(`INSERT INTO working_day_calendar(date, is_working) VALUES(?, ?) ON CONFLICT(date) DO UPDATE SET is_working = excluded.is_working`, d.Date, w); err != nil {
			_ = tx.Rollback()
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
	}
	if err := tx.Commit(); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	return presenter.OK(req.RequestID, map[string]any{"days": p.Days})
}

func (s *Service) applyCarryover(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	sid := req.PathParams["sprint_id"]
	var p struct {
		Decisions []struct {
			TaskID         string  `json:"task_id"`
			Action         string  `json:"action"`
			TargetSprintID *string `json:"target_sprint_id"`
		} `json:"decisions"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	for _, d := range p.Decisions {
		if !validator.ValidateCarryoverAction(d.Action) {
			return presenter.Error(req.RequestID, apperror.InvalidPathParam, "action must be carryover or keep")
		}
		if d.Action == "carryover" && d.TargetSprintID != nil {
			var target string
			err := db.QueryRow(`SELECT sprint_id FROM sprints WHERE sprint_id = ? AND project_id = ?`, *d.TargetSprintID, pid).Scan(&target)
			if errors.Is(err, sql.ErrNoRows) {
				return presenter.Error(req.RequestID, apperror.TargetSprintNotFound, "target sprint not found")
			}
			if err != nil {
				return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
			}
		}
	}
	tx, err := db.Begin()
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	now := time.Now().UTC().Format(time.RFC3339)
	applied := []map[string]any{}
	for _, d := range p.Decisions {
		if d.Action == "keep" {
			var current sql.NullString
			if err := tx.QueryRow(`SELECT sprint_id FROM tasks WHERE task_id = ?`, d.TaskID).Scan(&current); err != nil {
				_ = tx.Rollback()
				return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
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
		if _, err := tx.Exec(`UPDATE tasks SET sprint_id = ?, updated_at = ? WHERE task_id = ? AND sprint_id = ?`, target, now, d.TaskID, sid); err != nil {
			_ = tx.Rollback()
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		applied = append(applied, map[string]any{"task_id": d.TaskID, "action": d.Action, "sprint_id": target})
	}
	if err := tx.Commit(); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	return presenter.OK(req.RequestID, map[string]any{"applied": applied})
}

func (s *Service) listUsers(req model.ZMQRequest) model.ZMQResponse {
	rows, err := s.DB.SystemDB.Query(`SELECT user_id, name, email FROM users ORDER BY user_id ASC`)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	defer rows.Close()
	users := []map[string]any{}
	for rows.Next() {
		var id, name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		users = append(users, map[string]any{"user_id": id, "name": name, "email": email})
	}
	return presenter.OK(req.RequestID, map[string]any{"users": users})
}

func (s *Service) registerUser(req model.ZMQRequest) model.ZMQResponse {
	var p struct {
		UserID string `json:"user_id"`
		Name   string `json:"name"`
		Email  string `json:"email"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}
	var existing string
	err := s.DB.SystemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, p.UserID).Scan(&existing)
	if err == nil {
		return presenter.Error(req.RequestID, apperror.DuplicateUserID, "user_id already exists")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := s.DB.SystemDB.Exec(`INSERT INTO users(user_id,name,email,created_at,updated_at) VALUES(?,?,?,?,?)`, p.UserID, p.Name, p.Email, now, now); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	return presenter.OK(req.RequestID, map[string]any{"user_id": p.UserID, "name": p.Name, "email": p.Email, "created_at": now})
}

func (s *Service) updateUser(req model.ZMQRequest) model.ZMQResponse {
	uid := req.PathParams["user_id"]
	var p struct {
		Name  *string `json:"name"`
		Email *string `json:"email"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}
	if !validator.HasAnyUserUpdate(p.Name, p.Email) {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "at least one field is required")
	}
	var existing string
	err := s.DB.SystemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, uid).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.UserNotFound, "user not found")
	}
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
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
	args = append(args, now, uid)
	q := "UPDATE users SET " + strings.Join(sets, ", ") + " WHERE user_id = ?"
	if _, err := s.DB.SystemDB.Exec(q, args...); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	var name, email, updated string
	if err := s.DB.SystemDB.QueryRow(`SELECT name, email, updated_at FROM users WHERE user_id = ?`, uid).Scan(&name, &email, &updated); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	return presenter.OK(req.RequestID, map[string]any{"user_id": uid, "name": name, "email": email, "updated_at": updated})
}

func (s *Service) deleteUser(req model.ZMQRequest) model.ZMQResponse {
	uid := req.PathParams["user_id"]
	var existing string
	err := s.DB.SystemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, uid).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.UserNotFound, "user not found")
	}
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	if _, err := s.DB.SystemDB.Exec(`DELETE FROM users WHERE user_id = ?`, uid); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	rows, err := s.DB.SystemDB.Query(`SELECT project_id FROM projects`)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var pid string
		if err := rows.Scan(&pid); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		db, err := s.DB.ProjectDB(pid)
		if err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		if _, err := db.Exec(`DELETE FROM project_roles WHERE user_id = ?`, uid); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
	}
	return presenter.OK(req.RequestID, map[string]any{})
}

func (s *Service) getProjectRoles(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	var p string
	err := s.DB.SystemDB.QueryRow(`SELECT project_id FROM projects WHERE project_id = ?`, pid).Scan(&p)
	if errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.ProjectNotFound, "project not found")
	}
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	rows, err := db.Query(`SELECT user_id, role FROM project_roles WHERE project_id = ? ORDER BY user_id ASC`, pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	defer rows.Close()
	roles := []map[string]any{}
	for rows.Next() {
		var uid, role string
		if err := rows.Scan(&uid, &role); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		var exists string
		if err := s.DB.SystemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, uid).Scan(&exists); err != nil {
			continue
		}
		roles = append(roles, map[string]any{"user_id": uid, "role": role})
	}
	return presenter.OK(req.RequestID, map[string]any{"roles": roles})
}

func (s *Service) saveProjectRoles(req model.ZMQRequest) model.ZMQResponse {
	pid := req.PathParams["project_id"]
	var p struct {
		Roles []struct {
			UserID string `json:"user_id"`
			Role   string `json:"role"`
		} `json:"roles"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}
	var pidExists string
	err := s.DB.SystemDB.QueryRow(`SELECT project_id FROM projects WHERE project_id = ?`, pid).Scan(&pidExists)
	if errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.ProjectNotFound, "project not found")
	}
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	for _, r := range p.Roles {
		if !validator.ValidateRole(r.Role) {
			return presenter.Error(req.RequestID, apperror.InvalidRole, "role must be administrator, assignee, or viewer")
		}
		var uid string
		err := s.DB.SystemDB.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, r.UserID).Scan(&uid)
		if errors.Is(err, sql.ErrNoRows) {
			return presenter.Error(req.RequestID, apperror.UserNotFound, "user not found")
		}
		if err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
	}
	db, err := s.DB.ProjectDB(pid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	tx, err := db.Begin()
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	if _, err := tx.Exec(`DELETE FROM project_roles WHERE project_id = ?`, pid); err != nil {
		_ = tx.Rollback()
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	for _, r := range p.Roles {
		if _, err := tx.Exec(`INSERT INTO project_roles(project_id, user_id, role) VALUES(?, ?, ?)`, pid, r.UserID, r.Role); err != nil {
			_ = tx.Rollback()
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
	}
	if err := tx.Commit(); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	return presenter.OK(req.RequestID, map[string]any{"roles": p.Roles})
}

func (s *Service) resolveDefaultLocale(req model.ZMQRequest) model.ZMQResponse {
	uid := strings.TrimSpace(req.QueryParams["user_id"])
	if uid != "" {
		if code, msg, ok := ensureSystemUserExists(s.DB.SystemDB, uid); !ok {
			return presenter.Error(req.RequestID, code, msg)
		}
		var locale string
		err := s.DB.SystemDB.QueryRow(`SELECT locale FROM user_locale_settings WHERE user_id = ?`, uid).Scan(&locale)
		if err == nil {
			return presenter.OK(req.RequestID, map[string]any{"locale": locale, "source": "user_setting"})
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
	}

	candidates := parseAcceptLanguage(req.QueryParams["accept_language"])
	if locale, source, err := resolveLocaleFromCandidates(s.DB.SystemDB, candidates); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	} else if locale != "" {
		return presenter.OK(req.RequestID, map[string]any{"locale": locale, "source": source})
	}
	return presenter.OK(req.RequestID, map[string]any{"locale": "en", "source": "fallback"})
}

func (s *Service) getUserLocaleSetting(req model.ZMQRequest) model.ZMQResponse {
	uid := strings.TrimSpace(req.PathParams["user_id"])
	if uid == "" {
		return presenter.Error(req.RequestID, apperror.InvalidPathParam, "user_id is required")
	}
	if code, msg, ok := ensureSystemUserExists(s.DB.SystemDB, uid); !ok {
		return presenter.Error(req.RequestID, code, msg)
	}

	var locale string
	err := s.DB.SystemDB.QueryRow(`SELECT locale FROM user_locale_settings WHERE user_id = ?`, uid).Scan(&locale)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	if errors.Is(err, sql.ErrNoRows) {
		locale = ""
	}
	return presenter.OK(req.RequestID, map[string]any{"user_id": uid, "locale": locale, "locale_options": validator.SupportedLocales()})
}

func (s *Service) saveUserLocaleSetting(req model.ZMQRequest) model.ZMQResponse {
	uid := strings.TrimSpace(req.PathParams["user_id"])
	if uid == "" {
		return presenter.Error(req.RequestID, apperror.InvalidPathParam, "user_id is required")
	}
	if code, msg, ok := ensureSystemUserExists(s.DB.SystemDB, uid); !ok {
		return presenter.Error(req.RequestID, code, msg)
	}

	var p struct {
		Locale string `json:"locale"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}
	p.Locale = strings.TrimSpace(strings.ToLower(p.Locale))
	if p.Locale != "" && !validator.ValidateLocale(p.Locale) {
		return presenter.Error(req.RequestID, apperror.InvalidLocale, "locale is not allowed")
	}
	if p.Locale == "" {
		if _, err := s.DB.SystemDB.Exec(`DELETE FROM user_locale_settings WHERE user_id = ?`, uid); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		return presenter.OK(req.RequestID, map[string]any{"user_id": uid, "locale": "", "locale_options": validator.SupportedLocales()})
	}
	if _, err := s.DB.SystemDB.Exec(`INSERT INTO user_locale_settings(user_id, locale) VALUES(?, ?) ON CONFLICT(user_id) DO UPDATE SET locale = excluded.locale`, uid, p.Locale); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	return presenter.OK(req.RequestID, map[string]any{"user_id": uid, "locale": p.Locale, "locale_options": validator.SupportedLocales()})
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

func (s *Service) getTopMenu(req model.ZMQRequest) model.ZMQResponse {
	uid := strings.TrimSpace(req.QueryParams["user_id"])
	if uid == "" {
		return presenter.Error(req.RequestID, apperror.InvalidPathParam, "user_id is required")
	}
	if code, msg, ok := ensureSystemUserExists(s.DB.SystemDB, uid); !ok {
		return presenter.Error(req.RequestID, code, msg)
	}

	rows, err := s.DB.SystemDB.Query(`SELECT menu_key, is_enabled FROM user_menu_visibility WHERE user_id = ? ORDER BY menu_key ASC`, uid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	defer rows.Close()

	enabled := map[string]bool{}
	hasAny := false
	for rows.Next() {
		var key string
		var flag int
		if err := rows.Scan(&key, &flag); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		hasAny = true
		enabled[key] = flag == 1
	}

	buttons := make([]map[string]any, 0)
	if !hasAny {
		for _, key := range topMenuDefaultKeys {
			buttons = append(buttons, map[string]any{"menu_key": key, "label": menuLabels[key]})
		}
		return presenter.OK(req.RequestID, map[string]any{"user_id": uid, "menu_buttons": buttons})
	}

	for _, key := range menuVisibilityAllKeys {
		if enabled[key] {
			buttons = append(buttons, map[string]any{"menu_key": key, "label": menuLabels[key]})
		}
	}
	return presenter.OK(req.RequestID, map[string]any{"user_id": uid, "menu_buttons": buttons})
}

func (s *Service) getUserMenuVisibility(req model.ZMQRequest) model.ZMQResponse {
	uid := strings.TrimSpace(req.PathParams["user_id"])
	if uid == "" {
		return presenter.Error(req.RequestID, apperror.InvalidPathParam, "user_id is required")
	}
	if code, msg, ok := ensureSystemUserExists(s.DB.SystemDB, uid); !ok {
		return presenter.Error(req.RequestID, code, msg)
	}

	rows, err := s.DB.SystemDB.Query(`SELECT menu_key, is_enabled FROM user_menu_visibility WHERE user_id = ? ORDER BY menu_key ASC`, uid)
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	defer rows.Close()

	visibility := make([]map[string]any, 0)
	hasAny := false
	for rows.Next() {
		var key string
		var flag int
		if err := rows.Scan(&key, &flag); err != nil {
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
		hasAny = true
		visibility = append(visibility, map[string]any{"menu_key": key, "is_enabled": flag == 1})
	}

	if !hasAny {
		for _, key := range menuVisibilityAllKeys {
			visibility = append(visibility, map[string]any{"menu_key": key, "is_enabled": true})
		}
	}

	return presenter.OK(req.RequestID, map[string]any{"user_id": uid, "menu_visibility": visibility})
}

func (s *Service) saveUserMenuVisibility(req model.ZMQRequest) model.ZMQResponse {
	uid := strings.TrimSpace(req.PathParams["user_id"])
	if uid == "" {
		return presenter.Error(req.RequestID, apperror.InvalidPathParam, "user_id is required")
	}
	if code, msg, ok := ensureSystemUserExists(s.DB.SystemDB, uid); !ok {
		return presenter.Error(req.RequestID, code, msg)
	}

	var p struct {
		MenuVisibility []struct {
			MenuKey   string `json:"menu_key"`
			IsEnabled bool   `json:"is_enabled"`
		} `json:"menu_visibility"`
	}
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return presenter.Error(req.RequestID, apperror.InvalidJSON, "invalid json payload")
	}

	seen := map[string]struct{}{}
	for _, item := range p.MenuVisibility {
		if !validator.ValidateMenuKey(item.MenuKey) {
			return presenter.Error(req.RequestID, apperror.InvalidMenuKey, "menu_key is not allowed")
		}
		if _, ok := seen[item.MenuKey]; ok {
			return presenter.Error(req.RequestID, apperror.DuplicateMenuKey, "duplicate menu_key")
		}
		seen[item.MenuKey] = struct{}{}
	}

	tx, err := s.DB.SystemDB.Begin()
	if err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	if _, err := tx.Exec(`DELETE FROM user_menu_visibility WHERE user_id = ?`, uid); err != nil {
		_ = tx.Rollback()
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}
	for _, item := range p.MenuVisibility {
		flag := 0
		if item.IsEnabled {
			flag = 1
		}
		if _, err := tx.Exec(`INSERT INTO user_menu_visibility(user_id, menu_key, is_enabled) VALUES(?, ?, ?)`, uid, item.MenuKey, flag); err != nil {
			_ = tx.Rollback()
			return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
		}
	}
	if err := tx.Commit(); err != nil {
		return presenter.Error(req.RequestID, apperror.PersistenceError, err.Error())
	}

	visibility := make([]map[string]any, 0, len(p.MenuVisibility))
	for _, item := range p.MenuVisibility {
		visibility = append(visibility, map[string]any{"menu_key": item.MenuKey, "is_enabled": item.IsEnabled})
	}
	return presenter.OK(req.RequestID, map[string]any{"user_id": uid, "menu_visibility": visibility})
}

func ensureSystemUserExists(db *sql.DB, userID string) (string, string, bool) {
	var uid string
	err := db.QueryRow(`SELECT user_id FROM users WHERE user_id = ?`, userID).Scan(&uid)
	if errors.Is(err, sql.ErrNoRows) {
		return apperror.UserNotFound, "user not found", false
	}
	if err != nil {
		return apperror.PersistenceError, err.Error(), false
	}
	return "", "", true
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

func monthRange(month string) (string, string, error) {
	t, err := time.Parse("2006-01", month)
	if err != nil {
		return "", "", err
	}
	return t.Format("2006-01-02"), t.AddDate(0, 1, -1).Format("2006-01-02"), nil
}
