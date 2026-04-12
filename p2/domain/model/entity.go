package model

type Project struct {
	ProjectID   string `json:"project_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type Sprint struct {
	SprintID        string  `json:"sprint_id"`
	ProjectID       string  `json:"project_id"`
	Name            string  `json:"name"`
	StartDate       string  `json:"start_date"`
	EndDate         string  `json:"end_date"`
	AvailableHours  float64 `json:"available_hours"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
}

type Task struct {
	TaskID        string   `json:"task_id"`
	ProjectID     string   `json:"project_id"`
	SprintID      *string  `json:"sprint_id"`
	Title         string   `json:"title"`
	EstimateHours *float64 `json:"estimate_hours"`
	Impact        *string  `json:"impact"`
	Status        string   `json:"status"`
	CreatedAt     string   `json:"created_at"`
	UpdatedAt     string   `json:"updated_at"`
}
