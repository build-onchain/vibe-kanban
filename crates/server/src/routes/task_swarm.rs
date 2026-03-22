use std::collections::HashSet;

use db::models::task::Task;
use executors::profile::ExecutorProfileId;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct SwarmWorkerInput {
    pub role: String,
    pub executor_profile_id: ExecutorProfileId,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct SwarmAssignmentPlan {
    pub title: String,
    pub description: String,
    pub assigned_role: String,
    pub executor_profile_id: ExecutorProfileId,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct SwarmPlan {
    pub summary: String,
    pub subtasks: Vec<SwarmAssignmentPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct SwarmTaskExecution {
    pub task: Task,
    pub workspace_id: String,
    pub assigned_role: String,
    pub executor_profile_id: ExecutorProfileId,
    pub rationale: String,
}

fn parse_bullets(description: &str) -> Vec<String> {
    description
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let trimmed = trimmed
                .strip_prefix("- ")
                .or_else(|| trimmed.strip_prefix("* "))
                .or_else(|| trimmed.strip_prefix("+ "))
                .or_else(|| {
                    let mut chars = trimmed.chars();
                    let first = chars.next()?;
                    let second = chars.next()?;
                    if first.is_ascii_digit() && (second == '.' || second == ')') {
                        Some(chars.as_str().trim_start())
                    } else {
                        None
                    }
                })?;
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .take(8)
        .collect()
}

fn extract_keywords(raw: &str) -> HashSet<String> {
    raw.split(|c: char| !c.is_ascii_alphanumeric())
        .map(|part| part.trim().to_ascii_lowercase())
        .filter(|part| part.len() >= 3)
        .collect()
}

fn phase_subtasks(task: &Task) -> Vec<(String, String)> {
    let title = task.title.trim();
    let original_description = task.description.as_deref().unwrap_or("").trim();
    let task_text = format!("{title}\n{original_description}").to_ascii_lowercase();

    let mentions_frontend = [
        "frontend",
        "react",
        "ui",
        "ux",
        "css",
        "tailwind",
        "component",
    ]
    .iter()
    .any(|keyword| task_text.contains(keyword));
    let mentions_backend = [
        "backend",
        "api",
        "server",
        "database",
        "db",
        "migration",
        "sql",
        "rust",
    ]
    .iter()
    .any(|keyword| task_text.contains(keyword));
    let mentions_docs = ["docs", "documentation", "readme"]
        .iter()
        .any(|keyword| task_text.contains(keyword));

    let mut subtasks = Vec::new();

    if mentions_frontend {
        subtasks.push((
            format!("Implement frontend changes for {title}"),
            "Handle the UI-facing parts of the epic while keeping the scope strictly limited to the interfaces needed for this task.".to_string(),
        ));
    }

    if mentions_backend || subtasks.is_empty() {
        subtasks.push((
            format!("Implement backend changes for {title}"),
            "Handle the service, API, and data-layer work required for the epic. Keep implementation atomic and avoid adjacent cleanup.".to_string(),
        ));
    }

    subtasks.push((
        format!("Verify and polish {title}"),
        if mentions_docs {
            "Validate the integrated result, close testing gaps, and update any documentation that the epic changes make inaccurate.".to_string()
        } else {
            "Validate the integrated result, add or update tests where needed, and tighten any rough edges discovered during verification.".to_string()
        },
    ));

    subtasks
}

fn score_worker(subtask_text: &str, worker: &SwarmWorkerInput) -> usize {
    let worker_keywords = extract_keywords(&worker.role);
    if worker_keywords.is_empty() {
        return 0;
    }

    let subtask_keywords = extract_keywords(subtask_text);
    subtask_keywords
        .intersection(&worker_keywords)
        .count()
        .saturating_mul(3)
        + usize::from(
            worker.role.to_ascii_lowercase().contains("test")
                && subtask_text.to_ascii_lowercase().contains("test"),
        )
        + usize::from(
            worker.role.to_ascii_lowercase().contains("front")
                && subtask_text.to_ascii_lowercase().contains("front"),
        )
        + usize::from(
            worker.role.to_ascii_lowercase().contains("back")
                && subtask_text.to_ascii_lowercase().contains("back"),
        )
}

fn assign_worker<'a>(
    subtask_text: &str,
    workers: &'a [SwarmWorkerInput],
    index: usize,
) -> &'a SwarmWorkerInput {
    workers
        .iter()
        .enumerate()
        .max_by_key(|(worker_index, worker)| {
            let score = score_worker(subtask_text, worker);
            let fallback_priority = workers.len().saturating_sub(index.abs_diff(*worker_index));
            (score, fallback_priority)
        })
        .map(|(_, worker)| worker)
        .unwrap_or(&workers[index % workers.len()])
}

pub fn build_swarm_plan(task: &Task, workers: &[SwarmWorkerInput]) -> SwarmPlan {
    let bullet_subtasks = task
        .description
        .as_deref()
        .map(parse_bullets)
        .unwrap_or_default();

    let subtask_templates: Vec<(String, String)> = if bullet_subtasks.len() >= 2 {
        bullet_subtasks
            .into_iter()
            .map(|item| {
                (
                    item.clone(),
                    format!(
                        "Execute this slice of the parent task atomically. Parent task: {}.",
                        task.title
                    ),
                )
            })
            .collect()
    } else {
        phase_subtasks(task)
    };

    let subtasks = subtask_templates
        .into_iter()
        .enumerate()
        .map(|(index, (title, description))| {
            let assignment_text = format!("{title}\n{description}");
            let worker = assign_worker(&assignment_text, workers, index);
            let rationale = format!(
                "Assigned to the worker focused on '{}', based on the overlap between the subtask scope and the worker role.",
                worker.role.trim()
            );

            SwarmAssignmentPlan {
                title,
                description,
                assigned_role: worker.role.trim().to_string(),
                executor_profile_id: worker.executor_profile_id.clone(),
                rationale,
            }
        })
        .collect::<Vec<_>>();

    SwarmPlan {
        summary: format!(
            "Planner decomposed '{}' into {} atomic task(s) for coordinated execution.",
            task.title,
            subtasks.len()
        ),
        subtasks,
    }
}

pub fn planner_prompt(task: &Task, workers: &[SwarmWorkerInput], plan: &SwarmPlan) -> String {
    let workers_block = workers
        .iter()
        .enumerate()
        .map(|(index, worker)| {
            format!(
                "{}. {} [{}]",
                index + 1,
                worker.role.trim(),
                worker.executor_profile_id
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let subtasks_block = plan
        .subtasks
        .iter()
        .enumerate()
        .map(|(index, subtask)| {
            format!(
                "{}. {} -> {} [{}]\n   {}\n   {}",
                index + 1,
                subtask.title,
                subtask.assigned_role,
                subtask.executor_profile_id,
                subtask.description,
                subtask.rationale
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "You are the swarm planner and coordinator for this epic task.\n\nEpic task:\n{}\n\nOriginal description:\n{}\n\nAvailable workers:\n{}\n\nPlanned subtasks:\n{}\n\nYour job is coordination only:\n- Do not implement the epic directly.\n- Keep the swarm aligned on the plan above.\n- Push work back into atomic child tasks instead of expanding scope here.\n- If a child task drifts, redirect it and keep boundaries sharp.",
        task.title,
        task.description.as_deref().unwrap_or("(none)"),
        workers_block,
        subtasks_block
    )
}

pub fn child_task_description(
    parent_task: &Task,
    plan: &SwarmAssignmentPlan,
) -> String {
    format!(
        "{}\n\nSwarm assignment\n- Parent task: {}\n- Assigned role: {}\n- Executor profile: {}\n- Planner rationale: {}\n\nExecution rules\n- Keep the contribution atomic and tightly scoped to this child task.\n- Coordinate through the parent/planner task when you discover cross-cutting work.\n- Prefer the smallest complete change that moves the parent task forward.",
        plan.description,
        parent_task.title,
        plan.assigned_role,
        plan.executor_profile_id,
        plan.rationale
    )
}

pub fn worker_prompt(parent_task: &Task, plan: &SwarmAssignmentPlan) -> String {
    format!(
        "You are one worker in a coordinated agent swarm.\n\nParent task:\n{}\n\nParent description:\n{}\n\nYour atomic assignment:\n{}\n\nTask details:\n{}\n\nAssigned role:\n{}\nReasoning:\n{}\n\nConstraints:\n- Limit your work to this assignment.\n- Keep changes reviewable and atomic.\n- If another agent should own a discovered task, note it explicitly instead of taking it on yourself.",
        parent_task.title,
        parent_task.description.as_deref().unwrap_or("(none)"),
        plan.title,
        plan.description,
        plan.assigned_role,
        plan.rationale
    )
}
