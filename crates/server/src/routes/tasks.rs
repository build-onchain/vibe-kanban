use std::path::PathBuf;

use anyhow;
use axum::{
    Extension, Json, Router,
    extract::{
        Query, State,
        ws::{WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    middleware::from_fn_with_state,
    response::{IntoResponse, Json as ResponseJson},
    routing::{delete, get, post, put},
};
use db::models::{
    image::TaskImage,
    repo::{Repo, RepoError},
    task::{CreateTask, Task, TaskStatus, TaskWithAttemptStatus, UpdateTask},
    workspace::{CreateWorkspace, Workspace},
    workspace_repo::{CreateWorkspaceRepo, WorkspaceRepo},
};
use deployment::Deployment;
use executors::profile::ExecutorProfileId;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use services::services::{
    container::ContainerService, image::ImageService, remote_client::RemoteClient,
    workspace_manager::WorkspaceManager,
};
use sqlx::Error as SqlxError;
use ts_rs::TS;
use utils::response::ApiResponse;
use uuid::Uuid;

use crate::{
    DeploymentImpl, error::ApiError, middleware::load_task_middleware,
    routes::{
        task_attempts::WorkspaceRepoInput,
        task_swarm::{
            SwarmPlan, SwarmTaskExecution, SwarmWorkerInput, build_swarm_plan,
            child_task_description, planner_prompt, worker_prompt,
        },
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskQuery {
    pub project_id: Uuid,
}

pub async fn get_tasks(
    State(deployment): State<DeploymentImpl>,
    Query(query): Query<TaskQuery>,
) -> Result<ResponseJson<ApiResponse<Vec<TaskWithAttemptStatus>>>, ApiError> {
    let tasks =
        Task::find_by_project_id_with_attempt_status(&deployment.db().pool, query.project_id)
            .await?;

    Ok(ResponseJson(ApiResponse::success(tasks)))
}

pub async fn stream_tasks_ws(
    ws: WebSocketUpgrade,
    State(deployment): State<DeploymentImpl>,
    Query(query): Query<TaskQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_tasks_ws(socket, deployment, query.project_id).await {
            tracing::warn!("tasks WS closed: {}", e);
        }
    })
}

async fn handle_tasks_ws(
    socket: WebSocket,
    deployment: DeploymentImpl,
    project_id: Uuid,
) -> anyhow::Result<()> {
    // Get the raw stream and convert LogMsg to WebSocket messages
    let mut stream = deployment
        .events()
        .stream_tasks_raw(project_id)
        .await?
        .map_ok(|msg| msg.to_ws_message_unchecked());

    // Split socket into sender and receiver
    let (mut sender, mut receiver) = socket.split();

    // Drain (and ignore) any client->server messages so pings/pongs work
    tokio::spawn(async move { while let Some(Ok(_)) = receiver.next().await {} });

    // Forward server messages
    while let Some(item) = stream.next().await {
        match item {
            Ok(msg) => {
                if sender.send(msg).await.is_err() {
                    break; // client disconnected
                }
            }
            Err(e) => {
                tracing::error!("stream error: {}", e);
                break;
            }
        }
    }
    Ok(())
}

pub async fn get_task(
    Extension(task): Extension<Task>,
    State(_deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<Task>>, ApiError> {
    Ok(ResponseJson(ApiResponse::success(task)))
}

pub async fn create_task(
    State(deployment): State<DeploymentImpl>,
    Json(payload): Json<CreateTask>,
) -> Result<ResponseJson<ApiResponse<Task>>, ApiError> {
    let id = Uuid::new_v4();

    tracing::debug!(
        "Creating task '{}' in project {}",
        payload.title,
        payload.project_id
    );

    let task = Task::create(&deployment.db().pool, &payload, id).await?;

    if let Some(image_ids) = &payload.image_ids {
        TaskImage::associate_many_dedup(&deployment.db().pool, task.id, image_ids).await?;
    }

    deployment
        .track_if_analytics_allowed(
            "task_created",
            serde_json::json!({
            "task_id": task.id.to_string(),
            "project_id": payload.project_id,
            "has_description": task.description.is_some(),
            "has_images": payload.image_ids.is_some(),
            }),
        )
        .await;

    Ok(ResponseJson(ApiResponse::success(task)))
}

#[derive(Debug, Serialize, Deserialize, TS)]
pub struct LinkedIssueInfo {
    pub remote_project_id: Uuid,
    pub issue_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize, TS)]
pub struct CreateAndStartTaskRequest {
    pub task: CreateTask,
    pub executor_profile_id: ExecutorProfileId,
    pub repos: Vec<WorkspaceRepoInput>,
    pub linked_issue: Option<LinkedIssueInfo>,
}

#[derive(Debug, Serialize, Deserialize, TS)]
pub struct CreateAndStartSwarmTaskRequest {
    pub task: CreateTask,
    pub planner_profile_id: ExecutorProfileId,
    pub repos: Vec<WorkspaceRepoInput>,
    pub workers: Vec<SwarmWorkerInput>,
    pub linked_issue: Option<LinkedIssueInfo>,
}

#[derive(Debug, Serialize, Deserialize, TS)]
pub struct CreateAndStartSwarmTaskResponse {
    pub epic_task: TaskWithAttemptStatus,
    pub planner_workspace: Workspace,
    pub plan: SwarmPlan,
    pub spawned_tasks: Vec<SwarmTaskExecution>,
}

struct StartedWorkspace {
    workspace: Workspace,
    is_running: bool,
}

struct ImportedImage {
    image_id: Uuid,
    attachment_id: Uuid,
    vibe_path: String,
}

/// Downloads attachments from a remote issue and stores them in the local cache.
async fn import_issue_attachments(
    client: &RemoteClient,
    image_service: &ImageService,
    issue_id: Uuid,
) -> anyhow::Result<Vec<ImportedImage>> {
    let response = client.list_issue_attachments(issue_id).await?;

    let mut imported = Vec::new();

    for entry in response.attachments {
        // Only import image types
        let is_image = entry
            .attachment
            .mime_type
            .as_ref()
            .is_some_and(|m| m.starts_with("image/"));
        if !is_image {
            continue;
        }

        let file_url = match &entry.file_url {
            Some(url) => url,
            None => {
                tracing::warn!(
                    "No file_url for attachment {}, skipping",
                    entry.attachment.id
                );
                continue;
            }
        };
        let bytes = match client.download_from_url(file_url).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    "Failed to download attachment {}: {}",
                    entry.attachment.id,
                    e
                );
                continue;
            }
        };

        let image = match image_service
            .store_image(&bytes, &entry.attachment.original_name)
            .await
        {
            Ok(img) => img,
            Err(e) => {
                tracing::warn!(
                    "Failed to store imported image '{}': {}",
                    entry.attachment.original_name,
                    e
                );
                continue;
            }
        };

        let vibe_path = format!("{}/{}", utils::path::VIBE_IMAGES_DIR, image.file_path);

        imported.push(ImportedImage {
            image_id: image.id,
            attachment_id: entry.attachment.id,
            vibe_path,
        });
    }

    Ok(imported)
}

async fn enrich_task_from_linked_issue(
    deployment: &DeploymentImpl,
    task: &mut CreateTask,
    linked_issue: Option<&LinkedIssueInfo>,
) {
    if let Some(linked_issue) = linked_issue
        && let Ok(client) = deployment.remote_client()
    {
        match import_issue_attachments(&client, deployment.image(), linked_issue.issue_id).await {
            Ok(imported) if !imported.is_empty() => {
                if let Some(desc) = &mut task.description {
                    for img in &imported {
                        let placeholder = format!("attachment://{}", img.attachment_id);
                        *desc = desc.replace(&placeholder, &img.vibe_path);
                    }
                }

                let imported_ids: Vec<Uuid> = imported.iter().map(|i| i.image_id).collect();
                match &mut task.image_ids {
                    Some(ids) => ids.extend(imported_ids),
                    None => task.image_ids = Some(imported_ids),
                }

                tracing::info!(
                    "Imported {} images from issue {}",
                    imported.len(),
                    linked_issue.issue_id
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    "Failed to import issue attachments for issue {}: {}",
                    linked_issue.issue_id,
                    e
                );
            }
        }
    }
}

async fn track_task_created(
    deployment: &DeploymentImpl,
    task: &Task,
    image_ids: Option<&Vec<Uuid>>,
) {
    deployment
        .track_if_analytics_allowed(
            "task_created",
            serde_json::json!({
                "task_id": task.id.to_string(),
                "project_id": task.project_id,
                "has_description": task.description.is_some(),
                "has_images": image_ids.is_some(),
            }),
        )
        .await;
}

async fn compute_agent_working_dir(
    pool: &sqlx::SqlitePool,
    repos: &[WorkspaceRepoInput],
) -> Result<Option<String>, ApiError> {
    if repos.len() != 1 {
        return Ok(None);
    }

    let repo = Repo::find_by_id(pool, repos[0].repo_id)
        .await?
        .ok_or(RepoError::NotFound)?;

    Ok(match repo.default_working_dir {
        Some(subdir) => {
            let path = PathBuf::from(&repo.name).join(&subdir);
            Some(path.to_string_lossy().to_string())
        }
        None => Some(repo.name),
    })
}

async fn create_workspace_for_task(
    deployment: &DeploymentImpl,
    task: &Task,
    repos: &[WorkspaceRepoInput],
) -> Result<Workspace, ApiError> {
    let pool = &deployment.db().pool;
    let attempt_id = Uuid::new_v4();
    let git_branch_name = deployment
        .container()
        .git_branch_from_workspace(&attempt_id, &task.title)
        .await;
    let agent_working_dir = compute_agent_working_dir(pool, repos).await?;

    let workspace = Workspace::create(
        pool,
        &CreateWorkspace {
            branch: git_branch_name,
            agent_working_dir,
        },
        attempt_id,
        task.id,
    )
    .await?;

    let workspace_repos: Vec<CreateWorkspaceRepo> = repos
        .iter()
        .map(|repo| CreateWorkspaceRepo {
            repo_id: repo.repo_id,
            target_branch: repo.target_branch.clone(),
        })
        .collect();
    WorkspaceRepo::create_many(pool, workspace.id, &workspace_repos).await?;

    Ok(workspace)
}

async fn start_task_workspace(
    deployment: &DeploymentImpl,
    task: &Task,
    repos: &[WorkspaceRepoInput],
    executor_profile_id: ExecutorProfileId,
    prompt_override: Option<String>,
) -> Result<StartedWorkspace, ApiError> {
    let workspace = create_workspace_for_task(deployment, task, repos).await?;

    let start_result = match prompt_override {
        Some(prompt) => {
            deployment
                .container()
                .start_workspace_with_prompt(&workspace, executor_profile_id.clone(), prompt)
                .await
        }
        None => {
            deployment
                .container()
                .start_workspace(&workspace, executor_profile_id.clone())
                .await
        }
    };

    let is_running = start_result
        .inspect_err(|err| tracing::error!("Failed to start task attempt: {}", err))
        .is_ok();

    deployment
        .track_if_analytics_allowed(
            "task_attempt_started",
            serde_json::json!({
                "task_id": task.id.to_string(),
                "executor": &executor_profile_id.executor,
                "variant": &executor_profile_id.variant,
                "workspace_id": workspace.id.to_string(),
                "repository_count": repos.len(),
            }),
        )
        .await;

    Ok(StartedWorkspace {
        workspace,
        is_running,
    })
}

pub async fn create_task_and_start(
    State(deployment): State<DeploymentImpl>,
    Json(mut payload): Json<CreateAndStartTaskRequest>,
) -> Result<ResponseJson<ApiResponse<TaskWithAttemptStatus>>, ApiError> {
    if payload.repos.is_empty() {
        return Err(ApiError::BadRequest(
            "At least one repository is required".to_string(),
        ));
    }

    let pool = &deployment.db().pool;
    enrich_task_from_linked_issue(&deployment, &mut payload.task, payload.linked_issue.as_ref())
        .await;

    let task_id = Uuid::new_v4();
    let task = Task::create(pool, &payload.task, task_id).await?;

    if let Some(image_ids) = &payload.task.image_ids {
        TaskImage::associate_many_dedup(pool, task.id, image_ids).await?;
    }

    track_task_created(&deployment, &task, payload.task.image_ids.as_ref()).await;

    let started_workspace = start_task_workspace(
        &deployment,
        &task,
        &payload.repos,
        payload.executor_profile_id.clone(),
        None,
    )
    .await?;

    let task = Task::find_by_id(pool, task.id)
        .await?
        .ok_or(ApiError::Database(SqlxError::RowNotFound))?;

    tracing::info!("Started attempt for task {}", task.id);
    Ok(ResponseJson(ApiResponse::success(TaskWithAttemptStatus {
        task,
        has_in_progress_attempt: started_workspace.is_running,
        last_attempt_failed: false,
        executor: payload.executor_profile_id.executor.to_string(),
    })))
}

pub async fn create_swarm_task_and_start(
    State(deployment): State<DeploymentImpl>,
    Json(mut payload): Json<CreateAndStartSwarmTaskRequest>,
) -> Result<ResponseJson<ApiResponse<CreateAndStartSwarmTaskResponse>>, ApiError> {
    if payload.repos.is_empty() {
        return Err(ApiError::BadRequest(
            "At least one repository is required".to_string(),
        ));
    }

    if payload.workers.is_empty() {
        return Err(ApiError::BadRequest(
            "At least one swarm worker is required".to_string(),
        ));
    }

    let pool = &deployment.db().pool;
    enrich_task_from_linked_issue(&deployment, &mut payload.task, payload.linked_issue.as_ref())
        .await;

    let epic_task = Task::create(pool, &payload.task, Uuid::new_v4()).await?;

    if let Some(image_ids) = &payload.task.image_ids {
        TaskImage::associate_many_dedup(pool, epic_task.id, image_ids).await?;
    }

    track_task_created(&deployment, &epic_task, payload.task.image_ids.as_ref()).await;

    let plan = build_swarm_plan(&epic_task, &payload.workers);
    let planner_started_workspace = start_task_workspace(
        &deployment,
        &epic_task,
        &payload.repos,
        payload.planner_profile_id.clone(),
        Some(planner_prompt(&epic_task, &payload.workers, &plan)),
    )
    .await?;

    let mut spawned_tasks = Vec::with_capacity(plan.subtasks.len());

    for planned_subtask in &plan.subtasks {
        let child_task = Task::create(
            pool,
            &CreateTask {
                project_id: epic_task.project_id,
                title: planned_subtask.title.clone(),
                description: Some(child_task_description(&epic_task, planned_subtask)),
                status: Some(TaskStatus::Todo),
                parent_workspace_id: Some(planner_started_workspace.workspace.id),
                image_ids: None,
            },
            Uuid::new_v4(),
        )
        .await?;

        track_task_created(&deployment, &child_task, None).await;

        let child_started_workspace = start_task_workspace(
            &deployment,
            &child_task,
            &payload.repos,
            planned_subtask.executor_profile_id.clone(),
            Some(worker_prompt(&epic_task, planned_subtask)),
        )
        .await?;

        spawned_tasks.push(SwarmTaskExecution {
            task: child_task,
            workspace_id: child_started_workspace.workspace.id.to_string(),
            assigned_role: planned_subtask.assigned_role.clone(),
            executor_profile_id: planned_subtask.executor_profile_id.clone(),
            rationale: planned_subtask.rationale.clone(),
        });
    }

    deployment
        .track_if_analytics_allowed(
            "task_swarm_started",
            serde_json::json!({
                "task_id": epic_task.id.to_string(),
                "planner_executor": &payload.planner_profile_id.executor,
                "planner_variant": &payload.planner_profile_id.variant,
                "workspace_id": planner_started_workspace.workspace.id.to_string(),
                "worker_count": payload.workers.len(),
                "planned_subtasks": plan.subtasks.len(),
            }),
        )
        .await;

    let epic_task = Task::find_by_id(pool, epic_task.id)
        .await?
        .ok_or(ApiError::Database(SqlxError::RowNotFound))?;

    Ok(ResponseJson(ApiResponse::success(
        CreateAndStartSwarmTaskResponse {
            epic_task: TaskWithAttemptStatus {
                task: epic_task,
                has_in_progress_attempt: planner_started_workspace.is_running,
                last_attempt_failed: false,
                executor: payload.planner_profile_id.executor.to_string(),
            },
            planner_workspace: planner_started_workspace.workspace,
            plan,
            spawned_tasks,
        },
    )))
}

pub async fn update_task(
    Extension(existing_task): Extension<Task>,
    State(deployment): State<DeploymentImpl>,

    Json(payload): Json<UpdateTask>,
) -> Result<ResponseJson<ApiResponse<Task>>, ApiError> {
    // Use existing values if not provided in update
    let title = payload.title.unwrap_or(existing_task.title);
    let description = match payload.description {
        Some(s) if s.trim().is_empty() => None, // Empty string = clear description
        Some(s) => Some(s),                     // Non-empty string = update description
        None => existing_task.description,      // Field omitted = keep existing
    };
    let status = payload.status.unwrap_or(existing_task.status);
    let parent_workspace_id = payload
        .parent_workspace_id
        .or(existing_task.parent_workspace_id);

    let task = Task::update(
        &deployment.db().pool,
        existing_task.id,
        existing_task.project_id,
        title,
        description,
        status,
        parent_workspace_id,
    )
    .await?;

    if let Some(image_ids) = &payload.image_ids {
        TaskImage::delete_by_task_id(&deployment.db().pool, task.id).await?;
        TaskImage::associate_many_dedup(&deployment.db().pool, task.id, image_ids).await?;
    }

    Ok(ResponseJson(ApiResponse::success(task)))
}

pub async fn delete_task(
    Extension(task): Extension<Task>,
    State(deployment): State<DeploymentImpl>,
) -> Result<(StatusCode, ResponseJson<ApiResponse<()>>), ApiError> {
    let pool = &deployment.db().pool;

    // Gather task attempts data needed for background cleanup
    let attempts = Workspace::fetch_all(pool, Some(task.id))
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch task attempts for task {}: {}", task.id, e);
            ApiError::Workspace(e)
        })?;

    // Stop any running execution processes before deletion
    for workspace in &attempts {
        deployment.container().try_stop(workspace, true).await;
    }

    let repositories = WorkspaceRepo::find_unique_repos_for_task(pool, task.id).await?;

    // Collect workspace directories that need cleanup
    let workspace_dirs: Vec<PathBuf> = attempts
        .iter()
        .filter_map(|attempt| attempt.container_ref.as_ref().map(PathBuf::from))
        .collect();

    // Use a transaction to ensure atomicity: either all operations succeed or all are rolled back
    let mut tx = pool.begin().await?;

    // Nullify parent_workspace_id for all child tasks before deletion
    // This breaks parent-child relationships to avoid foreign key constraint violations
    let mut total_children_affected = 0u64;
    for attempt in &attempts {
        let children_affected =
            Task::nullify_children_by_workspace_id(&mut *tx, attempt.id).await?;
        total_children_affected += children_affected;
    }

    // Delete task from database (FK CASCADE will handle task_attempts)
    let rows_affected = Task::delete(&mut *tx, task.id).await?;

    if rows_affected == 0 {
        return Err(ApiError::Database(SqlxError::RowNotFound));
    }

    // Commit the transaction - if this fails, all changes are rolled back
    tx.commit().await?;

    if total_children_affected > 0 {
        tracing::info!(
            "Nullified {} child task references before deleting task {}",
            total_children_affected,
            task.id
        );
    }

    deployment
        .track_if_analytics_allowed(
            "task_deleted",
            serde_json::json!({
                "task_id": task.id.to_string(),
                "project_id": task.project_id.to_string(),
                "attempt_count": attempts.len(),
            }),
        )
        .await;

    let task_id = task.id;
    let pool = pool.clone();
    tokio::spawn(async move {
        tracing::info!(
            "Starting background cleanup for task {} ({} workspaces, {} repos)",
            task_id,
            workspace_dirs.len(),
            repositories.len()
        );

        for workspace_dir in &workspace_dirs {
            if let Err(e) = WorkspaceManager::cleanup_workspace(workspace_dir, &repositories).await
            {
                tracing::error!(
                    "Background workspace cleanup failed for task {} at {}: {}",
                    task_id,
                    workspace_dir.display(),
                    e
                );
            }
        }

        match Repo::delete_orphaned(&pool).await {
            Ok(count) if count > 0 => {
                tracing::info!("Deleted {} orphaned repo records", count);
            }
            Err(e) => {
                tracing::error!("Failed to delete orphaned repos: {}", e);
            }
            _ => {}
        }

        tracing::info!("Background cleanup completed for task {}", task_id);
    });

    // Return 202 Accepted to indicate deletion was scheduled
    Ok((StatusCode::ACCEPTED, ResponseJson(ApiResponse::success(()))))
}

pub fn router(deployment: &DeploymentImpl) -> Router<DeploymentImpl> {
    let task_actions_router = Router::new()
        .route("/", put(update_task))
        .route("/", delete(delete_task));

    let task_id_router = Router::new()
        .route("/", get(get_task))
        .merge(task_actions_router)
        .layer(from_fn_with_state(deployment.clone(), load_task_middleware));

    let inner = Router::new()
        .route("/", get(get_tasks).post(create_task))
        .route("/stream/ws", get(stream_tasks_ws))
        .route("/create-and-start", post(create_task_and_start))
        .route("/create-and-start-swarm", post(create_swarm_task_and_start))
        .nest("/{task_id}", task_id_router);

    // mount under /projects/:project_id/tasks
    Router::new().nest("/tasks", inner)
}
