use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

/// Contract state representing a todo list
#[derive(Serialize, Deserialize, Debug, Clone)]
struct TodoList {
    /// List of tasks
    tasks: Vec<Task>,
    /// State version for concurrency control
    version: u64,
}

/// Represents an individual task
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Task {
    /// Unique task identifier
    id: u64,
    /// Task title
    title: String,
    /// Task description
    description: String,
    /// Completion status
    completed: bool,
    /// Priority (1-5, where 5 is highest)
    priority: u8,
}

/// State summary structure
#[derive(Serialize, Deserialize, Debug)]
struct TodoListSummary {
    /// Number of tasks
    task_count: usize,
    /// State version
    version: u64,
    /// Hash of task IDs for consistency verification
    task_ids_hash: u64,
}

/// Task update operations
#[derive(Serialize, Deserialize, Debug)]
enum TodoOperation {
    /// Add a new task
    Add(Task),
    /// Update an existing task
    Update(Task),
    /// Remove a task by ID
    Remove(u64),
    /// Mark a task as completed
    Complete(u64),
}

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        // Deserialize the state
        let todo_list: TodoList = match serde_json::from_slice(state.as_ref()) {
            Ok(list) => list,
            Err(e) => return Err(ContractError::Deser(e.to_string())),
        };

        // Validate that all tasks have unique IDs
        let mut ids = std::collections::HashSet::new();
        for task in &todo_list.tasks {
            if !ids.insert(task.id) {
                return Err(ContractError::InvalidState);
            }

            // Validate that priority is in the correct range
            if task.priority < 1 || task.priority > 5 {
                return Err(ContractError::InvalidState);
            }
        }

        Ok(ValidateResult::Valid)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        // Deserialize the current state
        let original_state_bytes = state.as_ref();
        let mut todo_list: TodoList = match serde_json::from_slice(original_state_bytes) {
            Ok(list) => list,
            Err(e) => return Err(ContractError::Deser(e.to_string())),
        };

        // Track whether we received a delta (which requires version increment)
        // vs a full state replacement (which already has the correct version)
        let mut received_delta = false;

        // Process each update operation
        for update in data {
            match update {
                UpdateData::Delta(delta) => {
                    received_delta = true;
                    let operation: TodoOperation = match serde_json::from_slice(delta.as_ref()) {
                        Ok(op) => op,
                        Err(e) => return Err(ContractError::Deser(e.to_string())),
                    };

                    // Apply the operation to the state
                    match operation {
                        TodoOperation::Add(task) => {
                            // Verify that the ID doesn't already exist
                            if todo_list.tasks.iter().any(|t| t.id == task.id) {
                                return Err(ContractError::InvalidUpdate);
                            }
                            todo_list.tasks.push(task);
                        }
                        TodoOperation::Update(task) => {
                            // Find and update the task
                            if let Some(index) =
                                todo_list.tasks.iter().position(|t| t.id == task.id)
                            {
                                todo_list.tasks[index] = task;
                            } else {
                                return Err(ContractError::InvalidUpdate);
                            }
                        }
                        TodoOperation::Remove(id) => {
                            // Remove the task
                            if let Some(index) = todo_list.tasks.iter().position(|t| t.id == id) {
                                todo_list.tasks.remove(index);
                            } else {
                                return Err(ContractError::InvalidUpdate);
                            }
                        }
                        TodoOperation::Complete(id) => {
                            // Mark as completed
                            if let Some(index) = todo_list.tasks.iter().position(|t| t.id == id) {
                                todo_list.tasks[index].completed = true;
                            } else {
                                return Err(ContractError::InvalidUpdate);
                            }
                        }
                    }
                }
                UpdateData::State(new_state_data) => {
                    // Full state replacement.
                    let incoming: TodoList = match serde_json::from_slice(new_state_data.as_ref()) {
                        Ok(list) => list,
                        Err(e) => return Err(ContractError::Deser(e.to_string())),
                    };

                    // Validate the new state
                    let mut ids = std::collections::HashSet::new();
                    for task in &incoming.tasks {
                        if !ids.insert(task.id) {
                            return Err(ContractError::InvalidUpdate);
                        }

                        // Validate that priority is in the correct range
                        if task.priority < 1 || task.priority > 5 {
                            return Err(ContractError::InvalidUpdate);
                        }
                    }

                    // Determine if this is a client update or a network broadcast:
                    // - If incoming version > current version: network broadcast, use as-is
                    // - If incoming version == current version: client update, may need version bump
                    // - If incoming version < current version: stale broadcast, keep current state
                    if incoming.version > todo_list.version {
                        // Network broadcast with newer version - use it directly
                        todo_list = incoming;
                    } else if incoming.version == todo_list.version {
                        // Same version but potentially different content (client update)
                        // Will increment version below if content changed
                        todo_list = incoming;
                        received_delta = true; // Treat as a delta for version increment logic
                    } else {
                        // Incoming has older version (stale broadcast) - keep current state.
                        // This happens when e.g. a gateway echoes back an earlier PUT state
                        // after we've already applied a newer UPDATE locally.
                    }
                }
                _ => return Err(ContractError::InvalidUpdate),
            }
        }

        // Only increment version for delta updates (not full state replacements).
        // Delta updates apply operations locally, so we need to track the version change.
        // Full state replacements already include the correct version from the source peer.
        if received_delta {
            // Check if the state actually changed by comparing serialized forms
            let new_state_bytes =
                serde_json::to_vec(&todo_list).map_err(|e| ContractError::Other(e.to_string()))?;
            let state_changed = original_state_bytes != new_state_bytes.as_slice();

            if state_changed {
                todo_list.version += 1;
                // Re-serialize with incremented version
                let new_state = serde_json::to_vec(&todo_list)
                    .map_err(|e| ContractError::Other(e.to_string()))?;
                Ok(UpdateModification::valid(State::from(new_state)))
            } else {
                // Reuse already serialized bytes since state didn't change
                Ok(UpdateModification::valid(State::from(new_state_bytes)))
            }
        } else {
            // Full state replacement - return the state as-is (version already correct)
            let new_state_bytes =
                serde_json::to_vec(&todo_list).map_err(|e| ContractError::Other(e.to_string()))?;
            Ok(UpdateModification::valid(State::from(new_state_bytes)))
        }
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        // Deserialize the state
        let todo_list: TodoList = match serde_json::from_slice(state.as_ref()) {
            Ok(list) => list,
            Err(e) => return Err(ContractError::Deser(e.to_string())),
        };

        // Calculate a simple hash of task IDs
        let mut task_ids_hash: u64 = 0;
        for task in &todo_list.tasks {
            task_ids_hash = task_ids_hash.wrapping_add(task.id);
        }

        // Create the summary
        let summary = TodoListSummary {
            task_count: todo_list.tasks.len(),
            version: todo_list.version,
            task_ids_hash,
        };

        // Serialize the summary
        let summary_bytes = match serde_json::to_vec(&summary) {
            Ok(bytes) => bytes,
            Err(e) => return Err(ContractError::Other(e.to_string())),
        };

        Ok(StateSummary::from(summary_bytes))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        // Deserialize the current state
        let todo_list: TodoList = match serde_json::from_slice(state.as_ref()) {
            Ok(list) => list,
            Err(e) => return Err(ContractError::Deser(e.to_string())),
        };

        // Deserialize the summary
        let summary_data: TodoListSummary = match serde_json::from_slice(summary.as_ref()) {
            Ok(s) => s,
            Err(e) => return Err(ContractError::Deser(e.to_string())),
        };

        // If versions are equal, there are no changes
        if todo_list.version == summary_data.version {
            return Ok(StateDelta::from(Vec::new()));
        }

        // If summary version is greater, there's an error
        if summary_data.version > todo_list.version {
            return Err(ContractError::Other(
                "Summary version is newer than state".to_owned(),
            ));
        }

        // Calculate tasks that have changed since the summary
        let mut changed_tasks = Vec::new();

        // If hash or task count is different, send all tasks
        if summary_data.task_count != todo_list.tasks.len() || {
            let mut current_hash: u64 = 0;
            for task in &todo_list.tasks {
                current_hash = current_hash.wrapping_add(task.id);
            }
            current_hash != summary_data.task_ids_hash
        } {
            changed_tasks = todo_list.tasks.clone();
        }

        // Serialize the changed tasks as delta
        let delta_bytes = match serde_json::to_vec(&changed_tasks) {
            Ok(bytes) => bytes,
            Err(e) => return Err(ContractError::Other(e.to_string())),
        };

        Ok(StateDelta::from(delta_bytes))
    }
}

/// Creates an empty todo list
pub fn create_empty_todo_list() -> Vec<u8> {
    let todo_list = TodoList {
        tasks: Vec::new(),
        version: 0,
    };

    serde_json::to_vec(&todo_list).unwrap_or_default()
}

/// Creates an operation to add a task
pub fn create_add_task_operation(id: u64, title: &str, description: &str, priority: u8) -> Vec<u8> {
    let task = Task {
        id,
        title: title.to_string(),
        description: description.to_string(),
        completed: false,
        priority,
    };

    let operation = TodoOperation::Add(task);

    serde_json::to_vec(&operation).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a test state with some tasks
    fn create_test_state(tasks: Vec<Task>) -> State<'static> {
        let todo_list = TodoList { tasks, version: 1 };

        let bytes = serde_json::to_vec(&todo_list).unwrap();
        State::from(bytes)
    }

    // Helper function to create a test task
    fn create_test_task(id: u64, title: &str) -> Task {
        Task {
            id,
            title: title.to_string(),
            description: "Test description".to_string(),
            completed: false,
            priority: 3,
        }
    }

    // Helper function to create an add task operation
    fn create_add_task_delta(task: Task) -> UpdateData<'static> {
        let operation = TodoOperation::Add(task);
        let bytes = serde_json::to_vec(&operation).unwrap();
        UpdateData::Delta(StateDelta::from(bytes))
    }

    #[test]
    fn test_validate_state_valid() {
        // Create a valid state with unique task IDs
        let tasks = vec![create_test_task(1, "Task 1"), create_test_task(2, "Task 2")];
        let state = create_test_state(tasks);

        // Validate the state
        let result =
            Contract::validate_state(Parameters::from(vec![]), state, RelatedContracts::default());

        assert!(matches!(result, Ok(ValidateResult::Valid)));
    }

    #[test]
    fn test_validate_state_invalid_duplicate_ids() {
        // Create an invalid state with duplicate task IDs
        let tasks = vec![
            create_test_task(1, "Task 1"),
            create_test_task(1, "Task 1 duplicate"), // Same ID
        ];
        let state = create_test_state(tasks);

        // Validate the state
        let result =
            Contract::validate_state(Parameters::from(vec![]), state, RelatedContracts::default());

        assert!(matches!(result, Err(ContractError::InvalidState)));
    }

    #[test]
    fn test_validate_state_invalid_priority() {
        // Create an invalid state with invalid priority
        let mut task = create_test_task(1, "Task 1");
        task.priority = 6; // Invalid priority (should be 1-5)

        let state = create_test_state(vec![task]);

        // Validate the state
        let result =
            Contract::validate_state(Parameters::from(vec![]), state, RelatedContracts::default());

        assert!(matches!(result, Err(ContractError::InvalidState)));
    }

    #[test]
    fn test_update_state_add_task() {
        // Create an initial state with no tasks
        let initial_state = create_test_state(vec![]);

        // Create an add task operation
        let new_task = create_test_task(1, "New Task");
        let update_data = create_add_task_delta(new_task.clone());

        // Update the state
        let result =
            Contract::update_state(Parameters::from(vec![]), initial_state, vec![update_data]);

        // Verify the result
        assert!(result.is_ok());

        // Deserialize the updated state and verify the task was added
        if let Ok(update_mod) = result {
            let updated_state = update_mod.new_state.expect("Should have new state");
            let todo_list: TodoList = serde_json::from_slice(updated_state.as_ref()).unwrap();

            assert_eq!(todo_list.tasks.len(), 1);
            assert_eq!(todo_list.tasks[0].id, new_task.id);
            assert_eq!(todo_list.tasks[0].title, new_task.title);
            assert_eq!(todo_list.version, 2); // Version should be incremented
        } else {
            panic!("Unexpected result type");
        }
    }

    #[test]
    fn test_update_state_add_duplicate_task() {
        // Create an initial state with one task
        let existing_task = create_test_task(1, "Existing Task");
        let initial_state = create_test_state(vec![existing_task.clone()]);

        // Try to add a task with the same ID
        let duplicate_task = create_test_task(1, "Duplicate Task");
        let update_data = create_add_task_delta(duplicate_task);

        // Update the state
        let result =
            Contract::update_state(Parameters::from(vec![]), initial_state, vec![update_data]);

        // Verify the result is an error
        assert!(matches!(result, Err(ContractError::InvalidUpdate)));
    }

    #[test]
    fn test_update_state_update_task() {
        // Create an initial state with one task
        let existing_task = create_test_task(1, "Existing Task");
        let initial_state = create_test_state(vec![existing_task]);

        // Create an update task operation
        let mut updated_task = create_test_task(1, "Updated Task");
        updated_task.completed = true;

        let operation = TodoOperation::Update(updated_task);
        let bytes = serde_json::to_vec(&operation).unwrap();
        let update_data = UpdateData::Delta(StateDelta::from(bytes));

        // Update the state
        let result =
            Contract::update_state(Parameters::from(vec![]), initial_state, vec![update_data]);

        // Verify the result
        assert!(result.is_ok());

        // Deserialize the updated state and verify the task was updated
        if let Ok(update_mod) = result {
            let updated_state = update_mod.new_state.expect("Should have new state");
            let todo_list: TodoList = serde_json::from_slice(updated_state.as_ref()).unwrap();

            assert_eq!(todo_list.tasks.len(), 1);
            assert_eq!(todo_list.tasks[0].id, 1);
            assert_eq!(todo_list.tasks[0].title, "Updated Task");
            assert!(todo_list.tasks[0].completed);
            assert_eq!(todo_list.version, 2); // Version should be incremented
        } else {
            panic!("Unexpected result type");
        }
    }

    #[test]
    fn test_summarize_and_delta() {
        // Create a state with two tasks
        let tasks = vec![create_test_task(1, "Task 1"), create_test_task(2, "Task 2")];
        let state = create_test_state(tasks);

        // Create a summary
        let summary_result = Contract::summarize_state(Parameters::from(vec![]), state.clone());

        assert!(summary_result.is_ok());

        let summary = summary_result.unwrap();

        // Create a delta from the summary
        let delta_result = Contract::get_state_delta(Parameters::from(vec![]), state, summary);

        assert!(delta_result.is_ok());

        // The delta should be empty since the state and summary are from the same version
        let delta = delta_result.unwrap();
        assert_eq!(delta.as_ref().len(), 0);
    }
}
