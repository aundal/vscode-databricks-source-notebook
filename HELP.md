# Databricks Source Notebook Help

## Header buttons

The default `.py` experience now uses a custom Databricks editor header instead of the built-in notebook toolbar.

### Select Enviroment

- Choose the Databricks CLI profile.
- Choose the Databricks cluster.
- Sets the active execution target for this notebook.

### Login

- Opens Databricks login for the selected profile.
- Used when the profile needs authentication.
- Opens in a terminal editor.

### Select cluster

- Choose which Databricks cluster to use.
- Shown when no cluster is selected or when the selected cluster is not running.

### Starting cluster

- Shown while the selected cluster is starting.
- Execution waits until the cluster is ready.

### Stop cluster

- Stops the selected Databricks cluster.
- Also clears notebook execution sessions.

### Cluster timed out

- Shown when the selected cluster was auto-terminated while idle.
- Use it to choose or restart a cluster again.

### Restart session

- Resets the notebook execution session.
- Clears Python, SQL, Scala, and R state for this notebook.
- Variables, temporary session state, and execution contexts are removed.
- The cluster is not stopped.
- The selected profile and cluster are not changed.

### Run all

- Runs all cells in order from the custom editor.
- Uses the same Databricks session state as single-cell runs.

### Open PowerShell terminal

- Opens a PowerShell terminal in the editor area.
- Useful for Databricks CLI commands or manual checks.

### Help

- Opens this help document.

## Cluster label

The live cluster label in the custom editor shows current cluster info.

Example:

`🟢 my-cluster (4:54)`

Meaning:

- `🟢` means the cluster is running.
- `my-cluster` is the selected cluster name.
- `(4:54)` is an approximate remaining auto-termination time.

## Countdown behavior

- The countdown is approximate.
- It uses the cluster auto-termination setting.
- It uses the last notebook execution touch on the cluster.
- It can drift if the cluster is used outside this notebook or outside VS Code.

## Editing behavior

- The custom editor owns the top area and cell layout.
- You can add, delete, reorder, edit, and run cells there.
- If needed, use `Reopen With` to switch to the fallback `Databricks Source Notebook` editor.

## Autosave behavior

- Custom-editor edits are saved back to the `.py` source file automatically.
- This keeps the Databricks source notebook file live on disk.

## Profiles view

The `Databricks Profiles` view in Explorer shows Databricks CLI profiles.

- Ready profiles can be selected.
- Invalid profiles show login required.
- Inline actions let you select or log in.

## Current limitations

- `%sh`, `%fs`, `%run`, `%pip`, widgets, and debugger integration are not supported.
- Unsupported Databricks magics still round-trip in the custom editor, but they do not execute.
- The fallback notebook editor still has the normal VS Code notebook UI if you reopen the file with that editor.
