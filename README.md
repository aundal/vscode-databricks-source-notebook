# Databricks Source Notebook Prototype

Prototype VS Code extension that gives Databricks source-format `.py` notebooks a custom Databricks editor by default, with a notebook fallback when needed.

## What it does

- Opens `*.py` files in a custom Databricks editor by default.
- Owns the top control area, so built-in notebook buttons like `Generate`, `Code`, `Markdown`, and the notebook `...` menu are no longer part of the default `.py` experience.
- Keeps the notebook serializer/controller as a fallback editor through `Reopen With`.
- Parses native Databricks source format:
  - `# Databricks notebook source`
  - `# COMMAND ----------`
  - `%sql`, `%scala`, `%r`, `%md`
  - comment-style `# MAGIC` cells
- Executes `python`, `sql`, `scala`, and `r` cells on a running Databricks all-purpose cluster.
- Keeps one Databricks execution context per language per open notebook, so state survives between cell runs.
- Saves custom-editor changes back to the `.py` source file.
- Shows a `Databricks Profiles` view in Explorer with each CLI profile and its login state.
- Lets you click a profile to use it, or click `Login` for profiles that need re-login.
- Shows `Select Enviroment`, `Login`, cluster action button, `Restart session`, `Open PowerShell terminal`, `Help`, and `Run all` in the custom editor header.
- Uses `Select Enviroment` to pick a Databricks profile from all CLI connections and then pick a cluster.
- Shows `Login` and `Select profile` actions when execution fails because login expired.
- Automatically starts a terminated cluster before execution and waits until it is running.
- Waits for a pending or restarting cluster to become running before execution.
- Shows editable cells directly in the custom editor, including add, delete, move, and per-cell run actions.
- Saves source changes on each edit so the `.py` file stays live.
- Polls the selected cluster every minute when the notebook is idle.
- Shows the selected running cluster with a green dot and an approximate auto-termination countdown.
- Switches the cluster action button by state:
  - `Select cluster`
  - `Starting cluster`
  - `Stop cluster`
  - `Cluster timed out`
- Opens Databricks login and PowerShell terminals in the editor area instead of the bottom panel.

## What it does not do yet

- It does not support `%sh`, `%fs`, `%run`, `%pip`, widgets, or debugger integration.
- It does not reproduce Databricks-only runtime helpers like `_sqldf`.
- It only works with running classic all-purpose clusters, because the Databricks Command Execution API does not support serverless compute.

## Setup

1. Open this folder in VS Code: `C:\Users\Daniel\repo\vscode-databricks-source-notebook`
2. Press `F5` to launch an Extension Development Host.
3. In the Extension Development Host:
   - Open `sample.notebook.py`, or
   - Run `Databricks Notebook: New notebook`, or
   - Open any `.py` file
4. The file opens in the custom Databricks editor by default.
5. If you need the old notebook surface, use `Reopen With` and choose `Databricks Source Notebook`.
6. Open the `Databricks Profiles` view in Explorer.
7. Use `Select Enviroment` in the custom editor header, or use the `Databricks Profiles` view.
8. Pick a profile:
   - `ready` profiles become the active notebook profile.
   - `login required` profiles show a `Login` button that opens a terminal and runs `databricks auth login --profile ...`.
9. If the selected profile needs login, use `Login` in the header.
     - On success the login terminal closes automatically when shell integration is available.
10. Use the cluster button in the header to select or control the cluster.
11. If you pick a stopped cluster, the extension starts it right away.
12. Run cells or use `Run all`.
13. If login has expired, use the error notification actions:
    - `Login`
    - `Select profile`
14. If the chosen cluster is terminated, the extension starts it and waits automatically.
15. If cluster recovery still needs manual help, use the cluster button or `Select cluster` from the error notification.
16. If you started from an untitled file, the first real save still needs a file target.

## Databricks auth

The extension uses the Databricks CLI already installed on your machine.

It can start the selected cluster before execution when needed.

Profiles in the `Databricks Profiles` view come from `databricks auth profiles`.

You need valid CLI auth first, for example:

```powershell
databricks auth login --profile src-dev
```

## Settings

- `databricksSourceNotebook.cliPath`
- `databricksSourceNotebook.profile`
- `databricksSourceNotebook.clusterId`
- `databricksSourceNotebook.pollIntervalMs`
- `databricksSourceNotebook.commandTimeoutSeconds`

## Notes

- Non-Python cells are serialized with `# MAGIC` comments so the file stays readable as Python source.
- The extension also parses raw `%sql` and `%md` cells when opening a file.
- If the Databricks CLI refresh token is expired, the extension tells you which `databricks auth login --profile ...` command to run.
- Unsupported Databricks magics such as `%sh`, `%fs`, `%run`, `%pip`, and `%uv` round-trip in the custom editor, but they still do not execute in this prototype.
