const { spawn } = require('child_process');
const { TextDecoder, TextEncoder } = require('util');
const vscode = require('vscode');

const NOTEBOOK_TYPE = 'databricks-source-notebook';
const CUSTOM_EDITOR_TYPE = 'databricksSourceNotebook.editor';
const NOTEBOOK_HEADER = '# Databricks notebook source';
const CELL_SEPARATOR = '# COMMAND ----------';
const MAGIC_PREFIX = '# MAGIC';
const CONFIG_SECTION = 'databricksSourceNotebook';
const STATE_PROFILE = 'databricksSourceNotebook.selectedProfile';
const STATE_CLUSTER = 'databricksSourceNotebook.selectedCluster';
const ERROR_CODES = {
  authExpired: 'DATABRICKS_AUTH_EXPIRED',
  clusterNotReady: 'DATABRICKS_CLUSTER_NOT_READY',
};
const VIEWS = {
  profiles: 'databricksSourceNotebook.profiles',
};
const COMMANDS = {
  newNotebook: 'databricksSourceNotebook.newNotebook',
  saveToSourceFile: 'databricksSourceNotebook.saveToSourceFile',
  selectProfile: 'databricksSourceNotebook.selectProfile',
  selectCluster: 'databricksSourceNotebook.selectCluster',
  clusterActionSelect: 'databricksSourceNotebook.clusterActionSelect',
  clusterActionStart: 'databricksSourceNotebook.clusterActionStart',
  clusterActionStarting: 'databricksSourceNotebook.clusterActionStarting',
  clusterActionStop: 'databricksSourceNotebook.clusterActionStop',
  clusterActionTimedOut: 'databricksSourceNotebook.clusterActionTimedOut',
  restartSession: 'databricksSourceNotebook.restartSession',
  refreshProfiles: 'databricksSourceNotebook.refreshProfiles',
  connectProfile: 'databricksSourceNotebook.connectProfile',
  loginProfile: 'databricksSourceNotebook.loginProfile',
  openPowerShellTerminal: 'databricksSourceNotebook.openPowerShellTerminal',
  openHelp: 'databricksSourceNotebook.openHelp',
  openLog: 'databricksSourceNotebook.openLog',
};

const SUPPORTED_EXECUTION_LANGUAGES = new Set(['python', 'sql', 'scala', 'r']);
const IDLE_CLUSTER_POLL_MS = 60000;
const CLUSTER_LABEL_REFRESH_MS = 1000;
const RUNNING_CLUSTER_MARKER = '🟢';
const SOURCE_EDITOR_SUPPORTED_CELL_TYPES = ['python', 'sql', 'scala', 'r', 'md', 'sh', 'fs', 'run', 'pip', 'uv'];
const CLUSTER_TIMER_RESET_COMMAND = 'print(1);';
const INLINE_TABLE_MARKER = '__DATABRICKS_SOURCE_TABLE__:';
const PYTHON_DISPLAY_SHIM = [
  'import json as __vscode_dsn_json',
  '',
  'def __vscode_dsn_display_value(value):',
  '    if value is None:',
  '        return ""',
  '    try:',
  '        return value.isoformat()',
  '    except Exception:',
  '        pass',
  '    return str(value)',
  '',
  'def __vscode_dsn_emit_table(columns, rows, truncated=False):',
  `    print(${JSON.stringify(INLINE_TABLE_MARKER)} + __vscode_dsn_json.dumps({"columns": [str(column) for column in columns], "rows": rows, "truncated": bool(truncated)}, separators=(",", ":")))`,
  '',
  'def display(value, limit=200):',
  '    try:',
  '        if hasattr(value, "limit") and hasattr(value, "collect") and hasattr(value, "columns"):',
  '            preview = value.limit(limit + 1).collect()',
  '            truncated = len(preview) > limit',
  '            rows = preview[:limit]',
  '            __vscode_dsn_emit_table(value.columns, [[__vscode_dsn_display_value(row[column]) for column in value.columns] for row in rows], truncated)',
  '            return value',
  '    except Exception:',
  '        pass',
  '',
  '    try:',
  '        if hasattr(value, "head") and hasattr(value, "columns") and hasattr(value, "to_dict"):',
  '            preview = value.head(limit + 1)',
  '            data_rows = preview.to_dict(orient="records")',
  '            truncated = len(data_rows) > limit',
  '            rows = data_rows[:limit]',
  '            __vscode_dsn_emit_table(list(value.columns), [[__vscode_dsn_display_value(row.get(column)) for column in value.columns] for row in rows], truncated)',
  '            return value',
  '    except Exception:',
  '        pass',
  '',
  '    if hasattr(value, "show"):',
  '        try:',
  '            value.show(limit, truncate=False)',
  '            return value',
  '        except Exception:',
  '            pass',
  '',
  '    print(__vscode_dsn_display_value(value))',
  '    return value',
  '',
].join('\n');

const clusterActivityTimestamps = new Map();
const clusterStatusSnapshots = new Map();
const clusterTimerResetPromises = new Map();
const clusterTimerResetPendingKeys = new Set();
const lastDatabricksUiState = {
  selectedProfileNeedsLogin: false,
};

let sessionManagerSingleton;
let profilesProviderSingleton;
let extensionContextSingleton;
let databricksCliSingleton;
let activeAuthPromptKey;
let activeClusterPromptKey;
let activeClusterOperation;
let notebookControllerSingleton;
let autoSaveManagerSingleton;
let clusterMonitorSingleton;
let sourceEditorProviderSingleton;
let logOutputChannel;

function activate(context) {
  const serializer = new DatabricksNotebookSerializer();
  const cli = new DatabricksCli(context);
  const sessions = new NotebookSessionManager(context, cli);
  const profilesProvider = new DatabricksProfilesProvider(context, cli);
  const sourceEditorProvider = new DatabricksSourceEditorProvider(context, cli, sessions);
  sessionManagerSingleton = sessions;
  profilesProviderSingleton = profilesProvider;
  extensionContextSingleton = context;
  databricksCliSingleton = cli;
  sourceEditorProviderSingleton = sourceEditorProvider;

  const controller = new DatabricksNotebookController(context, cli, sessions, context);
  const autoSaveManager = new NotebookAutoSaveManager();
  const clusterMonitor = new ClusterIdleMonitor(context, cli);
  const clusterLabelRefreshTimer = setInterval(() => {
    void refreshNotebookControllerStatusFromCache();
    void sourceEditorProviderSingleton?.notifyUiStateChanged();
  }, CLUSTER_LABEL_REFRESH_MS);
  notebookControllerSingleton = controller;
  autoSaveManagerSingleton = autoSaveManager;
  clusterMonitorSingleton = clusterMonitor;

  context.subscriptions.push(
    vscode.workspace.registerNotebookSerializer(NOTEBOOK_TYPE, serializer),
    vscode.window.registerCustomEditorProvider(CUSTOM_EDITOR_TYPE, sourceEditorProvider, {
      webviewOptions: {
        retainContextWhenHidden: true,
      },
    }),
    vscode.window.registerTreeDataProvider(VIEWS.profiles, profilesProvider),
    profilesProvider,
    sourceEditorProvider,
    controller,
    autoSaveManager,
    clusterMonitor,
    { dispose: () => clearInterval(clusterLabelRefreshTimer) },
    vscode.workspace.onDidCloseNotebookDocument((document) => {
      void sessions.disposeForDocument(document).catch(() => {});
      autoSaveManager.clear(document);
      clusterMonitor.clear(document);
    }),
    vscode.workspace.onDidCloseTextDocument((document) => {
      clusterMonitor.clearSource(document);
    }),
    vscode.workspace.onDidChangeNotebookDocument((event) => {
      void autoSaveManager.handleChange(event.notebook);
      clusterMonitor.markActivity(event.notebook);
    }),
    vscode.workspace.onDidSaveNotebookDocument((document) => {
      clusterMonitor.markActivity(document);
    }),
    vscode.window.onDidChangeActiveNotebookEditor((editor) => {
      if (editor?.notebook?.notebookType === NOTEBOOK_TYPE) {
        clusterMonitor.markActivity(editor.notebook);
      }
    }),
    vscode.window.onDidChangeNotebookEditorSelection((event) => {
      if (event.notebookEditor.notebook.notebookType === NOTEBOOK_TYPE) {
        void autoSaveManager.handleFocusChange(event.notebookEditor.notebook);
        clusterMonitor.markActivity(event.notebookEditor.notebook);
      }
    }),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (
        event.affectsConfiguration(`${CONFIG_SECTION}.profile`) ||
        event.affectsConfiguration(`${CONFIG_SECTION}.cliPath`)
      ) {
        void refreshDatabricksUi();
      }
    }),
    vscode.window.onDidChangeVisibleNotebookEditors((editors) => {
      void migrateVisibleNotebookEditorsToCustomEditor(editors);
    }),
    vscode.commands.registerCommand(COMMANDS.newNotebook, async () => {
      const document = await vscode.workspace.openTextDocument({
        language: 'python',
        content: serializeSourceNotebook(createNewNotebookData()),
      });
      await vscode.commands.executeCommand('vscode.openWith', document.uri, CUSTOM_EDITOR_TYPE, {
        preview: false,
      });
    }),
    vscode.commands.registerCommand(COMMANDS.saveToSourceFile, async () => {
      await saveNotebookToSourceFile(vscode.window.activeNotebookEditor?.notebook);
    }),
    vscode.commands.registerCommand(COMMANDS.selectProfile, async () => {
      const profile = await selectProfile(context, cli, true);
      if (!profile) {
        return;
      }

      await applySelectedProfile(context, sessions, profile);
    }),
    vscode.commands.registerCommand(COMMANDS.selectCluster, async () => {
      await selectClusterCommand(context, cli, sessions);
    }),
    vscode.commands.registerCommand(COMMANDS.clusterActionSelect, async () => {
      await selectClusterCommand(context, cli, sessions);
    }),
    vscode.commands.registerCommand(COMMANDS.clusterActionStart, async () => {
      const selectedCluster = getSelectedClusterInfo(context);
      const profile = selectedCluster?.profile || (await ensureProfile(context, cli));
      const clusterId = selectedCluster?.id;
      if (!profile || !clusterId) {
        logWarning('Start cluster requested without a selected cluster.');
        await selectClusterCommand(context, cli, sessions);
        return;
      }

      await startClusterForExecution(profile, clusterId, { clusterName: selectedCluster.name });
    }),
    vscode.commands.registerCommand(COMMANDS.clusterActionStarting, async () => {
      // Toolbar state only.
    }),
    vscode.commands.registerCommand(COMMANDS.clusterActionStop, async () => {
      const selectedCluster = getSelectedClusterInfo(context);
      const profile = selectedCluster?.profile || (await ensureProfile(context, cli));
      const clusterId = selectedCluster?.id;
      if (!profile || !clusterId) {
        logWarning('Stop cluster requested without a selected cluster.');
        vscode.window.showWarningMessage('Select a Databricks cluster first.');
        return;
      }

      await stopClusterForExecution(profile, clusterId, { clusterName: selectedCluster.name });
    }),
    vscode.commands.registerCommand(COMMANDS.clusterActionTimedOut, async () => {
      await selectClusterCommand(context, cli, sessions);
    }),
    vscode.commands.registerCommand(COMMANDS.restartSession, async () => {
      const editor = vscode.window.activeNotebookEditor;
      if (editor && editor.notebook.notebookType === NOTEBOOK_TYPE) {
        await sessions.restartForDocument(editor.notebook);
        vscode.window.showInformationMessage('Databricks notebook session restarted.');
        return;
      }

      const activeSourceDocument = sourceEditorProviderSingleton?.getActiveDocument();
      if (!activeSourceDocument) {
        logWarning('Restart session requested without an open Databricks source notebook.');
        vscode.window.showWarningMessage('Open a Databricks source notebook first.');
        return;
      }

      await sessions.restartForDocument(activeSourceDocument);
      vscode.window.showInformationMessage('Databricks notebook session restarted.');
    }),
    vscode.commands.registerCommand(COMMANDS.refreshProfiles, () => {
      void refreshDatabricksUi();
    }),
    vscode.commands.registerCommand(COMMANDS.connectProfile, async (item) => {
      await connectProfileCommand(context, cli, sessions, item);
    }),
    vscode.commands.registerCommand(COMMANDS.loginProfile, async (item) => {
      await loginProfileCommand(context, item);
    }),
    vscode.commands.registerCommand(COMMANDS.openPowerShellTerminal, async () => {
      await openPowerShellTerminalInEditor();
    }),
    vscode.commands.registerCommand(COMMANDS.openHelp, async () => {
      await openHelpDocument(context);
    }),
    vscode.commands.registerCommand(COMMANDS.openLog, () => {
      showLogOutput();
    })
  );

  void refreshDatabricksUi();
  void migrateVisibleNotebookEditorsToCustomEditor(vscode.window.visibleNotebookEditors);
}

async function deactivate() {
  if (sessionManagerSingleton) {
    await sessionManagerSingleton.disposeAll();
  }
  logOutputChannel?.dispose();
}

function getLogOutputChannel() {
  if (!logOutputChannel) {
    logOutputChannel = vscode.window.createOutputChannel('Databricks Source Notebook');
  }

  return logOutputChannel;
}

function showLogOutput() {
  getLogOutputChannel().show(true);
}

function writeLog(level, message, details) {
  const timestamp = new Date().toISOString();
  const channel = getLogOutputChannel();
  channel.appendLine(`[${timestamp}] [${level}] ${message}`);
  if (details !== undefined && details !== null && details !== '') {
    channel.appendLine(formatLogDetails(details));
  }
}

function logVerbose(message, details) {
  writeLog('verbose', message, details);
}

function logWarning(message, details) {
  writeLog('warning', message, details);
}

function logError(message, details) {
  writeLog('error', message, details);
}

function formatLogDetails(details) {
  if (details instanceof Error) {
    return details.stack || details.message;
  }

  if (typeof details === 'string') {
    return details;
  }

  try {
    return JSON.stringify(details, null, 2);
  } catch {
    return String(details);
  }
}

async function migrateVisibleNotebookEditorsToCustomEditor(editors) {
  for (const editor of editors) {
    const notebook = editor?.notebook;
    if (!notebook || notebook.notebookType !== NOTEBOOK_TYPE) {
      continue;
    }

    const sourceUri = resolveNotebookSourceUri(notebook);
    if (!sourceUri || sourceUri.scheme !== 'file' || sourceUri.path.toLowerCase().endsWith('.py') === false) {
      continue;
    }

    const targetTab = findNotebookTab(sourceUri, notebook.notebookType);
    const alreadyCustom = vscode.window.tabGroups.all.some((group) =>
      group.tabs.some((tab) => tab.input?.uri?.toString?.() === sourceUri.toString() && tab.input?.viewType === CUSTOM_EDITOR_TYPE)
    );
    if (alreadyCustom) {
      continue;
    }

    try {
      await vscode.commands.executeCommand('vscode.openWith', sourceUri, CUSTOM_EDITOR_TYPE, {
        preview: false,
        preserveFocus: true,
      });
      if (targetTab) {
        await vscode.window.tabGroups.close(targetTab, true);
      }
    } catch {
      // Leave the notebook editor open if migration fails.
    }
  }
}

function resolveNotebookSourceUri(notebook) {
  const sourceUri = notebook?.metadata?.databricksSourceUri;
  if (typeof sourceUri === 'string' && sourceUri) {
    try {
      return vscode.Uri.parse(sourceUri);
    } catch {
      return notebook.uri;
    }
  }

  return notebook?.uri;
}

function findNotebookTab(resource, notebookType) {
  for (const group of vscode.window.tabGroups.all) {
    for (const tab of group.tabs) {
      const input = tab.input;
      if (input?.uri?.toString?.() === resource.toString() && input?.notebookType === notebookType) {
        return tab;
      }
    }
  }

  return undefined;
}

class DatabricksNotebookSerializer {
  async deserializeNotebook(content, _token) {
    const text = new TextDecoder().decode(content);
    const parsed = parseSourceNotebook(text);
    const cells = parsed.cells.map((cell) => {
      const data = new vscode.NotebookCellData(cell.kind, cell.value, cell.languageId);
      data.metadata = cell.metadata;
      return data;
    });

    const notebookData = new vscode.NotebookData(cells);
    notebookData.metadata = parsed.metadata;
    return notebookData;
  }

  async serializeNotebook(data, _token) {
    const text = serializeSourceNotebook(data);
    return new TextEncoder().encode(text);
  }
}

class DatabricksNotebookController {
  constructor(context, cli, sessions, extensionContext) {
    this._cli = cli;
    this._sessions = sessions;
    this._context = context;
    this._extensionContext = extensionContext;
    this._executionOrder = 0;
    this._controller = vscode.notebooks.createNotebookController(
      'databricks-source-notebook-kernel',
      NOTEBOOK_TYPE,
      'Databricks Cluster'
    );
    this._controller.supportedLanguages = ['python', 'sql', 'scala', 'r', 'shellscript', 'plaintext'];
    this._controller.supportsExecutionOrder = true;
    this._controller.executeHandler = this._executeAll.bind(this);
    this.setStatus({
      label: 'Select cluster',
      description: 'Select cluster',
      detail: 'No cluster selected',
    });
  }

  dispose() {
    this._controller.dispose();
  }

  setStatus(status) {
    this._controller.label = status.label;
    this._controller.description = status.description;
    this._controller.detail = status.detail;
  }

  async _executeAll(cells, notebook) {
    for (const cell of cells) {
      await this._executeCell(cell, notebook);
    }
  }

  async _executeCell(cell, notebook) {
    const execution = this._controller.createNotebookCellExecution(cell);
    execution.executionOrder = ++this._executionOrder;
    execution.start(Date.now());

    try {
      const outputData = await executeDatabricksCellRuntime(this._cli, this._sessions, this._extensionContext, cell, notebook);
      execution.replaceOutput(outputData.outputs);
      execution.end(outputData.ok, Date.now());
      return {
        ok: outputData.ok,
        outputs: outputData.webviewOutputs,
      };
    } catch (error) {
      const normalized = normalizeError(error);
      void showExecutionErrorActions(this._extensionContext, normalized);
      const outputItems = buildExecutionErrorOutputItems(normalized);
      execution.replaceOutput([
        new vscode.NotebookCellOutput(outputItems),
      ]);
      execution.end(false, Date.now());
      return {
        ok: false,
        outputs: mapOutputItemsToWebview(outputItems),
      };
    }
  }

  async executeCellForEditor(cell, notebook) {
    return this._executeCell(cell, notebook);
  }
}

class DatabricksSourceEditorProvider {
  constructor(context, cli, sessions) {
    this._context = context;
    this._cli = cli;
    this._sessions = sessions;
    this._panels = new Map();
    this._changeDisposables = new Map();
    this._runningOperations = new Map();
    this._suppressedDocumentEvents = new Map();
  }

  dispose() {
    for (const panels of this._panels.values()) {
      for (const panel of panels) {
        panel.dispose();
      }
    }
    for (const disposable of this._changeDisposables.values()) {
      disposable.dispose();
    }
    this._panels.clear();
    this._changeDisposables.clear();
    this._runningOperations.clear();
    this._suppressedDocumentEvents.clear();
  }

  async resolveCustomTextEditor(document, webviewPanel, _token) {
    const key = document.uri.toString();
    const panels = this._panels.get(key) || new Set();
    panels.add(webviewPanel);
    this._panels.set(key, panels);

    webviewPanel.webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(this._context.extensionUri, 'node_modules', 'monaco-editor', 'min'),
        vscode.Uri.joinPath(this._context.extensionUri, 'node_modules', 'markdown-it', 'dist'),
      ],
    };
    webviewPanel.webview.html = this._getHtml(webviewPanel.webview);

    const postState = async () => {
      await this._postDocumentState(document);
    };

    if (!this._changeDisposables.has(key)) {
      const disposable = vscode.workspace.onDidChangeTextDocument((event) => {
        if (event.document.uri.toString() !== key) {
          return;
        }

        const suppressedCount = this._suppressedDocumentEvents.get(key) || 0;
        if (suppressedCount > 0) {
          if (suppressedCount === 1) {
            this._suppressedDocumentEvents.delete(key);
          } else {
            this._suppressedDocumentEvents.set(key, suppressedCount - 1);
          }
          return;
        }

        void this._postDocumentState(document);
      });
      this._changeDisposables.set(key, disposable);
    }

    webviewPanel.onDidDispose(() => {
      const openPanels = this._panels.get(key);
      if (!openPanels) {
        return;
      }
      openPanels.delete(webviewPanel);
      if (!openPanels.size) {
        this._panels.delete(key);
        const disposable = this._changeDisposables.get(key);
        disposable?.dispose();
        this._changeDisposables.delete(key);
      }
    });

    webviewPanel.webview.onDidReceiveMessage(async (message) => {
      await this._handleMessage(document, message);
    });

    await postState();
    void resetSelectedRunningClusterTimer(this._context, this._cli, 'custom editor open');
    clusterMonitorSingleton?.markSourceActivity(document);
  }

  async notifyUiStateChanged() {
    for (const key of this._panels.keys()) {
      const document = vscode.workspace.textDocuments.find((item) => item.uri.toString() === key);
      if (!document) {
        continue;
      }

      await this._postUiState(document);
    }
  }

  getActiveDocument() {
    const activeTabInput = vscode.window.tabGroups.activeTabGroup.activeTab?.input;
    const activeUri = activeTabInput?.uri;
    if (!activeUri) {
      return undefined;
    }
    return vscode.workspace.textDocuments.find((item) => item.uri.toString() === activeUri.toString());
  }

  async _handleMessage(document, message) {
    if (!message || typeof message !== 'object') {
      return;
    }

    if (message.type === 'ready') {
      await this._postDocumentState(document);
      return;
    }

    if (message.type === 'applyDocument') {
      const applyId = Number(message.applyId);
      const serialized = serializeSourceNotebook(deserializeWebviewDocument(message.document || { cells: [] }));
      if (document.getText() !== serialized) {
        const key = document.uri.toString();
        this._suppressedDocumentEvents.set(key, (this._suppressedDocumentEvents.get(key) || 0) + 1);
        await this._replaceDocumentText(document, serialized);
      }
      await this._postApplyAck(document, Number.isFinite(applyId) ? applyId : null);
      clusterMonitorSingleton?.markSourceActivity(document);
      return;
    }

    if (message.type === 'runCell') {
      clusterMonitorSingleton?.markSourceActivity(document);
      const key = document.uri.toString();
      if (this._runningOperations.has(key)) {
        return;
      }

      const runPromise = this._runCell(document, Number(message.index));
      this._runningOperations.set(key, runPromise);
      try {
        await runPromise;
      } finally {
        if (this._runningOperations.get(key) === runPromise) {
          this._runningOperations.delete(key);
        }
      }
      return;
    }

    if (message.type === 'runAll') {
      clusterMonitorSingleton?.markSourceActivity(document);
      const key = document.uri.toString();
      if (this._runningOperations.has(key)) {
        return;
      }

      const runPromise = this._runAll(document);
      this._runningOperations.set(key, runPromise);
      try {
        await runPromise;
      } finally {
        if (this._runningOperations.get(key) === runPromise) {
          this._runningOperations.delete(key);
        }
      }
      return;
    }

    if (message.type === 'command') {
      const map = {
        connectProfile: COMMANDS.connectProfile,
        loginProfile: COMMANDS.loginProfile,
        selectCluster: COMMANDS.selectCluster,
        restartSession: COMMANDS.restartSession,
        openPowerShellTerminal: COMMANDS.openPowerShellTerminal,
        openHelp: COMMANDS.openHelp,
        openLog: COMMANDS.openLog,
      };
      const command = map[message.command];
      if (command) {
        await vscode.commands.executeCommand(command);
        await refreshDatabricksUi();
      }
      return;
    }

    if (message.type === 'copyText') {
      await vscode.env.clipboard.writeText(String(message.value || ''));
      return;
    }

    if (message.type === 'exportCsv') {
      const uri = await vscode.window.showSaveDialog({
        saveLabel: 'Export table as CSV',
        filters: {
          CSV: ['csv'],
        },
      });
      if (!uri) {
        return;
      }

      const csv = buildCsvFromTable(
        Array.isArray(message.columns) ? message.columns : [],
        Array.isArray(message.rows) ? message.rows : []
      );
      await vscode.workspace.fs.writeFile(uri, new TextEncoder().encode(csv));
      return;
    }

    if (message.type === 'clusterAction') {
      const state = String(message.state || '');
      if (state === 'runningUnknownTimer') {
        await resetSelectedRunningClusterTimer(this._context, this._cli, 'cluster button');
        return;
      }

      const command = {
        starting: COMMANDS.clusterActionStarting,
        running: COMMANDS.clusterActionStop,
        timedOut: COMMANDS.clusterActionTimedOut,
        stopped: COMMANDS.clusterActionSelect,
        selected: COMMANDS.clusterActionSelect,
        none: COMMANDS.clusterActionSelect,
      }[state] || COMMANDS.clusterActionSelect;
      if (command === COMMANDS.clusterActionStop) {
        await this._postRunningState(document, { cellIndex: null, runAll: false });
      }
      await vscode.commands.executeCommand(command);
      await refreshDatabricksUi();
      return;
    }
  }

  async _replaceDocumentText(document, text) {
    const fullRange = new vscode.Range(
      document.positionAt(0),
      document.positionAt(document.getText().length)
    );
    const edit = new vscode.WorkspaceEdit();
    edit.replace(document.uri, fullRange, text);
    await vscode.workspace.applyEdit(edit);
    if (document.uri.scheme !== 'untitled') {
      await document.save();
    }
  }

  async _runCell(document, index) {
    const cells = parseSourceNotebook(document.getText()).cells;
    if (!Number.isInteger(index) || index < 0 || index >= cells.length) {
      return;
    }

    await this._postRunningState(document, { cellIndex: index, runAll: false });
    try {
      const result = await executeDatabricksCellForSourceEditor(this._cli, this._sessions, this._context, document, cells[index]);
      await this._postEditorRunResult(document, index, result);
    } finally {
      await this._postRunningState(document, { cellIndex: null, runAll: false });
    }
  }

  async _runAll(document) {
    const cells = parseSourceNotebook(document.getText()).cells;

    await this._postRunningState(document, { cellIndex: null, runAll: true });
    try {
      for (let index = 0; index < cells.length; index += 1) {
        await this._postRunningState(document, { cellIndex: index, runAll: true });
        const result = await executeDatabricksCellForSourceEditor(this._cli, this._sessions, this._context, document, cells[index]);
        await this._postEditorRunResult(document, index, result);
      }
    } finally {
      await this._postRunningState(document, { cellIndex: null, runAll: false });
    }
  }

  async _postDocumentState(document) {
    const panels = this._panels.get(document.uri.toString());
    if (!panels?.size) {
      return;
    }

    const parsed = parseSourceNotebook(document.getText());
    const payload = {
      type: 'state',
      document: serializeParsedNotebookForWebview(parsed),
      ui: buildDatabricksSourceEditorUiState(this._context),
    };

    for (const panel of panels) {
      panel.webview.postMessage(payload);
    }
  }

  async _postEditorRunResult(document, index, result) {
    const panels = this._panels.get(document.uri.toString());
    if (!panels?.size) {
      return;
    }
    for (const panel of panels) {
      panel.webview.postMessage({
        type: 'runResult',
        index,
        result,
      });
    }
  }

  async _postUiState(document) {
    const panels = this._panels.get(document.uri.toString());
    if (!panels?.size) {
      return;
    }

    const payload = {
      type: 'uiState',
      ui: buildDatabricksSourceEditorUiState(this._context),
    };

    for (const panel of panels) {
      panel.webview.postMessage(payload);
    }
  }

  async _postApplyAck(document, applyId) {
    const panels = this._panels.get(document.uri.toString());
    if (!panels?.size) {
      return;
    }

    const payload = {
      type: 'applyAck',
      applyId,
    };

    for (const panel of panels) {
      panel.webview.postMessage(payload);
    }
  }

  async _postRunningState(document, state) {
    const panels = this._panels.get(document.uri.toString());
    if (!panels?.size) {
      return;
    }

    const payload = {
      type: 'runningState',
      cellIndex: Number.isInteger(state?.cellIndex) ? state.cellIndex : null,
      runAll: state?.runAll === true,
    };

    for (const panel of panels) {
      panel.webview.postMessage(payload);
    }
  }

  _getHtml(webview) {
    const nonce = createNonce();
    const monacoBaseUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this._context.extensionUri, 'node_modules', 'monaco-editor', 'min')
    );
    const markdownItScriptUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this._context.extensionUri, 'node_modules', 'markdown-it', 'dist', 'markdown-it.min.js')
    );
    const csp = [
      "default-src 'none'",
      `style-src ${webview.cspSource} 'unsafe-inline'`,
      `script-src ${webview.cspSource} 'nonce-${nonce}'`,
    ].join('; ');

    return String.raw`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Content-Security-Policy" content="${csp}" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Databricks Source Editor</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: var(--vscode-editor-background);
      --fg: var(--vscode-editor-foreground);
      --muted: var(--vscode-descriptionForeground);
      --border: var(--vscode-panel-border);
      --button-bg: transparent;
      --button-fg: var(--vscode-editor-foreground);
      --button-hover: var(--vscode-toolbar-hoverBackground, var(--vscode-list-hoverBackground));
      --input-bg: var(--vscode-input-background);
      --input-fg: var(--vscode-input-foreground);
      --input-border: var(--vscode-input-border);
      --badge-bg: var(--vscode-badge-background);
      --badge-fg: var(--vscode-badge-foreground);
      --list-hover: var(--vscode-list-hoverBackground);
      --list-active: var(--vscode-list-activeSelectionBackground);
      --warning: var(--vscode-editorWarning-foreground);
      --syntax-keyword: var(--vscode-symbolIcon-keywordForeground, #c586c0);
      --syntax-string: var(--vscode-debugTokenExpression-string, #ce9178);
      --syntax-number: var(--vscode-debugTokenExpression-number, #b5cea8);
      --syntax-comment: var(--vscode-editorCodeLens-foreground, #6a9955);
      --syntax-operator: var(--vscode-editor-foreground);
      --syntax-heading: var(--vscode-symbolIcon-classForeground, #4ec9b0);
      --syntax-link: var(--vscode-textLink-foreground);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--fg);
      font-family: var(--vscode-font-family);
      font-size: var(--vscode-font-size);
    }

    .layout {
      display: grid;
      grid-template-rows: auto auto 1fr;
      min-height: 100vh;
    }

    .topbar {
      position: sticky;
      top: 0;
      z-index: 10;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      padding: 8px 12px;
      border-bottom: 1px solid var(--border);
      background: var(--bg);
    }

    .topbar .spacer {
      flex: 1 1 auto;
    }

    .cell-kind {
      position: relative;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 24px;
      padding: 0 10px;
      border: 1px solid var(--border);
      border-radius: 999px;
      background: var(--badge-bg);
      color: var(--badge-fg);
      font-size: 12px;
      overflow: hidden;
      cursor: pointer;
      min-width: 0;
      min-height: 24px;
      font: inherit;
    }

    .cell-kind:hover {
      background: color-mix(in srgb, var(--badge-bg) 70%, var(--button-hover));
    }

    .cell-kind-value {
      pointer-events: none;
    }

    .cell-kind-menu {
      position: absolute;
      top: calc(100% + 6px);
      left: 0;
      z-index: 20;
      min-width: 160px;
      padding: 6px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--vscode-dropdown-background, var(--vscode-editorWidget-background, var(--bg)));
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.24);
      display: grid;
      gap: 4px;
    }

    .cell-kind-option {
      width: 100%;
      min-height: 28px;
      border: 1px solid transparent;
      border-radius: 6px;
      background: transparent;
      color: var(--vscode-dropdown-foreground, var(--vscode-editor-foreground));
      text-align: left;
      font: inherit;
      padding: 0 10px;
    }

    .cell-kind-option:hover,
    .cell-kind-option:focus-visible {
      background: var(--button-hover);
      outline: none;
    }

    .cell-kind-option[data-selected="true"] {
      background: var(--vscode-list-activeSelectionBackground, var(--badge-bg));
      color: var(--vscode-list-activeSelectionForeground, var(--badge-fg));
    }

    button,
    select {
      min-height: 28px;
      border: 1px solid var(--border);
      border-radius: 6px;
      background: var(--button-bg);
      color: var(--button-fg);
      padding: 0 10px;
      cursor: pointer;
    }

    button:hover,
    select:hover {
      background: var(--button-hover);
    }

    button.secondary {
      background: transparent;
      color: var(--fg);
    }

    button.toolbar-primary {
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
      border-color: transparent;
    }

    button.toolbar-primary:hover {
      background: var(--vscode-button-hoverBackground);
    }

    button.icon-button {
      min-width: 28px;
      padding: 0;
      font-size: 14px;
    }

    button.cell-toggle {
      min-width: 24px;
      width: 24px;
      font-size: 13px;
    }

    button.danger {
      color: var(--warning);
    }

    button:disabled {
      cursor: default;
      opacity: 0.6;
    }

    .cells {
      padding: 16px;
      display: grid;
      gap: 12px;
    }

    .cell {
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: visible;
      background: var(--bg);
    }

    .cell:focus-within {
      border-color: var(--vscode-focusBorder);
      box-shadow: 0 0 0 1px var(--vscode-focusBorder) inset;
    }

    .cell.collapsed .cell-header {
      border-bottom: 0;
    }

    .cell-header {
      position: relative;
      display: flex;
      gap: 8px;
      align-items: center;
      padding: 8px 10px;
      border-bottom: 1px solid var(--border);
      background: color-mix(in srgb, var(--bg) 97%, var(--list-hover));
    }

    .cell-actions {
      margin-left: auto;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }

    .cell-title-input {
      min-height: 24px;
      width: min(280px, 35vw);
      border: 1px solid transparent;
      border-radius: 5px;
      background: transparent;
      color: var(--muted);
      font: inherit;
      font-size: 12px;
      padding: 0 6px;
      min-width: 110px;
    }

    .cell-title-input:hover,
    .cell-title-input:focus {
      border-color: var(--input-border, var(--border));
      background: var(--input-bg);
      color: var(--input-fg);
      outline: none;
    }

    .editor-wrap {
      position: relative;
      background: var(--bg);
      display: grid;
      grid-template-columns: auto 1fr;
    }

    .cell-body[hidden] {
      display: none;
    }

    .line-numbers {
      user-select: none;
      color: var(--muted);
      background: color-mix(in srgb, var(--bg) 96%, var(--list-hover));
      border-right: 1px solid var(--border);
      margin: 0;
      padding: 12px 8px 12px 10px;
      text-align: right;
      font-family: var(--vscode-editor-font-family, Consolas, monospace);
      font-size: 13px;
      line-height: 20px;
      box-sizing: border-box;
      display: grid;
      grid-auto-rows: 20px;
      min-width: 44px;
      overflow: hidden;
    }

    .line-number,
    .code-line {
      height: 20px;
      line-height: 20px;
      white-space: pre;
    }

    .editor-main {
      position: relative;
      min-width: 0;
      overflow: auto;
      box-sizing: border-box;
    }

    .editor-main .code,
    .editor-wrap textarea {
      margin: 0;
      padding: 12px;
      font-family: var(--vscode-editor-font-family, Consolas, monospace);
      font-size: 13px;
      line-height: 20px;
      tab-size: 4;
      white-space: pre;
      overflow-wrap: normal;
      word-break: normal;
      overflow: hidden;
      box-sizing: border-box;
    }

    .editor-main .code {
      display: grid;
      grid-auto-rows: 20px;
      min-height: 48px;
      color: var(--input-fg);
      pointer-events: none;
    }

    .editor-main .code span {
      line-height: inherit;
    }

    textarea {
      width: 100%;
      min-height: 0;
      resize: none;
      overflow: hidden;
      border: 0;
      outline: none;
      position: absolute;
      inset: 0;
      background: transparent;
      color: transparent;
      caret-color: var(--input-fg);
      -webkit-text-fill-color: transparent;
    }

    textarea::selection {
      background: var(--vscode-editor-selectionBackground);
    }

    .tok-keyword {
      color: var(--syntax-keyword);
    }

    .tok-string {
      color: var(--syntax-string);
    }

    .tok-number {
      color: var(--syntax-number);
    }

    .tok-comment {
      color: var(--syntax-comment);
    }

    .tok-operator {
      color: var(--syntax-operator);
    }

    .tok-heading {
      color: var(--syntax-heading);
    }

    .tok-link {
      color: var(--syntax-link);
    }

    .mtk.keyword,
    .mtk.storage,
    .mtk.control,
    .mtk.tag,
    .mtk.metatag,
    .mtk.key {
      color: var(--syntax-keyword);
    }

    .mtk.string,
    .mtk.attribute.value,
    .mtk.escape {
      color: var(--syntax-string);
    }

    .mtk.number,
    .mtk.float,
    .mtk.hex {
      color: var(--syntax-number);
    }

    .mtk.comment,
    .mtk.doc,
    .mtk.quote {
      color: var(--syntax-comment);
    }

    .mtk.operator,
    .mtk.delimiter {
      color: var(--syntax-operator);
    }

    .mtk {
      color: var(--input-fg);
    }

    .monaco-colorized {
      color: var(--input-fg);
    }

    .mtk.identifier,
    .mtk.source,
    .mtk.variable,
    .mtk.plain {
      color: var(--input-fg);
    }

    .output {
      border-top: 1px solid var(--border);
      background: color-mix(in srgb, var(--bg) 98%, var(--input-bg));
      padding: 12px;
      display: grid;
      gap: 8px;
    }

    .output-pending {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
    }

    .output-spinner {
      width: 14px;
      height: 14px;
      border: 2px solid color-mix(in srgb, var(--fg) 20%, transparent);
      border-top-color: var(--fg);
      border-radius: 50%;
      animation: output-spin 0.8s linear infinite;
    }

    @keyframes output-spin {
      from { transform: rotate(0deg); }
      to { transform: rotate(360deg); }
    }

    .output-table {
      display: grid;
      gap: 8px;
    }

    .output-table-toolbar {
      display: flex;
      justify-content: flex-end;
    }

    .output-table-scroll {
      overflow: auto;
      border: 1px solid var(--border);
      border-radius: 6px;
    }

    .output-table-grid {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }

    .output-table-grid th,
    .output-table-grid td {
      border: 1px solid var(--border);
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
    }

    .output-table-grid tbody tr:hover {
      background: color-mix(in srgb, var(--bg) 88%, black);
    }

    .output-table-grid th {
      position: sticky;
      top: 0;
      background: var(--vscode-editorWidget-background, var(--bg));
      z-index: 1;
    }

    .output-table-heading {
      position: relative;
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .output-table-sort,
    .output-table-menu-toggle {
      min-height: 22px;
      padding: 0 6px;
      font-size: 12px;
    }

    .output-table-sort {
      flex: 1 1 auto;
      justify-content: flex-start;
      text-align: left;
    }

    .output-table-menu {
      position: absolute;
      top: calc(100% + 4px);
      right: 0;
      z-index: 5;
      min-width: 220px;
      padding: 6px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--vscode-dropdown-background, var(--vscode-editorWidget-background, var(--bg)));
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.24);
      display: grid;
      gap: 8px;
    }

    .output-table-menu button {
      width: 100%;
      justify-content: flex-start;
      text-align: left;
    }

    .output-table-menu-section {
      display: grid;
      gap: 6px;
    }

    .output-table-menu-label {
      font-size: 11px;
      color: var(--muted);
    }

    .output-table-menu-row {
      display: flex;
      gap: 6px;
      align-items: center;
    }

    .output-table-menu-row input[type="text"] {
      width: 100%;
      min-height: 28px;
      border: 1px solid var(--input-border);
      border-radius: 6px;
      background: var(--input-bg);
      color: var(--input-fg);
      padding: 0 8px;
      font: inherit;
    }

    .output-table-menu-row input[type="color"] {
      width: 40px;
      min-width: 40px;
      height: 28px;
      padding: 2px;
      border: 1px solid var(--input-border);
      border-radius: 6px;
      background: var(--input-bg);
    }

    .output-table-menu-row button {
      width: auto;
      white-space: nowrap;
    }

    .output-table-note {
      color: var(--muted);
      font-size: 12px;
    }

    .output pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font: 12px/1.5 var(--vscode-editor-font-family, Consolas, monospace);
    }

    .output iframe {
      width: 100%;
      min-height: 120px;
      border: 1px solid var(--border);
      background: white;
    }

    .warning {
      color: var(--warning);
    }

    .markdown-preview {
      padding: 12px;
      border-top: 1px solid var(--border);
      background: color-mix(in srgb, var(--bg) 98%, var(--input-bg));
    }

    .markdown-preview :first-child {
      margin-top: 0;
    }

    .markdown-preview :last-child {
      margin-bottom: 0;
    }

    .markdown-preview pre {
      padding: 12px;
      border-radius: 6px;
      overflow: auto;
      background: color-mix(in srgb, var(--bg) 94%, black);
    }

    .markdown-preview code {
      font-family: var(--vscode-editor-font-family, Consolas, monospace);
      font-size: 12px;
    }

    .markdown-preview blockquote {
      margin: 0;
      padding-left: 12px;
      border-left: 3px solid var(--border);
      color: var(--muted);
    }

    .empty {
      padding: 24px;
      border: 1px dashed var(--border);
      border-radius: 10px;
      text-align: center;
      color: var(--muted);
    }

    @media (max-width: 720px) {
      .topbar,
      .cell-header,
      .cell-actions {
        align-items: stretch;
      }

      .topbar .spacer,
      .cell-actions {
        display: none;
      }

      button,
      select {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="layout">
    <div class="topbar">
      <button id="environmentButton" data-command="connectProfile" class="toolbar-primary">Select Enviroment</button>
      <button id="loginButton" data-command="loginProfile" class="secondary" hidden>Login</button>
      <button id="clusterButton" data-cluster-action="none" class="toolbar-primary">Select cluster</button>
      <button data-command="restartSession" class="secondary">Restart session</button>
      <button data-command="openPowerShellTerminal" class="secondary">Open PowerShell terminal</button>
      <button data-command="openLog" class="secondary">Show log</button>
      <button data-command="openHelp" class="secondary">Help</button>
      <div class="spacer"></div>
      <button id="runAllButton" class="toolbar-primary">Run all</button>
    </div>
    <div id="cells" class="cells">
      <div class="empty">Loading Databricks source editor...</div>
    </div>
  </div>
  <script nonce="${nonce}" src="${markdownItScriptUri}"></script>
  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    const supportedCellTypes = ${JSON.stringify(SOURCE_EDITOR_SUPPORTED_CELL_TYPES)};
    const cellTypeLabels = {
      python: 'Python',
      sql: 'SQL',
      scala: 'Scala',
      r: 'R',
      md: 'Markdown',
      sh: 'Shell',
      fs: 'Filesystem (%fs)',
      run: 'Notebook workflow (%run)',
      pip: 'Package management (%pip)',
      uv: 'Package management (%uv)',
    };
    const BACKTICK = String.fromCharCode(96);
    const monacoBaseUri = ${JSON.stringify(monacoBaseUri.toString())};
    const monacoLanguageMap = {
      python: 'python',
      sql: 'sql',
      scala: 'scala',
      r: 'r',
      md: 'markdown',
      sh: 'shell',
    };
    const monacoThemeName = 'databricks-source-preview';
    const editorLineHeightPx = 20;
    const editorVerticalPaddingPx = 24;
    const editorMinHeightPx = 48;

    let state = {
      document: { cells: [] },
      ui: { clusterState: 'none', clusterLabel: 'cluster', hasSelectedProfile: false, selectedProfileNeedsLogin: false },
    };
    let applyTimer;
    let nextApplyId = 0;
    let pendingApplyId = null;
    let runResults = new Map();
    let pendingApply = false;
    let runningState = { cellIndex: null, runAll: false };
    let activeCellEdit = null;
    let openLanguagePickerIndex = null;
    let isApplyingLocalEdit = false;
    let monacoReady = false;
    let monacoReadyPromise;
    let markdownRenderer;
    const collapsedCellIndexes = new Set();
    const markdownPreviewIndexes = new Set();
    const tableViewState = new Map();
    let openTableMenu = null;
    const highlightedLanguageSupport = new Set(['python', 'sql', 'scala', 'r', 'md', 'sh']);
    const highlightedKeywords = {
      python: new Set(['and', 'as', 'assert', 'async', 'await', 'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except', 'False', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'None', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'True', 'try', 'while', 'with', 'yield']),
      sql: new Set(['select', 'from', 'where', 'group', 'by', 'order', 'having', 'limit', 'with', 'as', 'join', 'left', 'right', 'inner', 'outer', 'full', 'cross', 'on', 'union', 'all', 'distinct', 'case', 'when', 'then', 'else', 'end', 'into', 'create', 'replace', 'view', 'table', 'insert', 'update', 'delete', 'merge']),
      scala: new Set(['abstract', 'case', 'catch', 'class', 'def', 'do', 'else', 'extends', 'false', 'final', 'finally', 'for', 'forSome', 'if', 'implicit', 'import', 'lazy', 'match', 'new', 'null', 'object', 'override', 'package', 'private', 'protected', 'return', 'sealed', 'super', 'this', 'throw', 'trait', 'try', 'true', 'type', 'val', 'var', 'while', 'with', 'yield']),
      r: new Set(['function', 'if', 'else', 'repeat', 'while', 'for', 'in', 'next', 'break', 'TRUE', 'FALSE', 'NULL', 'NA', 'Inf']),
    };

    window.addEventListener('message', (event) => {
      const message = event.data;
      if (!message) {
        return;
      }
      if (message.type === 'runResult') {
        runResults.set(message.index, message.result || { outputs: [] });
        resetTableState(message.index);
        queueRender();
        return;
      }

      if (message.type === 'runningState') {
        runningState = {
          cellIndex: Number.isInteger(message.cellIndex) ? message.cellIndex : null,
          runAll: message.runAll === true,
        };
        queueRender();
        return;
      }

      if (message.type === 'uiState') {
        state = { ...state, ui: message.ui || {} };
        renderHeader();
        return;
      }

      if (message.type === 'applyAck') {
        if (pendingApplyId !== null && message.applyId === pendingApplyId) {
          pendingApply = false;
          isApplyingLocalEdit = false;
          pendingApplyId = null;
        }
        return;
      }

      if (message.type !== 'state') {
        return;
      }

      const previousDocument = state.document || { cells: [] };
      const nextDocument = message.document || { cells: [] };
      if (isApplyingLocalEdit) {
        state = { ...message, document: state.document };
        renderHeader();
        return;
      }

      if (documentsEqual(previousDocument, nextDocument)) {
        state = { ...message, document: state.document };
        pendingApply = false;
        renderHeader();
        return;
      }

      state = message;
      pendingApply = false;
      isApplyingLocalEdit = false;
      pendingApplyId = null;
      queueRender();
    });

    document.addEventListener('click', (event) => {
      const commandTarget = event.target.closest('[data-command]');
      if (commandTarget) {
        vscode.postMessage({ type: 'command', command: commandTarget.dataset.command });
        return;
      }

      const clusterTarget = event.target.closest('[data-cluster-action]');
      if (clusterTarget) {
        vscode.postMessage({ type: 'clusterAction', state: clusterTarget.dataset.clusterAction });
        return;
      }

      const kindBadge = event.target.closest('[data-cell-kind-badge]');
      if (kindBadge) {
        const index = Number(kindBadge.dataset.cellKindBadge);
        const disabled = runningState.runAll || runningState.cellIndex !== null;
        if (!disabled) {
          openLanguagePickerIndex = openLanguagePickerIndex === index ? null : index;
          queueRender();
        }
        return;
      }

      const kindOption = event.target.closest('[data-cell-kind-option]');
      if (kindOption) {
        const index = Number(kindOption.dataset.index);
        const value = kindOption.dataset.value;
        if (Number.isInteger(index) && state.document.cells[index] && value) {
          state.document.cells[index].type = value;
          openLanguagePickerIndex = null;
          syncCellPreview(index);
          queueApply();
          queueRender();
        }
        return;
      }

      const collapseTarget = event.target.closest('[data-cell-collapse]');
      if (collapseTarget) {
        const index = Number(collapseTarget.dataset.cellCollapse);
        if (Number.isInteger(index)) {
          if (collapsedCellIndexes.has(index)) {
            collapsedCellIndexes.delete(index);
          } else {
            collapsedCellIndexes.add(index);
          }
          queueRender();
        }
        return;
      }

      const markdownPreviewToggle = event.target.closest('[data-markdown-preview-toggle]');
      if (markdownPreviewToggle) {
        const index = Number(markdownPreviewToggle.dataset.markdownPreviewToggle);
        if (Number.isInteger(index)) {
          if (markdownPreviewIndexes.has(index)) {
            markdownPreviewIndexes.delete(index);
          } else {
            markdownPreviewIndexes.add(index);
          }
          queueRender();
        }
        return;
      }

      const tableSortTarget = event.target.closest('[data-table-sort]');
      if (tableSortTarget) {
        const cellIndex = Number(tableSortTarget.dataset.cellIndex);
        const outputIndex = Number(tableSortTarget.dataset.outputIndex);
        const columnIndex = Number(tableSortTarget.dataset.columnIndex);
        if (Number.isInteger(cellIndex) && Number.isInteger(outputIndex) && Number.isInteger(columnIndex)) {
          const state = ensureTableViewState(cellIndex, outputIndex);
          if (state.sortColumn === columnIndex) {
            state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
          } else {
            state.sortColumn = columnIndex;
            state.sortDirection = 'asc';
          }
          openTableMenu = null;
          queueRender();
        }
        return;
      }

      const tableMenuToggle = event.target.closest('[data-table-menu-toggle]');
      if (tableMenuToggle) {
        const cellIndex = Number(tableMenuToggle.dataset.cellIndex);
        const outputIndex = Number(tableMenuToggle.dataset.outputIndex);
        const columnIndex = Number(tableMenuToggle.dataset.columnIndex);
        if (Number.isInteger(cellIndex) && Number.isInteger(outputIndex) && Number.isInteger(columnIndex)) {
          if (openTableMenu && openTableMenu.cellIndex === cellIndex && openTableMenu.outputIndex === outputIndex && openTableMenu.columnIndex === columnIndex) {
            openTableMenu = null;
          } else {
            openTableMenu = { cellIndex, outputIndex, columnIndex };
          }
          queueRender();
        }
        return;
      }

      const tableMenuAction = event.target.closest('[data-table-menu-action]');
      if (tableMenuAction) {
        const actionName = tableMenuAction.dataset.tableMenuAction;
        const cellIndex = Number(tableMenuAction.dataset.cellIndex);
        const outputIndex = Number(tableMenuAction.dataset.outputIndex);
        const columnIndex = Number(tableMenuAction.dataset.columnIndex);
        handleTableMenuAction(actionName, cellIndex, outputIndex, columnIndex);
        return;
      }

      const tableFilterApply = event.target.closest('[data-table-filter-apply]');
      if (tableFilterApply) {
        applyTableFilter(
          Number(tableFilterApply.dataset.cellIndex),
          Number(tableFilterApply.dataset.outputIndex),
          Number(tableFilterApply.dataset.columnIndex)
        );
        return;
      }

      const tableFilterClear = event.target.closest('[data-table-filter-clear]');
      if (tableFilterClear) {
        clearTableFilter(
          Number(tableFilterClear.dataset.cellIndex),
          Number(tableFilterClear.dataset.outputIndex),
          Number(tableFilterClear.dataset.columnIndex)
        );
        return;
      }

      const tableColorClear = event.target.closest('[data-table-color-clear]');
      if (tableColorClear) {
        clearTableColor(
          Number(tableColorClear.dataset.cellIndex),
          Number(tableColorClear.dataset.outputIndex),
          Number(tableColorClear.dataset.columnIndex)
        );
        return;
      }

      const tableExportTarget = event.target.closest('[data-table-export]');
      if (tableExportTarget) {
        const cellIndex = Number(tableExportTarget.dataset.cellIndex);
        const outputIndex = Number(tableExportTarget.dataset.outputIndex);
        exportTableAsCsv(cellIndex, outputIndex);
        return;
      }

      const runCellTarget = event.target.closest('[data-run-cell]');
      if (runCellTarget) {
        if (runCellTarget.disabled) {
          return;
        }
        vscode.postMessage({ type: 'runCell', index: Number(runCellTarget.dataset.runCell) });
        return;
      }

      const action = event.target.closest('[data-cell-action]');
      if (!action) {
        return;
      }

      const index = Number(action.dataset.index);
      if (!Number.isInteger(index)) {
        return;
      }

      const kind = action.dataset.cellAction;
      if (kind === 'delete') {
        state.document.cells.splice(index, 1);
        reconcileCollapsedCells();
      }
      if (kind === 'addAbove') {
        state.document.cells.splice(index, 0, createEmptyCell('python'));
        reconcileCollapsedCells();
      }
      if (kind === 'addBelow') {
        state.document.cells.splice(index + 1, 0, createEmptyCell('python'));
        reconcileCollapsedCells();
      }

      queueApply();
      queueRender();
    });

    document.addEventListener('keydown', (event) => {
      const kindOption = event.target.closest('[data-cell-kind-option]');
      if (kindOption && (event.key === 'Enter' || event.key === ' ')) {
        event.preventDefault();
        kindOption.click();
        return;
      }

      const tableFilterInput = event.target.closest('input[data-table-filter-input]');
      if (tableFilterInput && event.key === 'Enter') {
        event.preventDefault();
        applyTableFilter(
          Number(tableFilterInput.dataset.cellIndex),
          Number(tableFilterInput.dataset.outputIndex),
          Number(tableFilterInput.dataset.columnIndex)
        );
        return;
      }

      const textarea = event.target.closest('textarea[data-cell-text]');
      if (!textarea) {
        return;
      }

      if (event.key !== 'Tab' || event.isComposing || event.keyCode === 229) {
        return;
      }

      event.preventDefault();
      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;
      const value = textarea.value;
      const indent = '\t';
      const nextValue = value.slice(0, start) + indent + value.slice(end);
      textarea.value = nextValue;
      textarea.selectionStart = start + indent.length;
      textarea.selectionEnd = start + indent.length;
      textarea.dispatchEvent(new Event('input', { bubbles: true }));
    });

    document.addEventListener('pointerdown', (event) => {
      if (!event.target.closest('[data-cell-kind-badge]') && !event.target.closest('[data-cell-kind-menu]')) {
        if (openLanguagePickerIndex !== null) {
          openLanguagePickerIndex = null;
          queueRender();
        }
      }

      if (!event.target.closest('[data-table-menu]') && !event.target.closest('[data-table-menu-toggle]')) {
        if (openTableMenu !== null) {
          openTableMenu = null;
          queueRender();
        }
      }
    });

    document.addEventListener('focusin', (event) => {
      const textarea = event.target.closest('textarea[data-cell-text]');
      if (!textarea) {
        return;
      }

      activeCellEdit = {
        index: Number(textarea.dataset.cellText),
        selectionStart: textarea.selectionStart,
        selectionEnd: textarea.selectionEnd,
        scrollTop: textarea.scrollTop,
      };
    });

    document.addEventListener('focusout', (event) => {
      const textarea = event.target.closest('textarea[data-cell-text]');
      if (!textarea) {
        return;
      }

      if (activeCellEdit && activeCellEdit.index === Number(textarea.dataset.cellText)) {
        activeCellEdit.selectionStart = textarea.selectionStart;
        activeCellEdit.selectionEnd = textarea.selectionEnd;
        activeCellEdit.scrollTop = textarea.scrollTop;
      }
    });

    document.addEventListener('input', (event) => {
      const tableColorInput = event.target.closest('input[data-table-color-input]');
      if (tableColorInput) {
        const cellIndex = Number(tableColorInput.dataset.cellIndex);
        const outputIndex = Number(tableColorInput.dataset.outputIndex);
        const columnIndex = Number(tableColorInput.dataset.columnIndex);
        if (Number.isInteger(cellIndex) && Number.isInteger(outputIndex) && Number.isInteger(columnIndex)) {
          const tableState = ensureTableViewState(cellIndex, outputIndex);
          tableState.columnColors[columnIndex] = String(tableColorInput.value || '');
          queueRender();
        }
        return;
      }

      const titleInput = event.target.closest('input[data-cell-title]');
      if (titleInput) {
        const index = Number(titleInput.dataset.cellTitle);
        if (!Number.isInteger(index) || !state.document.cells[index]) {
          return;
        }
        state.document.cells[index].title = titleInput.value;
        queueApply();
        return;
      }

      const textarea = event.target.closest('textarea[data-cell-text]');
      if (!textarea) {
        return;
      }
      autoSizeTextarea(textarea);
      const index = Number(textarea.dataset.cellText);
      if (!Number.isInteger(index) || !state.document.cells[index]) {
        return;
      }
      state.document.cells[index].value = textarea.value;
      syncCellPreview(index);
      colorizeCell(index);
      activeCellEdit = {
        index,
        selectionStart: textarea.selectionStart,
        selectionEnd: textarea.selectionEnd,
        scrollTop: textarea.scrollTop,
      };
      queueApply();
    });

    document.getElementById('runAllButton').addEventListener('click', () => {
      if (document.getElementById('runAllButton').disabled) {
        return;
      }
      vscode.postMessage({ type: 'runAll' });
    });

    function queueApply() {
      clearTimeout(applyTimer);
      applyTimer = setTimeout(() => {
        pendingApply = true;
        isApplyingLocalEdit = true;
        pendingApplyId = ++nextApplyId;
        vscode.postMessage({ type: 'applyDocument', applyId: pendingApplyId, document: state.document });
      }, 200);
    }

    function createEmptyCell(type) {
      return {
        type,
        label: cellTypeLabels[type] || type,
        value: '',
        title: '',
        hasCellMarker: false,
      };
    }

    function render() {
      renderHeader();
      renderCells();
      autoSizeAllTextareas();
    }

    function queueRender() {
      requestAnimationFrame(() => {
        void renderAsync();
      });
    }

    async function renderAsync() {
      await ensureMonacoReady();
      render();
    }

    function renderHeader() {
      const ui = state.ui || {};
      const environmentButton = document.getElementById('environmentButton');
      environmentButton.textContent = ui.environmentButtonLabel || 'Select Enviroment';

      const loginButton = document.getElementById('loginButton');
      loginButton.hidden = !ui.hasSelectedProfile || !ui.selectedProfileNeedsLogin;

      const clusterButton = document.getElementById('clusterButton');
      clusterButton.dataset.clusterAction = ui.clusterActionState || ui.clusterState || 'none';
      clusterButton.textContent = ui.clusterActionLabel || 'Select cluster';
      clusterButton.disabled = ui.clusterActionDisabled === true;

      const runAllButton = document.getElementById('runAllButton');
      const runEnabled = ui.canRun === true && !runningState.runAll && runningState.cellIndex === null;
      runAllButton.disabled = !runEnabled;
      runAllButton.textContent = runningState.runAll ? '🟥' : 'Run all';
      runAllButton.title = runningState.runAll ? 'Running all cells' : 'Run all';
      document.querySelectorAll('[data-run-cell]').forEach((button) => {
        const buttonIndex = Number(button.dataset.runCell);
        button.disabled = ui.canRun !== true || runningState.runAll || runningState.cellIndex !== null;
        button.textContent = runningState.cellIndex === buttonIndex ? '🟥' : '▶️';
        button.title = runningState.cellIndex === buttonIndex ? 'Running cell' : 'Run cell';
      });

      document.querySelectorAll('[data-cell-action]').forEach((button) => {
        button.disabled = runningState.runAll || runningState.cellIndex !== null;
      });

      document.querySelectorAll('[data-cell-collapse]').forEach((button) => {
        button.disabled = runningState.runAll || runningState.cellIndex !== null;
      });

      document.querySelectorAll('.cell-kind').forEach((badge) => {
        badge.style.opacity = runningState.runAll || runningState.cellIndex !== null ? '0.6' : '1';
      });

      document.querySelectorAll('[data-cell-kind-badge]').forEach((badge) => {
        badge.style.cursor = runningState.runAll || runningState.cellIndex !== null ? 'default' : 'pointer';
      });
    }

    function renderCells() {
      const cellsRoot = document.getElementById('cells');
      const cells = Array.isArray(state.document?.cells) ? state.document.cells : [];
      reconcileCollapsedCells();
      if (!cells.length) {
        cellsRoot.innerHTML = '<div class="empty"><p>No cells yet.</p><p><button id="addFirstCell" type="button">Add Python cell</button></p></div>';
        const addFirstCell = document.getElementById('addFirstCell');
        addFirstCell?.addEventListener('click', () => {
          state.document.cells.push(createEmptyCell('python'));
          queueApply();
          render();
        });
        return;
      }

      cellsRoot.innerHTML = cells.map((cell, index) => {
        const type = supportedCellTypes.includes(cell.type) ? cell.type : 'python';
        const label = cellTypeLabels[type] || type;
        const title = typeof cell.title === 'string' ? cell.title : '';
        const codeClass = highlightedLanguageSupport.has(type) ? 'code ' + escapeHtmlAttribute(type) : 'code';
        const isRunning = runningState.cellIndex === index;
        const runButtonLabel = isRunning ? '🟥' : '▶️';
        const lineNumbers = renderLineNumbers(cell.value || '');
        const isPickerOpen = openLanguagePickerIndex === index;
        const isCollapsed = collapsedCellIndexes.has(index);
        const isMarkdown = type === 'md';
        const isMarkdownPreview = isMarkdown && markdownPreviewIndexes.has(index);
        const cellBodyId = 'cell-body-' + index;
        return '' +
          '<section class="cell' + (isCollapsed ? ' collapsed' : '') + '" data-cell-index="' + index + '">' +
            '<div class="cell-header">' +
              '<button type="button" class="icon-button cell-toggle" data-cell-collapse="' + index + '" aria-expanded="' + (!isCollapsed) + '" aria-controls="' + cellBodyId + '" title="' + (isCollapsed ? 'Expand cell' : 'Collapse cell') + '">' + (isCollapsed ? '▶' : '▼') + '</button>' +
              '<button type="button" class="cell-kind" data-cell-kind-badge="' + index + '" title="Select cell language">' +
                '<span class="cell-kind-value">' + (index + 1) + '. ' + escapeHtml(label) + '</span>' +
              '</button>' +
              (isPickerOpen
                ? '<div class="cell-kind-menu" data-cell-kind-menu="' + index + '" role="menu" aria-label="Cell language">' +
                    supportedCellTypes.map((item) => {
                      const optionLabel = cellTypeLabels[item] || item;
                      return '<button type="button" class="cell-kind-option" data-cell-kind-option="true" data-index="' + index + '" data-value="' + escapeHtmlAttribute(item) + '" data-selected="' + (item === type ? 'true' : 'false') + '" role="menuitemradio" aria-checked="' + (item === type ? 'true' : 'false') + '">' + escapeHtml(optionLabel) + '</button>';
                    }).join('') +
                  '</div>'
                : '') +
              '<input type="text" class="cell-title-input" data-cell-title="' + index + '" value="' + escapeHtmlAttribute(title) + '" placeholder="Title" aria-label="Cell title" />' +
              '<div class="cell-actions">' +
                (isMarkdown ? '<button type="button" data-markdown-preview-toggle="' + index + '" class="secondary" title="Toggle markdown preview">' + (isMarkdownPreview ? 'Edit' : 'Preview') + '</button>' : '') +
                '<button type="button" data-run-cell="' + index + '" class="icon-button" title="' + (isRunning ? 'Running cell' : 'Run cell') + '">' + runButtonLabel + '</button>' +
                '<button type="button" data-cell-action="addAbove" data-index="' + index + '" class="icon-button" title="Add cell above">＋↑</button>' +
                '<button type="button" data-cell-action="addBelow" data-index="' + index + '" class="icon-button" title="Add cell below">＋↓</button>' +
                '<button type="button" data-cell-action="delete" data-index="' + index + '" class="icon-button danger" title="Delete cell">🗑</button>' +
              '</div>' +
            '</div>' +
            '<div class="cell-body" id="' + cellBodyId + '"' + (isCollapsed ? ' hidden' : '') + '>' +
              (isMarkdownPreview
                ? '<div class="markdown-preview">' + renderMarkdownPreview(cell.value || '') + '</div>'
                : '<div class="editor-wrap">' +
                    '<pre class="line-numbers" data-line-numbers="' + index + '" aria-hidden="true">' + lineNumbers + '</pre>' +
                    '<div class="editor-main">' +
                      '<pre class="' + codeClass + '" aria-hidden="true">' + renderCodePreview(cell.value || '', type) + '</pre>' +
                      '<textarea data-cell-text="' + index + '" spellcheck="false">' + escapeHtml(cell.value || '') + '</textarea>' +
                    '</div>' +
                  '</div>') +
              renderOutput(index) +
            '</div>' +
          '</section>';
      }).join('');

      applyMonacoTokenization();
      restoreActiveCellEdit();
    }

    function reconcileCollapsedCells() {
      const cells = Array.isArray(state.document?.cells) ? state.document.cells : [];
      for (const index of Array.from(collapsedCellIndexes)) {
        if (index < 0 || index >= cells.length) {
          collapsedCellIndexes.delete(index);
        }
      }
      for (const index of Array.from(markdownPreviewIndexes)) {
        if (index < 0 || index >= cells.length || cells[index]?.type !== 'md') {
          markdownPreviewIndexes.delete(index);
        }
      }
    }

    function renderOutput(index) {
      const result = runResults.get(index);
      if (runningState.runAll || runningState.cellIndex === index) {
        return '<div class="output"><div class="output-pending"><span class="output-spinner" aria-hidden="true"></span><span>Running...</span></div></div>';
      }
      if (!result || !Array.isArray(result.outputs) || !result.outputs.length) {
        return '';
      }

      return '<div class="output">' + result.outputs.map((output, outputIndex) => {
        const mime = output.mime || 'text/plain';
        const value = String(output.value || '');
        if (mime === 'application/x-databricks-table+json') {
          return renderInteractiveTable(index, outputIndex, value);
        }
        if (mime === 'text/html') {
          return '<iframe sandbox="allow-same-origin" srcdoc="' + escapeHtmlAttribute(value) + '"></iframe>';
        }
        return '<pre>' + escapeHtml(value) + '</pre>';
      }).join('') + '</div>';
    }

    function renderInteractiveTable(cellIndex, outputIndex, rawValue) {
      const payload = parseInteractiveTablePayload(rawValue);
      if (!payload) {
        return '<pre>Invalid table output.</pre>';
      }

      const state = ensureTableViewState(cellIndex, outputIndex);
      const tableData = getRenderedTableData(cellIndex, outputIndex, payload);
      const header = payload.columns.map((column, columnIndex) => {
        const sortIndicator = state.sortColumn === columnIndex ? (state.sortDirection === 'asc' ? ' ↑' : ' ↓') : '';
        const filterIndicator = state.filters[columnIndex] ? ' *' : '';
        const headingStyle = state.columnColors[columnIndex] ? ' style="color:' + escapeHtmlAttribute(state.columnColors[columnIndex]) + ';"' : '';
        const menuOpen = openTableMenu && openTableMenu.cellIndex === cellIndex && openTableMenu.outputIndex === outputIndex && openTableMenu.columnIndex === columnIndex;
        return '<th' + headingStyle + '>' +
          '<div class="output-table-heading">' +
            '<button type="button" class="output-table-sort" data-table-sort="true" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '" data-column-index="' + columnIndex + '">' + escapeHtml(column) + sortIndicator + filterIndicator + '</button>' +
            '<button type="button" class="output-table-menu-toggle" data-table-menu-toggle="true" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '" data-column-index="' + columnIndex + '">▾</button>' +
            (menuOpen ? renderTableMenu(cellIndex, outputIndex, columnIndex) : '') +
          '</div>' +
        '</th>';
      }).join('');

      const body = tableData.rows.map((row) => '<tr>' + row.map((value, columnIndex) => {
        const cellStyle = state.columnColors[columnIndex] ? ' style="color:' + escapeHtmlAttribute(state.columnColors[columnIndex]) + ';"' : '';
        return '<td' + cellStyle + '>' + escapeHtml(value === null || value === undefined ? '' : String(value)) + '</td>';
      }).join('') + '</tr>').join('');

      return '<div class="output-table">' +
        '<div class="output-table-toolbar"><button type="button" data-table-export="true" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '">Export CSV</button></div>' +
        '<div class="output-table-scroll"><table class="output-table-grid"><thead><tr>' + header + '</tr></thead><tbody>' + body + '</tbody></table></div>' +
        (payload.truncated ? '<div class="output-table-note">Output truncated by Databricks.</div>' : '') +
      '</div>';
    }

    function renderTableMenu(cellIndex, outputIndex, columnIndex) {
      const tableState = ensureTableViewState(cellIndex, outputIndex);
      const filterValue = tableState.filters[columnIndex] || '';
      const colorValue = normalizeColorPickerValue(tableState.columnColors[columnIndex]);
      return '<div class="output-table-menu" data-table-menu="true">' +
        '<div class="output-table-menu-section">' +
          '<div class="output-table-menu-label">Filter column</div>' +
          '<div class="output-table-menu-row">' +
            '<input type="text" data-table-filter-input="true" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '" data-column-index="' + columnIndex + '" value="' + escapeHtmlAttribute(filterValue) + '" placeholder="Contains text" />' +
            '<button type="button" data-table-filter-apply="true" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '" data-column-index="' + columnIndex + '">Apply</button>' +
            '<button type="button" data-table-filter-clear="true" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '" data-column-index="' + columnIndex + '">Clear</button>' +
          '</div>' +
        '</div>' +
        '<button type="button" data-table-menu-action="copy" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '" data-column-index="' + columnIndex + '">Copy column name</button>' +
        '<div class="output-table-menu-section">' +
          '<div class="output-table-menu-label">Change column color</div>' +
          '<div class="output-table-menu-row">' +
            '<input type="color" data-table-color-input="true" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '" data-column-index="' + columnIndex + '" value="' + escapeHtmlAttribute(colorValue) + '" />' +
            '<button type="button" data-table-color-clear="true" data-cell-index="' + cellIndex + '" data-output-index="' + outputIndex + '" data-column-index="' + columnIndex + '">Clear</button>' +
          '</div>' +
        '</div>' +
      '</div>';
    }

    function handleTableMenuAction(actionName, cellIndex, outputIndex, columnIndex) {
      if (!Number.isInteger(cellIndex) || !Number.isInteger(outputIndex) || !Number.isInteger(columnIndex)) {
        return;
      }

      const payload = getInteractiveTablePayload(cellIndex, outputIndex);
      if (!payload) {
        return;
      }

      if (actionName === 'copy') {
        vscode.postMessage({ type: 'copyText', value: payload.columns[columnIndex] || '' });
        openTableMenu = null;
        queueRender();
      }
    }

    function applyTableFilter(cellIndex, outputIndex, columnIndex) {
      if (!Number.isInteger(cellIndex) || !Number.isInteger(outputIndex) || !Number.isInteger(columnIndex)) {
        return;
      }

      const input = document.querySelector('input[data-table-filter-input="true"][data-cell-index="' + cellIndex + '"][data-output-index="' + outputIndex + '"][data-column-index="' + columnIndex + '"]');
      const tableState = ensureTableViewState(cellIndex, outputIndex);
      tableState.filters[columnIndex] = String(input?.value || '').trim();
      queueRender();
    }

    function clearTableFilter(cellIndex, outputIndex, columnIndex) {
      if (!Number.isInteger(cellIndex) || !Number.isInteger(outputIndex) || !Number.isInteger(columnIndex)) {
        return;
      }

      const tableState = ensureTableViewState(cellIndex, outputIndex);
      tableState.filters[columnIndex] = '';
      queueRender();
    }

    function clearTableColor(cellIndex, outputIndex, columnIndex) {
      if (!Number.isInteger(cellIndex) || !Number.isInteger(outputIndex) || !Number.isInteger(columnIndex)) {
        return;
      }

      const tableState = ensureTableViewState(cellIndex, outputIndex);
      tableState.columnColors[columnIndex] = '';
      queueRender();
    }

    function normalizeColorPickerValue(value) {
      return /^#[0-9a-fA-F]{6}$/.test(String(value || '')) ? String(value) : '#ffffff';
    }

    function exportTableAsCsv(cellIndex, outputIndex) {
      const payload = getInteractiveTablePayload(cellIndex, outputIndex);
      if (!payload) {
        return;
      }

      const tableData = getRenderedTableData(cellIndex, outputIndex, payload);
      vscode.postMessage({
        type: 'exportCsv',
        columns: tableData.columns,
        rows: tableData.rows,
      });
    }

    function getRenderedTableData(cellIndex, outputIndex, payload) {
      const state = ensureTableViewState(cellIndex, outputIndex);
      let rows = payload.rows.slice();
      state.filters.forEach((filterValue, columnIndex) => {
        if (!filterValue) {
          return;
        }
        const needle = filterValue.toLowerCase();
        rows = rows.filter((row) => String(row[columnIndex] === undefined || row[columnIndex] === null ? '' : row[columnIndex]).toLowerCase().includes(needle));
      });

      if (state.sortColumn !== null) {
        rows = rows.slice().sort((left, right) => compareTableValues(left[state.sortColumn], right[state.sortColumn], state.sortDirection));
      }

      return {
        columns: payload.columns,
        rows,
      };
    }

    function compareTableValues(left, right, direction) {
      const multiplier = direction === 'desc' ? -1 : 1;
      const leftNumber = Number(left);
      const rightNumber = Number(right);
      if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
        return (leftNumber - rightNumber) * multiplier;
      }

      return String(left === undefined || left === null ? '' : left).localeCompare(String(right === undefined || right === null ? '' : right), undefined, { numeric: true, sensitivity: 'base' }) * multiplier;
    }

    function getInteractiveTablePayload(cellIndex, outputIndex) {
      const result = runResults.get(cellIndex);
      const output = result?.outputs?.[outputIndex];
      if (!output || output.mime !== 'application/x-databricks-table+json') {
        return undefined;
      }
      return parseInteractiveTablePayload(output.value);
    }

    function parseInteractiveTablePayload(rawValue) {
      try {
        const payload = JSON.parse(String(rawValue || '{}'));
        if (!Array.isArray(payload.columns) || !Array.isArray(payload.rows)) {
          return undefined;
        }
        return payload;
      } catch {
        return undefined;
      }
    }

    function ensureTableViewState(cellIndex, outputIndex) {
      const key = getTableStateKey(cellIndex, outputIndex);
      const existing = tableViewState.get(key);
      if (existing) {
        return existing;
      }

      const next = {
        sortColumn: null,
        sortDirection: 'asc',
        filters: [],
        columnColors: {},
      };
      tableViewState.set(key, next);
      return next;
    }

    function resetTableState(cellIndex) {
      Array.from(tableViewState.keys()).forEach((key) => {
        if (key.startsWith(cellIndex + ':')) {
          tableViewState.delete(key);
        }
      });
      if (openTableMenu && openTableMenu.cellIndex === cellIndex) {
        openTableMenu = null;
      }
    }

    function getTableStateKey(cellIndex, outputIndex) {
      return cellIndex + ':' + outputIndex;
    }

    function escapeHtml(value) {
      return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    }

    function escapeHtmlAttribute(value) {
      return String(value)
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    }

    function renderCodePreview(value, type) {
      const text = String(value || '');
      return fallbackRenderCodePreview(text, type);
    }

    function renderMarkdownPreview(value) {
      if (!markdownRenderer && typeof window.markdownit === 'function') {
        markdownRenderer = window.markdownit({
          html: false,
          linkify: true,
          typographer: true,
        });
      }

      if (!markdownRenderer) {
        return '<pre>' + escapeHtml(String(value || '')) + '</pre>';
      }

      return markdownRenderer.render(String(value || ''));
    }

    function syncCellPreview(index) {
      const cellRoot = document.querySelector('[data-cell-index="' + index + '"]');
      if (!cellRoot) {
        return;
      }

      const preview = cellRoot.querySelector('pre.code');
      if (!preview) {
        return;
      }

      const cell = state.document.cells[index] || createEmptyCell('python');
      const type = supportedCellTypes.includes(cell.type) ? cell.type : 'python';
      preview.className = highlightedLanguageSupport.has(type) ? 'code ' + type : 'code';
      preview.innerHTML = renderCodePreview(cell.value || '', type);

      const badge = cellRoot.querySelector('.cell-kind');
      if (badge) {
        const valueNode = badge.querySelector('.cell-kind-value');
        if (valueNode) {
          valueNode.textContent = (index + 1) + '. ' + (cellTypeLabels[type] || type);
        }
      }

      const lineNumbers = cellRoot.querySelector('[data-line-numbers="' + index + '"]');
      if (lineNumbers) {
        lineNumbers.innerHTML = renderLineNumbers(cell.value || '');
      }

      const title = cellRoot.querySelector('.cell-title-input');
      if (title) {
        title.value = cell.title || '';
      }
    }

    function ensureMonacoReady() {
      if (monacoReady) {
        return Promise.resolve();
      }

      if (monacoReadyPromise) {
        return monacoReadyPromise;
      }

      monacoReadyPromise = new Promise((resolve) => {
        const onReady = () => {
          if (window.monaco?.editor?.defineTheme && window.monaco?.editor?.setTheme) {
            window.monaco.editor.defineTheme(monacoThemeName, {
              base: 'vs-dark',
              inherit: true,
              rules: [],
              colors: {},
            });
            window.monaco.editor.setTheme(monacoThemeName);
          }
          monacoReady = true;
          resolve();
        };

        if (window.monaco?.editor?.colorize) {
          onReady();
          return;
        }

        window.require = { paths: { vs: monacoBaseUri + '/vs' } };
        const script = document.createElement('script');
        script.src = monacoBaseUri + '/vs/loader.js';
        script.onload = () => {
          window.require(['vs/editor/editor.main'], () => {
            onReady();
            applyMonacoTokenization();
          });
        };
        script.onerror = () => {
          resolve();
        };
        document.head.appendChild(script);
      });

      return monacoReadyPromise;
    }

    function applyMonacoTokenization() {
      if (!monacoReady || !window.monaco?.editor?.colorize) {
        return;
      }

      document.querySelectorAll('section[data-cell-index]').forEach((cellRoot) => {
        const index = Number(cellRoot.dataset.cellIndex);
        if (!Number.isInteger(index) || !state.document.cells[index]) {
          return;
        }

        const preview = cellRoot.querySelector('pre.code');
        if (!preview) {
          return;
        }

        colorizeCell(index);
      });
    }

    function colorizeCell(index) {
      const cellRoot = document.querySelector('[data-cell-index="' + index + '"]');
      if (!cellRoot || !state.document.cells[index]) {
        return;
      }

      const preview = cellRoot.querySelector('pre.code');
      if (!preview) {
        return;
      }

      const cell = state.document.cells[index];
      const type = supportedCellTypes.includes(cell.type) ? cell.type : 'python';
      const text = String(cell.value || '');
      const monacoLanguage = monacoLanguageMap[type];
      if (!monacoReady || !window.monaco?.editor?.colorize || !monacoLanguage) {
        preview.className = highlightedLanguageSupport.has(type) ? 'code ' + type : 'code';
        preview.innerHTML = fallbackRenderCodePreview(text, type);
        return;
      }

      window.monaco.editor.colorize(text || ' ', monacoLanguage, { tabSize: 4 }).then((html) => {
        const currentCell = state.document.cells[index];
        if (!currentCell || String(currentCell.value || '') !== text) {
          return;
        }

        const latestPreview = cellRoot.querySelector('pre.code');
        if (!latestPreview) {
          return;
        }

        latestPreview.className = 'code monaco-colorized ' + escapeHtmlAttribute(type);
        latestPreview.innerHTML = renderCodeRowsFromMonacoHtml(html, text);
      }).catch(() => {
        const latestPreview = cellRoot.querySelector('pre.code');
        if (latestPreview) {
          latestPreview.className = highlightedLanguageSupport.has(type) ? 'code ' + type : 'code';
          latestPreview.innerHTML = fallbackRenderCodePreview(text, type);
        }
      });
    }

    function fallbackRenderCodePreview(text, type) {
      let highlighted;
      if (!highlightedLanguageSupport.has(type)) {
        highlighted = escapeHtml(text) || ' ';
        return renderCodeRowsFromHtml(highlighted, text);
      }

      if (type === 'md') {
        highlighted = highlightMarkdown(text);
        return renderCodeRowsFromHtml(highlighted, text);
      }

      highlighted = highlightCode(text, type);
      return renderCodeRowsFromHtml(highlighted, text);
    }

    function renderCodeRowsFromMonacoHtml(html, sourceText) {
      const rows = String(html || '').replace(/<br\/>$/u, '').split('<br/>');
      return renderCodeRows(rows, getCellLineCount(sourceText));
    }

    function renderCodeRowsFromHtml(html, sourceText) {
      return renderCodeRows(String(html || '').split('\n'), getCellLineCount(sourceText));
    }

    function renderCodeRows(rows, lineCount) {
      const renderedRows = [];
      for (let index = 0; index < lineCount; index += 1) {
        renderedRows.push('<span class="code-line">' + (rows[index] || ' ') + '</span>');
      }
      return renderedRows.join('');
    }

    function renderLineNumbers(text) {
      const lineCount = getCellLineCount(text);
      const lines = [];
      for (let index = 1; index <= lineCount; index += 1) {
        lines.push('<span class="line-number">' + index + '</span>');
      }
      return lines.join('');
    }

    function getCellLineCount(text) {
      return Math.max(1, String(text || '').split('\n').length);
    }

    function documentsEqual(leftDocument, rightDocument) {
      return JSON.stringify(leftDocument || { cells: [] }) === JSON.stringify(rightDocument || { cells: [] });
    }

    function restoreActiveCellEdit() {
      if (!activeCellEdit) {
        return;
      }

      const textarea = document.querySelector('textarea[data-cell-text="' + activeCellEdit.index + '"]');
      if (!textarea) {
        activeCellEdit = null;
        return;
      }

      if (document.activeElement === textarea) {
        return;
      }

      textarea.focus({ preventScroll: true });
      const maxSelection = textarea.value.length;
      const selectionStart = Math.min(activeCellEdit.selectionStart ?? maxSelection, maxSelection);
      const selectionEnd = Math.min(activeCellEdit.selectionEnd ?? selectionStart, maxSelection);
      textarea.setSelectionRange(selectionStart, selectionEnd);
      textarea.scrollTop = activeCellEdit.scrollTop || 0;
    }

    function highlightMarkdown(text) {
      const lines = text.split('\n');
      const highlighted = lines.map((line) => {
        if (/^#{1,6}\s+/.test(line)) {
          return '<span class="tok-heading">' + highlightInlineMarkdown(line) + '</span>';
        }

        return highlightInlineMarkdown(line);
      }).join('\n');

      return highlighted || ' ';
    }

    function highlightInlineMarkdown(line) {
      let html = '';
      let index = 0;
      while (index < line.length) {
        if (line[index] === BACKTICK) {
          const closingIndex = line.indexOf(BACKTICK, index + 1);
          if (closingIndex > index + 1) {
            const codeSegment = line.slice(index, closingIndex + 1);
            html += '<span class="tok-string">' + escapeHtml(codeSegment) + '</span>';
            index = closingIndex + 1;
            continue;
          }
        }

        const linkMatch = line.slice(index).match(/^\[([^\]]+)\]\(([^)]+)\)/);
        if (linkMatch) {
          html += '<span class="tok-link">' + escapeHtml(linkMatch[0]) + '</span>';
          index += linkMatch[0].length;
          continue;
        }

        html += escapeHtml(line[index]);
        index += 1;
      }

      return html;
    }

    function highlightCode(text, type) {
      const keywords = highlightedKeywords[type] || new Set();
      let html = '';
      let index = 0;

      while (index < text.length) {
        const blockCommentLength = getBlockCommentLength(text, index, type);
        if (blockCommentLength) {
          html += '<span class="tok-comment">' + escapeHtml(text.slice(index, index + blockCommentLength)) + '</span>';
          index += blockCommentLength;
          continue;
        }

        const lineCommentLength = getLineCommentLength(text, index, type);
        if (lineCommentLength) {
          html += '<span class="tok-comment">' + escapeHtml(text.slice(index, index + lineCommentLength)) + '</span>';
          index += lineCommentLength;
          continue;
        }

        const stringLength = getStringLength(text, index);
        if (stringLength) {
          html += '<span class="tok-string">' + escapeHtml(text.slice(index, index + stringLength)) + '</span>';
          index += stringLength;
          continue;
        }

        const numberMatch = text.slice(index).match(/^\d+(?:\.\d+)?/);
        if (numberMatch) {
          html += '<span class="tok-number">' + escapeHtml(numberMatch[0]) + '</span>';
          index += numberMatch[0].length;
          continue;
        }

        const identifierMatch = text.slice(index).match(/^[A-Za-z_][A-Za-z0-9_]*/);
        if (identifierMatch) {
          const identifier = identifierMatch[0];
          const keywordKey = type === 'sql' ? identifier.toLowerCase() : identifier;
          if (keywords.has(keywordKey)) {
            html += '<span class="tok-keyword">' + escapeHtml(identifier) + '</span>';
          } else {
            html += escapeHtml(identifier);
          }
          index += identifier.length;
          continue;
        }

        if (/^[=+\-*/%<>!&|^~:,.;()\[\]{}]$/.test(text[index])) {
          html += '<span class="tok-operator">' + escapeHtml(text[index]) + '</span>';
          index += 1;
          continue;
        }

        html += escapeHtml(text[index]);
        index += 1;
      }

      return html || ' ';
    }

    function getLineCommentLength(text, index, type) {
      const startsWith = (value) => text.startsWith(value, index);
      const isPythonLike = type === 'python' || type === 'r';
      if ((isPythonLike && startsWith('#')) || (type === 'sql' && startsWith('--')) || (type === 'scala' && startsWith('//'))) {
        const newlineIndex = text.indexOf('\n', index);
        return newlineIndex === -1 ? text.length - index : newlineIndex - index;
      }

      return 0;
    }

    function getBlockCommentLength(text, index, type) {
      if (type !== 'sql' && type !== 'scala') {
        return 0;
      }

      if (!text.startsWith('/*', index)) {
        return 0;
      }

      const endIndex = text.indexOf('*/', index + 2);
      return endIndex === -1 ? text.length - index : (endIndex + 2 - index);
    }

    function getStringLength(text, index) {
      const quote = text[index];
      if (quote !== '\'' && quote !== '"' && quote !== BACKTICK) {
        return 0;
      }

      let cursor = index + 1;
      while (cursor < text.length) {
        const current = text[cursor];
        if (current === '\\') {
          cursor += 2;
          continue;
        }
        if (current === quote) {
          return cursor + 1 - index;
        }
        cursor += 1;
      }

      return text.length - index;
    }

    function autoSizeAllTextareas() {
      document.querySelectorAll('textarea[data-cell-text]').forEach((textarea) => autoSizeTextarea(textarea));
    }

    function autoSizeTextarea(textarea) {
      const editorMain = textarea.parentElement;
      const lineNumbers = textarea.closest('.editor-wrap')?.querySelector('.line-numbers');
      const nextHeight = Math.max(editorMinHeightPx, getCellLineCount(textarea.value) * editorLineHeightPx + editorVerticalPaddingPx);
      textarea.style.height = nextHeight + 'px';
      if (editorMain) {
        editorMain.style.minHeight = nextHeight + 'px';
      }
      if (lineNumbers) {
        lineNumbers.style.minHeight = nextHeight + 'px';
      }
    }

    vscode.postMessage({ type: 'ready' });
  </script>
</body>
</html>`;
  }
}

class NotebookSessionManager {
  constructor(context, cli) {
    this._context = context;
    this._cli = cli;
    this._sessions = new Map();
  }

  async ensureSessionForDocument(document) {
    const selection = await ensureExecutionSelection(this._context, this._cli);
    const key = document.uri.toString();
    const existing = this._sessions.get(key);
    if (existing && (existing.profile !== selection.profile || existing.clusterId !== selection.clusterId)) {
      await this._disposeSession(key, existing);
    }

    if (!this._sessions.has(key)) {
      this._sessions.set(key, {
        profile: selection.profile,
        clusterId: selection.clusterId,
        clusterName: selection.clusterName,
        contexts: new Map(),
      });
    }

    return this._sessions.get(key);
  }

  async ensureLanguageContext(document, dbLanguage) {
    const session = await this.ensureSessionForDocument(document);
    if (session.contexts.has(dbLanguage)) {
      return session.contexts.get(dbLanguage);
    }

    const contextId = await this._cli.createContextAndWait(session.profile, session.clusterId, dbLanguage);
    session.contexts.set(dbLanguage, contextId);
    return contextId;
  }

  async prepareExecutionForDocument(document) {
    const session = await this.ensureSessionForDocument(document);
    const readiness = await this._cli.ensureClusterReady(session.profile, session.clusterId);
    if (readiness.started) {
      await this.restartForDocument(document);
      return this.ensureSessionForDocument(document);
    }

    return session;
  }

  async restartForDocument(document) {
    const key = document.uri.toString();
    const session = this._sessions.get(key);
    if (!session) {
      return;
    }

    await this._disposeSession(key, session);
  }

  async disposeForDocument(document) {
    const key = document.uri.toString();
    const session = this._sessions.get(key);
    if (!session) {
      return;
    }

    await this._disposeSession(key, session);
  }

  async disposeAll() {
    const entries = Array.from(this._sessions.entries());
    for (const [key, session] of entries) {
      await this._disposeSession(key, session);
    }
  }

  async _disposeSession(key, session) {
    this._sessions.delete(key);
    for (const contextId of session.contexts.values()) {
      try {
        await this._cli.destroyContext(session.profile, session.clusterId, contextId);
      } catch {
        // Ignore cleanup errors.
      }
    }
  }
}

class NotebookAutoSaveManager {
  constructor() {
    this._activeWrites = new Map();
  }

  dispose() {
    this._activeWrites.clear();
  }

  async handleChange(notebook) {
    if (!isDatabricksNotebookDocument(notebook)) {
      return;
    }

    await this._flush(notebook);
  }

  async handleFocusChange(notebook) {
    if (!isDatabricksNotebookDocument(notebook)) {
      return;
    }

    await this._flush(notebook);
  }

  clear(notebook) {
    this._activeWrites.delete(notebook.uri.toString());
  }

  async _flush(notebook) {
    if (!isDatabricksNotebookDocument(notebook)) {
      return;
    }

    if (notebook.uri.scheme === 'untitled' && !(notebook.metadata || {}).databricksSourceUri) {
      return;
    }

    const key = notebook.uri.toString();
    if (this._activeWrites.has(key)) {
      return this._activeWrites.get(key);
    }

    const writePromise = (async () => {
      try {
        await saveNotebookToSourceFile(notebook, { silent: true });
      } catch {
        // Keep autosave non-blocking.
      } finally {
        this._activeWrites.delete(key);
      }
    })();

    this._activeWrites.set(key, writePromise);

    await writePromise;
  }
}

class ClusterIdleMonitor {
  constructor(context, cli) {
    this._context = context;
    this._cli = cli;
    this._lastActivity = new Map();
    this._lastSourceActivity = new Map();
    this._timer = setInterval(() => {
      void this._poll();
    }, IDLE_CLUSTER_POLL_MS);
  }

  dispose() {
    clearInterval(this._timer);
    this._lastActivity.clear();
    this._lastSourceActivity.clear();
  }

  markActivity(notebook) {
    if (!isDatabricksNotebookDocument(notebook)) {
      return;
    }

    this._lastActivity.set(notebook.uri.toString(), Date.now());
  }

  clear(notebook) {
    this._lastActivity.delete(notebook.uri.toString());
  }

  markSourceActivity(document) {
    if (!document?.uri) {
      return;
    }

    this._lastSourceActivity.set(document.uri.toString(), Date.now());
  }

  clearSource(document) {
    if (!document?.uri) {
      return;
    }

    this._lastSourceActivity.delete(document.uri.toString());
  }

  async _poll() {
    const editor = vscode.window.activeNotebookEditor;
    const sourceDocument = sourceEditorProviderSingleton?.getActiveDocument();
    if ((!editor || !isDatabricksNotebookDocument(editor.notebook)) && !sourceDocument) {
      return;
    }

    const activityKey = sourceDocument?.uri?.toString() || editor.notebook.uri.toString();
    const lastActivity = sourceDocument
      ? (this._lastSourceActivity.get(activityKey) || 0)
      : (this._lastActivity.get(activityKey) || 0);
    if (Date.now() - lastActivity < IDLE_CLUSTER_POLL_MS) {
      return;
    }

    const selectedProfile = getSelectedProfileInfo(this._context);
    const selectedCluster = getSelectedClusterInfo(this._context);
    if (!selectedProfile?.name || !selectedCluster?.id) {
      return;
    }

    try {
      const cluster = await this._cli.getCluster(selectedProfile.name, selectedCluster.id);
      const rawState = String(cluster?.state || '').toUpperCase();
      const clusterName = cluster.cluster_name || selectedCluster.name || selectedCluster.id;
      const autoterminationMinutes = parseAutoterminationMinutes(cluster.autotermination_minutes);
      if (rawState === 'TERMINATED') {
        setClusterStatusSnapshot(selectedProfile.name, selectedCluster.id, {
          profile: selectedProfile.name,
          clusterId: selectedCluster.id,
          clusterName,
          clusterState: 'timedOut',
          autoterminationMinutes,
          startTimeMs: Number(cluster.start_time) || undefined,
        });
        const clusterLabel = formatClusterDisplayLabel({
          profile: selectedProfile.name,
          clusterId: selectedCluster.id,
          clusterName,
          clusterState: 'timedOut',
          autoterminationMinutes,
          startTimeMs: Number(cluster.start_time) || undefined,
        });
        await vscode.commands.executeCommand('setContext', 'databricksSourceNotebook.clusterState', 'timedOut');
        await updateNotebookControllerStatus({
          clusterState: 'timedOut',
          clusterLabel,
        });
      } else {
        await refreshDatabricksUi();
      }
    } catch {
      // Ignore passive poll errors.
    }
  }
}

class DatabricksCli {
  constructor(_context) {}

  async listProfiles() {
    const response = await this._runJson(['auth', 'profiles']);
    return Array.isArray(response.profiles) ? response.profiles : [];
  }

  async listClusters(profile) {
    const response = await this._runJson(['api', 'get', '/api/2.1/clusters/list'], profile);
    return Array.isArray(response.clusters) ? response.clusters : [];
  }

  async getCluster(profile, clusterId) {
    return this._runJson(['clusters', 'get', clusterId], profile);
  }

  async startClusterAndWait(profile, clusterId) {
    await this._runJson(['clusters', 'start', clusterId, '--timeout', formatCliTimeout()], profile);
    return this.waitForClusterRunning(profile, clusterId);
  }

  async stopClusterAndWait(profile, clusterId) {
    await this._runJson(['clusters', 'delete', clusterId, '--timeout', formatCliTimeout()], profile);
    return this.waitForClusterTerminated(profile, clusterId);
  }

  async waitForClusterRunning(profile, clusterId) {
    const timeoutMs = getTimeoutMs();
    return waitFor(async () => {
      const cluster = await this.getCluster(profile, clusterId);
      const state = String(cluster?.state || '').toUpperCase();
      if (state === 'RUNNING') {
        return { done: true, value: cluster };
      }

      if (['ERROR', 'TERMINATED', 'UNKNOWN'].includes(state)) {
        return { done: true, error: createClusterStateError(clusterId, cluster?.state) };
      }

      return { done: false };
    }, timeoutMs, `Timed out waiting for cluster ${clusterId} to reach RUNNING state.`);
  }

  async waitForClusterTerminated(profile, clusterId) {
    const timeoutMs = getTimeoutMs();
    return waitFor(async () => {
      const cluster = await this.getCluster(profile, clusterId);
      const state = String(cluster?.state || '').toUpperCase();
      if (state === 'TERMINATED') {
        return { done: true, value: cluster };
      }

      if (['ERROR', 'UNKNOWN'].includes(state)) {
        return { done: true, error: createClusterStateError(clusterId, cluster?.state) };
      }

      return { done: false };
    }, timeoutMs, `Timed out waiting for cluster ${clusterId} to reach TERMINATED state.`);
  }

  async ensureClusterReady(profile, clusterId) {
    const cluster = await this.getCluster(profile, clusterId);
    const state = String(cluster?.state || '').toUpperCase();
    if (state === 'RUNNING') {
      return { cluster, started: false };
    }

    if (state === 'TERMINATED') {
      const startedCluster = await this.startClusterAndWait(profile, clusterId);
      return { cluster: startedCluster, started: true };
    }

    if (['PENDING', 'RESTARTING', 'RESIZING'].includes(state)) {
      const readyCluster = await this.waitForClusterRunning(profile, clusterId);
      return { cluster: readyCluster, started: false };
    }

    throw createClusterStateError(clusterId, cluster?.state);
  }

  async createContextAndWait(profile, clusterId, dbLanguage) {
    const created = await this._runJson(
      ['api', 'post', '/api/1.2/contexts/create', '--json', JSON.stringify({ clusterId, language: dbLanguage })],
      profile
    );
    if (!created.id) {
      throw new Error('Databricks did not return an execution context ID.');
    }

    const timeoutMs = getTimeoutMs();
    return waitFor(async () => {
      const status = await this._runJson(
        ['api', 'get', withQuery('/api/1.2/contexts/status', { clusterId, contextId: created.id })],
        profile
      );

      if (status.status === 'Running') {
        return { done: true, value: created.id };
      }

      if (status.status === 'Error') {
        return { done: true, error: new Error('Databricks failed to create an execution context.') };
      }

      return { done: false };
    }, timeoutMs, `Timed out waiting for a ${dbLanguage} execution context on cluster ${clusterId}.`);
  }

  async executeAndWait(profile, clusterId, contextId, dbLanguage, command) {
    const created = await this._runJson(
      [
        'api',
        'post',
        '/api/1.2/commands/execute',
        '--json',
        JSON.stringify({ clusterId, contextId, language: dbLanguage, command }),
      ],
      profile
    );
    if (!created.id) {
      throw new Error('Databricks did not return a command ID.');
    }

    const timeoutMs = getTimeoutMs();
    return waitFor(async () => {
      const status = await this._runJson(
        [
          'api',
          'get',
          withQuery('/api/1.2/commands/status', {
            clusterId,
            contextId,
            commandId: created.id,
          }),
        ],
        profile
      );

      if (status.status === 'Finished' || status.status === 'Error') {
        return { done: true, value: status };
      }

      if (status.status === 'Cancelled' || status.status === 'Cancelling') {
        return { done: true, error: new Error('Databricks command was cancelled.') };
      }

      return { done: false };
    }, timeoutMs, `Timed out waiting for Databricks to finish executing the ${dbLanguage} cell.`);
  }

  async destroyContext(profile, clusterId, contextId) {
    await this._runJson(
      ['api', 'post', '/api/1.2/contexts/destroy', '--json', JSON.stringify({ clusterId, contextId })],
      profile
    );
  }

  async _runJson(args, profile) {
    const cliPath = getConfiguration().get('cliPath', 'databricks');
    const fullArgs = [...args];
    if (profile) {
      fullArgs.push('-p', profile);
    }
    fullArgs.push('-o', 'json');

    const result = await runProcess(cliPath, fullArgs, getWorkspaceCwd());
    try {
      return JSON.parse(result.stdout || '{}');
    } catch (error) {
      logError('Databricks CLI returned invalid JSON.', {
        command: cliPath,
        args: fullArgs,
        stdout: result.stdout,
        stderr: result.stderr,
        error: normalizeError(error).message,
      });
      throw new Error(`Databricks CLI returned invalid JSON. ${error.message}`);
    }
  }
}

class DatabricksProfilesProvider {
  constructor(context, cli) {
    this._context = context;
    this._cli = cli;
    this._onDidChangeTreeData = new vscode.EventEmitter();
    this.onDidChangeTreeData = this._onDidChangeTreeData.event;
  }

  dispose() {
    this._onDidChangeTreeData.dispose();
  }

  refresh() {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element) {
    return element;
  }

  async getChildren(element) {
    if (element) {
      return [];
    }

    try {
      const selectedProfile = getSelectedProfileInfo(this._context);
      const profiles = sortProfiles(await this._cli.listProfiles(), selectedProfile?.name);
      return profiles.map((profile) => new DatabricksProfileTreeItem(profile, selectedProfile));
    } catch (error) {
      logWarning('Failed to load Databricks profiles.', error);
      return [new DatabricksMessageTreeItem(normalizeError(error).message)];
    }
  }
}

async function executeDatabricksCellRuntime(cli, sessions, extensionContext, cellLike, notebookLike) {
  const spec = getCellExecutionSpec(cellLike);
  if (cellLike.kind === vscode.NotebookCellKind.Markup) {
    return {
      ok: true,
      outputs: [],
      webviewOutputs: [],
    };
  }

  if (!SUPPORTED_EXECUTION_LANGUAGES.has(spec.dbLanguage)) {
    throw new Error(
      `${spec.label} cells are not supported by this prototype. Supported execution languages: python, sql, scala, r.`
    );
  }

  const sessionTarget = getSessionTargetForNotebookLike(notebookLike);
  const session = await sessions.prepareExecutionForDocument(sessionTarget);
  rememberClusterTouch(session.profile, session.clusterId);
  const contextId = await sessions.ensureLanguageContext(sessionTarget, spec.dbLanguage);
  const commandText = prepareExecutableCode(getCellText(cellLike), spec.magic);
  const response = await cli.executeAndWait(session.profile, session.clusterId, contextId, spec.dbLanguage, commandText);
  logVerbose('Databricks command finished.', {
    language: spec.dbLanguage,
    profile: session.profile,
    clusterId: session.clusterId,
    response,
  });
  return {
    ok: response.status !== 'Error',
    ...buildOutputsFromResponse(response),
  };
}

async function executeDatabricksCellForSourceEditor(cli, sessions, extensionContext, document, cell) {
  const documentTarget = {
    uri: document.uri,
    metadata: {
      databricksSourceUri: document.uri.toString(),
    },
  };

  try {
    const outputData = await executeDatabricksCellRuntime(cli, sessions, extensionContext, cell, documentTarget);
    return {
      ok: outputData.ok,
      outputs: outputData.webviewOutputs,
    };
  } catch (error) {
    const normalized = normalizeError(error);
    logError('Databricks source-editor cell execution failed.', normalized);
    void showExecutionErrorActions(extensionContext, normalized);
    return {
      ok: false,
      outputs: mapOutputItemsToWebview(buildExecutionErrorOutputItems(normalized)),
    };
  }
}

function getSessionTargetForNotebookLike(notebookLike) {
  const sourceUri = notebookLike?.metadata?.databricksSourceUri;
  if (!sourceUri) {
    return notebookLike;
  }

  const normalizedUri = typeof sourceUri === 'string' ? sourceUri : sourceUri.toString();
  return {
    uri: {
      toString: () => normalizedUri,
    },
    metadata: notebookLike.metadata,
  };
}

class DatabricksProfileTreeItem extends vscode.TreeItem {
  constructor(profile, selectedProfile) {
    super(profile.name, vscode.TreeItemCollapsibleState.None);
    this.profile = profile;
    this.contextValue = profile.valid ? 'databricksProfile' : 'databricksProfileNeedsLogin';
    this.description = describeProfile(profile, selectedProfile);
    this.tooltip = buildProfileTooltip(profile, selectedProfile);
    this.command = {
      command: profile.valid ? COMMANDS.connectProfile : COMMANDS.loginProfile,
      title: profile.valid ? 'Select Enviroment' : 'Login',
      arguments: [this],
    };
    this.iconPath = new vscode.ThemeIcon(profile.valid ? 'check' : 'warning');
  }
}

class DatabricksMessageTreeItem extends vscode.TreeItem {
  constructor(message) {
    super(message, vscode.TreeItemCollapsibleState.None);
    this.contextValue = 'databricksMessage';
    this.iconPath = new vscode.ThemeIcon('warning');
  }
}

function createNewNotebookData() {
  const firstCell = new vscode.NotebookCellData(vscode.NotebookCellKind.Code, '', 'python');
  firstCell.metadata = { databricks: { magic: 'python', dbLanguage: 'python', label: 'Python' } };
  const notebookData = new vscode.NotebookData([firstCell]);
  notebookData.metadata = {};
  return notebookData;
}

function buildExecutionErrorOutputItems(error) {
  if (isDatabricksAuthExpiredError(error)) {
    return [vscode.NotebookCellOutputItem.text(error.message)];
  }

  if (isDatabricksClusterNotReadyError(error)) {
    return [vscode.NotebookCellOutputItem.text(error.message)];
  }

  return [vscode.NotebookCellOutputItem.error(error)];
}

function buildDatabricksSourceEditorUiState(context) {
  const selectedProfile = getSelectedProfileInfo(context);
  const selectedCluster = getSelectedClusterInfo(context);
  const activeOperation = activeClusterOperation;
  let clusterState = 'none';
  let clusterLabel = 'Select cluster';
  let clusterActionLabel = 'Select cluster';
  let clusterActionState = 'none';
  let clusterActionDisabled = false;
  let canRun = false;
  const environmentButtonLabel = selectedProfile?.name
    ? `Enviroment: ${selectedProfile.name}`
    : 'Select Enviroment';

  if (activeOperation?.type === 'starting') {
    clusterState = 'starting';
    clusterActionState = 'starting';
    clusterLabel = activeOperation.name || activeOperation.id || 'cluster';
    const startingAt = typeof activeOperation.startedAt === 'number' ? activeOperation.startedAt : Date.now();
    clusterActionLabel = `🟡 Starting ${clusterLabel} (${formatMinutesSeconds(Date.now() - startingAt)})`;
    clusterActionDisabled = true;
  } else if (selectedProfile?.name && selectedCluster?.id) {
    const snapshot = getClusterStatusSnapshot(selectedProfile.name, selectedCluster.id);
    const isTimerResetPending = clusterTimerResetPendingKeys.has(getClusterKey(selectedProfile.name, selectedCluster.id));
    clusterState = snapshot?.clusterState || 'selected';
    clusterLabel = snapshot ? formatClusterDisplayLabel(snapshot) : (selectedCluster.name || selectedCluster.id);
    const clusterName = snapshot?.clusterName || selectedCluster.name || selectedCluster.id;
    if (clusterState === 'running') {
      const timerLabel = formatClusterTimerLabel(snapshot);
      clusterActionState = isTimerResetPending ? 'starting' : (timerLabel === '-:--' ? 'runningUnknownTimer' : 'running');
      clusterActionLabel = isTimerResetPending
        ? `🟡 Syncing ${clusterName} (-:--)`
        : timerLabel === '-:--'
        ? `🟢 Reset timer ${clusterName} (-:--)`
        : `🟢 Stop ${clusterName} (${timerLabel})`;
      clusterActionDisabled = isTimerResetPending;
      canRun = !isTimerResetPending;
    } else if (clusterState === 'starting') {
      clusterActionState = 'starting';
      clusterActionLabel = `🟡 Starting ${clusterName}`;
      clusterActionDisabled = true;
    } else if (clusterState === 'timedOut') {
      clusterActionState = 'timedOut';
      clusterActionLabel = `🔴 ${clusterName}`;
    } else {
      clusterActionState = clusterState;
      clusterActionLabel = 'Select cluster';
    }
  }

  return {
    hasSelectedProfile: Boolean(selectedProfile?.name),
    selectedProfileName: selectedProfile?.name || '',
    selectedProfileNeedsLogin: lastDatabricksUiState.selectedProfileNeedsLogin,
    hasSelectedCluster: Boolean(selectedCluster?.id),
    clusterState,
    clusterLabel,
    clusterActionLabel,
    clusterActionState,
    clusterActionDisabled,
    environmentButtonLabel,
    canRun,
  };
}

function serializeParsedNotebookForWebview(parsed) {
  return {
    cells: parsed.cells.map((cell) => {
      const meta = (cell.metadata && cell.metadata.databricks) || {};
      const type = normalizeMagic(meta.magic || languageIdToMagic(cell.languageId));
      const presentation = meta.presentation || {};
      return {
        type,
        label: meta.label || magicToCellSpec(type).label,
        value: cell.value,
        title: typeof presentation.title === 'string' ? presentation.title : '',
        hasCellMarker: presentation.hasCellMarker === true,
      };
    }),
  };
}

function deserializeWebviewDocument(document) {
  const cells = Array.isArray(document?.cells) ? document.cells : [];
  return {
    cells: cells.map((cell) => {
      const magic = normalizeMagic(cell.type || 'python');
      const spec = magicToCellSpec(magic);
      const metadata = {
        databricks: {
          magic,
          dbLanguage: spec.dbLanguage,
          label: spec.label,
          presentation: {
            title: typeof cell.title === 'string' ? cell.title : '',
            hasCellMarker: cell.hasCellMarker === true,
          },
        },
      };
      return {
        kind: spec.kind,
        languageId: spec.languageId,
        value: String(cell.value || ''),
        metadata,
      };
    }),
  };
}

function createNonce() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let index = 0; index < 32; index += 1) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

async function saveNotebookToSourceFile(notebook, options = {}) {
  if (!notebook || notebook.notebookType !== NOTEBOOK_TYPE) {
    if (!options.silent) {
      vscode.window.showWarningMessage('Open a Databricks source notebook first.');
    }
    return;
  }

  if (notebook.uri.scheme !== 'untitled' && !(notebook.metadata || {}).databricksSourceUri) {
    await notebook.save();
    return;
  }

  const sourceUri = await resolveSaveTargetUri(notebook);
  if (!sourceUri) {
    return;
  }

  const text = serializeSourceNotebook(notebookDocumentToSerializableData(notebook));
  await vscode.workspace.fs.writeFile(sourceUri, new TextEncoder().encode(text));
  if (!options.silent) {
    vscode.window.showInformationMessage(`Notebook saved to ${sourceUri.fsPath || sourceUri.toString()}.`);
  }
}

async function resolveSaveTargetUri(notebook) {
  const metadata = notebook.metadata || {};
  const existingUri = metadata.databricksSourceUri ? vscode.Uri.parse(metadata.databricksSourceUri) : undefined;
  if (existingUri) {
    return existingUri;
  }

  if (notebook.uri.scheme !== 'untitled') {
    return notebook.uri;
  }

  return vscode.window.showSaveDialog({
    saveLabel: 'Save Databricks source notebook',
    filters: {
      'Python source notebook': ['py'],
    },
  });
}

function notebookDocumentToSerializableData(notebook) {
  return {
    cells: notebook.getCells().map((cell) => ({
      kind: cell.kind,
      value: cell.document.getText(),
      metadata: cell.metadata,
      languageId: cell.document.languageId,
    })),
  };
}

function isDatabricksNotebookDocument(notebook) {
  return Boolean(notebook && notebook.notebookType === NOTEBOOK_TYPE);
}

function parseSourceNotebook(text) {
  const normalized = text.replace(/^\uFEFF/, '').replace(/\r\n/g, '\n');
  const lines = normalized.split('\n');
  const metadata = {};

  let startIndex = 0;
  if ((lines[0] || '').trim() === NOTEBOOK_HEADER) {
    startIndex = 1;
    if (lines[startIndex] === '') {
      startIndex += 1;
    }
  }

  const blocks = [];
  let current = [];
  for (const line of lines.slice(startIndex)) {
    if (isCellSeparator(line)) {
      blocks.push(current);
      current = [];
      continue;
    }

    current.push(line);
  }
  blocks.push(current);

  const cells = blocks.length === 1 && blocks[0].length === 0
    ? [makePythonCodeCell('')]
    : blocks.map((block) => parseCellBlock(stripDatabricksCellPadding(block)));

  return { cells, metadata };
}

function stripDatabricksCellPadding(lines) {
  if (!lines.length) {
    return lines;
  }

  // Databricks source files place a formatting newline before separators.
  const normalized = [...lines];
  if (normalized[normalized.length - 1] === '') {
    normalized.pop();
  }
  return normalized;
}

function parseCellBlock(lines) {
  const presentation = extractCellPresentation(lines);
  const normalizedLines = presentation.bodyLines;

  const commentMagic = unwrapMagicCommentCell(normalizedLines);
  if (commentMagic) {
    return createCellFromMagic(commentMagic.magic, commentMagic.bodyLines, presentation.metadata);
  }

  const rawMagic = unwrapRawMagicCell(normalizedLines);
  if (rawMagic) {
    return createCellFromMagic(rawMagic.magic, rawMagic.bodyLines, presentation.metadata);
  }

  return makePythonCodeCell(normalizedLines.join('\n'), presentation.metadata);
}

function extractCellPresentation(lines) {
  const metadata = {
    title: '',
    hasCellMarker: false,
  };
  const bodyLines = [...lines];

  while (bodyLines.length) {
    const line = bodyLines[0];
    const dbTitleMatch = line.match(/^\s*#\s*DBTITLE\s+\d+\s*,\s*(.*)$/);
    if (dbTitleMatch) {
      metadata.title = dbTitleMatch[1] || '';
      bodyLines.shift();
      continue;
    }

    if (/^\s*#\s*%%(?:\s.*)?$/.test(line)) {
      metadata.hasCellMarker = true;
      bodyLines.shift();
      continue;
    }

    break;
  }

  return {
    metadata,
    bodyLines,
  };
}

function unwrapMagicCommentCell(lines) {
  if (!lines.length) {
    return undefined;
  }

  const match = lines[0].match(/^\s*# MAGIC\s+%([^\s]+)(?:\s+(.*))?\s*$/);
  if (!match) {
    return undefined;
  }

  const bodyLines = [];
  if (match[2]) {
    bodyLines.push(match[2]);
  }

  for (const line of lines.slice(1)) {
    const magicLine = line.match(/^\s*# MAGIC(?:\s?(.*))?$/);
    if (!magicLine) {
      bodyLines.push(line);
      continue;
    }

    bodyLines.push(magicLine[1] || '');
  }

  return { magic: normalizeMagic(match[1]), bodyLines };
}

function unwrapRawMagicCell(lines) {
  if (!lines.length) {
    return undefined;
  }

  const match = lines[0].match(/^\s*%([^\s]+)(?:\s+(.*))?\s*$/);
  if (!match) {
    return undefined;
  }

  const bodyLines = [];
  if (match[2]) {
    bodyLines.push(match[2]);
  }

  bodyLines.push(...lines.slice(1));
  return { magic: normalizeMagic(match[1]), bodyLines };
}

function createCellFromMagic(magic, bodyLines, presentation = {}) {
  const value = bodyLines.join('\n');
  const spec = magicToCellSpec(magic);
  const metadata = {
    databricks: {
      magic,
      dbLanguage: spec.dbLanguage,
      label: spec.label,
      presentation: {
        title: typeof presentation.title === 'string' ? presentation.title : '',
        hasCellMarker: presentation.hasCellMarker === true,
      },
    },
  };

  if (spec.kind === vscode.NotebookCellKind.Markup) {
    return {
      kind: vscode.NotebookCellKind.Markup,
      languageId: 'markdown',
      value,
      metadata,
    };
  }

  return {
    kind: vscode.NotebookCellKind.Code,
    languageId: spec.languageId,
    value,
    metadata,
  };
}

function makePythonCodeCell(value, presentation = {}) {
  return {
    kind: vscode.NotebookCellKind.Code,
    languageId: 'python',
    value,
    metadata: {
      databricks: {
        magic: 'python',
        dbLanguage: 'python',
        label: 'Python',
        presentation: {
          title: typeof presentation.title === 'string' ? presentation.title : '',
          hasCellMarker: presentation.hasCellMarker === true,
        },
      },
    },
  };
}

function serializeSourceNotebook(data) {
  const lines = [NOTEBOOK_HEADER];
  data.cells.forEach((cell, index) => {
    lines.push('');
    if (index > 0) {
      lines.push(CELL_SEPARATOR);
    }

    lines.push(...serializeCell(cell));
  });

  if (!data.cells.length) {
    lines.push('');
  }

  return `${lines.join('\n')}\n`;
}

function serializeCell(cell) {
  const spec = getCellSerializationSpec(cell);
  const text = cell.value || '';
  const cellLines = text.split(/\r?\n/);
  const lines = [];

  if (spec.title) {
    lines.push(`# DBTITLE 1,${spec.title}`);
  }

  if (spec.hasCellMarker) {
    lines.push('# %%');
  }

  if (cell.kind === vscode.NotebookCellKind.Markup) {
    return lines.concat(serializeMagicCell('md', cellLines));
  }

  if (spec.magic && spec.magic !== 'python') {
    return lines.concat(serializeMagicCell(spec.magic, cellLines));
  }

  return lines.concat(cellLines);
}

function serializeMagicCell(magic, bodyLines) {
  const lines = [`${MAGIC_PREFIX} %${magic}`];
  if (bodyLines.length === 1 && bodyLines[0] === '') {
    return lines;
  }

  for (const line of bodyLines) {
    lines.push(line ? `${MAGIC_PREFIX} ${line}` : MAGIC_PREFIX);
  }

  return lines;
}

function getCellExecutionSpec(cell) {
  const meta = (cell.metadata && cell.metadata.databricks) || {};
  const magic = normalizeMagic(meta.magic || languageIdToMagic(getCellLanguageId(cell)));
  const spec = magicToCellSpec(magic);
  return {
    ...spec,
    magic,
  };
}

function getCellSerializationSpec(cell) {
  const meta = (cell.metadata && cell.metadata.databricks) || {};
  const presentation = meta.presentation || {};
  const magic = normalizeMagic(meta.magic || languageIdToMagic(getCellLanguageId(cell)));
  return {
    magic,
    title: typeof presentation.title === 'string' ? presentation.title : '',
    hasCellMarker: presentation.hasCellMarker === true,
  };
}

function getCellLanguageId(cell) {
  return cell?.document?.languageId || cell?.languageId || 'python';
}

function getCellText(cell) {
  if (typeof cell?.document?.getText === 'function') {
    return cell.document.getText();
  }

  return typeof cell?.value === 'string' ? cell.value : '';
}

function magicToCellSpec(magic) {
  switch (normalizeMagic(magic)) {
    case 'python':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'python', dbLanguage: 'python', label: 'Python' };
    case 'sql':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'sql', dbLanguage: 'sql', label: 'SQL' };
    case 'scala':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'scala', dbLanguage: 'scala', label: 'Scala' };
    case 'r':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'r', dbLanguage: 'r', label: 'R' };
    case 'md':
      return { kind: vscode.NotebookCellKind.Markup, languageId: 'markdown', dbLanguage: undefined, label: 'Markdown' };
    case 'sh':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'shellscript', dbLanguage: undefined, label: 'Shell' };
    case 'fs':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'plaintext', dbLanguage: undefined, label: 'Filesystem (%fs)' };
    case 'run':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'plaintext', dbLanguage: undefined, label: 'Notebook workflow (%run)' };
    case 'pip':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'plaintext', dbLanguage: undefined, label: 'Package management (%pip)' };
    case 'uv':
      return { kind: vscode.NotebookCellKind.Code, languageId: 'plaintext', dbLanguage: undefined, label: 'Package management (%uv)' };
    default:
      return { kind: vscode.NotebookCellKind.Code, languageId: 'plaintext', dbLanguage: undefined, label: magic || 'Unsupported' };
  }
}

function languageIdToMagic(languageId) {
  switch (languageId) {
    case 'python':
      return 'python';
    case 'sql':
      return 'sql';
    case 'scala':
      return 'scala';
    case 'r':
      return 'r';
    case 'markdown':
      return 'md';
    case 'shellscript':
      return 'sh';
    default:
      return languageId || 'python';
  }
}

function normalizeMagic(value) {
  if (!value) {
    return 'python';
  }

  const normalized = String(value).trim().toLowerCase();
  if (normalized === 'py') {
    return 'python';
  }
  if (normalized === 'markdown') {
    return 'md';
  }
  return normalized;
}

function isCellSeparator(line) {
  return /^\s*# COMMAND ----------\s*$/.test(line);
}

function prepareExecutableCode(text, magic) {
  const normalizedMagic = normalizeMagic(magic);
  const lines = text.replace(/\r\n/g, '\n').split('\n');
  const firstLine = lines[0] || '';
  const rawMagic = firstLine.match(/^\s*%([^\s]+)(?:\s+(.*))?\s*$/);
  if (!rawMagic || normalizeMagic(rawMagic[1]) !== normalizedMagic) {
    return normalizedMagic === 'python' ? wrapPythonDisplayCode(text) : text;
  }

  const rest = [];
  if (rawMagic[2]) {
    rest.push(rawMagic[2]);
  }
  rest.push(...lines.slice(1));
  return normalizedMagic === 'python' ? wrapPythonDisplayCode(rest.join('\n')) : rest.join('\n');
}

function wrapPythonDisplayCode(text) {
  return `${PYTHON_DISPLAY_SHIM}\n${text}`;
}

function buildOutputsFromResponse(response) {
  const outputs = [];
  const webviewOutputs = [];
  const results = response && response.results;
  if (!results) {
    return { outputs, webviewOutputs };
  }

  const resultType = String(readResultField(results, 'resultType', 'result_type') || '').toLowerCase();
  if (response.status === 'Error' || resultType === 'error') {
    const error = new Error(
      [readResultField(results, 'summary'), readResultField(results, 'cause')]
        .filter(Boolean)
        .join('\n') || 'Databricks reported an execution error.'
    );
    const item = vscode.NotebookCellOutputItem.error(error);
    return {
      outputs: [new vscode.NotebookCellOutput([item])],
      webviewOutputs: mapOutputItemsToWebview([item]),
    };
  }

  const items = [];
  if (resultType === 'table') {
    const webviewTable = buildWebviewTablePayload(results);
    const htmlTable = tryBuildHtmlTable(results);
    if (htmlTable) {
      items.push(vscode.NotebookCellOutputItem.text(htmlTable, 'text/html'));
    }

    const markdownTable = tryBuildMarkdownTable(results);
    if (markdownTable) {
      items.push(vscode.NotebookCellOutputItem.text(markdownTable, 'text/markdown'));
    }

    items.push(
      vscode.NotebookCellOutputItem.text(
        JSON.stringify(
          {
            schema: readResultField(results, 'schema'),
            data: readResultField(results, 'data'),
            truncated: readResultField(results, 'truncated'),
          },
          null,
          2
        ),
        'text/x-json'
      )
    );

    return {
      outputs: [new vscode.NotebookCellOutput(items)],
      webviewOutputs: webviewTable
        ? [{ mime: 'application/x-databricks-table+json', value: JSON.stringify(webviewTable) }]
        : mapOutputItemsToWebview(items),
    };
  } else if (resultType === 'image') {
    const fileName = readResultField(results, 'fileName', 'file_name');
    if (typeof fileName === 'string' && fileName.startsWith('data:image/')) {
      items.push(vscode.NotebookCellOutputItem.text(`<img src="${escapeHtmlAttribute(fileName)}" />`, 'text/html'));
    } else {
      items.push(vscode.NotebookCellOutputItem.text(fileName || 'Image output produced.'));
    }
  } else if (resultType === 'images') {
    const rawImages = readResultField(results, 'fileNames', 'file_names');
    const images = Array.isArray(rawImages) ? rawImages : [];
    const html = images
      .filter((value) => typeof value === 'string' && value.startsWith('data:image/'))
      .map((value) => `<img src="${escapeHtmlAttribute(value)}" />`)
      .join('<br/>');

    if (html) {
      items.push(vscode.NotebookCellOutputItem.text(html, 'text/html'));
    }

    if (!html || images.some((value) => typeof value !== 'string' || !value.startsWith('data:image/'))) {
      items.push(vscode.NotebookCellOutputItem.text(JSON.stringify(images, null, 2), 'text/x-json'));
    }
  } else if (readTextResult(results) !== undefined) {
    const textResult = readTextResult(results);
    const inlineTable = extractInlineTableResult(textResult);
    if (inlineTable) {
      const htmlTable = tryBuildHtmlTable(inlineTable);
      if (htmlTable) {
        items.push(vscode.NotebookCellOutputItem.text(htmlTable, 'text/html'));
      }

      const markdownTable = tryBuildMarkdownTable(inlineTable);
      if (markdownTable) {
        items.push(vscode.NotebookCellOutputItem.text(markdownTable, 'text/markdown'));
      }

      items.push(vscode.NotebookCellOutputItem.text(JSON.stringify({
        columns: inlineTable.schema.map((column) => column.name),
        data: inlineTable.data,
        truncated: inlineTable.truncated,
      }, null, 2), 'text/x-json'));

      return {
        outputs: [new vscode.NotebookCellOutput(items)],
        webviewOutputs: [{ mime: 'application/x-databricks-table+json', value: JSON.stringify(buildWebviewTablePayload(inlineTable)) }],
      };
    }

    items.push(vscode.NotebookCellOutputItem.text(textResult));
  } else if (readResultField(results, 'data') !== undefined) {
    items.push(vscode.NotebookCellOutputItem.text(JSON.stringify(readResultField(results, 'data'), null, 2), 'text/x-json'));
  }

  if (readResultField(results, 'truncated')) {
    items.push(vscode.NotebookCellOutputItem.text('Output truncated by Databricks.'));
  }

  if (!items.length) {
    items.push(vscode.NotebookCellOutputItem.text('Cell finished without output.'));
  }

  return {
    outputs: [new vscode.NotebookCellOutput(items)],
    webviewOutputs: mapOutputItemsToWebview(items),
  };
}

function mapOutputItemsToWebview(items) {
  return items.map((item) => {
    const mime = item.mime || 'text/plain';
    if (mime === 'application/vnd.code.notebook.error') {
      try {
        const parsed = JSON.parse(Buffer.from(item.data).toString('utf8'));
        return {
          mime,
          value: [parsed.name, parsed.message, parsed.stack].filter(Boolean).join('\n'),
        };
      } catch {
        return {
          mime,
          value: Buffer.from(item.data).toString('utf8'),
        };
      }
    }

    return {
      mime,
      value: Buffer.from(item.data).toString('utf8'),
    };
  });
}

function tryBuildMarkdownTable(results) {
  const table = readResultTable(results);
  if (!table) {
    return undefined;
  }

  const { columns, rows } = table;
  const header = `| ${columns.join(' | ')} |`;
  const divider = `| ${columns.map(() => '---').join(' | ')} |`;
  const body = rows.map((row) => `| ${row.map(formatMarkdownCellValue).join(' | ')} |`);
  return [header, divider, ...body].join('\n');
}

function tryBuildHtmlTable(results) {
  const table = readResultTable(results);
  if (!table) {
    return undefined;
  }

  const { columns, rows } = table;
  const header = columns
    .map((column) => `<th style="border:1px solid #444;padding:6px 8px;text-align:left;background:#1f1f1f;color:#f0f0f0;">${escapeHtml(column)}</th>`)
    .join('');
  const body = rows
    .map((row) => `<tr>${row.map((value) => `<td style="border:1px solid #444;padding:6px 8px;text-align:left;white-space:pre-wrap;vertical-align:top;">${escapeHtml(value === null || value === undefined ? '' : String(value))}</td>`).join('')}</tr>`)
    .join('');

  return `<table style="border-collapse:collapse;width:100%;font-family:var(--vscode-editor-font-family, sans-serif);font-size:12px;"><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table>`;
}

function buildWebviewTablePayload(results) {
  const table = readResultTable(results);
  if (!table) {
    return undefined;
  }

  return {
    columns: table.columns,
    rows: table.rows,
    truncated: readResultField(results, 'truncated') === true,
  };
}

function extractInlineTableResult(text) {
  const lines = String(text || '').split(/\r?\n/);
  const markerLine = lines.find((line) => line.startsWith(INLINE_TABLE_MARKER));
  if (!markerLine) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(markerLine.slice(INLINE_TABLE_MARKER.length));
    if (!Array.isArray(parsed.columns) || !Array.isArray(parsed.rows)) {
      return undefined;
    }

    return {
      schema: parsed.columns.map((name) => ({ name })),
      data: parsed.rows,
      truncated: parsed.truncated === true,
    };
  } catch {
    return undefined;
  }
}

function readTextResult(results) {
  const value = readResultField(results, 'data', 'summary', 'result');
  return typeof value === 'string' ? value : undefined;
}

function readResultTable(results) {
  const schema = Array.isArray(readResultField(results, 'schema')) ? readResultField(results, 'schema') : [];
  const columns = schema.map((column, index) => column.name || column.column_name || `col_${index + 1}`);
  if (!columns.length) {
    return undefined;
  }

  let rows = [];
  const data = readResultField(results, 'data');
  if (Array.isArray(data) && data.every((row) => Array.isArray(row))) {
    rows = data;
  } else if (Array.isArray(data) && data.every((row) => row && typeof row === 'object' && !Array.isArray(row))) {
    rows = data.map((row) => columns.map((column) => row[column]));
  } else {
    return undefined;
  }

  return { columns, rows };
}

function formatMarkdownCellValue(value) {
  if (value === null || value === undefined) {
    return '';
  }

  return String(value).replace(/\|/g, '\\|');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function buildCsvFromTable(columns, rows) {
  const csvRows = [columns, ...rows];
  return `${csvRows.map((row) => row.map(formatCsvCell).join(',')).join('\n')}\n`;
}

function formatCsvCell(value) {
  const text = value === null || value === undefined ? '' : String(value);
  if (!/[",\n\r]/.test(text)) {
    return text;
  }

  return `"${text.replace(/"/g, '""')}"`;
}

function escapeHtmlAttribute(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function readResultField(results, ...keys) {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(results, key)) {
      return results[key];
    }
  }

  return undefined;
}

async function ensureExecutionSelection(context, cli) {
  const profile = await ensureProfile(context, cli);
  if (!profile) {
    throw new Error('No Databricks profile selected.');
  }

  const cluster = await ensureCluster(context, cli, profile);
  if (!cluster) {
    throw new Error('No Databricks cluster selected.');
  }

  return {
    profile,
    clusterId: cluster.id,
    clusterName: cluster.name,
  };
}

async function applySelectedProfile(context, sessions, profile, options = {}) {
  const configured = (getConfiguration().get('profile', '') || '').trim();
  if (configured && configured !== profile) {
    const openSettings = 'Open Settings';
    const choice = await vscode.window.showWarningMessage(
      `databricksSourceNotebook.profile is set to ${configured} in settings. Clear or change that setting to switch profiles from the view.`,
      openSettings
    );
    if (choice === openSettings) {
      await vscode.commands.executeCommand('workbench.action.openSettings', `${CONFIG_SECTION}.profile`);
    }
    return false;
  }

  await context.workspaceState.update(STATE_PROFILE, profile);
  await context.workspaceState.update(STATE_CLUSTER, undefined);
  await sessions.disposeAll();
  await refreshDatabricksUi();
  if (options.showMessage !== false) {
    vscode.window.showInformationMessage(`Databricks profile set to ${profile}.`);
  }
  return true;
}

async function connectProfileCommand(context, cli, sessions, item) {
  if (item instanceof DatabricksProfileTreeItem) {
    await connectProfileItem(context, sessions, item);
    return;
  }

  const profile = await selectProfile(context, cli, true);
  if (!profile) {
    return;
  }

  const applied = await applySelectedProfile(context, sessions, profile, { showMessage: false });
  if (!applied) {
    return;
  }

  const cluster = await selectCluster(context, cli, profile, true);
  if (!cluster) {
    vscode.window.showInformationMessage(`Databricks profile set to ${profile}.`);
    return;
  }

  await context.workspaceState.update(STATE_CLUSTER, cluster);
  await sessions.disposeAll();
  await refreshDatabricksUi();
  vscode.window.showInformationMessage(`Databricks environment set to ${profile} on ${cluster.name || cluster.id}.`);
}

async function connectProfileItem(context, sessions, item) {
  if (!(item instanceof DatabricksProfileTreeItem)) {
    return;
  }

  if (!item.profile.valid) {
    await loginProfileCommand(context, item);
    return;
  }

  await applySelectedProfile(context, sessions, item.profile.name);
}

async function loginProfileCommand(context, item) {
  const explicitProfile = item instanceof DatabricksProfileTreeItem ? item.profile.name : undefined;
  const selectedProfile = getSelectedProfileInfo(context)?.name;
  const profile = explicitProfile || selectedProfile || (await promptForProfileToLogin());
  if (!profile) {
    return;
  }

  await reconnectProfile(profile, 'Complete Databricks login in the terminal.');
}

async function promptForProfileToLogin() {
  const profile = await vscode.window.showInputBox({
    prompt: 'Enter the Databricks profile to log in',
    ignoreFocusOut: true,
    validateInput: (value) => (value.trim() ? undefined : 'Profile name is required.'),
  });
  return profile ? profile.trim() : undefined;
}

async function refreshDatabricksUi() {
  profilesProviderSingleton?.refresh();
  if (extensionContextSingleton && databricksCliSingleton) {
    await updateDatabricksContexts(extensionContextSingleton, databricksCliSingleton);
  }
  await sourceEditorProviderSingleton?.notifyUiStateChanged();
}

async function updateNotebookControllerStatus({ clusterState, clusterLabel }) {
  if (!notebookControllerSingleton) {
    return;
  }

  const label = clusterLabel || 'Select cluster';
  if (clusterState === 'timedOut') {
    notebookControllerSingleton.setStatus({
      label,
      description: 'Stopped',
      detail: clusterLabel || 'Stopped',
    });
    return;
  }

  if (clusterState === 'running') {
    notebookControllerSingleton.setStatus({
      label,
      description: 'Running',
      detail: clusterLabel || 'Running',
    });
    return;
  }

  if (clusterState === 'starting') {
    notebookControllerSingleton.setStatus({
      label,
      description: 'Starting',
      detail: clusterLabel || 'Starting',
    });
    return;
  }

  if (clusterState === 'stopped') {
    notebookControllerSingleton.setStatus({
      label,
      description: 'Stopped',
      detail: clusterLabel || 'Stopped',
    });
    return;
  }

  if (clusterState === 'selected') {
    notebookControllerSingleton.setStatus({
      label,
      description: 'Selected',
      detail: clusterLabel || 'Cluster selected',
    });
    return;
  }

  notebookControllerSingleton.setStatus({
    label: 'Select cluster',
    description: 'Select cluster',
    detail: 'No cluster selected',
  });
}

function getClusterKey(profile, clusterId) {
  return `${profile}::${clusterId}`;
}

function rememberClusterTouch(profile, clusterId, options = {}) {
  if (!profile || !clusterId) {
    return;
  }

  const key = getClusterKey(profile, clusterId);
  if (options.ifAbsent && clusterActivityTimestamps.has(key)) {
    return;
  }

  clusterActivityTimestamps.set(key, typeof options.at === 'number' ? options.at : Date.now());
}

function clearClusterTouch(profile, clusterId) {
  if (!profile || !clusterId) {
    return;
  }

  clusterActivityTimestamps.delete(getClusterKey(profile, clusterId));
}

function setClusterStatusSnapshot(profile, clusterId, snapshot) {
  if (!profile || !clusterId) {
    return;
  }

  clusterStatusSnapshots.set(getClusterKey(profile, clusterId), {
    ...snapshot,
    profile,
    clusterId,
  });
}

function getClusterStatusSnapshot(profile, clusterId) {
  if (!profile || !clusterId) {
    return undefined;
  }

  return clusterStatusSnapshots.get(getClusterKey(profile, clusterId));
}

function parseAutoterminationMinutes(value) {
  const minutes = Number(value);
  return Number.isFinite(minutes) && minutes > 0 ? minutes : undefined;
}

function mapClusterState(rawState) {
  const normalized = String(rawState || '').toUpperCase();
  if (normalized === 'RUNNING') {
    return 'running';
  }
  if (normalized === 'TERMINATED') {
    return 'stopped';
  }
  if (['PENDING', 'RESTARTING', 'RESIZING'].includes(normalized)) {
    return 'starting';
  }
  return 'selected';
}

function getClusterElapsedMs(snapshot) {
  if (!snapshot?.profile || !snapshot?.clusterId) {
    return undefined;
  }

  const lastTouchAt = clusterActivityTimestamps.get(getClusterKey(snapshot.profile, snapshot.clusterId));
  return typeof lastTouchAt === 'number' ? Math.max(0, Date.now() - lastTouchAt) : undefined;
}

function formatClusterTimerLabel(snapshot) {
  const elapsedMs = getClusterElapsedMs(snapshot);
  const timeoutMinutes = parseAutoterminationMinutes(snapshot?.autoterminationMinutes);
  if (elapsedMs === undefined || timeoutMinutes === undefined) {
    return '-:--';
  }

  return formatMinutesSeconds((timeoutMinutes * 60000) - elapsedMs);
}

function formatMinutesSeconds(durationMs) {
  const totalSeconds = Math.max(0, Math.floor(durationMs / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, '0')}`;
}

function formatClusterDisplayLabel(snapshot) {
  const clusterName = snapshot?.clusterName || 'cluster';
  if (!snapshot || snapshot.clusterState !== 'running') {
    return clusterName;
  }

  return `${RUNNING_CLUSTER_MARKER} ${clusterName} (${formatClusterTimerLabel(snapshot)})`;
}

async function refreshNotebookControllerStatusFromCache() {
  if (!notebookControllerSingleton || !extensionContextSingleton) {
    return;
  }

  const editor = vscode.window.activeNotebookEditor;
  if (!editor || editor.notebook.notebookType !== NOTEBOOK_TYPE) {
    return;
  }

  if (activeClusterOperation?.type === 'starting') {
    await updateNotebookControllerStatus({
      clusterState: 'starting',
      clusterLabel: activeClusterOperation.name || activeClusterOperation.id,
    });
    return;
  }

  const selectedProfile = getSelectedProfileInfo(extensionContextSingleton);
  const selectedCluster = getSelectedClusterInfo(extensionContextSingleton);
  if (!selectedProfile?.name || !selectedCluster?.id) {
    return;
  }

  const snapshot = getClusterStatusSnapshot(selectedProfile.name, selectedCluster.id);
  if (!snapshot) {
    return;
  }

  await updateNotebookControllerStatus({
    clusterState: snapshot.clusterState,
    clusterLabel: formatClusterDisplayLabel(snapshot),
  });
}

async function updateDatabricksContexts(context, cli) {
  const selectedProfile = getSelectedProfileInfo(context);
  const selectedCluster = getSelectedClusterInfo(context);
  let needsLogin = false;
  let clusterState = 'none';
  let clusterLabel = selectedCluster?.name || selectedCluster?.id || 'cluster';

  if (selectedProfile) {
    try {
      const profiles = await cli.listProfiles();
      const profile = profiles.find((item) => item.name === selectedProfile.name);
      needsLogin = Boolean(profile && !profile.valid);
    } catch {
      needsLogin = false;
    }
  }

  lastDatabricksUiState.selectedProfileNeedsLogin = needsLogin;

  if (activeClusterOperation?.type === 'starting') {
    clusterState = 'starting';
    clusterLabel = activeClusterOperation.name || activeClusterOperation.id || clusterLabel;
  } else if (selectedCluster && selectedProfile) {
    try {
      const cluster = await cli.getCluster(selectedProfile.name, selectedCluster.id);
      const clusterName = cluster.cluster_name || selectedCluster.name || selectedCluster.id;
      clusterState = mapClusterState(cluster.state);
      const snapshot = {
        profile: selectedProfile.name,
        clusterId: selectedCluster.id,
        clusterName,
        clusterState,
        autoterminationMinutes: parseAutoterminationMinutes(cluster.autotermination_minutes),
        startTimeMs: Number(cluster.start_time) || undefined,
      };
      setClusterStatusSnapshot(selectedProfile.name, selectedCluster.id, snapshot);
      clusterLabel = formatClusterDisplayLabel(snapshot);
    } catch {
      clusterState = 'selected';
      const snapshot = {
        profile: selectedProfile.name,
        clusterId: selectedCluster.id,
        clusterName: selectedCluster.name || selectedCluster.id,
        clusterState,
      };
      setClusterStatusSnapshot(selectedProfile.name, selectedCluster.id, snapshot);
      clusterLabel = formatClusterDisplayLabel(snapshot);
    }
  }

  await Promise.all([
    vscode.commands.executeCommand('setContext', 'databricksSourceNotebook.hasSelectedProfile', Boolean(selectedProfile)),
    vscode.commands.executeCommand('setContext', 'databricksSourceNotebook.selectedProfileNeedsLogin', needsLogin),
    vscode.commands.executeCommand('setContext', 'databricksSourceNotebook.hasSelectedCluster', Boolean(selectedCluster)),
    vscode.commands.executeCommand('setContext', 'databricksSourceNotebook.clusterState', clusterState),
    vscode.commands.executeCommand('setContext', 'databricksSourceNotebook.clusterLabel', clusterLabel),
  ]);

  await updateNotebookControllerStatus({ clusterState, clusterLabel });
}

async function showExecutionErrorActions(context, error) {
  if (await showDatabricksAuthErrorActions(context, error)) {
    return true;
  }

  return showDatabricksClusterErrorActions(context, error);
}

async function reconnectProfile(profile, infoMessage) {
  const result = await openDatabricksLoginTerminal(profile);
  await refreshDatabricksUi();
  if (result === 'succeeded') {
    vscode.window.showInformationMessage(`Databricks login updated for profile ${profile}.`);
    return;
  }
  if (result === 'fallback') {
    vscode.window.showInformationMessage(
      `Databricks login started for profile ${profile}. Shell integration is unavailable, so the terminal may stay open.`
    );
    return;
  }
  if (result === 'failed') {
    vscode.window.showWarningMessage(`Databricks login failed for profile ${profile}. The terminal stayed open so you can inspect it.`);
    return;
  }
  if (infoMessage) {
    vscode.window.showInformationMessage(`${infoMessage} Profile: ${profile}.`);
  }
}

async function showDatabricksAuthErrorActions(context, error) {
  if (!isDatabricksAuthExpiredError(error)) {
    return false;
  }

  logError('Databricks authentication error.', error);
  const profile = getDatabricksAuthErrorProfile(context, error);
  const promptKey = profile || '__unknown__';
  if (activeAuthPromptKey === promptKey) {
    return true;
  }

  activeAuthPromptKey = promptKey;
  try {
    const reconnect = 'Login';
    const selectProfileAction = 'Select profile';
    const choice = await vscode.window.showErrorMessage(
      profile ? `Databricks login expired for profile ${profile}.` : 'Databricks login expired.',
      reconnect,
      selectProfileAction
    );

    if (choice === reconnect) {
      if (profile) {
        await reconnectProfile(profile, 'Complete Databricks login in the terminal, then rerun the cell.');
      } else {
        await vscode.commands.executeCommand(COMMANDS.selectProfile);
      }
    }

    if (choice === selectProfileAction) {
      await vscode.commands.executeCommand(COMMANDS.selectProfile);
    }

    await refreshDatabricksUi();
    return true;
  } finally {
    if (activeAuthPromptKey === promptKey) {
      activeAuthPromptKey = undefined;
    }
  }
}

async function showDatabricksClusterErrorActions(context, error) {
  if (!isDatabricksClusterNotReadyError(error)) {
    return false;
  }

  logError('Databricks cluster readiness error.', error);
  const selectedCluster = getSelectedClusterInfo(context);
  const promptKey = `${error.clusterId || selectedCluster?.id || '__unknown__'}:${error.clusterState || '__unknown__'}`;
  if (activeClusterPromptKey === promptKey) {
    return true;
  }

  activeClusterPromptKey = promptKey;
  try {
    const startClusterAction = 'Start cluster';
    const selectClusterAction = 'Select cluster';
    const actions = error.clusterState?.toLowerCase() === 'terminated'
      ? [startClusterAction, selectClusterAction]
      : [selectClusterAction];
    const choice = await vscode.window.showErrorMessage(error.message, ...actions);
    if (choice === startClusterAction) {
      const profile = getDatabricksClusterErrorProfile(context, error);
      const clusterId = error.clusterId || selectedCluster?.id;
      if (profile && clusterId) {
        await startClusterForExecution(profile, clusterId);
      }
    }

    if (choice === selectClusterAction) {
      await vscode.commands.executeCommand(COMMANDS.selectCluster);
    }

    return true;
  } finally {
    if (activeClusterPromptKey === promptKey) {
      activeClusterPromptKey = undefined;
    }
  }
}

async function openDatabricksLoginTerminal(profile) {
  const cliPath = getConfiguration().get('cliPath', 'databricks');
  const terminal = vscode.window.createTerminal({
    name: `Databricks Login: ${profile}`,
    isTransient: true,
    location: vscode.TerminalLocation.Editor,
  });
  terminal.show(true);

  const shellIntegration = await waitForTerminalShellIntegration(terminal, 4000);
  if (!shellIntegration) {
    terminal.sendText(`${formatTerminalArgument(cliPath)} auth login --profile ${formatTerminalArgument(profile)}`, true);
    return 'fallback';
  }

  try {
    const execution = shellIntegration.executeCommand(cliPath, ['auth', 'login', '--profile', profile]);
    const exitCode = await execution.exitCode;
    if (exitCode === 0) {
      terminal.dispose();
      return 'succeeded';
    }
    return 'failed';
  } catch {
    terminal.sendText(`${formatTerminalArgument(cliPath)} auth login --profile ${formatTerminalArgument(profile)}`, true);
    return 'fallback';
  }
}

async function openPowerShellTerminalInEditor() {
  const terminal = vscode.window.createTerminal({
    name: 'PowerShell',
    isTransient: true,
    location: vscode.TerminalLocation.Editor,
    shellPath: 'pwsh',
  });
  terminal.show(false);
}

async function openHelpDocument(context) {
  const helpUri = vscode.Uri.joinPath(context.extensionUri, 'HELP.md');
  const document = await vscode.workspace.openTextDocument(helpUri);
  await vscode.window.showTextDocument(document, { preview: false });
}

function waitForTerminalShellIntegration(terminal, timeoutMs) {
  if (terminal.shellIntegration) {
    return Promise.resolve(terminal.shellIntegration);
  }

  return new Promise((resolve) => {
    let settled = false;
    const complete = (value) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      integrationListener.dispose();
      closeListener.dispose();
      resolve(value);
    };

    const timeout = setTimeout(() => complete(undefined), timeoutMs);
    const integrationListener = vscode.window.onDidChangeTerminalShellIntegration((event) => {
      if (event.terminal === terminal) {
        complete(event.shellIntegration);
      }
    });
    const closeListener = vscode.window.onDidCloseTerminal((closedTerminal) => {
      if (closedTerminal === terminal) {
        complete(undefined);
      }
    });
  });
}

function formatTerminalArgument(value) {
  const text = String(value || '');
  return /\s/.test(text) ? `"${text}"` : text;
}

function getSelectedProfileInfo(context) {
  const configured = (getConfiguration().get('profile', '') || '').trim();
  if (configured) {
    return { name: configured, source: 'setting' };
  }

  const saved = context.workspaceState.get(STATE_PROFILE);
  if (!saved) {
    return undefined;
  }

  return { name: saved, source: 'workspaceState' };
}

function getSelectedClusterInfo(context) {
  const configured = (getConfiguration().get('clusterId', '') || '').trim();
  if (configured) {
    const profile = getSelectedProfileInfo(context)?.name;
    return { id: configured, name: configured, source: 'setting', profile };
  }

  const saved = context.workspaceState.get(STATE_CLUSTER);
  if (!saved?.id) {
    return undefined;
  }

  return { ...saved, source: 'workspaceState' };
}

function getDatabricksClusterErrorProfile(context, error) {
  return error?.profile || getSelectedProfileInfo(context)?.name;
}

function sortProfiles(profiles, selectedProfileName) {
  return [...profiles].sort((left, right) => {
    const leftRank = getProfileSortRank(left, selectedProfileName);
    const rightRank = getProfileSortRank(right, selectedProfileName);
    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }

    return String(left.name || '').localeCompare(String(right.name || ''));
  });
}

function getProfileSortRank(profile, selectedProfileName) {
  if (profile.name === selectedProfileName) {
    return 0;
  }
  if (profile.default) {
    return 1;
  }
  return 2;
}

function describeProfile(profile, selectedProfile) {
  const parts = [];
  if (selectedProfile && profile.name === selectedProfile.name) {
    parts.push(selectedProfile.source === 'setting' ? 'selected in settings' : 'selected');
  } else if (profile.default) {
    parts.push('default');
  }

  parts.push(profile.valid ? 'ready' : 'login required');
  return parts.join(' • ');
}

function buildProfileTooltip(profile, selectedProfile) {
  const lines = [profile.name];
  if (selectedProfile && profile.name === selectedProfile.name) {
    lines.push(selectedProfile.source === 'setting' ? 'Selected by setting' : 'Selected for this workspace');
  }
  if (profile.host) {
    lines.push(`Host: ${profile.host}`);
  }
  if (profile.workspace_id) {
    lines.push(`Workspace: ${profile.workspace_id}`);
  }
  if (profile.cloud) {
    lines.push(`Cloud: ${profile.cloud}`);
  }
  if (profile.auth_type) {
    lines.push(`Auth: ${profile.auth_type}`);
  }
  lines.push(profile.valid ? 'Status: ready' : 'Status: login required');
  lines.push(profile.valid ? 'Click to use this profile.' : 'Click to start databricks auth login in a terminal.');
  return lines.join('\n');
}

async function ensureProfile(context, cli) {
  const configured = (getConfiguration().get('profile', '') || '').trim();
  if (configured) {
    return configured;
  }

  const saved = context.workspaceState.get(STATE_PROFILE);
  if (saved) {
    return saved;
  }

  const selected = await selectProfile(context, cli, false);
  if (selected) {
    await context.workspaceState.update(STATE_PROFILE, selected);
  }
  return selected;
}

async function selectProfile(_context, cli, allowManualInput) {
  try {
    const profiles = await cli.listProfiles();
    if (!profiles.length) {
      if (!allowManualInput) {
        return undefined;
      }
      return promptForProfileName();
    }

    const items = sortProfiles(profiles, getSelectedProfileInfo(_context)?.name).map((profile) => ({
      label: profile.name,
      description: profile.valid ? profile.host : `${profile.host} • auth needs login`,
      detail: profile.valid ? 'Press Enter to use this profile.' : 'Press Enter to reconnect this profile in a terminal.',
      profile: profile.name,
      valid: profile.valid,
    }));
    if (allowManualInput) {
      items.push({ label: 'Manual profile…', description: 'Type a Databricks CLI profile name', profile: '__manual__' });
    }

    const picked = await vscode.window.showQuickPick(items, {
      placeHolder: 'Select a Databricks CLI profile',
      ignoreFocusOut: true,
    });
    if (!picked) {
      return undefined;
    }

    if (picked.profile === '__manual__') {
      return promptForProfileName();
    }

    if (picked.valid === false) {
      await reconnectProfile(picked.profile, 'Complete Databricks login in the terminal, then select the profile again.');
      return undefined;
    }

    return picked.profile;
  } catch (error) {
    if (!allowManualInput) {
      throw error;
    }

    vscode.window.showWarningMessage(normalizeError(error).message);
    return promptForProfileName();
  }
}

async function promptForProfileName() {
  return vscode.window.showInputBox({
    prompt: 'Enter the Databricks CLI profile name',
    ignoreFocusOut: true,
    validateInput: (value) => (value.trim() ? undefined : 'Profile name is required.'),
  });
}

async function ensureCluster(context, cli, profile) {
  const configured = (getConfiguration().get('clusterId', '') || '').trim();
  if (configured) {
    return { id: configured, name: configured, profile };
  }

  const saved = context.workspaceState.get(STATE_CLUSTER);
  if (saved && saved.profile === profile && saved.id) {
    return saved;
  }

  const selected = await selectCluster(context, cli, profile, false);
  if (selected) {
    await context.workspaceState.update(STATE_CLUSTER, selected);
  }
  return selected;
}

async function selectCluster(_context, cli, profile, allowManualInput) {
  try {
    const clusters = await cli.listClusters(profile);
    const items = clusters
      .map((cluster) => ({
        label: cluster.cluster_name || cluster.cluster_id,
        description: `${cluster.state || 'Unknown state'} • ${cluster.cluster_id}`,
        detail: cluster.spark_version || undefined,
        cluster: {
          profile,
          id: cluster.cluster_id,
          name: cluster.cluster_name || cluster.cluster_id,
        },
      }))
      .sort((left, right) => left.label.localeCompare(right.label, undefined, { sensitivity: 'base', numeric: true }));
    if (allowManualInput) {
      items.push({
        label: 'Manual cluster ID…',
        description: 'Type a Databricks cluster ID',
        cluster: { manual: true },
      });
    }

    if (!items.length) {
      if (!allowManualInput) {
        return undefined;
      }
      return promptForClusterId(profile);
    }

    const picked = await vscode.window.showQuickPick(items, {
      placeHolder: `Select a Databricks cluster for profile ${profile}`,
      ignoreFocusOut: true,
    });
    if (!picked) {
      return undefined;
    }

    if (picked.cluster.manual) {
      return promptForClusterId(profile);
    }

    return picked.cluster;
  } catch (error) {
    const normalized = normalizeError(error);
    logWarning('Failed to list Databricks clusters.', normalized);
    if (await showDatabricksAuthErrorActions(_context, normalized)) {
      return undefined;
    }

    if (!allowManualInput) {
      throw normalized;
    }

    logWarning('Falling back to manual cluster ID input.', normalized);
    vscode.window.showWarningMessage(normalized.message);
    return promptForClusterId(profile);
  }
}

async function promptForClusterId(profile) {
  const clusterId = await vscode.window.showInputBox({
    prompt: `Enter the Databricks cluster ID for profile ${profile}`,
    ignoreFocusOut: true,
    validateInput: (value) => (value.trim() ? undefined : 'Cluster ID is required.'),
  });
  if (!clusterId) {
    return undefined;
  }

  return {
    profile,
    id: clusterId.trim(),
    name: clusterId.trim(),
  };
}

async function selectClusterCommand(context, cli, sessions) {
  const configured = (getConfiguration().get('clusterId', '') || '').trim();
  if (configured) {
    const openSettings = 'Open Settings';
    const choice = await vscode.window.showWarningMessage(
      `databricksSourceNotebook.clusterId is set to ${configured} in settings. Clear or change that setting to switch clusters from the toolbar.`,
      openSettings
    );
    if (choice === openSettings) {
      await vscode.commands.executeCommand('workbench.action.openSettings', `${CONFIG_SECTION}.clusterId`);
    }
    return;
  }

  const profile = await ensureProfile(context, cli);
  if (!profile) {
    return;
  }

  const cluster = await selectCluster(context, cli, profile, true);
  if (!cluster) {
    return;
  }

  await context.workspaceState.update(STATE_CLUSTER, cluster);
  await sessions.disposeAll();
  await refreshDatabricksUi();
  vscode.window.showInformationMessage(`Databricks cluster set to ${cluster.name || cluster.id}.`);

  try {
    const clusterInfo = await cli.getCluster(profile, cluster.id);
    if (String(clusterInfo?.state || '').toUpperCase() === 'TERMINATED') {
      await startClusterForExecution(profile, cluster.id, { clusterName: cluster.name || cluster.id });
    }
  } catch (error) {
    logWarning('Failed to check selected cluster state after selection.', error);
    // Keep the selected cluster even if the immediate state check fails.
  }
}

async function startClusterForExecution(profile, clusterId, options = {}) {
  if (!databricksCliSingleton) {
    throw new Error('Databricks CLI is not available.');
  }

  activeClusterOperation = {
    type: 'starting',
    id: clusterId,
    name: options.clusterName || clusterId,
    startedAt: Date.now(),
  };
  await refreshDatabricksUi();

  try {
    await vscode.window.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title: `Starting Databricks cluster ${options.clusterName || clusterId}`,
        cancellable: false,
      },
      async (progress) => {
        await databricksCliSingleton.startClusterAndWait(profile, clusterId);
        progress.report({ message: 'Resetting cluster timer' });
        const resetAt = await resetClusterTimer(profile, clusterId).catch((error) => {
          logWarning(`Databricks cluster timer reset failed for cluster ${clusterId}.`, error);
          return undefined;
        });
        if (resetAt) {
          rememberClusterTouch(profile, clusterId, { at: resetAt });
        }
      }
    );

    if (sessionManagerSingleton) {
      await sessionManagerSingleton.disposeAll();
    }
    vscode.window.showInformationMessage(`Databricks cluster ${options.clusterName || clusterId} is running.`);
  } finally {
    activeClusterOperation = undefined;
    await refreshDatabricksUi();
  }
}

async function resetSelectedRunningClusterTimer(context, cli, reason) {
  if (activeClusterOperation?.type === 'starting') {
    return undefined;
  }

  const selectedProfile = getSelectedProfileInfo(context);
  const selectedCluster = getSelectedClusterInfo(context);
  if (!selectedProfile?.name || !selectedCluster?.id) {
    return undefined;
  }

  const profile = selectedProfile.name;
  const clusterId = selectedCluster.id;
  const key = getClusterKey(profile, clusterId);
  const existing = clusterTimerResetPromises.get(key);
  if (existing) {
    return existing;
  }

  clusterTimerResetPendingKeys.add(key);
  void refreshDatabricksUi();

  const resetPromise = (async () => {
    try {
      logVerbose('Checking selected cluster before timer reset.', { profile, clusterId, reason });
      const cluster = await cli.getCluster(profile, clusterId);
      const clusterName = cluster.cluster_name || selectedCluster.name || clusterId;
      const snapshot = {
        profile,
        clusterId,
        clusterName,
        clusterState: mapClusterState(cluster.state),
        autoterminationMinutes: parseAutoterminationMinutes(cluster.autotermination_minutes),
        startTimeMs: Number(cluster.start_time) || undefined,
      };
      setClusterStatusSnapshot(profile, clusterId, snapshot);

      if (String(cluster?.state || '').toUpperCase() !== 'RUNNING') {
        logVerbose('Skipping cluster timer reset because selected cluster is not running.', {
          profile,
          clusterId,
          state: cluster?.state,
          reason,
        });
        await refreshDatabricksUi();
        return undefined;
      }

      const resetAt = await resetClusterTimer(profile, clusterId);
      if (resetAt) {
        rememberClusterTouch(profile, clusterId, { at: resetAt });
        logVerbose('Selected running cluster timer reset.', { profile, clusterId, reason });
      }
      await refreshDatabricksUi();
      return resetAt;
    } catch (error) {
      logWarning('Failed to reset selected running cluster timer.', { profile, clusterId, reason, error: normalizeError(error).message });
      await refreshDatabricksUi();
      return undefined;
    } finally {
      clusterTimerResetPendingKeys.delete(key);
      clusterTimerResetPromises.delete(key);
      await refreshDatabricksUi();
    }
  })();

  clusterTimerResetPromises.set(key, resetPromise);
  return resetPromise;
}

async function resetClusterTimer(profile, clusterId) {
  if (!databricksCliSingleton) {
    logWarning('Cannot reset cluster timer because Databricks CLI is unavailable.', { profile, clusterId });
    return undefined;
  }

  logVerbose('Resetting cluster timer with warm-up cell.', { profile, clusterId, command: CLUSTER_TIMER_RESET_COMMAND });
  const contextId = await databricksCliSingleton.createContextAndWait(profile, clusterId, 'python');
  try {
    const response = await databricksCliSingleton.executeAndWait(profile, clusterId, contextId, 'python', CLUSTER_TIMER_RESET_COMMAND);
    if (response.status === 'Error') {
      logError('Cluster timer reset cell returned Databricks error.', response);
      throw new Error('Databricks cluster timer reset cell failed.');
    }
    logVerbose('Cluster timer reset cell succeeded.', { profile, clusterId, contextId });
    return Date.now();
  } finally {
    await databricksCliSingleton.destroyContext(profile, clusterId, contextId).catch(() => {});
  }
}

async function stopClusterForExecution(profile, clusterId, options = {}) {
  if (!databricksCliSingleton) {
    throw new Error('Databricks CLI is not available.');
  }

  await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: `Stopping Databricks cluster ${options.clusterName || clusterId}`,
      cancellable: false,
    },
      async () => {
        await databricksCliSingleton.stopClusterAndWait(profile, clusterId);
      }
    );

  clearClusterTouch(profile, clusterId);
  if (sessionManagerSingleton) {
    await sessionManagerSingleton.disposeAll();
  }
  await refreshDatabricksUi();
  vscode.window.showInformationMessage(`Databricks cluster ${options.clusterName || clusterId} is stopped.`);
}

function getConfiguration() {
  return vscode.workspace.getConfiguration(CONFIG_SECTION);
}

function getTimeoutMs() {
  return Number(getConfiguration().get('commandTimeoutSeconds', 1200)) * 1000;
}

function getPollIntervalMs() {
  return Number(getConfiguration().get('pollIntervalMs', 1000));
}

function formatCliTimeout() {
  const seconds = Math.max(30, Math.ceil(getTimeoutMs() / 1000));
  return `${seconds}s`;
}

function getWorkspaceCwd() {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}

function withQuery(pathname, params) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      query.set(key, value);
    }
  }

  const encoded = query.toString();
  return encoded ? `${pathname}?${encoded}` : pathname;
}

function runProcess(command, args, cwd) {
  return new Promise((resolve, reject) => {
    logVerbose('Starting process.', { command, args, cwd });
    const child = spawn(command, args, {
      cwd,
      env: process.env,
      shell: false,
    });

    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });
    child.on('error', (error) => {
      logError('Process failed to start.', { command, args, cwd, error });
      if (error.code === 'ENOENT') {
        reject(new Error(`Databricks CLI not found at '${command}'. Update databricksSourceNotebook.cliPath if needed.`));
        return;
      }
      reject(error);
    });
    child.on('close', (code) => {
      logVerbose('Process finished.', {
        command,
        args,
        cwd,
        code,
        stdout: stdout.trim(),
        stderr: stderr.trim(),
      });
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }

      const message = stderr.trim() || stdout.trim() || `Databricks CLI exited with code ${code}.`;
      logError('Process exited with non-zero code.', { command, args, cwd, code, stdout: stdout.trim(), stderr: stderr.trim() });
      reject(createDatabricksCliError(message, command, args));
    });
  });
}

function createClusterStateError(clusterId, clusterState, profile) {
  const state = String(clusterState || '').trim();
  const error = new Error(formatClusterStateMessage(clusterId, state));
  error.code = ERROR_CODES.clusterNotReady;
  error.clusterId = clusterId;
  error.clusterState = state;
  error.profile = profile;
  return error;
}

function formatClusterStateMessage(clusterId, clusterState) {
  const state = String(clusterState || '').trim();
  if (state.toLowerCase() === 'terminated') {
    return `Databricks cluster ${clusterId} is terminated. Starting it now.`;
  }

  if (state) {
    return `Databricks cluster ${clusterId} is not ready yet (${state}). Waiting for it to be ready.`;
  }

  return `Databricks cluster ${clusterId} is not ready yet. Waiting for it to be ready.`;
}

function createDatabricksCliError(message, command, args) {
  const formatted = formatDatabricksCliError(message, command, args);
  const error = new Error(formatted);
  if (formatted.startsWith('Databricks login expired')) {
    error.code = ERROR_CODES.authExpired;
    error.profile = getCliProfile(args);
  }

  const clusterNotReady = parseClusterNotReadyError(message);
  if (clusterNotReady) {
    error.code = ERROR_CODES.clusterNotReady;
    error.clusterId = clusterNotReady.clusterId;
    error.clusterState = clusterNotReady.clusterState;
    error.profile = getCliProfile(args);
  }

  return error;
}

function formatDatabricksCliError(message, command, args) {
  const profile = getCliProfile(args);
  const normalized = String(message || '').trim();
  const lower = normalized.toLowerCase();
  const loginExpired =
    lower.includes('refresh token is invalid') ||
    lower.includes('error_description":"refresh token is invalid"') ||
    lower.includes('a new access token could not be retrieved because the refresh token is invalid');

  if (loginExpired) {
    if (profile) {
      return `Databricks login expired for profile ${profile}. Run: ${command} auth login --profile ${profile}`;
    }

    return `Databricks login expired. Run: ${command} auth login`;
  }

  const clusterNotReady = parseClusterNotReadyError(normalized);
  if (clusterNotReady) {
    if (clusterNotReady.clusterState?.toLowerCase() === 'terminated') {
      return `Databricks cluster ${clusterNotReady.clusterId} is terminated. Starting it now.`;
    }

    if (clusterNotReady.clusterState) {
      return `Databricks cluster ${clusterNotReady.clusterId} is not ready yet (${clusterNotReady.clusterState}). Wait for it to be ready or select another cluster.`;
    }

    return `Databricks cluster ${clusterNotReady.clusterId} is not ready yet. Wait for it to be ready or select another cluster.`;
  }

  return normalized;
}

function parseClusterNotReadyError(message) {
  const normalized = String(message || '').trim();
  const match = normalized.match(
    /ClusterNotReadyException:\s*Cluster\s+([^\s]+)\s+not currently ready for driver client(?:\s+\(currently\s+([^)]+)\))?/i
  );
  if (!match) {
    return undefined;
  }

  return {
    clusterId: match[1],
    clusterState: match[2],
  };
}

function getCliProfile(args) {
  for (let index = 0; index < args.length; index += 1) {
    if ((args[index] === '-p' || args[index] === '--profile') && args[index + 1]) {
      return args[index + 1];
    }
  }

  return undefined;
}

function isDatabricksAuthExpiredError(error) {
  return error?.code === ERROR_CODES.authExpired || /^Databricks login expired\b/.test(error?.message || '');
}

function isDatabricksClusterNotReadyError(error) {
  return error?.code === ERROR_CODES.clusterNotReady || /^Databricks cluster\b/.test(error?.message || '');
}

function getDatabricksAuthErrorProfile(context, error) {
  if (error?.profile) {
    return error.profile;
  }

  const match = String(error?.message || '').match(/^Databricks login expired for profile ([^.]+)\./);
  if (match) {
    return match[1];
  }

  return getSelectedProfileInfo(context)?.name;
}

async function waitFor(poll, timeoutMs, timeoutMessage) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await poll();
    if (result.done) {
      if (result.error) {
        throw result.error;
      }
      return result.value;
    }

    await delay(getPollIntervalMs());
  }

  throw new Error(timeoutMessage);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeError(error) {
  if (error instanceof Error) {
    return error;
  }

  return new Error(String(error));
}

module.exports = {
  activate,
  deactivate,
  __test: {
    buildCsvFromTable,
    buildOutputsFromResponse,
    prepareExecutableCode,
    parseSourceNotebook,
    serializeSourceNotebook,
    tryBuildHtmlTable,
    tryBuildMarkdownTable,
  },
};
