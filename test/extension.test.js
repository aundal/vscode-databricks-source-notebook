const assert = require('node:assert/strict');
const Module = require('node:module');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

function loadExtensionModule() {
  const originalLoad = Module._load;
  Module._load = function patchedLoad(request, parent, isMain) {
    if (request === 'vscode') {
      return createVscodeStub();
    }
    return originalLoad(request, parent, isMain);
  };

  const extensionPath = path.join(__dirname, '..', 'extension.js');
  delete require.cache[extensionPath];

  try {
    return require(extensionPath);
  } finally {
    Module._load = originalLoad;
  }
}

function createVscodeStub() {
  class TreeItem {}
  class NotebookCellOutput {
    constructor(items) {
      this.items = items;
    }
  }

  return {
    NotebookCellKind: {
      Code: 1,
      Markup: 2,
    },
    NotebookCellOutput,
    NotebookCellOutputItem: {
      text(value, mime = 'text/plain') {
        return {
          mime,
          data: Buffer.from(String(value), 'utf8'),
        };
      },
      error(error) {
        return {
          mime: 'application/vnd.code.notebook.error',
          data: Buffer.from(JSON.stringify({
            name: error.name,
            message: error.message,
            stack: error.stack,
          }), 'utf8'),
        };
      },
    },
    TreeItem,
    TreeItemCollapsibleState: { None: 0 },
    ThemeIcon: class ThemeIcon {},
    ProgressLocation: { Notification: 1 },
    Range: class Range {},
    WorkspaceEdit: class WorkspaceEdit {
      replace() {}
    },
  };
}

const extension = loadExtensionModule();
const {
  buildCsvFromTable,
  buildOutputsFromResponse,
  prepareExecutableCode,
  parseSourceNotebook,
  serializeSourceNotebook,
} = extension.__test;

test('table results include html table output', () => {
  const result = buildOutputsFromResponse({
    status: 'Finished',
    results: {
      resultType: 'table',
      schema: [{ name: 'name' }, { name: 'value' }],
      data: [
        ['alpha', 1],
        ['<beta>', 2],
      ],
      truncated: false,
    },
  });

  const items = result.outputs[0].items.map((item) => ({
    mime: item.mime,
    value: Buffer.from(item.data).toString('utf8'),
  }));

  assert.equal(items[0].mime, 'text/html');
  assert.match(items[0].value, /<table/);
  assert.match(items[0].value, /<th[^>]*>name<\/th>/);
  assert.match(items[0].value, /&lt;beta&gt;/);
  assert.equal(items[1].mime, 'text/markdown');
  assert.match(items[1].value, /\| name \| value \|/);
  assert.equal(items[2].mime, 'text/x-json');

  assert.equal(result.webviewOutputs[0].mime, 'application/x-databricks-table+json');
  assert.deepEqual(JSON.parse(result.webviewOutputs[0].value), {
    columns: ['name', 'value'],
    rows: [
      ['alpha', 1],
      ['<beta>', 2],
    ],
    truncated: false,
  });
});

test('csv export escapes commas quotes and newlines', () => {
  const csv = buildCsvFromTable(['name', 'note'], [['alpha', 'x,y'], ['beta', '"quoted"\nnext']]);
  assert.equal(csv, 'name,note\nalpha,"x,y"\nbeta,"""quoted""\nnext"\n');
});

test('python execution code is wrapped with display shim', () => {
  const prepared = prepareExecutableCode('display(df)', 'python');
  assert.match(prepared, /def display\(value, limit=200\):/);
  assert.match(prepared, /display\(df\)/);
});

test('inline table marker in text output becomes interactive table payload', () => {
  const result = buildOutputsFromResponse({
    status: 'Finished',
    results: {
      data: '__DATABRICKS_SOURCE_TABLE__:{"columns":["category","price"],"rows":[["Books","10.5"]],"truncated":false}',
    },
  });

  assert.equal(result.webviewOutputs[0].mime, 'application/x-databricks-table+json');
  assert.deepEqual(JSON.parse(result.webviewOutputs[0].value), {
    columns: ['category', 'price'],
    rows: [['Books', '10.5']],
    truncated: false,
  });
});

test('source notebook parse and serialize does not add blank lines to cells', () => {
  const samplePath = path.join(__dirname, '..', 'sample.notebook.py');
  const source = fs.readFileSync(samplePath, 'utf8').replace(/\r\n/g, '\n');

  const parsed = parseSourceNotebook(source);
  const serialized = serializeSourceNotebook(parsed);

  assert.equal(serialized, source);
  assert.equal(parsed.cells[0].value.endsWith('\n'), false);
  assert.equal(parsed.cells[1].value.endsWith('\n'), false);
});
