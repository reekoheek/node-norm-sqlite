const sqlite3 = require('better-sqlite3');
const { Manager } = require('node-norm');
const adapter = require('../');
const NInteger = require('node-norm/schemas/ninteger');
const assert = require('assert');

describe('definition', () => {
  let db;
  let manager;

  beforeEach(() => {
    db = sqlite3(':memory:');
    db.prepare('CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, nint INT)').run();

    const schemas = [
      {
        name: 'foo',
        fields: [
          new NInteger('nint'),
        ],
      },
      {
        name: 'bar',
        fields: [
          new NInteger('nint'),
        ],
      },
    ];

    manager = new Manager({ connections: [{ adapter, db, schemas }] });
  });

  afterEach(() => {
    manager.end();
  });

  describe('defined()', () => {
    it('check if table defined', async () => {
      await manager.runSession(async session => {
        assert.strictEqual(await session.factory('foo').defined(), true);
        assert.strictEqual(await session.factory('bar').defined(), false);
      });
    });
  });

  describe('define()', () => {
    it('define table', async () => {
      await manager.runSession(async session => {
        assert.strictEqual(await session.factory('bar').defined(), false);
        await session.factory('bar').define();
        assert.strictEqual(await session.factory('bar').defined(), true);
      });
    });
  });

  describe('undefine()', () => {
    it('undefine table', async () => {
      await manager.runSession(async session => {
        assert.strictEqual(await session.factory('foo').defined(), true);
        await session.factory('foo').undefine();
        assert.strictEqual(await session.factory('foo').defined(), false);
      });
    });
  });
});
