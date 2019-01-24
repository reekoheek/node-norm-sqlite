const sqlite3 = require('better-sqlite3');
const { Manager } = require('node-norm');
const adapter = require('../');
const assert = require('assert');
const path = require('path');
const fs = require('fs');

describe('Cases', () => {
  const DB_FILE = path.join(process.cwd(), 'test.db');
  let db;
  let manager;

  beforeEach(async () => {
    try {
      fs.unlinkSync(DB_FILE);
    } catch (err) {
      // noop
    }

    db = await sqlite3(DB_FILE);
    await db.prepare('CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, foo TEXT)').run();
    await db.prepare('CREATE TABLE bar (id INTEGER PRIMARY KEY AUTOINCREMENT, datetime DATETIME)').run();
    await db.prepare('CREATE TABLE test1 (id INTEGER PRIMARY KEY AUTOINCREMENT, `order` TEXT,`key` TEXT,`group` TEXT)').run();
    await db.prepare('INSERT INTO foo (foo) VALUES (?), (?)').run('1', '2');
    await db.prepare('INSERT INTO bar (datetime) VALUES (?)').run('2019-11-21 00:00:00');
    await db.prepare('INSERT INTO test1 (`order`,`key`,`group`) VALUES (?,?,?), (?,?,?)').run('1', '2', '3', '4', '5', '6');

    manager = new Manager({
      connections: [
        {
          adapter,
          db,
        },
      ],
    });
  });

  afterEach(() => {
    try {
      fs.unlinkSync(DB_FILE);
    } catch (err) {
      // noop
    }
  });

  it('create new record with escape character', async () => {
    await manager.runSession(async session => {
      let { affected, rows } = await session.factory('test1')
        .insert({ order: 'bar', key: '1' })
        .insert({ order: 'bar1', key: '2' })
        .save();
      assert.strictEqual(affected, 2);
      assert.strictEqual(rows.length, 2);
      let foos = await db.prepare('SELECT * FROM test1').all();
      assert.strictEqual(foos.length, 4);
    });
  });

  it('update record with escape character', async () => {
    await manager.runSession(async session => {
      let { affected } = await session.factory('test1', 1).set({ key: 'bar2' }).save();
      assert.strictEqual(affected, 1);
      let foo = await db.prepare('SELECT * FROM test1 WHERE id = 1').get();
      assert.strictEqual(foo.key, 'bar2');
    });
  });

  it('update record with fields: date', async () => {
    await manager.runSession(async session => {
      let { affected } = await session.factory('bar', 1)
        .set({
          datetime: new Date(),
        })
        .save();
      assert.strictEqual(affected, 1);
      let bar = await db.prepare('SELECT * FROM bar WHERE id = 1').get();
      assert(bar);
    });
  });

  it('delete record with escape character', async () => {
    await manager.runSession(async session => {
      await session.factory('test1', { group: '3' }).delete();

      let foos = await db.prepare('SELECT * FROM test1').all();
      assert.strictEqual(foos.length, 1);
    });
  });

  it('create new record', async () => {
    await manager.runSession(async session => {
      let { affected, rows } = await session.factory('foo').insert({ foo: 'bar' }).insert({ foo: 'bar1' }).save();
      assert.strictEqual(affected, 2);
      assert.strictEqual(rows.length, 2);
      let foos = await db.prepare('SELECT * FROM foo').all();

      assert.strictEqual(foos.length, 4);
    });
  });

  it('read record', async () => {
    await manager.runSession(async session => {
      let foos = await session.factory('foo').all();
      assert.strictEqual(foos.length, 2);
    });
  });

  it('update record', async () => {
    await manager.runSession(async session => {
      let { affected } = await session.factory('foo', 2).set({ foo: 'bar' }).save();
      assert.strictEqual(affected, 1);
      let foo = await db.prepare('SELECT * FROM foo WHERE id = 2').get();

      assert.strictEqual(foo.foo, 'bar');
    });
  });

  it('delete record', async () => {
    await manager.runSession(async session => {
      await session.factory('foo').delete();

      let foos = await db.prepare('SELECT * FROM foo').all();
      assert.strictEqual(foos.length, 0);
    });
  });
});
