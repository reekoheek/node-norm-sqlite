const sqlite = require('sqlite');
const { Manager } = require('node-norm');
const adapter = require('../');
const assert = require('assert');

describe('Cases', () => {
  let db;
  let manager;

  beforeEach(async () => {
    db = await sqlite.open(':memory:');
    await db.run('CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, foo TEXT);');
    await db.run('CREATE TABLE test1 (id INTEGER PRIMARY KEY AUTOINCREMENT, `order` TEXT,`key` TEXT,`group` TEXT);');
    await db.run('INSERT INTO foo (foo) VALUES (?), (?)', ['1', '2']);
    await db.run('INSERT INTO test1 (`order`,`key`,`group`) VALUES (?,?,?), (?,?,?)', ['1', '2', '3', '4', '5', '6']);

    manager = new Manager({
      connections: [
        {
          adapter,
          db,
        },
      ],
    });
  });

  it('create new record with esacpe character', async () => {
    await manager.runSession(async session => {
      let { affected, rows } = await session.factory('test1').insert({ order: 'bar', key: '1' }).insert({ order: 'bar1', key: '2' }).save();
      assert.strictEqual(affected, 2);
      assert.strictEqual(rows.length, 2);
      let foos = await db.all('SELECT * FROM test1');
      assert.strictEqual(foos.length, 4);
    });
  });

  it('update record with escape character', async () => {
    await manager.runSession(async session => {
      let { affected } = await session.factory('test1', 1).set({ key: 'bar2' }).save();
      assert.strictEqual(affected, 1);
      let foo = await db.get('SELECT * FROM test1 WHERE id = 1');
      assert.strictEqual(foo.key, 'bar2');
    });
  });

  it('delete record with escape character', async () => {
    await manager.runSession(async session => {
      await session.factory('test1', { group: '3' }).delete();

      let foos = await db.all('SELECT * FROM test1');
      assert.strictEqual(foos.length, 1);
    });
  });

  it('create new record', async () => {
    await manager.runSession(async session => {
      let { affected, rows } = await session.factory('foo').insert({ foo: 'bar' }).insert({ foo: 'bar1' }).save();
      assert.strictEqual(affected, 2);
      assert.strictEqual(rows.length, 2);
      let foos = await db.all('SELECT * FROM foo');

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
      let foo = await db.get('SELECT * FROM foo WHERE id = 2');

      assert.strictEqual(foo.foo, 'bar');
    });
  });

  it('delete record', async () => {
    await manager.runSession(async session => {
      await session.factory('foo').delete();

      let foos = await db.all('SELECT * FROM foo');
      assert.strictEqual(foos.length, 0);
    });
  });
});
