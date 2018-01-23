const sqlite = require('sqlite');
const Manager = require('node-norm');
const adapter = require('../');
const assert = require('assert');

describe('Cases', () => {
  let db;
  let manager;

  beforeEach(async () => {
    db = await sqlite.open(':memory:');
    await db.run('CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, foo TEXT);')
    await db.run('INSERT INTO foo (foo) VALUES (?), (?)', ['1', '2']);

    manager = new Manager({
      connections: [
        {
          adapter,
          db,
        },
      ],
    });
  });

  it('create new record', async () => {
    await manager.runSession(async session => {
      let { inserted, rows } = await session.factory('foo').insert({ foo: 'bar' }).insert({ foo: 'bar1' }).save();
      assert.equal(inserted, 2);
      assert.equal(rows.length, 2);
      let foos = await db.all('SELECT * FROM foo');
      assert.equal(foos.length, 4);
    });
  });

  it('read record', async () => {
    await manager.runSession(async session => {
      let foos = await session.factory('foo').all();
      assert.equal(foos.length, 2);
    });
  });

  it('update record', async () => {
    await manager.runSession(async session => {
      let { affected } = await session.factory('foo', 2).set({ foo: 'bar' }).save();
      assert.equal(affected, 1);
      let foo = await db.get('SELECT * FROM foo WHERE id = 2');
      assert.equal(foo.foo, 'bar');
    });
  });

  it('delete record', async () => {
    await manager.runSession(async session => {
      await session.factory('foo').delete();

      let foos = await db.all('SELECT * FROM foo');
      assert.equal(foos.length, 0);
    });
  });
});
