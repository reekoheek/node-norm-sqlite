const sqlite3 = require('better-sqlite3');
const { Manager } = require('node-norm');
const adapter = require('..');
const path = require('path');
const fs = require('fs');
const assert = require('assert');

process.on('unhandledRejection', err => console.error('Unhandled', err));

describe('Concurrency', () => {
  const DB_FILE = path.join(process.cwd(), 'test.db');
  let manager;

  beforeEach(async () => {
    try {
      fs.unlinkSync(DB_FILE);
    } catch (err) {
      // noop
    }

    const db = await sqlite3(DB_FILE);
    await db.prepare('CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, foo TEXT)').run();
    db.close();

    manager = new Manager({
      connections: [
        {
          adapter,
          file: DB_FILE,
        },
      ],
    });
  });

  afterEach(async () => {
    await manager.end();
    try {
      fs.unlinkSync(DB_FILE);
    } catch (err) {
      // noop
    }
  });

  async function runSession (index, timeout = 500, wait) {
    if (wait) {
      await new Promise(resolve => setTimeout(resolve, wait));
    }

    await manager.runSession(async session => {
      await new Promise((resolve, reject) => {
        let running = true;
        let runner;
        setTimeout(() => {
          running = false;
          clearTimeout(runner);
          resolve();
        }, timeout);

        const run = async () => {
          if (!running) {
            return;
          }

          try {
            await session.factory('foo')
              .insert({
                foo: `${index} ${Date.now()}`,
              })
              .save();

            await session.flush();
          } catch (err) {
            return reject(err);
          }

          runner = setTimeout(run);
        };
        run();
      });
    });
  }

  it('race transactions', async () => {
    await Promise.all([
      runSession(1),
      runSession(2),
    ]);

    await manager.runSession(async session => {
      const rows = await session.factory('foo').all();
      assert(rows.length > 100);
    });
  }).timeout(5000);
});
