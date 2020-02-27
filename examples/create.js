const sqlite = require('sqlite');
(async () => {
  const db = await sqlite.open('./foo.db');
  await db.run('CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, foo TEXT);');
})();
