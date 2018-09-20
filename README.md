# node-norm-sqlite

```js
const { Manager } = require('node-norm');

let manager = new Manager({
  connections: [
    {
      adapter: require('node-norm-sqlite'),
      // db,
      // file: ':memory:',
    },
  ],
})

(async () => {
  await manager.runSession(async session => {
    let foos = await session.factory('foo').all();
    console.log(foos);
  });
})();

```

## Options

- db: Already open sqlite database object (optional)
- file: File name (default: :memory:)
