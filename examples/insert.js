const { Manager } = require('node-norm');

const ID = process.argv[2];

(async () => {
  let manager = new Manager({
    connections: [
      {
        adapter: require('..'),
        file: './foo.db',
      },
    ],
  });

  await manager.runSession(async session => {
    await session.acquire();

    let run = async () => {
      await session.factory('foo')
        .insert({
          foo: ID + ' ' + Date.now(),
        })
        .save();

      await session.flush();

      setTimeout(run, 0);
    };

    run();
  });
})();
