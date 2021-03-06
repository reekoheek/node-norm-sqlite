const Connection = require('node-norm/connection');
const betterSqlite3 = require('better-sqlite3');
// const debug = require('debug')('node-norm-sqlite:index');
const debugQuery = require('debug')('node-norm-sqlite:query');
const path = require('path');
const fs = require('fs-extra');

const OPERATORS = {
  eq: '=',
  gt: '>',
  lt: '<',
  gte: '>=',
  lte: '<=',
  like: 'like',
};

const DEFAULT_TYPES = {
  nbig: 'INTEGER',
  nboolean: 'TINYINT',
  ndouble: 'DECIMAL(25,8)',
  ninteger: 'INTEGER',
  nstring: 'VARCHAR(255)',
  ntext: 'TEXT',
  ndatetime: 'DATETIME',
};

const FALLBACK_TYPE = 'TEXT';

let index = 0;
class Sqlite extends Connection {
  constructor (options) {
    super(options);

    this.index = index++;
    this.dbProvided = Boolean(options.db);
    this._db = options.db;
    if (this.dbProvided) {
      return;
    }
    this.file = options.file || ':memory:';
  }

  async insert (query, callback = () => {}) {
    let fieldNames = query.schema.fields.map(field => field.name);
    if (!fieldNames.length) {
      fieldNames = query.rows.reduce((result, row) => {
        for (const f in row) {
          if (result.indexOf(f) === -1) {
            result.push(f);
          }
        }
        return result;
      }, []);
    }

    const placeholder = fieldNames.map(f => '?').join(', ');
    const sql = `INSERT INTO ${this.escape(query.schema.name)}` +
      ` (${fieldNames.map(f => this.escape(f)).join(', ')})` +
      ` VALUES (${placeholder})`;

    let changes = 0;
    await Promise.all(query.rows.map(async row => {
      const rowData = fieldNames.map(f => {
        const value = this.serialize(row[f]);
        return value;
      });

      const { result } = await this.rawQuery(sql, rowData);
      row.id = result.lastInsertRowid;
      changes += result.changes;

      callback(row);
    }));

    return changes;
  }

  async load (query, callback = () => {}) {
    const { length, offset } = query;
    const sqlArr = [`SELECT * FROM ${this.escape(query.schema.name)}`];
    const [wheres, data] = this.getWhere(query);
    if (wheres) {
      sqlArr.push(wheres);
    }

    const orderBys = this.getOrderBy(query);
    if (orderBys) {
      sqlArr.push(orderBys);
    }

    if (length >= 0) {
      sqlArr.push(`LIMIT ${length}`);

      if (offset > 0) {
        sqlArr.push(`OFFSET ${offset}`);
      }
    }

    const sql = sqlArr.join(' ');

    const { result } = await this.rawQuery(sql, data);

    return result.map(row => {
      callback(row);
      return row;
    });
  }

  async count (query, useSkipAndLimit = false) {
    const { schema, length, offset } = query;

    const sqlArr = [`SELECT count(*) as ${this.escape('count')} FROM ${this.escape(schema.name)}`];
    const [wheres, data] = this.getWhere(query);
    if (wheres) {
      sqlArr.push(wheres);
    }

    if (useSkipAndLimit) {
      if (length >= 0) {
        sqlArr.push(`LIMIT ${length}`);

        if (offset > 0) {
          sqlArr.push(`OFFSET ${offset}`);
        }
      }
    }

    const sql = sqlArr.join(' ');

    const { result: [row] } = await this.rawQuery(sql, data);
    return row.count;
  }

  async delete (query) {
    const [wheres, data] = this.getWhere(query);
    const sqlArr = [`DELETE FROM ${query.schema.name}`];
    if (wheres) {
      sqlArr.push(wheres);
    }

    const sql = sqlArr.join(' ');

    await this.rawQuery(sql, data);
  }

  getOrderBy (query) {
    const orderBys = [];
    for (const key in query.sorts) {
      const val = query.sorts[key];

      orderBys.push(`${this.escape(key)} ${val > 0 ? 'ASC' : 'DESC'}`);
    }

    if (!orderBys.length) {
      return;
    }

    return `ORDER BY ${orderBys.join(', ')}`;
  }

  async update (query) {
    const keys = Object.keys(query.sets);

    const params = keys.map(k => this.serialize(query.sets[k]));
    const placeholder = keys.map(k => `${this.escape(k)} = ?`);

    const [wheres, data] = this.getWhere(query);
    const sql = `UPDATE ${query.schema.name} SET ${placeholder.join(', ')} ${wheres}`;
    const { result } = await this.rawQuery(sql, params.concat(data));

    return result.changes;
  }

  getWhere (query) {
    const wheres = [];
    let data = [];
    for (const key in query.criteria) {
      let value = query.criteria[key];

      if (key === '!or') {
        const or = this.getOr(value);
        wheres.push(or.where);
        data = data.concat(or.data);
        continue;
      }

      const [field, operator = 'eq'] = key.split('!');

      // add by januar: for chek if operator like value change to %
      if (operator === 'like') {
        value = `%${value}%`;
      }

      data.push(this.serialize(value));
      wheres.push(`${this.escape(field)} ${OPERATORS[operator]} ?`);
    }

    if (!wheres.length) {
      return [];
    }

    return [`WHERE ${wheres.join(' AND ')}`, data];
  }

  getOr (query) {
    const wheres = [];
    const data = [];
    for (let i = 0; i < query.length; i++) {
      const key = Object.keys(query[i])[0];
      let value = Object.values(query[i])[0];
      const [field, operator = 'eq'] = key.split('!');
      if (operator === 'like') {
        value = '%' + value + '%';
      }
      data.push(value);
      wheres.push(`${this.escape(field)} ${OPERATORS[operator]} ?`);
    }
    return { where: `(${wheres.join(' OR ')})`, data };
  }

  async getRaw () {
    if (!this._db) {
      await fs.ensureDir(path.dirname(this.file));
      this._db = betterSqlite3(this.file);
      this._db.pragma('journal_mode = WAL');
      await new Promise(resolve => setTimeout(resolve));
    }

    return this._db;
  }

  async rawQuery (sql, params = []) {
    if (debugQuery.enabled && !['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)) {
      debugQuery('SQL %d %s', this.index, sql);
      debugQuery('??? %d %o', this.index, params);
    }

    const conn = await this.getRaw();
    if (sql.startsWith('SELECT')) {
      const result = await conn.prepare(sql).all(...params);
      return { result };
    } else {
      const result = await conn.prepare(sql).run(...params);
      return { result };
    }
  }

  escape (field) {
    return '`' + field + '`';
  }

  async _begin () {
    await this.rawQuery('BEGIN');
  }

  async _commit () {
    if (!this._db || !this._db.inTransaction) {
      return;
    }
    await this.rawQuery('COMMIT');
  }

  async _rollback () {
    if (!this._db || !this._db.inTransaction) {
      return;
    }
    await this.rawQuery('ROLLBACK');
  }

  serialize (value) {
    if (value === null) {
      return value;
    }

    if (value instanceof Date) {
      // return value.toISOString();
      // return value.toISOString().slice(0, 19).replace('T', ' ');
      return value.getTime();
    }

    const valueType = typeof value;
    if (valueType === 'object') {
      if (typeof value.toJSON === 'function') {
        return value.toJSON();
      } else {
        return JSON.stringify(value);
      }
    }

    if (valueType === 'boolean') {
      return value ? 1 : 0;
    }

    return value;
  }

  end () {
    if (this.dbProvided || !this._db || !this._db.open) {
      return;
    }

    this._db.close();
  }

  async defined ({ name }) {
    try {
      await this.rawQuery(`SELECT * FROM ${this.escape(name)} LIMIT 1`);
      return true;
    } catch (err) {
      return false;
    }
  }

  async define ({ name, fields }) {
    const fieldLines = fields.map(field => {
      const overridden = field.get('sqlite.ddl.override');
      if (overridden) {
        return `${this.escape(field.name)} ${overridden.trim()}`;
      }

      const schemaType = field.constructor.name.toLowerCase();
      const dataType = field.get('sqlite.ddl.type') || getDefaultType(schemaType);
      const lineTokens = [`${this.escape(field.name)} ${dataType}`];
      lineTokens.push(`${getFilter(field, 'required') ? 'NOT NULL' : 'NULL'}`);
      if (getFilter(field, 'unique')) {
        lineTokens.push('UNIQUE');
      }
      return lineTokens.join(' ').trim();
    });

    fieldLines.unshift(`${this.escape('id')} INTEGER PRIMARY KEY AUTOINCREMENT`);

    const sql = `
CREATE TABLE ${this.escape(name)} (
  ${fieldLines.join(',\n  ')}
)
    `.trim();

    await this.rawQuery(sql);
  }

  async undefine ({ name }) {
    await this.rawQuery(`DROP TABLE ${this.escape(name)}`);
  }
}

module.exports = Sqlite;

function getDefaultType (schemaType) {
  return DEFAULT_TYPES[schemaType] || FALLBACK_TYPE;
}

function getFilter (field, name) {
  return field.rawFilters.find(f => f[0] === name);
}
