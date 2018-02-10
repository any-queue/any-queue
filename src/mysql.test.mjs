import mysql from "mysql";
import { promisify } from "util";
import testIntegration from "./integration.test.mjs";

const config = {
  host: "localhost",
  rootUser: "root",
  rootPassword: "admin",
  user: "one_queue_test_user",
  password: "one_queue_test_password",
  database: "one_queue_test"
};

const globals = {};

const initializeDatabase = function initializeDatabase() {
  const connection = mysql.createConnection({
    host: config.host,
    user: config.rootUser,
    password: config.rootPassword
  });

  return promisify(connection.connect.bind(connection))()
    .then(() => promisify(connection.query.bind(connection)))
    .then(query =>
      query(`DROP DATABASE ${config.database}`)
        .then(query(`CREATE DATABASE IF NOT EXISTS ${config.database}`))
        .then(() =>
          query(`CREATE USER IF NOT EXISTS '${config.user}'@'${config.host}'`)
        )
        .then(() =>
          query(
            `GRANT ALL ON ${config.database}.* To '${config.user}'@'${
              config.host
            }' IDENTIFIED BY '${config.password}';`
          )
        )
        .then(() => query("FLUSH PRIVILEGES"))
        .then(() => query(`USE ${config.database}`))
        .then(() =>
          query(`CREATE TABLE jobs(
          _id int not null primary key auto_increment,
          queue varchar(255) not null,
          data json not null,
          priority int not null,
          status enum ('new', 'blocked', 'done', 'failed') not null,
          error varchar(1000)
        )`)
        )
        .then(() =>
          query(`CREATE TABLE locks(
          _id int not null primary key auto_increment,
          queue varchar(255) not null,
          worker char(36),
          blocker char(36),
          job int not null,
          status enum ('locking', 'locked', 'backed-off'),

          FOREIGN KEY fk_job(job)
          REFERENCES jobs(_id)
          ON UPDATE CASCADE
          ON DELETE CASCADE
        )`)
        )
    )
    .then(promisify(connection.end.bind(connection)));
};

const connectToDatabase = function connectToDatabase() {
  const connection = mysql.createConnection({
    host: config.host,
    user: config.user,
    password: config.password,
    database: config.database
  });

  return promisify(connection.connect.bind(connection))()
    .then(() => promisify(connection.query.bind(connection)))
    .then(query => {
      const formatValue = function formatValue(value) {
        switch (typeof value) {
          case "number":
            return value;
          case "string":
            return `'${value}'`;
          case "undefined":
            return "null";
          default:
            return `'${JSON.stringify(value)}'`;
        }
      };

      const wheres = function wheres(obj = {}) {
        const conditions = Object.keys(obj)
          .map(k => `${k} = ${formatValue(obj[k])}`)
          .join(" AND ");

        return conditions ? `WHERE ${conditions}` : "";
      };

      const environment = {
        createJob: function createJob({ queue, data, priority, status }) {
          return query(`
          INSERT INTO jobs(queue, data, priority, status)
          VALUES('${queue}', '${JSON.stringify(
            data
          )}', ${priority}, '${status}')
          `).then(({ insertId }) => insertId);
        },
        readJob: function readJob(criteria) {
          return query(`
          SELECT * FROM jobs 
          ${wheres(criteria)}
          `).then(jobs =>
            jobs.map(j => Object.assign({}, j, { data: JSON.parse(j.data) }))
          );
        },
        updateJob: function updateJob(id, { status, error }) {
          return query(`
          UPDATE jobs
          SET status = '${status}' ${error ? `, error = ${error}` : ""}
          WHERE _id = ${id}
          `);
        },
        createLock: function createLock({
          job,
          queue,
          worker,
          status,
          blocker
        }) {
          return query(`
          INSERT INTO locks(job, queue, worker, blocker, status)
          VALUES(${job}, '${queue}', ${formatValue(worker)}, ${formatValue(
            blocker
          )}, '${status}')
          `).then(({ insertId }) => insertId);
        },
        readLock: function readLock(criteria) {
          return query(`
          SELECT * FROM locks 
          ${wheres(criteria)}
          `);
        },
        updateLock: function updateLock(criteria, { status }) {
          return query(`
          UPDATE locks
          SET status = '${status}'
          ${wheres(criteria)}
          `);
        }
      };

      globals.connection = connection;
      globals.query = query;

      return environment;
    });
};

const setup = function() {
  return initializeDatabase().then(connectToDatabase);
};

const refresh = function() {
  return globals.query("DELETE FROM jobs");
};

const teardown = function() {
  const connection = globals.connection;
  return promisify(connection.end.bind(connection))();
};

testIntegration("mysql", setup, refresh, teardown);
