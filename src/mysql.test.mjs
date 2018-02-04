/* eslint-env mocha */
import mysql from "mysql";
import assert from "assert";
import { promisify } from "util";
import debug from "debug";
import testIntegration from "./integration.test.mjs";

const log = debug("test:mysql");

const config = {
  host: "localhost",
  rootUser: "root",
  rootPassword: "admin",
  user: "one_queue_test_user",
  password: "one_queue_test_password",
  database: "one_queue_test"
};

const globals = {};

const initializeDatabase = function initializeDatabase(done) {
  const connection = mysql.createConnection({
    host: config.host,
    user: config.rootUser,
    password: config.rootPassword
  });

  connection.connect(err => {
    assert.equal(null, err);

    const query = promisify(connection.query.bind(connection));

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
          status enum ('new', 'done', 'failed') not null,
          error varchar(1000)
        )`)
      )
      .then(() =>
        query(`CREATE TABLE locks(
          _id int not null primary key auto_increment,
          queue varchar(255) not null,
          worker char(36) not null,
          job int not null,
          status enum ('locking', 'locked', 'backed-off'),

          FOREIGN KEY fk_job(job)
          REFERENCES jobs(_id)
          ON UPDATE CASCADE
          ON DELETE CASCADE
        )`)
      )
      .then(promisify(connection.end.bind(connection)))
      .then(() => {
        done();
      })
      .catch(done);
  });
};

describe("Mysql integration", function() {
  this.timeout(60000);

  before("Initialize DB", initializeDatabase);

  before("Connect to DB", done => {
    const connection = mysql.createConnection({
      host: config.host,
      user: config.user,
      password: config.password,
      database: config.database
    });

    connection.connect(err => {
      assert.equal(null, err);
      log("Connected to server.");

      const query = promisify(connection.query.bind(connection));

      const wheres = function wheres(obj = {}) {
        const formatValue = function formatValue(value) {
          switch (typeof value) {
            case "number":
              return value;
            case "string":
              return `'${value}'`;
            default:
              return `'${JSON.stringify(value)}'`;
          }
        };

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
          `);
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
        createLock: function createLock({ job, queue, worker, status }) {
          return query(`
          INSERT INTO locks(job, queue, worker, status)
          VALUES(${job}, '${queue}', '${worker}', '${status}')
          `);
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
      globals.environment = environment;
      done();
    });
  });

  afterEach("Refresh DB", () => {
    return globals.query("DELETE FROM jobs").then(() => {
      log("DB refreshed");
    });
  });

  after("Close DB connection", done => {
    globals.connection.end(error => {
      if (!error) log("DB connection closed");
      done(error);
    });
  });

  describe("1 worker, 1 job", () => {
    it("Processes jobs", done =>
      testIntegration(globals.environment, 1, 1)(done));
  });

  describe("1 worker, 2 jobs", () => {
    it("Processes jobs", done =>
      testIntegration(globals.environment, 1, 2)(done));
  });

  describe("2 workers, 1 job", () => {
    it("Processes jobs", done =>
      testIntegration(globals.environment, 2, 1)(done));
  });

  describe("1 worker, 100 jobs", () => {
    it("Processes jobs", done =>
      testIntegration(globals.environment, 1, 100)(done));
  });

  describe("100 workers, 1 job", () => {
    it("Processes jobs", done =>
      testIntegration(globals.environment, 100, 1)(done));
  });

  describe("100 workers, 100 jobs", () => {
    it("Processes jobs", done =>
      testIntegration(globals.environment, 100, 100)(done));
  });

  describe("10 workers, 1000 jobs", () => {
    it("Processes jobs", done =>
      testIntegration(globals.environment, 10, 1000)(done));
  });

  describe("100 workers, 1000 jobs", () => {
    it("Processes jobs", done =>
      testIntegration(globals.environment, 100, 1000)(done));
  });
});
