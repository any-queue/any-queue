/* eslint-env mocha */
import mysql from "mysql";
import assert from "assert";
import { promisify } from "util";
import { contains } from "ramda";
import { Queue, Worker } from "./index.mjs";
import Countdown from "./countdown.mjs";
import debug from "debug";
import { spy } from "sinon";

const log = debug("test:mysql");
const config = {
  host: "localhost",
  rootUser: "root",
  rootPassword: "admin",
  user: "one_queue_test_user",
  password: "one_queue_test_password",
  database: "one_queue_test"
};

process.on("unhandledRejection", function(err) {
  throw err;
});
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
  // DB connections and cleanups take too long sometimes.
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

  const testProcessing = function testProcessing(workerCount, jobCount) {
    return done => {
      const queue = new Queue(globals.environment, "test-queue");
      const createWorker = () => new Worker(globals.environment, "test-queue");
      const createJob = i => queue.now({ i });
      const emptyArray = n => Array.from(Array(n));
      const workers = emptyArray(workerCount).map(createWorker);
      const createdJobs = emptyArray(jobCount).map((_, i) => createJob(i));

      log(`created ${workers.length} workers and ${createdJobs.length} jobs.`);

      const countdown = new Countdown(jobCount).then(() => {
        log("countdown reached 0");
        stop()
          .then(() => {
            const actualData = flagJobHandled.args
              .map(args => args[0])
              .map(j => j.data);

            const expectedData = emptyArray(jobCount).map((_, i) => ({ i }));

            assert(
              expectedData.every(data => contains(data, actualData)),
              "Each job must be handled exactly once"
            );
          })
          .then(done);
      });
      const flagJobHandled = spy(() => {
        countdown.tick();
      });

      const stop = (() => {
        const stops = workers.map(w => w.process(flagJobHandled));
        return () => Promise.all(stops.map(stop => stop()));
      })();
    };
  };

  const testPostConditions = function testPostConditions(
    workerCount,
    jobCount
  ) {
    return done => {
      const queue = new Queue(globals.environment, "test-queue");
      const createWorker = () => new Worker(globals.environment, "test-queue");
      const createJob = i => queue.now({ i });
      const emptyArray = n => Array.from(Array(n));
      const workers = emptyArray(workerCount).map(createWorker);
      const createdJobs = emptyArray(jobCount).map((_, i) => createJob(i));

      log(`created ${workers.length} workers and ${createdJobs.length} jobs.`);

      const countdown = new Countdown(jobCount).then(() => {
        log("countdown reached 0");
        stop()
          .then(() => globals.environment.readJob({}))
          .then(jobs => {
            assert.equal(
              jobs.length,
              createdJobs.length,
              "All jobs are created"
            );
            const doneJobs = jobs.filter(j => j.status === "done");
            assert.equal(doneJobs.length, jobCount, "All jobs must be done");
          })
          .then(() => globals.environment.readLock({}))
          .then(locks => {
            assert(
              locks.every(j => ["locked", "backed-off"].includes(j.status)),
              "All locks must be cleared"
            );
          })
          .then(() =>
            assert.equal(
              flagJobHandled.callCount,
              jobCount,
              "Handled job count must be equal to created job count"
            )
          )
          .then(done);
      });
      const flagJobHandled = spy(() => {
        countdown.tick();
      });

      const stop = (() => {
        const stops = workers.map(w => w.process(flagJobHandled));
        return () => Promise.all(stops.map(stop => stop()));
      })();
    };
  };

  describe("One worker, one job", () => {
    it("Processes jobs", testProcessing(1, 1));
    it("Postconditions", testPostConditions(1, 1));
  });

  describe("One worker, two jobs", () => {
    it("Processes jobs", testProcessing(1, 2));
    it("Postconditions", testPostConditions(1, 2));
  });

  describe("Two workers, one job", () => {
    it("Processes jobs", testProcessing(2, 1));
    it("Postconditions", testPostConditions(2, 1));
  });

  describe("One worker, 100 jobs", () => {
    it("Processes jobs", testProcessing(1, 100));
    it("Postconditions", testPostConditions(1, 100));
  });

  describe("100 workers, 1 job", () => {
    it("Processes jobs", testProcessing(100, 1));
    it("Postconditions", testPostConditions(100, 1));
  });

  describe("100 workers, 100 jobs", () => {
    it("Processes jobs", testProcessing(100, 100));
    it("Postconditions", testPostConditions(100, 100));
  });

  describe("100 workers, 1000 jobs", () => {
    it("Processes jobs", testProcessing(10, 1000));
    it("Postconditions", testPostConditions(10, 1000));
  });
});
