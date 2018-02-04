/* eslint-env mocha */
import { MongoClient, ObjectID } from "mongodb";
import { promisify } from "util";
import debug from "debug";
import testIntegration from "./integration.test.mjs";

const log = debug("test:integration:mongo");

const globals = {};

describe("MongoDB integration", function() {
  this.timeout(60000);

  before("Connect to DB", done => {
    const url = "mongodb://localhost:27017";
    const dbName = "one-queue-test";

    MongoClient.connect(url, (error, client) => {
      if (error) return done(error);
      log("Connected to server.");

      const db = client.db(dbName);
      const jobs = db.collection("jobs");
      const locks = db.collection("locks");

      const promisifyToArray = fn => (...args) => {
        return new Promise((resolve, reject) => {
          fn(...args).toArray(
            (error, result) => (error ? reject(error) : resolve(result))
          );
        });
      };

      const environment = {
        createJob: function createJob(job) {
          return promisify(jobs.insertOne.bind(jobs))(job);
        },
        readJob: function readJob(query) {
          return promisifyToArray(jobs.find.bind(jobs))(query);
        },
        updateJob: function updateJob(id, props) {
          return promisify(jobs.updateOne.bind(jobs))(
            { _id: ObjectID(id) },
            { $set: props }
          );
        },
        createLock: function createLock(lock) {
          return promisify(locks.insertOne.bind(locks))(lock);
        },
        readLock: function readLock(query) {
          return promisifyToArray(locks.find.bind(locks))(query);
        },
        updateLock: function updateLock(query, props) {
          return promisify(locks.updateOne.bind(locks))(query, { $set: props });
        }
      };

      globals.client = client;
      globals.environment = environment;
      globals.jobs = jobs;
      globals.locks = locks;
      done();
    });
  });

  beforeEach("Refresh DB", () => {
    const deleteJobs = promisify(
      globals.jobs.deleteMany.bind(globals.jobs, {})
    );

    const deleteLocks = promisify(
      globals.locks.deleteMany.bind(globals.locks, {})
    );

    return Promise.all([deleteJobs(), deleteLocks()]).then(() =>
      log("DB refreshed")
    );
  });

  after("Close DB connection", done => {
    globals.client.close(() => {
      log("DB connection closed");
      done();
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
