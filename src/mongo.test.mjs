/* eslint-env mocha */
import { MongoClient, ObjectID } from "mongodb";
import assert from "assert";
import { promisify } from "util";
import { contains } from "ramda";
import { Queue, Worker } from "./index.mjs";
import Countdown from "./countdown.mjs";
import debug from "debug";
import { spy } from "sinon";

const log = debug("test:mongo");

process.on("unhandledRejection", function(err) {
  throw err;
});
const globals = {};

describe("one-queue", function() {
  // DB connections and cleanups take too long sometimes.
  this.timeout(10000);
  before("Connect to DB", done => {
    const url = "mongodb://localhost:27017";
    const dbName = "one-queue-test";

    MongoClient.connect(url, (err, client) => {
      assert.equal(null, err);
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

  afterEach("Refresh DB", () => {
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

  const delay = function delay(milliseconds) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
  };

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
        stop();
        // 2000 ms is enough time for workers to stop.
        delay(2000)
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
        return () => {
          stops.forEach(stop => stop());
        };
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
        stop();

        // 2000 ms is enough time for workers to stop.
        delay(2000)
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
        return () => {
          stops.forEach(stop => stop());
        };
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

  describe("100 workers, 1000 jobs", function() {
    this.timeout = 15000;
    it("Processes jobs", testProcessing(10, 1000));
    it("Postconditions", testPostConditions(10, 1000));
  });
});
