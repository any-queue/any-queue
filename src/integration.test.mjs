/* eslint-env mocha */
import { Queue, Worker } from "./index.mjs";
import Countdown from "./countdown.mjs";
import debug from "debug";
import { contains } from "ramda";
import assert from "assert";
import { spy } from "sinon";

const log = debug("test:integration");

export default function testIntegration(environment, workerCount, jobCount) {
  return done => {
    log(environment)
    const queue = new Queue(environment, "test-queue");
    const createWorker = () => new Worker(environment, "test-queue");
    const createJob = i => queue.now({ i });
    const emptyArray = n => Array.from(Array(n));
    const workers = emptyArray(workerCount).map(createWorker);
    const createdJobs = emptyArray(jobCount).map((_, i) => createJob(i));

    log(`created ${workers.length} workers and ${createdJobs.length} jobs.`);

    const countdown = new Countdown(jobCount).then(() => {
      log("countdown reached 0");
      stop()
        .then(() => environment.readJob({}))
        .then(jobs => {
          assert.equal(
            jobs.length,
            createdJobs.length,
            "All jobs must be created"
          );
          const doneJobs = jobs.filter(j => j.status === "done");
          assert.equal(doneJobs.length, jobCount, "All jobs must be done");
        })
        .then(() => environment.readLock({}))
        .then(locks => {
          assert(
            locks.every(j => ["locked", "backed-off"].includes(j.status)),
            "All locks must be cleared"
          );
        })
        .then(() => {
          const actualData = flagJobHandled.args
            .map(args => args[0])
            .map(j => j.data);

          const expectedData = emptyArray(jobCount).map((_, i) => ({ i }));

          assert(
            expectedData.every(data => contains(data, actualData)),
            "Each job must be handled exactly once"
          );
          assert(
            actualData.every(data => contains(data, expectedData)),
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
}
