/* eslint-env mocha */
import { Queue, Worker } from "./index.mjs";
import Countdown from "./countdown.mjs";
import debug from "debug";
import { countBy, head, prop, sortBy } from "ramda";
import assert from "assert";
import { spy } from "sinon";

const emptyArray = length => Array.from(Array(length));

export default function testIntegration(name, setup, refresh, teardown) {
  const log = debug(`test:integration:${name}`);
  let environment;

  const assertJobsCreated = function assertJobsCreated(jobCount) {
    return environment.readJob({}).then(jobs => {
      assert.equal(jobs.length, jobCount, "Some jobs were not created.");
    });
  };

  const assertJobsDone = function assertJobsDone(jobCount) {
    return environment.readJob({}).then(jobs => {
      const doneJobs = jobs.filter(j => j.status === "done");
      assert.equal(doneJobs.length, jobCount, "Some jobs have not been done.");
    });
  };

  const assertJobsHandled = function assertJobsHandled(jobCount, handledJobs) {
    const handledData = handledJobs.map(prop("data"));
    const timesHandled = Object.values(countBy(prop("i"))(handledData));
    const wasAnyHandledTwice = timesHandled.some(times => times > 1);

    assert(handledJobs.length >= jobCount, "Some jobs were not handled.");
    assert(!wasAnyHandledTwice, "Some jobs were handled twice.");
  };

  const testJobProcessing = function testJobProcessing(workerCount, jobCount) {
    return done => {
      const countdown = new Countdown(jobCount)
        .then(() => stop())
        .then(() => assertJobsCreated(jobCount))
        .then(() => assertJobsDone(jobCount))
        .then(() => assertJobsHandled(jobCount, flagJobHandled.args.map(head)))
        .then(done);

      const flagJobHandled = spy(() => {
        countdown.tick();
      });

      const queue = new Queue(environment, "test-queue");
      const createWorker = () =>
        Worker({
          persistenceInterface: environment,
          queueId: "test-queue",
          workInstructions: flagJobHandled
        });
      const createJob = i => queue.now({ i });
      const workers = emptyArray(workerCount).map(createWorker);
      const createdJobs = emptyArray(jobCount).map((_, i) => createJob(i));

      log(`created ${workers.length} workers and ${createdJobs.length} jobs.`);

      const stop = (() => {
        workers.forEach(w => w.punchIn());
        return () => Promise.all(workers.map(w => w.punchOut()));
      })();
    };
  };

  const assertPriority = function assertPriority(handledJobs) {
    const handledData = handledJobs.map(prop("data"));
    assert.deepEqual(
      handledData,
      sortBy(data => -data.i, handledData),
      "Job priority was not observed."
    );
  };

  const testJobPriority = function testJobPriority(workerCount, jobCount) {
    return done => {
      const countdown = new Countdown(jobCount)
        .then(() => stop())
        .then(() => assertPriority(flagJobHandled.args.map(head)))
        .then(done);

      const flagJobHandled = spy(() => {
        countdown.tick();
      });

      const queue = new Queue(environment, "test-queue");
      const createWorker = () =>
        Worker({
          persistenceInterface: environment,
          queueId: "test-queue",
          workInstructions: flagJobHandled
        });
      const createJob = i => queue.now({ i }, { priority: i });
      const workers = emptyArray(workerCount).map(createWorker);
      const createdJobs = emptyArray(jobCount).map((_, i) => createJob(i));

      log(`created ${workers.length} workers and ${createdJobs.length} jobs.`);

      const stop = (() => {
        workers.forEach(w => w.punchIn());
        return () => Promise.all(workers.map(w => w.punchOut()));
      })();
    };
  };

  const assertHandlingOrder = function assertHandlingOrder(handledJobs) {
    const handledData = handledJobs.map(prop("data"));
    assert.deepEqual(
      handledData,
      sortBy(prop("i"), handledData),
      "Job blockers were not observed."
    );
  };

  const testJobBlocker = function testJobBlocker(workerCount, jobCount) {
    return done => {
      const queue = new Queue(environment, "test-queue");
      const createJob = i => blockers => queue.now({ i }, { blockers });
      const creatingJobs = emptyArray(jobCount)
        .map((_, i) => createJob(i))
        .reduce(
          (creatingJobs, creator) =>
            creatingJobs.then(jobs =>
              creator(jobs).then(nextJob => jobs.concat(nextJob))
            ),
          Promise.resolve([])
        );

      creatingJobs.then(createdJobs => {
        const countdown = new Countdown(jobCount)
          .then(() => stop())
          .then(() => assertHandlingOrder(flagJobHandled.args.map(head)))
          .then(done);

        const flagJobHandled = spy(() => {
          countdown.tick();
        });

        const createWorker = () =>
          Worker({
            persistenceInterface: environment,
            queueId: "test-queue",
            workInstructions: flagJobHandled
          });
        const workers = emptyArray(workerCount).map(createWorker);

        log(
          `created ${workers.length} workers and ${createdJobs.length} jobs.`
        );

        const stop = (() => {
          workers.forEach(w => w.punchIn());
          return () => Promise.all(workers.map(w => w.punchOut()));
        })();
      });
    };
  };

  const jobProcessingTestCase = function jobProcessingTestCase(
    workerCount,
    jobCount
  ) {
    it(
      `Processes ${jobCount} jobs with ${workerCount} workers`,
      testJobProcessing(workerCount, jobCount)
    );
  };

  const priorityTestCase = function priorityTestCase(workerCount, jobCount) {
    it(
      `Processes ${jobCount} jobs with ${workerCount} workers, observing priority`,
      testJobPriority(workerCount, jobCount)
    );
  };

  const blockersTestCase = function blockersTestCase(workerCount, jobCount) {
    it(
      `Processes ${jobCount} jobs with ${workerCount} workers, observing blockers`,
      testJobBlocker(workerCount, jobCount)
    );
  };

  describe(`Test integration with ${name}`, function() {
    this.timeout(60000);
    before("Set up", () => setup().then(env => (environment = env)));
    beforeEach("Refresh", refresh);
    after("Teardown", teardown);

    jobProcessingTestCase(1, 1);
    jobProcessingTestCase(1, 2);
    jobProcessingTestCase(2, 1);
    priorityTestCase(1, 1);
    priorityTestCase(1, 20);
    blockersTestCase(1, 1);
    blockersTestCase(5, 20);
    jobProcessingTestCase(1, 100);
    jobProcessingTestCase(100, 1);
    jobProcessingTestCase(10, 1000);
  });
}
