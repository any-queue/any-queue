import debug from "debug";
import { taggedSum as sum } from "daggy";
import delay from "delay";

const wrapInPromise = function wrapInPromise(x) {
  return Promise.resolve().then(() => x);
};

const randomInteger = function randomInteger(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
};

const exponentialBackoff = function exponentialBackoff(
  backoffDelay,
  collisionCount
) {
  return randomInteger(0, 2 ** collisionCount) * backoffDelay;
};

const LaborOutcome = sum("LaborOutcome", {
  EmptyQueue: [],
  LockCollision: [],
  Done: [],
  Accident: ["details"]
});

export default function createWorker({
  persistenceInterface,
  queueName,
  workerName,
  pollingDelay,
  backoffDelay,
  instructions
}) {
  const log = debug(`anyqueue:worker:${workerName}:pure:`);

  const workOnJob = async function workOnJob() {
    const {
      readJob,
      pickJob,
      tryLock,
      unlock,
      unblock,
      updateFinishedJob,
      updateFailedJob
    } = persistenceInterface;

    const jobs = await readJob();

    log("got", jobs.length, "jobs");

    if (jobs.length === 0) return LaborOutcome.EmptyQueue;

    const job = pickJob(jobs);

    const didLock = await tryLock(job);

    if (!didLock) {
      await unlock(job);
      return LaborOutcome.LockCollision;
    }

    log("locked into job", job.id);

    // `instructions` might return a value or a Promise, so we convert either type to Promise, so that we can handled them homogenously.
    return wrapInPromise(instructions(job)).then(
      () =>
        updateFinishedJob(job)
          .then(() => unblock(job.id))
          .then(() => LaborOutcome.Done),
      error =>
        updateFailedJob(job, error).then(() => LaborOutcome.Accident(error))
    );
  };

  const planNextJob = function planNextJob(previousCollisionCount, result) {
    return result.cata({
      LockCollision: () => ({
        // Exponential backoff is a common algorithm to handle collisions (see https://en.wikipedia.org/wiki/Exponential_backoff).
        delayTime: exponentialBackoff(backoffDelay, previousCollisionCount),
        collisionCount: previousCollisionCount + 1
      }),
      EmptyQueue: () => ({
        delayTime: pollingDelay,
        collisionCount: 0
      }),
      Done: () => ({
        delayTime: 0,
        collisionCount: 0
      }),
      Accident: () => ({
        delayTime: 0,
        collisionCount: 0
      })
    });
  };

  const workUntilStopped = function workUntilStopped(waitingToStop) {
    const { connect, disconnect } = persistenceInterface;

    // Since we race between working and stopping, use special resolve value for stopping, so that we can easily evaluate which of the racing promises has fulfilled first.
    const STOPPED = "@@stopped";
    const stopping = waitingToStop.then(() => STOPPED);

    const workAgain = function workAgain({ delayTime, collisionCount }) {
      const working = delay(delayTime).then(() => workOnJob());

      return (
        Promise.race([stopping, working])
          .then(
            result =>
              result === STOPPED
                ? // When user stops the worker, we still need to wait for current labor to finish to allow for grateful shutdown and to propagate any exception it raises.
                  working
                    .then(() => disconnect())
                    // Hide implementation details by not returning internal results.
                    .then(() => undefined)
                : working
                    .then(result => planNextJob(collisionCount, result))
                    .then(({ delayTime, collisionCount }) => {
                      log(
                        `will back off for ${delayTime} milliseconds after ${collisionCount} collisions`
                      );
                      return workAgain({ delayTime, collisionCount });
                    })
          )
          // We cannot recover at this point, so dispose resources (ie close DB connection).
          .catch(error =>
            disconnect()
              // The user cares more about the original error, so ignore `dbError`.
              .catch(dbError => {
                log(dbError);
              })
              .then(() => {
                throw error;
              })
          )
      );
    };

    return connect().then(() => workAgain({ delayTime: 0, collisionCount: 0 }));
  };

  return { work: workUntilStopped, name: workerName, queue: queueName };
}
