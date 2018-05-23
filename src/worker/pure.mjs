import debug from "debug";
import { taggedSum as sum } from "daggy";
import delay from "delay";

const LaborOutcome = sum("LaborOutcome", {
  EmptyQueue: [],
  LockCollision: [],
  Done: [],
  Accident: ["details"]
});

const randomInteger = function randomInteger(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
};

const wrapPromise = function wrapPromise(x) {
  return Promise.resolve().then(() => x);
};

const exponentialBackoff = function exponentialBackoff(
  backoffDelay,
  collisionCount
) {
  return randomInteger(0, 2 ** collisionCount) * backoffDelay;
};

export default function createWorker({
  persistence,
  queueId,
  workerId,
  pollingDelay,
  backoffDelay,
  workInstructions
}) {
  const log = debug(`worker:${workerId}:`);

  const workOnJob = async function workOnJob() {
    const {
      readJob,
      pickJob,
      tryLock,
      unlock,
      unblock,
      updateFinishedJob,
      updateFailedJob
    } = persistence;

    const jobs = await readJob();

    log("got", jobs.length, "jobs");

    if (jobs.length === 0) return LaborOutcome.EmptyQueue;

    const job = pickJob(jobs);

    const didLock = await tryLock(job);

    if (!didLock) {
      await unlock(job);
      return LaborOutcome.LockCollision;
    }

    // `followInstructions` might return a value or a Promise, so we convert either type to Promise, so that we can handled them homogenously.
    return wrapPromise(workInstructions(job)).then(
      () =>
        updateFinishedJob(job)
          .then(() => unblock(job._id))
          .then(() => LaborOutcome.Done),
      error =>
        updateFailedJob(job, error).then(() => LaborOutcome.Accident(error))
    );
  };

  const planNextJob = function planNextJob(previousCollisionCount, result) {
    return result.cata({
      LockCollision: () => ({
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
    // Make sure user-provided promise does not do anything weird.
    const STOPPED = "@@stopped";
    const stopping = waitingToStop.catch(() => undefined).then(() => STOPPED);

    const workAgain = function workAgain({ delayTime, collisionCount }) {
      const working = delay(delayTime).then(() => workOnJob());

      return Promise.race([stopping, working]).then(
        result =>
          // When user stops the worker, we still need to wait for current labor to finish to allow for grateful shutdown and to propagate any exception it raises.
          result === STOPPED
            ? working
                // Hide implementation details by not returning a `LaborOutcome`.
                .then(() => undefined)
            : working
                .then(result => planNextJob(collisionCount, result))
                .then(({ delayTime, collisionCount }) => {
                  log(
                    `will back off for ${delayTime} milliseconds after ${collisionCount} collisions`
                  );
                  return workAgain({ delayTime, collisionCount });
                })
      );
    };

    return workAgain({ delayTime: 0, collisionCount: 0 });
  };

  return { work: workUntilStopped, id: workerId, queue: queueId };
}
