import uuid from "uuid/v4";
import debug from "debug";
import { max } from "ramda";
import { taggedSum as sum } from "daggy";
import delay from "delay";

const randomInteger = function randomInteger(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
};

const JobProcessResult = sum("WorkdayResult", {
  QueueEmpty: [],
  LockCollision: [],
  Handled: [],
  HandlerError: ["details"]
});

const WorkerState = sum("WorkerState", {
  AtHome: [],
  GoingHome: [],
  AtWork: [],
  Sick: ["details"]
});

const wrapPromise = function wrapPromise(x) {
  return Promise.resolve().then(() => x);
};

const exponentialBackoff = function exponentialBackoff(
  backoffDelay,
  collisionCount
) {
  return randomInteger(0, 2 ** collisionCount - 1) * backoffDelay;
};

export default function createWorker(environment, queue, name = uuid()) {
  const log = debug(`worker:${name}`);
  const pollingDelay = 1000; // milliseconds
  const backoffDelay = 50; // milliseconds
  let state = WorkerState.AtHome;

  // ------------------
  // Persistence layer.
  // ------------------

  const pickJob = function pickJob(jobs) {
    const maxPriority = jobs.map(j => j.priority).reduce(max, -Infinity);
    const maxPriorityJobs = jobs.filter(j => j.priority === maxPriority);
    return maxPriorityJobs[randomInteger(0, maxPriorityJobs.length)];
  };

  const unlock = function unlock(job) {
    const { updateLock } = environment;
    return updateLock(
      {
        job: job._id,
        queue,
        worker: name,
        status: "locked"
      },
      { status: "backed-off" }
    );
  };

  const tryLock = async function tryLock(job) {
    const { createLock, readLock } = environment;
    await createLock({
      job: job._id,
      queue,
      worker: name,
      status: "locked"
    });
    const locks = await readLock({ job: job._id, queue, status: "locked" });
    const lockCount = locks.length;

    log(
      "got",
      lockCount,
      "locks for job",
      job._id,
      "from",
      locks.map(l => l.worker).join(", ")
    );

    if (lockCount < 1) throw Error("Corrupt database: cannot find lock.");

    return lockCount === 1;
  };

  const updateFinishedJob = function updateFinishedJob(job) {
    const { updateJob } = environment;
    return updateJob(job._id, { status: "done" });
  };

  const updateFailedJob = function updateFailedJob(job, error) {
    const { updateJob } = environment;
    return updateJob(job._id, {
      status: "failed",
      error: error.toString()
    });
  };

  const readJob = function readJob() {
    const { readJob } = environment;
    return readJob({ queue, status: "new" });
  };

  const unblock = async function unblock(blockerId) {
    const { updateJob, readLock, updateLock } = environment;

    const removeLock = async function removeLock(lock) {
      // Working under the asumptions that:
      // * jobs that have blockers are in 'blocked' status.
      // * all locks of a job in 'blocked' status are caused by blockers (not workers).
      const remainingLocks = await readLock({
        queue,
        job: lock.job,
        status: "locked"
      });
      if (remainingLocks.length === 0) {
        log(`No lock remaining for job ${lock.job}, will unblock job.`);
        await updateJob(lock.job, { status: "new" });
      }
    };

    log(`Blocker ${blockerId} finished.`);

    const locksToRemove = await readLock({
      queue,
      blocker: blockerId,
      status: "locked"
    });

    log(`Will remove locks [${locksToRemove.map(l => l._id).join(",")}].`);

    await updateLock(
      { queue, blocker: blockerId, status: "locked" },
      { status: "backed-off" }
    );

    return Promise.all(locksToRemove.map(removeLock));
  };

  // ------------------
  // Work flow logic.
  // ------------------

  const processJob = async function processJob(handler) {
    const jobs = await readJob();

    log("got", jobs.length, "jobs");

    if (jobs.length === 0) return JobProcessResult.QueueEmpty;

    const job = pickJob(jobs);

    const didLock = await tryLock(job);

    if (!didLock) {
      await unlock(job);
      return JobProcessResult.LockCollision;
    }

    return wrapPromise(handler(job)).then(
      () =>
        updateFinishedJob(job)
          .then(() => unblock(job._id))
          .then(() => JobProcessResult.Handled),
      error =>
        updateFailedJob(job, error).then(() =>
          JobProcessResult.HandlerError(error)
        )
    );
  };

  const setState = function setState(newState) {
    if (WorkerState.Sick.is(state)) {
      log(`Cannot update worker state to ${newState} from ${state}.`);
      return Promise.reject("worker in error state");
    }

    if (!WorkerState.is(newState)) {
      log(`Cannot update worker state. Invalid new state: ${newState}`);
      state = WorkerState.Sick(`Invalid new state: ${newState}`);
      return Promise.reject("invalid new state");
    }

    state = newState;
    return Promise.resolve();
  };

  const parseResult = function parseResult(previousCollisionCount, result) {
    return result.cata({
      LockCollision: () => ({
        delayTime: exponentialBackoff(backoffDelay, previousCollisionCount),
        collisionCount: previousCollisionCount + 1
      }),
      QueueEmpty: () => ({
        delayTime: pollingDelay,
        collisionCount: 0
      }),
      Handled: () => ({
        delayTime: 0,
        collisionCount: 0
      }),
      HandledError: () => ({
        delayTime: 0,
        collisionCount: 0
      })
    });
  };

  const loop = async function loop({ handler, delayTime, collisionCount }) {
    return state.cata({
      AtHome: () => {
        return setState(WorkerState.AtWork).then(() =>
          loop({ handler, delayTime, collisionCount })
        );
      },
      GoingHome: () => {
        return setState(WorkerState.AtHome);
      },
      Sick: details => {
        log("Loop called while on error state");
        return Promise.reject(details);
      },
      AtWork: () => {
        return delay(delayTime)
          .then(() => processJob(handler))
          .then(result => parseResult(collisionCount, result))
          .then(({ delayTime, collisionCount }) => {
            log(
              `will back off for ${delayTime} milliseconds after ${collisionCount} collisions`
            );
            return loop({ handler, delayTime, collisionCount });
          });
      }
    });
  };

  return {
    process: function process(
      handler,
      handleError = error => {
        throw error;
      }
    ) {
      return state.cata({
        AtWork: () => {
          log("already at work");
          return () => {};
        },
        GoingHome: () => {
          log("going home");
          return () => {};
        },
        Sick: () => {
          log("sick");
          return () => {};
        },
        AtHome: () => {
          const running = loop({
            handler,
            delayTime: 0,
            collisionCount: 0
          }).catch(handleError);

          return function stop() {
            return state.cata({
              AtHome: () => Promise.reject(Error("Cannot stop an idle worker")),
              GoingHome: () =>
                Promise.reject(Error("Worker is already stopping")),
              AtWork: () => {
                setState(WorkerState.GoingHome);
                return running;
              }
            });
          };
        }
      });
    }
  };
}
