import debug from "debug";
import { max } from "ramda";

const randomInteger = function randomInteger(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
};

export default function createWorkerPersistenceFacade(
  persistenceInterface,
  queueId,
  workerId
) {
  const log = debug(`worker:${workerId}:persistence:`);

  const pickJob = function pickJob(jobs) {
    const maxPriority = jobs.map(j => j.priority).reduce(max, -Infinity);
    const maxPriorityJobs = jobs.filter(j => j.priority === maxPriority);
    return maxPriorityJobs[randomInteger(0, maxPriorityJobs.length)];
  };

  const unlock = function unlock(job) {
    const { updateLock } = persistenceInterface;

    return updateLock(
      {
        job: job._id,
        queue: queueId,
        worker: workerId,
        status: "locked"
      },
      { status: "backed-off" }
    );
  };

  const tryLock = async function tryLock(job) {
    const { createLock, readLock } = persistenceInterface;

    await createLock({
      job: job._id,
      queue: queueId,
      worker: workerId,
      status: "locked"
    });
    const locks = await readLock({
      job: job._id,
      queue: queueId,
      status: "locked"
    });
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
    const { updateJob } = persistenceInterface;

    return updateJob(job._id, { status: "done" });
  };

  const updateFailedJob = function updateFailedJob(job, error) {
    const { updateJob } = persistenceInterface;

    return updateJob(job._id, {
      status: "failed",
      error: error.toString()
    });
  };

  const readJob = function readJob() {
    log(`reading jobs in queueId ${queueId}`);
    const { readJob } = persistenceInterface;
    return readJob({ queue: queueId, status: "new" });
  };

  const unblock = async function unblock(blockerId) {
    const { updateJob, readLock, updateLock } = persistenceInterface;

    const removeLock = async function removeLock(lock) {
      // Working under the asumptions that:
      // * jobs that have blockers are in 'blocked' status.
      // * all locks of a job in 'blocked' status are caused by blockers (not workers).
      const remainingLocks = await readLock({
        queue: queueId,
        job: lock.job,
        status: "locked"
      });
      if (remainingLocks.length === 0) {
        log(`No lock remaining for job ${lock.job}, will unblock job.`);
        await updateJob(lock.job, { status: "new" });
      }
    };

    log(`unblocking jobs blocked by ${blockerId}`);

    const locksToRemove = await readLock({
      queue: queueId,
      blocker: blockerId,
      status: "locked"
    });

    log(`Will remove locks [${locksToRemove.map(l => l._id).join(",")}].`);

    await updateLock(
      { queue: queueId, blocker: blockerId, status: "locked" },
      { status: "backed-off" }
    );

    return Promise.all(locksToRemove.map(removeLock));
  };

  return {
    readJob,
    pickJob,
    tryLock,
    unlock,
    unblock,
    updateFinishedJob,
    updateFailedJob
  };
}
