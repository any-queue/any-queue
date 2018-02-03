import uuid from "uuid/v4";
import debug from "debug";

export default class Worker {
  constructor(environment, queue, name = uuid()) {
    this.environment = environment;
    this.queue = queue;
    this.name = name;
    this.log = debug(`worker:${name}`);
  }

  cancel() {}

  retry() {}

  promote() {}

  process(handler) {
    let stopped = false;
    const { environment, queue, name, log } = this;
    const {
      readJob,
      updateJob,
      createLock,
      readLock,
      updateLock
    } = environment;

    log("setting process up");

    setTimeout(async function _process() {
      stopped ? log("stopped") : log("processing");

      if (stopped) return;

      //const pickJob = reduce((acc, next) => maxBy(prop("priority"))(acc, next));
      const DatabaseError = Error("Corrupt database");
      const delay = function delay(milliseconds) {
        stopped ? log("stopped. wont delay") : log("delaying", milliseconds);
        return stopped
          ? Promise.resolve()
          : new Promise(resolve => {
              setTimeout(resolve, milliseconds);
            });
      };
      const wrapPromise = function wrapPromise(maybePromise) {
        return Promise.resolve().then(() => maybePromise);
      };
      const randomInteger = function randomInteger(min, max) {
        return Math.floor(Math.random() * (max - min) + min);
      };
      const pickJob = jobs => jobs[randomInteger(0, jobs.length)];

      // Tries to lock. If lock is successful, runs fn. Unlocks.
      const unlock = function unlock(job) {
        return updateLock(
          {
            job: job._id,
            queue,
            name,
            status: "locking"
          },
          { status: "backed-off" }
        );
      };

      const sealLock = function sealLock(job) {
        return updateLock(
          {
            job: job._id,
            queue,
            name,
            status: "locking"
          },
          { status: "locked" }
        );
      };

      const tryLock = async function tryLock(job) {
        await createLock({
          job: job._id,
          queue,
          name,
          status: "locking"
        });
        const locks = await Promise.all([
          readLock({ job: job._id, queue, status: "locking" }),
          readLock({ job: job._id, queue, status: "locked" })
        ]);
        const lockCount = locks[0].length + locks[1].length;

        log("got", lockCount, "locks");

        if (lockCount < 1) throw DatabaseError;

        return lockCount === 1;
      };

      const jobs = await readJob({ queue, status: "new" });
      log("got", jobs.length, "jobs");

      if (jobs.length === 0) return delay(1000).then(_process);

      const job = pickJob(jobs);

      const didLock = await tryLock(job);

      return didLock
        ? wrapPromise(handler(job))
            .then(() =>
              updateJob(job._id, { status: "done" }).then(() => sealLock(job))
            )
            .catch(error =>
              updateJob(job._id, {
                status: "failed",
                error: error.toString()
              }).then(() => unlock(job))
            )
            .then(() => delay(0))
            .then(_process)
        : unlock(job)
            .then(() => delay(randomInteger(0, 1000)))
            .then(_process);
    }, 0);

    return function stop() {
      stopped = true;
    };
  }
}
