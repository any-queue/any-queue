import uuid from "uuid/v4";
import debug from "debug";

export default class Worker {
  constructor(environment, queue, name = uuid()) {
    this.environment = environment;
    this.queue = queue;
    this.name = name;
    this.log = debug(`worker:${name}`);
    this.stopped = false;
    this.failedLocks = 0;

    this.pollingDelay = 1000; // milliseconds
    this.backoffDelay = 50; // milliseconds
  }

  cancel() {}

  retry() {}

  promote() {}

  wrapPromise(maybePromise) {
    return this.delay().then(() => maybePromise);
  }

  randomInteger(min, max) {
    return Math.floor(Math.random() * (max - min) + min);
  }

  pickJob(jobs) {
    // todo: use priority
    return jobs[this.randomInteger(0, jobs.length)];
  }

  unlock(job) {
    const { environment, queue, name } = this;
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
  }

  async tryLock(job) {
    const { environment, queue, name, log } = this;
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
  }

  updateFinishedJob(job) {
    const { environment } = this;
    const { updateJob } = environment;
    return updateJob(job._id, { status: "done" });
  }

  updateFailedJob(job, error) {
    const { environment } = this;
    const { updateJob } = environment;
    return updateJob(job._id, {
      status: "failed",
      error: error.toString()
    });
  }

  delay(milliseconds = 0) {
    return new Promise(resolve => {
      setTimeout(resolve, milliseconds);
    });
  }

  readJob() {
    const { environment, queue } = this;
    const { readJob } = environment;
    return readJob({ queue, status: "new" });
  }

  exponentialBackoff() {
    const delay =
      this.randomInteger(0, 2 ** this.failedLocks - 1) * this.backoffDelay;
    this.log(`will back off for ${delay} milliseconds`);
    return delay;
  }

  async _process(handler, done) {
    const { log } = this;

    this.stopped ? log("stopped") : log("processing");

    if (this.stopped) return done();

    const jobs = await this.readJob();

    log("got", jobs.length, "jobs");

    if (jobs.length === 0) {
      return this.delay(this.stopped ? 0 : this.pollindDelay).then(() =>
        this._process(handler, done)
      );
    }

    const job = this.pickJob(jobs);

    const didLock = await this.tryLock(job);

    if (didLock) this.failedLocks = 0;
    else ++this.failedLocks;

    return didLock
      ? this.wrapPromise(handler(job))
          .then(() => this.updateFinishedJob(job))
          .catch(error =>
            this.updateFailedJob(job, error).then(() => this.unlock(job))
          )
          .then(() => this.delay())
          .then(() => this._process(handler, done))
      : this.unlock(job)
          .then(() => this.delay(this.stopped ? 0 : this.exponentialBackoff()))
          .then(() => this._process(handler, done));
  }

  process(handler) {
    const worker = this;
    const stopping = new Promise(resolve => {
      setTimeout(() => this._process(handler, resolve), 0);
    });

    return function stop() {
      worker.stopped = true;
      return stopping;
    };
  }
}
