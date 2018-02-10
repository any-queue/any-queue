export default class Queue {
  constructor(environment, name) {
    this.environment = environment;
    this.name = name;
  }

  async now(data, { priority = Queue.priority.NORMAL, blockers = [] } = {}) {
    const { environment, name } = this;
    const { createJob, createLock } = environment;

    const job = await createJob({
      queue: name,
      data,
      priority,
      status: blockers.length > 0 ? "blocked" : "new",
      attempts: 0,
      outcome: [],
      processDate: [],
      scheduledDate: Date().toString()
    });

    await Promise.all(
      blockers.map(blocker =>
        createLock({ queue: name, job, blocker, status: "locked" })
      )
    );

    return job;
  }

  later() {}

  repeat() {}

  after() {}
}

Queue.priority = {
  VERY_LOW: 30,
  LOW: 40,
  NORMAL: 50,
  HIGH: 60,
  VERY_HIGH: 70
};
