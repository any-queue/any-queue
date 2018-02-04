export default class Queue {
  constructor(environment, name) {
    this.environment = environment;
    this.name = name;
  }

  now(data, priority = Queue.priority.NORMAL, blockers = []) {
    const { environment, name } = this;
    const { createJob } = environment;
    return createJob({
      queue: name,
      data,
      priority,
      blockers,
      status: "new",
      attempts: 0,
      outcome: [],
      processDate: [],
      scheduledDate: Date().toString()
    });
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
