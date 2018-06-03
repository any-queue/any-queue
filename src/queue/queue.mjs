export default function createQueue({ persistenceInterface, name }) {
  const PRIORITY = {
    VERY_LOW: 30,
    LOW: 40,
    NORMAL: 50,
    HIGH: 60,
    VERY_HIGH: 70
  };

  const now = async function now(
    data,
    { priority = PRIORITY.NORMAL, blockers = [] } = {}
  ) {
    const { createJob, createLock } = persistenceInterface;

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
  };

  const later = function later() {
    // TBI
  };

  const repeat = function repeat() {
    // TBI
  };

  const after = function after() {
    // TBI
  };

  return {
    PRIORITY,
    now,
    later,
    repeat,
    after
  };
}
