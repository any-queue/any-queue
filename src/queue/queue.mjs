export default function createQueue({ persistenceInterface, name }) {
  const { connect, disconnect, createJob, createLock } = persistenceInterface;

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
    await connect();

    const job = await createJob({
      queue: name,
      data,
      priority,
      status: blockers.length > 0 ? "blocked" : "new",
      scheduledDate: Date().toString()
    });

    await Promise.all(
      blockers.map(blocker =>
        createLock({ queue: name, job, blocker, status: "locked" })
      )
    );

    disconnect();

    return job;
  };

  const later = function later() {
    // @TODO
  };

  const repeat = function repeat() {
    // @TODO
  };

  const after = function after() {
    // @TODO
  };

  return {
    PRIORITY,
    now,
    later,
    repeat,
    after
  };
}
