import uuid from "uuid/v4";
import createStatefulWorker from "./stateful.mjs";
import createPureWorker from "./pure.mjs";
import createPersistenceInterface from "./persistence.mjs";

export default function createWorker({
  queueId,
  workerId = uuid(),
  persistenceInterface,
  workInstructions
}) {
  const persistence = createPersistenceInterface(
    persistenceInterface,
    queueId,
    workerId
  );

  const worker = createPureWorker({
    persistence,
    queueId,
    workerId,
    pollingDelay: 1000, // milliseconds
    backoffDelay: 50, // milliseconds
    workInstructions
  });

  return createStatefulWorker(worker);
}
