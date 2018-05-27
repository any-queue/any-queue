import uuid from "uuid/v4";
import createStatefulWorker from "./stateful.mjs";
import createPureWorker from "./pure.mjs";
import createPersistenceFacade from "./persistence.mjs";

export default function createWorker({
  queueName,
  workerName = uuid(),
  persistenceInterface,
  instructions,
  pollingDelay = 1000, // milliseconds
  backoffDelay = 50 // milliseconds
}) {
  const persistenceFacade = createPersistenceFacade(
    persistenceInterface,
    queueName,
    workerName
  );

  const worker = createPureWorker({
    persistenceInterface: persistenceFacade,
    queueName,
    workerName,
    pollingDelay,
    backoffDelay,
    instructions
  });

  return createStatefulWorker(worker);
}
