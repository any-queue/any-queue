import debug from "debug";
import { taggedSum as sum } from "daggy";

const WorkerState = sum("WorkerState", {
  AtHome: [],
  AtWork: ["working", "stop"],
  GoingHome: [],
  Injured: ["details"]
});

const createDeferred = function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((_resolve, _reject) => {
    resolve = _resolve;
    reject = _reject;
  });

  return {
    resolve,
    reject,
    promise
  };
};

export default function createStatefulWorker(worker) {
  const log = debug(`worker:${worker.id}:`);
  let state = WorkerState.AtHome;

  const setState = function setState(newState) {
    const allowedTransitions = [
      [WorkerState.AtHome, WorkerState.AtWork],
      [WorkerState.AtWork, WorkerState.GoingHome],
      [WorkerState.GoingHome, WorkerState.AtHome],
      [WorkerState.AtWork, WorkerState.Injured]
    ];

    const isAllowed = (oldState, newState) =>
      allowedTransitions.some(
        ([fromState, toState]) => fromState.is(oldState) && toState.is(newState)
      );

    if (isAllowed(state, newState)) {
      log(`changed from ${state} to ${newState}.`);
      state = newState;
    } else {
      const error = Error("");
      log(error);
      state = WorkerState.Injured(error);
    }
  };

  const punchIn = function punchIn() {
    return state.cata({
      GoingHome: () => Promise.reject("Worker is going home."),
      AtWork: () => Promise.reject("Worker is already at work."),
      Injured: sickness => Promise.reject(`Worker is sick with ${sickness}.`),
      AtHome: () => {
        const waitingToStop = createDeferred();

        const working = worker
          .work(waitingToStop.promise)
          .then(() => {
            setState(WorkerState.AtHome);
          })
          .catch(error => {
            setState(WorkerState.Injured(error));
            throw error;
          });

        setState(WorkerState.AtWork(working, waitingToStop.resolve));

        return working;
      }
    });
  };

  const punchOut = function punchOut() {
    return state.cata({
      GoingHome: () => Promise.reject("Worker is already going home."),
      AtHome: () => Promise.reject("Worker is already at home."),
      Injured: sickness => Promise.reject(`Worker is sick with ${sickness}.`),
      AtWork: (working, stop) => {
        setState(WorkerState.GoingHome);
        stop();
        return working;
      }
    });
  };

  log("created");

  return {
    punchIn,
    punchOut
  };
}
