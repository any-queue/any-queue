import debug from "debug";
const log = debug("countdown:");

class Task {
  constructor(createPromise) {
    this.createPromise = createPromise;
  }

  static of(x) {
    return new Task(() => Promise.resolve(x));
  }

  then(fn) {
    return new Task(() => this.createPromise().then(fn));
  }

  run() {
    return this.createPromise();
  }
}

export default class Countdown {
  constructor(n, { __task = Task.of() } = {}) {
    this.__n = n;
    this.__task = __task;
  }

  then(fn) {
    return new Countdown(this.__n, { __task: this.__task.then(fn) });
  }

  chain(fn) {
    return this.then(fn);
  }

  map(fn) {
    return this.then(fn);
  }

  tick() {
    log("tick");
    this.__n = this.__n - 1;
    if (this.__n === 0) {
      log("tack");
      this.__task.run();
    }
    return this;
  }
}
