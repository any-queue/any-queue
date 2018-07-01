#

# any-queue

[![npm version](https://img.shields.io/npm/v/any-queue.svg)](https://www.npmjs.com/package/any-queue)

> Technology-agnostic, persistent job queue

## Install

Install it together with a connector:

```
$ npm install --save any-queue any-queue-mysql
```

## Test

```
$ npm test
```

## Usage

```js
import { Queue, Worker } from "any-queue";
import anyQueueMysql from "any-queue-mysql";

const persistenceInterface = anyQueueMysql({
  uri: "mysql://root:nt3yx7ao2e9@localhost/any-queue-demo"
});

const queue = Queue({ persistenceInterface, name: "foo" });
const worker = Worker({
  persistenceInterface,
  queueName: "foo",
  instructions: job => {
    console.log(job);
  }
});

worker.punchIn();

queue.now({ bar: "foobar" });
// Will eventually print "{ bar: 'foobar' }".

setTimeout(() => worker.punchOut(), 100);
```

## API

wip

## License

MIT © Gerardo Munguia
