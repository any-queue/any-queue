# a-queue
WIP

```js
import { Queue, Worker } from "any-queue";
import persistenceInterface from "any-queue-mongodb";

const queue = Queue({ persistenceInterface, name: "a-queue" });
const worker = Worker({
  persistenceInterface,
  queueName: "a-queue",
  instructions: job => {
    console.log(job);
  }
});

queue.now({ "some-data": "foobar" });
worker.punchIn();
// Will eventually print "[Object object]".

setTimeout(() => worker.punchOut(), 100);
```
