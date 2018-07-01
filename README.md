# a-queue

WIP

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

queue.now({ "bar": "foobar" });
// Will eventually print "[Object object]".

setTimeout(() => worker.punchOut(), 100);
```
