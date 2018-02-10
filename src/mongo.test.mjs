import { MongoClient, ObjectID } from "mongodb";
import testIntegration from "./integration.test.mjs";

const globals = {};
const dbName = "one-queue-test";
const url = "mongodb://localhost:27017";

const setup = function setup() {
  return MongoClient.connect(url).then(client => {
    globals.client = client;
    const db = client.db(dbName);
    const jobs = db.collection("jobs");
    const locks = db.collection("locks");

    const environment = {
      createJob: function createJob(job) {
        return jobs.insertOne(job);
      },
      readJob: function readJob(query) {
        return jobs.find(query).toArray();
      },
      updateJob: function updateJob(id, props) {
        return jobs.updateOne({ _id: ObjectID(id) }, { $set: props });
      },
      createLock: function createLock(lock) {
        return locks.insertOne(lock);
      },
      readLock: function readLock(query) {
        return locks.find(query).toArray();
      },
      updateLock: function updateLock(query, props) {
        return locks.updateOne(query, { $set: props });
      }
    };

    return environment;
  });
};

const refresh = function refresh() {
  const db = globals.client.db(dbName);
  const jobs = db.collection("jobs");
  const locks = db.collection("locks");

  return Promise.all([jobs.deleteMany({}), locks.deleteMany({})]);
};

const teardown = function teardown() {
  return globals.client.close();
};

testIntegration("mongo", setup, refresh, teardown);
