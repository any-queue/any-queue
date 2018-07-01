"use strict";
const testIntegration = require("any-queue-test");
const sqliteConnector = require("any-queue-sqlite");
const { Queue, Worker } = require("./lib/bundle.js");

process.on("unhandledRejection", err => {
  throw err;
});

testIntegration({
  Queue,
  Worker,
  name: "sqlite",
  createPersistenceInterface: () =>
    sqliteConnector({
      uri: "sqlite://localhost/any-queue"
    })
});
