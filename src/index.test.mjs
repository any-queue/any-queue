import "./mongo.test.mjs";
import "./mysql.test.mjs";

process.on("unhandledRejection", err => {
  throw err;
});
