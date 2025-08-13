'use strict';

const Consumer = require('../kafka/Consumer.js');
const eachBatch = require('./eachBatch.js');

const args = process.argv;
const parameters = JSON.parse(args[2]);

const onError = async (error, message) => {
  console.error("Error with message", message.value.toString());
  console.error("Error: ", error);
};

const onProcess = async (message) => {
  console.log("Processing message", message.value.toString());
};

(async () => {
  const kafka = new Consumer(parameters.kafka);
  await kafka.start({ groupId: parameters.topic, });
  kafka.subscribe([{ topic: parameters.topic, fromBeginning: false }]);
  const config = {
    autoCommit: false,
    partitionsConsumedConcurrently: 3,
    eachBatchAutoResolve: false,
  };
  kafka.eachBatch(config, eachBatch({ limit: 500, ping: 15000, onError, onProcess }))
  process.on("message", async (message) => {
    if (message?.status === "stop") {
      await kafka.stop();
      process.exit(0);
    }
  });
  process.on("SIGINT", () => { });
  // rejection handling manually ? as process.send({status: "error"});
})();
