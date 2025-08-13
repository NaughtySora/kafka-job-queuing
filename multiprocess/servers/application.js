'use strict';

const Producer = require('../kafka/Producer.js');
const batching = require('./batching.js');
const { Partitioners } = require("kafkajs");

const args = process.argv;
const parameters = JSON.parse(args[2]);

(async () => {
  const kafka = new Producer(parameters.kafka);
  await kafka.start({ createPartitioner: Partitioners.LegacyPartitioner, });
  const { stop } = batching(kafka.producer, { topic: parameters.topic });
  process.on("message", async (message) => {
    if (message?.status === "stop") {
      await stop();
      await kafka.stop();
      process.exit(0);
    }
  });
  process.on("SIGINT", () => { });
  // rejection handling manually ? as process.send({status: "error"});
})();