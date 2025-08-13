'use strict';

process.removeAllListeners("warning");

const kafka = require("./producer.js");
const config = require("./config.json");
const mock = require("../internal/mock.js");
const consumer = require("./consumer.js");

const transactions = async (producer, { ms = 100, count = 35 } = {}) => {
  const transaction = () => ({
    id: mock.uuid(),
    content: mock.hash(),
    date: mock.date(),
    amount: mock.int(1, 2000)
  });
  const timer = setTimeout(() => {
    const messages = [];
    let i = count;
    while (i-- !== 0) {
      messages.push({ value: JSON.stringify(transaction()) })
    }
    producer.send({ topic: "transactions", messages, });
    timer.refresh();
  }, ms);
};

const sales = async (producer) => {
  await producer.send({ topic, messages, });
};

const third_party = async (producer) => {
  await producer.send({ topic, messages, });
};

const main = async () => {
  const { start, stop } = kafka(config);
  const producer = await start();
  transactions(producer, { ms: 500, count: 3 });
  // sales(producer);
  // third_party(producer);

  process.on("SIGINT", async () => {
    await stop();
    console.log("Graceful shutdown");
    process.exit(0);
  });

  process.on("uncaughtException", async (e) => {
    await stop();
    console.error("Error", e);
    process.exit(1);
  });
};

main();
consumer({ groupId: "group-1", topic: "transactions" });
