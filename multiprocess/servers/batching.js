'use strict';

const { CompressionTypes } = require("kafkajs");
const { EventEmitter, once } = require("node:events");

const BATCH_SIZE = 500;
const FLUSH_INTERVAL = 100;
const queue = [];

const ee = new EventEmitter();

module.exports = (producer, { topic, size = 1000, intension = 50 } = {}) => {
  const timer = setTimeout(async () => {
    if (queue.length === 0) ee.emit("drain");
    const messages = queue.splice(0, BATCH_SIZE);
    try {
      await producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages,
      });
    } catch (err) {
      messages.forEach(i => queue.push(i));
      console.error("Batching failed");
    }
    timer.refresh();
  }, FLUSH_INTERVAL);

  const generator = setTimeout(() => {
    for (let i = 0; i < size; i++) {
      queue.push({
        key: `key-${Date.now()}-${i}`,
        value: JSON.stringify({
          timestamp: Date.now(),
          userId: Math.floor(Math.random() * 1e6),
          action: topic,
        }),
      });
    }
    generator.refresh();
  }, intension);

  return {
    async stop() {
      generator[Symbol.dispose]();
      await once(ee, "drain");
      timer[Symbol.dispose]();
    },
  };
};