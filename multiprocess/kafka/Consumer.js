'use strict';

const { Kafka } = require("kafkajs");

class Consumer {
  kafka = null;
  instance = null;

  constructor(config) {
    this.kafka = new Kafka(config);
  }

  async start(config) {
    const consumer = this.instance = this.kafka.consumer(config);
    consumer.on("consumer.connect", () => {
      console.log("Consumer connect");
    });
    consumer.on("consumer.disconnect", () => {
      console.log("Consumer disconnect");
    });
    consumer.on("consumer.crash", () => {
      console.log("Consumer crash");
    });
    consumer.on("consumer.stop", () => {
      console.log("Consumer stop");
    });
    await consumer.connect();
  }

  async stop() {
    await this.instance.disconnect();
    this.kafka = null;
    this.instance = null;
    this.config = null;
  }

  async subscribe(topics) {
    const consumer = this.instance;
    await Promise.all(topics.map(consumer.subscribe));
  }

  async eachBatch(config, callback) {
    await this.instance.run({
      ...config,
      eachBatch: callback,
    });
  }

  async eachMessage(config, callback) {
    await this.instance.run({
      ...config,
      eachMessage: callback,
    });
  }
}

module.exports = Consumer;

