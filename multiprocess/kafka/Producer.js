'use strict';

const { Kafka } = require('kafkajs');

class Producer {
  instance = null;
  kafka = null;

  constructor(config) {
    this.kafka = new Kafka(config);
  }

  async start(config) {
    const producer = this.instance = this.kafka.producer(config);
    producer.on("producer.connect", () => {
      console.log("Producer connect");
    });
    producer.on("producer.disconnect", () => {
      console.log("Producer disconnect");
    });
    await producer.connect();
  }

  async stop() {
    await this.instance.disconnect();
    this.instance = null;
    this.kafka = null;
  }

  get producer(){
    return this.instance;
  }
}

module.exports = Producer;

