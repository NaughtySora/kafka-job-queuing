'use strict';

const { Kafka, Partitioners } = require('kafkajs');

module.exports = config => {
  let producer = null;
  return {
    async start() {
      const kafka = new Kafka(config);
      producer = kafka.producer({
        createPartitioner: Partitioners.LegacyPartitioner,
      });
      await producer.connect();
      return producer;
    },
    async stop() {
      await producer.disconnect();
      producer = null;
    }
  }
};
