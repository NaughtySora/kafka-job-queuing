'use strict';

const { Kafka } = require('kafkajs');
const config = require("./config.json");

const kafka = new Kafka(config);

module.exports = async ({ groupId, topic }) => {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log('Received:', message.value.toString());
    },
  });
};
