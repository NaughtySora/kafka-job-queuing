'use strict';

const { Kafka } = require("kafkajs");

class Consumer {
  kafka = null;
  instance = null;
  config = null;

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

//! Kafka
// The consumer must send a heartbeat every 20 seconds.
// Kafka broker waits up to 60 seconds without a heartbeat before considering the consumer dead.
// {
//       clientId: "root",
//       brokers: ["localhost:4500", "localhost:4501", "localhost:4502", "localhost:4503",],
//       consumer: {
//         sessionTimeout: 60000,
//         heartbeatInterval: 20000,
//       },
//     }

//! consumer
// { groupId: "event-processors", }

//! topic 
// { topic: "events", fromBeginning: false }

//! consumer.run
// const metadata = await admin.fetchTopicMetadata({ topics: ['my-topic'] }); partition count
// 12 topics / 3 consumers = 4 partitions
// partitionsConsumedConcurrently should be ≤ the number of partitions assigned to this consumer.

//! heartbeat
// Setting session.timeout.ms to 1 minute means Kafka will wait up to 60 seconds without a heartbeat before considering your consumer dead.
// Proper error handling and graceful shutdown (calling consumer.disconnect() or consumer.stop() on process exit) tells Kafka immediately you’re leaving, so it rebalances partitions right away.
// Using a stable cloud environment like AWS reduces crashes to very rare events.
// If a crash does happen (e.g., instance unexpectedly dies), Kafka waits up to the session timeout before marking the consumer dead and triggering a rebalance.
// Because your consumer commits offsets only after successful processing, any messages not confirmed will remain uncommitted and become available for other consumers in the group after rebalance.
// This ensures at-least-once delivery: messages might be reprocessed but won’t be lost.

//! run
// autoCommit: false,
// partitionsConsumedConcurrently: 3,
// eachBatchAutoResolve: false,
