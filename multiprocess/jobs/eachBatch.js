'use strict';

//!
// batch, // messages
// resolveOffset, // marking when processed messages (like callback in streams)
// heartbeat, // ping-pong
// commitOffsetsIfNecessary, // commit offsets if due ? 
// isRunning, // is consumer running
// isStale, // is batch valid

//!
// true if Kafka rebalanced partitions while we were processing — meaning these messages might be reassigned elsewhere.
// if (!isRunning() || isStale()) break;

//!
// catch (e) {
//    console.error(e, message);
//    await producer.send({
//       topic: "dlq", // dead queue
//       messages: [{ key: message.key, value: message.value }],
//    });
//  }

//! 
// set timeout higher like 1 min, implement graceful shutdown and close consumer. or use it in every loop above.
// await heartbeat();

//!
// onError
// await producer.send({
//   topic: "dlq",
//   messages: [{ key: message.key, value: message.value }],
// });

module.exports = ({ limit, ping, onError, onProcess }) => async (options) => {
  const { batch, resolveOffset, heartbeat,
    commitOffsetsIfNecessary, isRunning, isStale, } = options;

  const timer = setTimeout(async () => {
    if (!isRunning() || isStale()) {
      return void timer[Symbol.dispose]();
    }
    await heartbeat();
    timer.refresh();
  }, ping);

  const promises = [];
  const commit = async () => {
    if (promises.length === 0) return;
    await Promise.all(promises);
    await commitOffsetsIfNecessary();
    promises.length = 0;
  };
  for (const message of batch.messages) {
    if (!isRunning() || isStale()) break;
    const exec = async () => {
      try {
        await onProcess(message);
      } catch (e) {
        await onError(e, message);
      } finally {
        resolveOffset(message.offset);
      }
    };
    promises.push(exec());
    if (promises.length > limit) await commit();
  }
  await commit();
  timer[Symbol.dispose]();
};