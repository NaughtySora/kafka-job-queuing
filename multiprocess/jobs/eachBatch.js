'use strict';

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
    promises.push(
      onProcess(message)
        .catch(e => onError(e, message))
        .finally(() => resolveOffset(message.offset))
    );
    if (promises.length >= limit) await commit();
  }
  await commit();
  timer[Symbol.dispose]();
};