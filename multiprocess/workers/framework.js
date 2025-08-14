'use strict';

const { fork } = require("node:child_process");

const JITTER = 1.75;
const workers = new Map();

const create = (path, parameters, retry = 2000) => {
  const worker = fork(path, [JSON.stringify(parameters)]);
  const pid = worker.pid;
  workers.set(pid, worker);
  worker.once("exit", () => {
    console.log(`Worker ${pid} exited`);
  });
  worker.once("error", (err) => {
    console.error(`Worker ${pid} exited with error`, err);
    workers.delete(pid);
    setTimeout(() => {
      create(path, parameters, retry * JITTER);
    }, retry);
  });
};

module.exports = options => {
  for (const { path, parameters } of options) {
    create(path, parameters);
  }
  return {
    stop() {
      return new Promise((resolve, reject) => {
        try {
          let waiting = workers.size;
          for (const worker of workers.values()) {
            worker.once("exit", () => {
              if (--waiting === 0) resolve();
            });
            worker.send({ status: "stop" });
            workers.delete(worker.pid);
          }
        } catch (e) {
          reject(e);
        }
      })
    }
  }
};
