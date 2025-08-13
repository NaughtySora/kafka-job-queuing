'use strict';

const { fork } = require("node:child_process");

const JITTER = 1.75;

const create = (workers, path, parameters, retry = 2000) => {
  const worker = fork(path, [JSON.stringify(parameters)]);
  const pid = worker.pid;
  workers.set(pid, worker);
  worker.on("exit", () => {
    console.log(`Worker ${pid} exited`);
  });
  worker.on("error", (err) => {
    console.error(`Worker ${pid} exited with error`, err);
    workers.delete(pid);
    setTimeout(() => {
      create(workers, path, parameters, retry * JITTER);
    }, retry);
  });
};

module.exports = (options) => {
  const workers = new Map();
  for (const { path, parameters } of options) {
    create(workers, path, parameters);
  }
  return {
    stop() { // timeout 
      return new Promise((resolve, reject) => {
        try {
          let waiting = workers.size;
          for (const worker of workers.values()) {
            worker.on("exit", () => {
              if (--waiting === 0) resolve();
            });
            worker.send({ status: "stop" });
          }
        } catch (e) {
          reject(e);
        }
      })
    }
  }
};
