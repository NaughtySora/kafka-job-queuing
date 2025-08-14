'use strict';

const fork = require('../workers/framework.js');
const parameters = require("./config.json");
const path = require("node:path");

const application = path.resolve(__dirname, "./application.js");

const main = async () => {
  const { stop } = fork([
    { path: application, parameters: { ...parameters.bootstrap, topic: parameters.topics[0] }, },
    { path: application, parameters: { ...parameters.bootstrap, topic: parameters.topics[1] }, },
    { path: application, parameters: { ...parameters.bootstrap, topic: parameters.topics[2] }, },
    { path: application, parameters: { ...parameters.bootstrap, topic: parameters.topics[3] }, },
  ]);

  const exit = async () => {
    setTimeout(() => {
      console.error('[servers]: root stopped with timeout');
      process.exit(0);
    }, 20000);
    await stop();
  };

  process.on('SIGINT', async () => {
    console.log("stopping servers...");
    await exit();
    console.log("[servers]: root stopped gracefully");
    process.exit(0);
  });

  process.on('uncaughtException', async (error) => {
    await exit();
    console.error("[servers]: root stopped with error", error);
    process.exit(1);
  });
};

main();