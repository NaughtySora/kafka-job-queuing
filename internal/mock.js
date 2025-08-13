'use strict';

const { randomUUID } = require("node:crypto");

const hash = () => Math.random().toString(32).substring(2);
const int = (min = 1, max = 1) => Math.floor(Math.random() * (max - min) + min);

module.exports = {
  hash,
  uuid: randomUUID,
  int,
  date: Date.now,
};
