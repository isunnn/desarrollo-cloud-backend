const { Kafka, ConfigResourceTypes } = require("kafkajs");

const brokers = (
  process.env.KAFKA_BROKERS ||
  "kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092"
)
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

const RETRY_MS = 5000;

const kafka = new Kafka({
  clientId: process.env.KAFKA_ADMIN_CLIENT_ID || "atlas-kafka-admin",
  brokers,
  retry: {
    initialRetryTime: 300,
    retries: 8,
    maxRetryTime: 3000,
  },
});

const admin = kafka.admin();

let connected = false;
let connecting = false;
let retryTimer = null;

function scheduleRetry() {
  if (retryTimer) return;
  retryTimer = setTimeout(() => {
    retryTimer = null;
    connect();
  }, RETRY_MS);
}

function isConnectionError(err) {
  return /closed|disconnect|connection|econn|timeout|not connected/i.test(
    err.message || ""
  );
}

async function connect() {
  if (connected || connecting) return;
  connecting = true;
  try {
    await admin.connect();
    connected = true;
    console.log("[kafka-admin] conectado a", brokers.join(","));
  } catch (err) {
    console.warn(
      "[kafka-admin] no se pudo conectar:",
      err.message,
      "- reintento en",
      RETRY_MS,
      "ms"
    );
    scheduleRetry();
  } finally {
    connecting = false;
  }
}

function isConnected() {
  return connected;
}

function getBrokers() {
  return brokers;
}

async function withAdmin(fn) {
  if (!connected) await connect();
  if (!connected) {
    throw new Error(`No hay conexión con Kafka (${brokers.join(",")})`);
  }
  try {
    return await fn(admin);
  } catch (err) {
    if (!isConnectionError(err)) throw err;
    connected = false;
    await connect();
    if (!connected) throw err;
    return fn(admin);
  }
}

async function close() {
  if (retryTimer) {
    clearTimeout(retryTimer);
    retryTimer = null;
  }
  if (connected) {
    await admin.disconnect();
    connected = false;
  }
}

module.exports = {
  ConfigResourceTypes,
  admin,
  close,
  connect,
  getBrokers,
  isConnected,
  withAdmin,
};
