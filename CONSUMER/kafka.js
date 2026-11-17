const fs = require("fs");
const path = require("path");
const { Kafka } = require("kafkajs");
const { KafkaEventError, ContractCreatedEvent } = require("./kafkaDto");

const KAFKA_BROKERS = (
  process.env.KAFKA_BROKERS ||
  "kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092"
)
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "atlas-consumer";
const LOG_DIR = process.env.LOG_DIR || path.join(__dirname, "logs");
const RETRY_MS = Number(process.env.KAFKA_RETRY_MS || 5000);
const HANDLER_ATTEMPTS = Number(process.env.KAFKA_HANDLER_ATTEMPTS || 3);
const FROM_BEGINNING = process.env.KAFKA_FROM_BEGINNING === "true";

if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

const logFile = path.join(LOG_DIR, "consumer.log");
const errFile = path.join(LOG_DIR, "errors.log");

function log(level, msg, data) {
  const line = `${new Date().toISOString()} [${level}] ${msg}${
    data ? " " + JSON.stringify(data) : ""
  }`;
  console.log(line);
  fs.appendFileSync(logFile, line + "\n");
  if (level === "ERROR") fs.appendFileSync(errFile, line + "\n");
}

function handleNotification(event) {
  log("INFO", "NOTIFICACION enviada", {
    topico: "atlas.notifications",
    eventId: event.eventId,
    contrato: event.contract.id,
    titulo: event.contract.titulo,
  });
}

function handleTicket(event) {
  log("INFO", "TICKET de soporte generado", {
    topico: "atlas.tickets",
    eventId: event.eventId,
    contrato: event.contract.id,
    titulo: event.contract.titulo,
  });
}

function handleDocument(event) {
  log("INFO", "DOCUMENTO generado", {
    topico: "atlas.documents",
    eventId: event.eventId,
    contrato: event.contract.id,
    titulo: event.contract.titulo,
  });
}

const handlers = {
  "atlas.notifications": handleNotification,
  "atlas.tickets": handleTicket,
  "atlas.documents": handleDocument,
};

const subscriptions = [
  { topic: "atlas.notifications", groupId: "consumer-notifications" },
  { topic: "atlas.tickets", groupId: "consumer-tickets" },
  { topic: "atlas.documents", groupId: "consumer-documents" },
];

const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS,
  retry: {
    initialRetryTime: 300,
    retries: 8,
    maxRetryTime: 5000,
  },
});

async function runWithRetry(fn, action, context) {
  for (let attempt = 1; attempt <= HANDLER_ATTEMPTS; attempt += 1) {
    try {
      await fn();
      return;
    } catch (err) {
      if (attempt === HANDLER_ATTEMPTS) throw err;
      log("WARN", `reintento ${attempt}/${HANDLER_ATTEMPTS - 1} de ${action}`, {
        ...context,
        error: err.message,
      });
      await new Promise((resolve) => setTimeout(resolve, attempt * 250));
    }
  }
}

async function commitOffset(consumer, topic, partition, offset) {
  const nextOffset = (BigInt(offset) + 1n).toString();
  await consumer.commitOffsets([{ topic, partition, offset: nextOffset }]);
}

async function processMessage(consumer, { topic, partition, message }) {
  const context = { topico: topic, partition, offset: message.offset };
  try {
    const event = ContractCreatedEvent.fromMessage(message.value);
    if (event.forceFail === true) {
      throw new Error("fallo forzado por forceFail=true (demo Kafka)");
    }
    await runWithRetry(() => handlers[topic](event), "handler", context);
    await commitOffset(consumer, topic, partition, message.offset);
  } catch (err) {
    const expected = err instanceof KafkaEventError || err.message.includes("forceFail");
    log("ERROR", expected ? "mensaje kafka documentado, offset confirmado" : "mensaje kafka fallido, offset confirmado", {
      ...context,
      error: err.message,
    });
    try {
      await commitOffset(consumer, topic, partition, message.offset);
    } catch (commitErr) {
      log("ERROR", "no se pudo confirmar offset", {
        ...context,
        error: commitErr.message,
      });
    }
  }
}

async function startConsumer(config) {
  const consumer = kafka.consumer({
    groupId: config.groupId,
    allowAutoTopicCreation: false,
    retry: {
      initialRetryTime: 300,
      retries: 8,
      maxRetryTime: 5000,
    },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning: FROM_BEGINNING });
  await consumer.run({
    autoCommit: false,
    eachMessage: (payload) => processMessage(consumer, payload),
  });
  log("INFO", "consumidor kafka escuchando", {
    topico: config.topic,
    groupId: config.groupId,
  });
}

async function startWithRetry(config) {
  try {
    await startConsumer(config);
  } catch (err) {
    log("ERROR", "no se pudo iniciar consumidor kafka", {
      topico: config.topic,
      groupId: config.groupId,
      error: err.message,
    });
    setTimeout(() => {
      startWithRetry(config).catch(() => {});
    }, RETRY_MS);
  }
}

async function main() {
  await Promise.all(subscriptions.map(startWithRetry));
  log("INFO", "atlas-consumer kafka listo", {
    brokers: KAFKA_BROKERS.join(","),
  });
}

log("INFO", "atlas-consumer kafka iniciado");
main();
