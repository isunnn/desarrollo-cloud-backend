const { Kafka } = require("kafkajs");
const { ContractCreatedEvent } = require("./dtos/contractCreatedEvent");

const KAFKA_BROKERS = (
  process.env.KAFKA_BROKERS ||
  "kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092"
)
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "atlas-api";
const RETRY_MS = 5000;

let kafka = null;
let producer = null;
let connectingPromise = null;

function getKafka() {
  if (!kafka) {
    kafka = new Kafka({
      clientId: KAFKA_CLIENT_ID,
      brokers: KAFKA_BROKERS,
      retry: {
        initialRetryTime: 300,
        retries: 8,
        maxRetryTime: 5000,
      },
    });
  }
  return kafka;
}

async function createProducer() {
  try {
    const nextProducer = getKafka().producer({
      allowAutoTopicCreation: false,
      transactionTimeout: 30000,
    });
    await nextProducer.connect();
    producer = nextProducer;
    console.log(`[kafka] conectado a ${KAFKA_BROKERS.join(",")}`);
  } catch (err) {
    producer = null;
    console.warn(
      `[kafka] no se pudo conectar: ${err.message} - reintento en ${RETRY_MS} ms`
    );
    setTimeout(() => {
      connect().catch(() => {});
    }, RETRY_MS);
  }
}

async function connect() {
  if (producer) return;
  if (connectingPromise) return connectingPromise;
  connectingPromise = createProducer();
  try {
    await connectingPromise;
  } finally {
    connectingPromise = null;
  }
}

async function publishEvent(topic, payload) {
  try {
    if (!producer) {
      await connect();
      if (!producer) throw new Error("producer no disponible");
    }
    if (!(payload instanceof ContractCreatedEvent)) {
      throw new TypeError("payload debe ser ContractCreatedEvent");
    }

    const key = `${payload.event}:${payload.contract.id}`;
    await producer.send({
      topic,
      acks: -1,
      messages: [
        {
          key,
          value: JSON.stringify(payload.toJSON()),
          headers: {
            "content-type": "application/json",
            "event-type": payload.event,
            "event-id": payload.eventId,
          },
        },
      ],
    });
    console.log(`[kafka] evento publicado -> ${topic} | key=${key}`);
    return true;
  } catch (err) {
    console.warn(`[kafka] no se pudo publicar en ${topic}: ${err.message}`);
    return false;
  }
}

async function close() {
  if (!producer) return;
  await producer.disconnect();
  producer = null;
}

module.exports = { connect, publishEvent, close, KAFKA_BROKERS };
