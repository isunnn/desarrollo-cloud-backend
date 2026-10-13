const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");

const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://atlas:atlas123@localhost:5672";
const LOG_DIR = process.env.LOG_DIR || path.join(__dirname, "logs");
const RETRY_MS = 5000;

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

// Topologia: exchanges, colas y DLQs (declaracion idempotente)
const TOPOLOGY = {
  exchanges: [
    { name: "ex.notifications", type: "direct" },
    { name: "ex.contracts", type: "topic" },
  ],
  queues: [
    { name: "q.notifications", dlq: "q.notifications.dlq" },
    { name: "q.tickets", dlq: "q.tickets.dlq" },
    { name: "q.documents", dlq: "q.documents.dlq" },
  ],
  bindings: [
    { exchange: "ex.notifications", queue: "q.notifications", key: "created" },
    { exchange: "ex.contracts", queue: "q.tickets", key: "contract.created" },
    { exchange: "ex.contracts", queue: "q.documents", key: "contract.*" },
  ],
};

async function assertTopology(ch) {
  for (const ex of TOPOLOGY.exchanges) {
    await ch.assertExchange(ex.name, ex.type, { durable: true });
  }
  for (const q of TOPOLOGY.queues) {
    await ch.assertQueue(q.name, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": q.dlq,
      },
    });
    await ch.assertQueue(q.dlq, { durable: true });
  }
  for (const b of TOPOLOGY.bindings) {
    await ch.bindQueue(b.queue, b.exchange, b.key);
  }
  log("INFO", "topologia declarada", {
    exchanges: TOPOLOGY.exchanges.length,
    queues: TOPOLOGY.queues.length,
    bindings: TOPOLOGY.bindings.length,
  });
}

// Procesamiento de mensajes por cola
const handlers = {
  "q.notifications": async (msg) => {
    log("INFO", "NOTIFICACION enviada", {
      cola: "q.notifications",
      contrato: msg.contract?.id,
      titulo: msg.contract?.titulo,
    });
  },
  "q.tickets": async (msg) => {
    log("INFO", "TICKET de soporte generado", {
      cola: "q.tickets",
      contrato: msg.contract?.id,
      titulo: msg.contract?.titulo,
    });
  },
  "q.documents": async (msg) => {
    log("INFO", "DOCUMENTO generado", {
      cola: "q.documents",
      contrato: msg.contract?.id,
      titulo: msg.contract?.titulo,
    });
  },
};

async function consume(queue, ch) {
  await ch.prefetch(1);
  ch.consume(queue, async (raw) => {
    if (!raw) return;
    let msg;
    try {
      msg = JSON.parse(raw.content.toString());
    } catch (e) {
      // mensaje corrupto: se rechaza sin requeue -> DLQ
      log("ERROR", "JSON invalido, mensaje enviado a DLQ", {
        cola: queue,
        error: e.message,
      });
      return ch.nack(raw, false, false);
    }

    try {
      if (msg.forceFail === true) {
        throw new Error("fallo forzado por forceFail=true (demo DLQ)");
      }
      await handlers[queue](msg);
      ch.ack(raw);
    } catch (err) {
      // requeue=false -> RabbitMQ aplica x-dead-letter-* -> cola DLQ
      log("ERROR", "mensaje fallido, enviado a DLQ", {
        cola: queue,
        dlq: queue + ".dlq",
        error: err.message,
        messageId: raw.properties.messageId,
      });
      ch.nack(raw, false, false);
    }
  });
  log("INFO", "consumidor escuchando", { cola: queue });
}

async function main() {
  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    conn.on("close", () => {
      log("WARN", "conexion cerrada, reintentando en " + RETRY_MS + "ms");
      setTimeout(main, RETRY_MS);
    });
    conn.on("error", (e) => log("ERROR", "error de conexion", { error: e.message }));

    const ch = await conn.createConfirmChannel();
    await assertTopology(ch);
    for (const q of TOPOLOGY.queues) {
      await consume(q.name, ch);
    }
    log("INFO", "atlas-consumer listo", { url: RABBITMQ_URL.replace(/:[^:@]*@/, ":***@") });
  } catch (err) {
    log("ERROR", "no se pudo conectar", { error: err.message });
    setTimeout(main, RETRY_MS);
  }
}

log("INFO", "atlas-consumer iniciado");
main();
