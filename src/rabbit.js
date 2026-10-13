const amqp = require("amqplib");

const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://atlas:atlas123@localhost:5672";
const RETRY_MS = 5000;

let connection = null;
let channel = null;
let connecting = false;

async function connect() {
  if (connecting || channel) return;
  connecting = true;
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    connection.on("close", () => {
      console.warn("[rabbit] conexion cerrada, reintentando...");
      connection = null;
      channel = null;
      setTimeout(connect, RETRY_MS);
    });
    connection.on("error", (err) => {
      console.warn("[rabbit] error de conexion:", err.message);
    });

    channel = await connection.createConfirmChannel();
    channel.on("close", () => {
      channel = null;
    });
    channel.on("error", (err) => {
      console.warn("[rabbit] error de canal:", err.message);
    });

    console.log("[rabbit] conectado a", RABBITMQ_URL.replace(/:[^:@]*@/, ":***@"));
  } catch (err) {
    console.warn("[rabbit] no se pudo conectar:", err.message, "- reintento en", RETRY_MS, "ms");
    setTimeout(connect, RETRY_MS);
  } finally {
    connecting = false;
  }
}

// Publica un evento JSON en un exchange. Nunca rompe la API:
// si RabbitMQ esta caido, solo registra el warning y devuelve false.
async function publishEvent(exchange, routingKey, payload) {
  try {
    if (!channel) {
      await connect();
      if (!channel) throw new Error("canal no disponible");
    }
    const body = Buffer.from(JSON.stringify(payload));
    channel.publish(
      exchange,
      routingKey,
      body,
      {
        contentType: "application/json",
        persistent: true,
        timestamp: Date.now(),
        messageId: `${payload.event || "event"}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      },
      (err) => {
        if (err) console.warn("[rabbit] publish fallido:", err.message);
      }
    );
    console.log(`[rabbit] evento publicado -> ${exchange} | key=${routingKey}`);
    return true;
  } catch (err) {
    console.warn(
      `[rabbit] no se pudo publicar en ${exchange} (${routingKey}):`,
      err.message
    );
    return false;
  }
}

module.exports = { connect, publishEvent };
