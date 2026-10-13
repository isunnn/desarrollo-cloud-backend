const express = require("express");

const MGMT_URL = process.env.RABBITMQ_MGMT_URL || "http://localhost:15672";
const USER = process.env.RABBITMQ_USER || "atlas";
const PASS = process.env.RABBITMQ_PASS || "atlas123";
const PORT = process.env.PORT || 4500;
const VHOST = "%2F";

const auth = "Basic " + Buffer.from(`${USER}:${PASS}`).toString("base64");

async function mgmt(method, path, body) {
  const res = await fetch(`${MGMT_URL}/api${path}`, {
    method,
    headers: { Authorization: auth, "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!res.ok) {
    const err = new Error(
      (data && (data.reason || data.error)) || `HTTP ${res.status}`
    );
    err.status = res.status;
    throw err;
  }
  return data;
}

const app = express();
app.use(express.json());

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// ---- Estado del cluster ----
app.get("/overview", async (req, res, next) => {
  try {
    const o = await mgmt("GET", "/overview");
    res.json({
      cluster: o.cluster_name,
      nodes: o.nodes,
      rabbitmq: o.rabbitmq_version,
      queues: o.queue_totals?.messages,
      object_counts: o.object_totals,
    });
  } catch (e) {
    next(e);
  }
});

// ---- COLAS ----
app.get("/queues", async (req, res, next) => {
  try {
    const q = await mgmt("GET", `/queues/${VHOST}`);
    res.json(
      q.map((x) => ({
        name: x.name,
        type: x.type,
        messages: x.messages,
        consumers: x.consumers,
        durable: x.durable,
        arguments: x.arguments,
        node: x.node,
      }))
    );
  } catch (e) {
    next(e);
  }
});

app.post("/queues", async (req, res, next) => {
  try {
    const { name, durable = true, arguments: args = {} } = req.body;
    if (!name) return res.status(400).json({ error: "falta 'name'" });
    await mgmt("PUT", `/queues/${VHOST}/${encodeURIComponent(name)}`, {
      durable,
      arguments: args,
    });
    res.status(201).json({ name, durable, arguments: args });
  } catch (e) {
    next(e);
  }
});

app.delete("/queues/:name", async (req, res, next) => {
  try {
    await mgmt("DELETE", `/queues/${VHOST}/${encodeURIComponent(req.params.name)}`);
    res.json({ deleted: req.params.name });
  } catch (e) {
    next(e);
  }
});

// ---- EXCHANGES ----
app.get("/exchanges", async (req, res, next) => {
  try {
    const ex = await mgmt("GET", `/exchanges/${VHOST}`);
    res.json(
      ex
        .filter((x) => !x.name.startsWith("amq."))
        .map((x) => ({
          name: x.name,
          type: x.type,
          durable: x.durable,
          vhost: x.vhost,
        }))
    );
  } catch (e) {
    next(e);
  }
});

app.post("/exchanges", async (req, res, next) => {
  try {
    const { name, type = "direct", durable = true } = req.body;
    if (!name) return res.status(400).json({ error: "falta 'name'" });
    if (!["direct", "topic", "fanout", "headers"].includes(type)) {
      return res.status(400).json({ error: "type invalido" });
    }
    await mgmt(
      "PUT",
      `/exchanges/${VHOST}/${encodeURIComponent(name)}`,
      { type, durable }
    );
    res.status(201).json({ name, type, durable });
  } catch (e) {
    next(e);
  }
});

app.delete("/exchanges/:name", async (req, res, next) => {
  try {
    await mgmt("DELETE", `/exchanges/${VHOST}/${encodeURIComponent(req.params.name)}`);
    res.json({ deleted: req.params.name });
  } catch (e) {
    next(e);
  }
});

// ---- BINDINGS ----
app.get("/bindings", async (req, res, next) => {
  try {
    const b = await mgmt("GET", `/bindings/${VHOST}`);
    res.json(
      b.map((x) => ({
        source: x.source,
        destination: x.destination,
        destination_type: x.destination_type,
        routing_key: x.routing_key,
        properties_key: x.properties_key,
      }))
    );
  } catch (e) {
    next(e);
  }
});

app.post("/bindings", async (req, res, next) => {
  try {
    const {
      source,
      destination,
      destination_type = "queue",
      routing_key = "",
    } = req.body;
    if (!source || !destination) {
      return res.status(400).json({ error: "falta 'source' y/o 'destination'" });
    }
    const prefix = destination_type === "exchange" ? "e" : "q";
    await mgmt(
      "POST",
      `/bindings/${VHOST}/e/${encodeURIComponent(source)}/${prefix}/${encodeURIComponent(
        destination
      )}`,
      { routing_key }
    );
    res.status(201).json({ source, destination, destination_type, routing_key });
  } catch (e) {
    next(e);
  }
});

app.delete("/bindings/:source/:destination/:routing_key", async (req, res, next) => {
  try {
    const { source, destination, routing_key } = req.params;
    await mgmt(
      "DELETE",
      `/bindings/${VHOST}/e/${encodeURIComponent(source)}/q/${encodeURIComponent(
        destination
      )}/${encodeURIComponent(routing_key)}`
    );
    res.json({ deleted: { source, destination, routing_key } });
  } catch (e) {
    next(e);
  }
});

app.use((err, req, res, next) => {
  res.status(err.status || 500).json({ error: err.message });
});

app.listen(PORT, () => {
  console.log(`atlas-admin escuchando en puerto ${PORT} -> ${MGMT_URL}`);
});
