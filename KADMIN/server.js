require("dotenv").config();

const express = require("express");
const kafkaAdmin = require("./kafkaAdmin");
const topicsUtil = require("./topics");

const { ConfigResourceTypes } = kafkaAdmin;

const app = express();
const PORT = process.env.PORT || 4500;

app.use(express.json());

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

async function getConfigs(name) {
  const response = await kafkaAdmin.withAdmin((admin) =>
    admin.describeConfigs({
      resources: [{ type: ConfigResourceTypes.TOPIC, name }],
      includeSynonyms: false,
    })
  );
  return topicsUtil.extractConfigs(response);
}

async function getTopic(name) {
  if (name.startsWith("__")) {
    const error = new Error("los tópicos internos '__*' no son administrables");
    error.status = 400;
    throw error;
  }
  const metadata = await kafkaAdmin.withAdmin((admin) =>
    admin.fetchTopicMetadata({ topics: [name] })
  );
  const topic = (metadata.topics || []).find((item) => item.name === name);
  if (!topic || !topic.partitions || topic.partitions.length === 0) {
    const error = new Error(`tópico '${name}' no existe`);
    error.status = 404;
    throw error;
  }
  return topic;
}

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    connected: kafkaAdmin.isConnected(),
    brokers: kafkaAdmin.getBrokers(),
    port: PORT,
  });
});

app.get("/brokers", async (req, res, next) => {
  try {
    const result = await kafkaAdmin.withAdmin((admin) => admin.describeCluster());
    const brokers = Array.isArray(result) ? result : result.brokers || [];
    res.json(
      brokers.map((broker) => ({
        id:
          broker.nodeId === undefined
            ? broker.brokerId === undefined
              ? broker.id
              : broker.brokerId
            : broker.nodeId,
        host: broker.host,
        port: broker.port,
        rack: broker.rack || null,
      }))
    );
  } catch (error) {
    next(error);
  }
});

app.get("/topics", async (req, res, next) => {
  try {
    const names = (
      await kafkaAdmin.withAdmin((admin) => admin.listTopics())
    ).filter((name) => !name.startsWith("__"));
    const metadata = names.length
      ? await kafkaAdmin.withAdmin((admin) => admin.fetchTopicMetadata({ topics: names }))
      : { topics: [] };
    const result = await Promise.all(
      (metadata.topics || []).map(async (topic) =>
        topicsUtil.normalizeTopicMetadata(topic, await getConfigs(topic.name))
      )
    );
    res.json(result);
  } catch (error) {
    next(error);
  }
});

app.get("/topics/:name/config", async (req, res, next) => {
  try {
    await getTopic(req.params.name);
    const configs = await getConfigs(req.params.name);
    res.json(topicsUtil.configsToObject(configs));
  } catch (error) {
    next(error);
  }
});

app.get("/topics/:name", async (req, res, next) => {
  try {
    const topic = await getTopic(req.params.name);
    const configs = await getConfigs(req.params.name);
    res.json(topicsUtil.normalizeTopicMetadata(topic, configs));
  } catch (error) {
    next(error);
  }
});

app.post("/topics", async (req, res, next) => {
  try {
    const input = topicsUtil.buildTopicInput(req.body);
    const created = await kafkaAdmin.withAdmin((admin) =>
      admin.createTopics({
        validateOnly: false,
        waitForLeaders: true,
        topics: [input.kafkaTopic],
      })
    );
    if (!created) {
      const error = new Error(`el tópico '${input.name}' ya existe`);
      error.status = 409;
      throw error;
    }
    res.status(201).json({
      name: input.name,
      partitions: input.partitions,
      replication: input.replication,
      retentionMs: input.retentionMs,
      cleanupPolicy: input.cleanupPolicy,
    });
  } catch (error) {
    next(error);
  }
});

app.put("/topics/:name/config", async (req, res, next) => {
  try {
    await getTopic(req.params.name);
    const overrides = topicsUtil.buildConfigEntries(req.body);
    const current = await getConfigs(req.params.name);
    const configEntries = topicsUtil.mergeConfigs(current, overrides);
    await kafkaAdmin.withAdmin((admin) =>
      admin.alterConfigs({
        validateOnly: false,
        resources: [
          {
            type: ConfigResourceTypes.TOPIC,
            name: req.params.name,
            configEntries,
          },
        ],
      })
    );
    res.json(topicsUtil.configsToObject(await getConfigs(req.params.name)));
  } catch (error) {
    next(error);
  }
});

app.put("/topics/:name/partitions", async (req, res, next) => {
  try {
    await getTopic(req.params.name);
    const count = topicsUtil.toPartitionsCount(req.body.count);
    await kafkaAdmin.withAdmin((admin) =>
      admin.createPartitions({
        validateOnly: false,
        topicPartitions: [{ topic: req.params.name, count }],
      })
    );
    const topic = await getTopic(req.params.name);
    res.json(
      topicsUtil.normalizeTopicMetadata(topic, await getConfigs(req.params.name))
    );
  } catch (error) {
    next(error);
  }
});

app.delete("/topics/:name", async (req, res, next) => {
  try {
    await kafkaAdmin.withAdmin((admin) =>
      admin.deleteTopics({ topics: [req.params.name] })
    );
    res.json({ deleted: req.params.name });
  } catch (error) {
    next(error);
  }
});

app.get("/groups", async (req, res, next) => {
  try {
    const result = await kafkaAdmin.withAdmin((admin) => admin.listGroups());
    const groups = Array.isArray(result) ? result : result.groups || [];
    res.json(
      groups.map((group) => ({
        groupId: group.groupId,
        protocolType: group.protocolType || null,
        memberId: group.memberId || null,
      }))
    );
  } catch (error) {
    next(error);
  }
});

app.get("/groups/:id", async (req, res, next) => {
  try {
    const result = await kafkaAdmin.withAdmin((admin) =>
      admin.describeGroups([req.params.id])
    );
    const group = (result.groups || [])[0];
    if (!group) {
      const error = new Error(`grupo '${req.params.id}' no existe`);
      error.status = 404;
      throw error;
    }
    let offsets = [];
    try {
      offsets = await kafkaAdmin.withAdmin((admin) =>
        admin.fetchOffsets({ groupId: req.params.id })
      );
    } catch (error) {
      offsets = [];
    }
    res.json({
      groupId: group.groupId,
      state: group.state || null,
      protocol: group.protocol || null,
      protocolType: group.protocolType || null,
      members: (group.members || []).map((member) => ({
        memberId: member.memberId,
        clientId: member.clientId,
        clientHost: member.clientHost,
        memberAssignment: member.memberAssignment,
      })),
      offsets,
    });
  } catch (error) {
    next(error);
  }
});

app.delete("/groups/:id", async (req, res, next) => {
  try {
    await kafkaAdmin.withAdmin((admin) =>
      admin.deleteGroups([req.params.id])
    );
    res.json({ deleted: req.params.id });
  } catch (error) {
    next(error);
  }
});

function statusFor(error) {
  if (error.status) return error.status;
  if (error.type === "TOPIC_ALREADY_EXISTS") return 409;
  if (
    error.type === "UNKNOWN_TOPIC_OR_PARTITION" ||
    error.type === "GROUP_ID_NOT_FOUND"
  ) {
    return 404;
  }
  if (/^INVALID_/.test(error.type || "")) return 400;
  return 500;
}

app.use((error, req, res, next) => {
  res.status(statusFor(error)).json({
    error: error.message,
    type: error.type || null,
  });
});

const server = app.listen(PORT, () => {
  console.log(
    `atlas-kafka-admin escuchando en puerto ${PORT} -> ${kafkaAdmin.getBrokers().join(",")}`
  );
  kafkaAdmin.connect();
});

async function shutdown() {
  server.close();
  await kafkaAdmin.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
