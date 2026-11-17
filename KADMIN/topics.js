const DEFAULTS = {
  partitions: 3,
  replication: 3,
  retentionMs: "604800000",
  cleanupPolicy: "delete",
};

const CLEANUP_POLICIES = new Set(["delete", "compact"]);

function validationError(message) {
  const error = new Error(message);
  error.status = 400;
  return error;
}

function firstDefined(...values) {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}

function toInt(value, field, min, max, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  const number = Number(value);
  if (!Number.isInteger(number) || number < min || number > max) {
    throw validationError(`${field} debe ser un entero entre ${min} y ${max}`);
  }
  return number;
}

function parseTopicName(value) {
  const name = String(value || "").trim();
  if (!/^[a-zA-Z0-9._-]+$/.test(name)) {
    throw validationError("name inválido: use letras, números, punto, guion o guion bajo");
  }
  if (name.startsWith("__")) {
    throw validationError("name inválido: los tópicos internos '__*' están reservados");
  }
  return name;
}

function parseRetentionMs(value) {
  const raw = String(value);
  if (!/^\d+$/.test(raw) || Number(raw) <= 0) {
    throw validationError("retentionMs debe ser un entero positivo en milisegundos");
  }
  return raw;
}

function parseCleanupPolicy(value) {
  const policy = String(value);
  if (!CLEANUP_POLICIES.has(policy)) {
    throw validationError("cleanupPolicy debe ser 'delete' o 'compact'");
  }
  return policy;
}

function buildTopicInput(body = {}) {
  const name = parseTopicName(body.name);
  const partitions = toInt(
    firstDefined(body.partitions, body.numPartitions),
    "partitions",
    1,
    10,
    DEFAULTS.partitions
  );
  const replication = toInt(
    firstDefined(body.replication, body.replicationFactor),
    "replication",
    1,
    3,
    DEFAULTS.replication
  );
  const retentionMs = parseRetentionMs(
    firstDefined(body.retentionMs, body["retention.ms"], DEFAULTS.retentionMs)
  );
  const cleanupPolicy = parseCleanupPolicy(
    firstDefined(body.cleanupPolicy, body["cleanup.policy"], DEFAULTS.cleanupPolicy)
  );

  return {
    name,
    partitions,
    replication,
    retentionMs,
    cleanupPolicy,
    kafkaTopic: {
      topic: name,
      numPartitions: partitions,
      replicationFactor: replication,
      configEntries: [
        { name: "retention.ms", value: retentionMs },
        { name: "cleanup.policy", value: cleanupPolicy },
      ],
    },
  };
}

function buildConfigEntries(body = {}) {
  const entries = [];
  const retentionMs = firstDefined(body.retentionMs, body["retention.ms"]);
  const cleanupPolicy = firstDefined(body.cleanupPolicy, body["cleanup.policy"]);

  if (retentionMs !== undefined) {
    entries.push({ name: "retention.ms", value: parseRetentionMs(retentionMs) });
  }
  if (cleanupPolicy !== undefined) {
    entries.push({ name: "cleanup.policy", value: parseCleanupPolicy(cleanupPolicy) });
  }
  if (entries.length === 0) {
    throw validationError("falta al menos una configuración: retentionMs y/o cleanupPolicy");
  }
  return entries;
}

function toPartitionsCount(value) {
  if (value === undefined || value === null || value === "") {
    throw validationError("falta 'count'");
  }
  return toInt(value, "count", 1, 10);
}

function extractConfigs(response) {
  const resource = response && response.resources && response.resources[0];
  const entries = (resource && (resource.configEntries || resource.configs)) || [];
  return entries
    .map((entry) => ({
      name: entry.name === undefined ? entry.configName : entry.name,
      value: entry.value === undefined ? entry.configValue : entry.value,
      readOnly: entry.readOnly,
      isSensitive: entry.isSensitive,
    }))
    .filter((entry) => entry.name !== undefined && entry.value !== undefined);
}

function configsToObject(configs) {
  return configs.reduce((result, config) => {
    if (config && config.name && config.value !== undefined) {
      result[config.name] = String(config.value);
    }
    return result;
  }, {});
}

function mergeConfigs(configs, entries) {
  const merged = new Map();
  for (const config of configs) {
    if (config && config.name && config.readOnly !== true && config.value !== undefined) {
      merged.set(config.name, String(config.value));
    }
  }
  for (const entry of entries) {
    merged.set(entry.name, entry.value);
  }
  return Array.from(merged, ([name, value]) => ({ name, value }));
}

function normalizeTopicMetadata(topic, configs = []) {
  const partitions = Array.isArray(topic.partitions) ? topic.partitions : [];
  const firstPartition = partitions[0] || {};
  const replicas = Array.isArray(firstPartition.replicas) ? firstPartition.replicas : [];

  return {
    name: topic.name,
    partitions: partitions.length,
    replicationFactor: replicas.length,
    partitionDetails: partitions.map((partition) => ({
      partition: partition.partitionId,
      leader: partition.leader === undefined ? null : partition.leader,
      replicas: partition.replicas || [],
      isr: partition.isr || [],
    })),
    config: configsToObject(configs),
  };
}

module.exports = {
  DEFAULTS,
  buildConfigEntries,
  buildTopicInput,
  configsToObject,
  extractConfigs,
  mergeConfigs,
  normalizeTopicMetadata,
  toPartitionsCount,
};
