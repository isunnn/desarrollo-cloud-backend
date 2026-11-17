const EVENT_NAME = "contract.created";

class KafkaEventError extends Error {
  constructor(message) {
    super(message);
    this.name = "KafkaEventError";
  }
}

function normalizeDate(value) {
  if (value === null || value === undefined || value === "") return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isNaN(number) ? null : number;
}

class ContractCreatedEvent {
  constructor({ event, eventId, occurredAt, source, contract, forceFail }) {
    this.event = event;
    this.eventId = eventId;
    this.occurredAt = occurredAt;
    this.source = source;
    this.contract = contract;
    this.forceFail = forceFail;
  }

  static fromMessage(raw) {
    let value = raw;
    if (Buffer.isBuffer(value)) value = value.toString("utf8");
    if (typeof value === "string") {
      const text = value.trim();
      if (!text) throw new KafkaEventError("payload vacio");
      try {
        value = JSON.parse(text);
      } catch (err) {
        throw new KafkaEventError(`JSON invalido: ${err.message}`);
      }
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new KafkaEventError("payload debe ser un objeto");
    }
    if (value.event !== EVENT_NAME) {
      throw new KafkaEventError(`event inesperado: ${value.event}`);
    }
    if (!value.contract || typeof value.contract !== "object") {
      throw new KafkaEventError("contract requerido");
    }

    const id = toNumber(value.contract.id);
    if (id === null) {
      throw new KafkaEventError("contract.id requerido");
    }

    return new ContractCreatedEvent({
      event: value.event,
      eventId: value.eventId || `${EVENT_NAME}-${id}`,
      occurredAt: normalizeDate(value.occurredAt) || new Date().toISOString(),
      source: value.source || "unknown",
      contract: {
        id,
        cliente_id: toNumber(value.contract.cliente_id ?? value.contract.clienteId),
        titulo: value.contract.titulo ?? null,
        descripcion: value.contract.descripcion ?? null,
        fecha_inicio: normalizeDate(value.contract.fecha_inicio ?? value.contract.fechaInicio),
        fecha_fin: normalizeDate(value.contract.fecha_fin ?? value.contract.fechaFin),
        created_at: normalizeDate(value.contract.created_at ?? value.contract.createdAt),
      },
      forceFail: value.forceFail === true || value.forceFail === "true",
    });
  }

  toJSON() {
    return {
      event: this.event,
      eventId: this.eventId,
      occurredAt: this.occurredAt,
      source: this.source,
      contract: { ...this.contract },
      forceFail: this.forceFail,
    };
  }
}

module.exports = { EVENT_NAME, KafkaEventError, ContractCreatedEvent };
