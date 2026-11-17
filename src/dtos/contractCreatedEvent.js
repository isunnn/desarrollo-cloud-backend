const EVENT_NAME = "contract.created";

function normalizeDate(value) {
  if (value === null || value === undefined || value === "") return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toISOString();
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

  static fromContract(contract, forceFail = false) {
    if (!contract || typeof contract !== "object") {
      throw new TypeError("contract requerido para construir ContractCreatedEvent");
    }

    const id = toNumber(contract.id);
    if (id === null) {
      throw new TypeError("contract.id requerido para construir ContractCreatedEvent");
    }

    return new ContractCreatedEvent({
      event: EVENT_NAME,
      eventId: `${EVENT_NAME}-${id}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      occurredAt: new Date().toISOString(),
      source: "atlas-api",
      contract: {
        id,
        cliente_id: toNumber(contract.cliente_id ?? contract.clienteId),
        titulo: contract.titulo ?? null,
        descripcion: contract.descripcion ?? null,
        fecha_inicio: normalizeDate(contract.fecha_inicio ?? contract.fechaInicio),
        fecha_fin: normalizeDate(contract.fecha_fin ?? contract.fechaFin),
        created_at: normalizeDate(contract.created_at ?? contract.createdAt),
      },
      forceFail: forceFail === true || forceFail === "true",
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

module.exports = { EVENT_NAME, ContractCreatedEvent };
