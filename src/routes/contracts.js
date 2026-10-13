const path = require("path");
const fs = require("fs");
const express = require("express");
const multer = require("multer");
const { pool } = require("../db");
const { getAuthUser } = require("../middleware/validateEntra");
const { publishEvent } = require("../rabbit");

const router = express.Router();

// Función helper para simular latencia
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const storage = multer.memoryStorage();
const upload = multer({ storage });

// Crear contrato con archivo PDF (guardado en DB como bytea)
router.post("/", upload.single("file"), async (req, res) => {
  const {
    cliente_id,
    titulo,
    descripcion = null,
    fecha_inicio,
    fecha_fin,
    forceFail = false,
  } = req.body;
  try {
    const clientIdNum = Number(cliente_id);
    const contractInsert = await pool.query(
      "INSERT INTO contratos (cliente_id, titulo, descripcion, fecha_inicio, fecha_fin) VALUES ($1, $2, $3, $4, $5) RETURNING *",
      [clientIdNum, titulo, descripcion, fecha_inicio, fecha_fin]
    );

    if (req.file && req.file.buffer) {
      await pool.query(
        "INSERT INTO documentos_contrato (contract_id, document) VALUES ($1, $2)",
        [contractInsert.rows[0].id, req.file.buffer]
      );
    }

    const result = contractInsert;
    const authUser = getAuthUser(req);
    await pool.query(
      "INSERT INTO auditoria (usuario_id, accion) VALUES ($1, $2)",
      [
        authUser?.nombreUsuario || null,
        `CONTRATOS_CREAR: creó contrato ${titulo} para cliente ${cliente_id}`,
      ]
    );

    // RabbitMQ: publica el evento contract.created en los dos exchanges.
    // forceFail=true marca el mensaje para que el consumidor lo rechace (-> DLQ).
    const contract = result.rows[0];
    const eventPayload = {
      event: "contract.created",
      forceFail: forceFail === true || forceFail === "true",
      contract,
      publishedAt: new Date().toISOString(),
    };
    await publishEvent("ex.notifications", "created", eventPayload);
    await publishEvent("ex.contracts", "contract.created", eventPayload);

    res.status(201).json(contract);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al crear contrato",
      glosa: err.message || err.toString(),
    });
  }
});

// Listar contratos
router.get("/", async (req, res) => {
  try {
    const authUser = getAuthUser(req);

    if (!authUser) {
      return res.status(401).json({
        error: "No autorizado",
        glosa: "Token inválido",
      });
    }

    const empresaFilter = authUser.empresaId ? "WHERE cl.empresa_id = $1" : "";
    const result = await pool.query(
      `SELECT 
        c.id, c.titulo, c.descripcion, c.fecha_inicio, c.fecha_fin, c.created_at,
        cl.nombre AS cliente_nombre
      FROM contratos c
      INNER JOIN clientes cl ON cl.id = c.cliente_id
      ${empresaFilter}
      ORDER BY c.id DESC`,
      authUser.empresaId ? [authUser.empresaId] : []
    );
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al listar contratos",
      glosa: err.message || err.toString(),
    });
  }
});

// Servicio 1: Contratos por estado (por_vencer y vencido)
router.get("/estados", async (req, res) => {
  try {
    const authUser = getAuthUser(req);

    if (!authUser) {
      return res.status(401).json({
        error: "No autorizado",
        glosa: "Token inválido",
      });
    }

    const empresaFilter = authUser.empresaId ? "AND cl.empresa_id = $1" : "";
    const empresaParam = authUser.empresaId ? [authUser.empresaId] : [];
    const hoy = new Date();
    const hoyFormateado = hoy.toISOString().split("T")[0];
    const fecha30Dias = new Date(hoy);
    fecha30Dias.setDate(fecha30Dias.getDate() + 30);
    const fecha30DiasFormateada = fecha30Dias.toISOString().split("T")[0];

    const porVencer = await pool.query(
      `SELECT c.id, c.cliente_id, c.titulo, c.descripcion, c.fecha_inicio, c.fecha_fin, c.created_at
       FROM contratos c
       INNER JOIN clientes cl ON cl.id = c.cliente_id
       WHERE 1=1 ${empresaFilter}
         AND c.fecha_fin >= $${empresaParam.length + 1}
         AND c.fecha_fin <= $${empresaParam.length + 2}
       ORDER BY c.fecha_fin ASC
       LIMIT 3`,
      [...empresaParam, hoyFormateado, fecha30DiasFormateada]
    );

    const vencidos = await pool.query(
      `SELECT c.id, c.cliente_id, c.titulo, c.descripcion, c.fecha_inicio, c.fecha_fin, c.created_at
       FROM contratos c
       INNER JOIN clientes cl ON cl.id = c.cliente_id
       WHERE 1=1 ${empresaFilter}
         AND c.fecha_fin < $${empresaParam.length + 1}
       ORDER BY c.fecha_fin DESC
       LIMIT 3`,
      [...empresaParam, hoyFormateado]
    );

    const porVencerConEstado = porVencer.rows.map((contrato) => ({
      ...contrato,
      estado: "por_vencer",
    }));

    const vencidosConEstado = vencidos.rows.map((contrato) => ({
      ...contrato,
      estado: "vencido",
    }));

    res.json([...porVencerConEstado, ...vencidosConEstado]);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al obtener contratos por estado",
      glosa: err.message || err.toString(),
    });
  }
});

// Servicio 2: Dashboard con estadísticas de la empresa
router.get("/dashboard", async (req, res) => {
  try {
    const authUser = getAuthUser(req);

    if (!authUser) {
      return res.status(401).json({
        error: "No autorizado",
        glosa: "Token inválido",
      });
    }

    const empresaFilter = authUser.empresaId ? "WHERE empresa_id = $1" : "";
    const empresaParam = authUser.empresaId ? [authUser.empresaId] : [];
    const hoy = new Date();
    const hoyFormateado = hoy.toISOString().split("T")[0];

    const fecha7Dias = new Date(hoy);
    fecha7Dias.setDate(fecha7Dias.getDate() + 7);
    const fecha7DiasFormateada = fecha7Dias.toISOString().split("T")[0];

    const fecha15Dias = new Date(hoy);
    fecha15Dias.setDate(fecha15Dias.getDate() + 15);
    const fecha15DiasFormateada = fecha15Dias.toISOString().split("T")[0];

    const fecha30Dias = new Date(hoy);
    fecha30Dias.setDate(fecha30Dias.getDate() + 30);
    const fecha30DiasFormateada = fecha30Dias.toISOString().split("T")[0];

    const totalUsuariosResult = await pool.query(
      authUser.empresaId
        ? "SELECT COUNT(*) as total FROM empresa_usuarios WHERE empresa_id = $1"
        : "SELECT COUNT(*) as total FROM empresa_usuarios",
      empresaParam
    );
    const totalUsuarios = parseInt(totalUsuariosResult.rows[0].total);

    const totalClientesResult = await pool.query(
      `SELECT COUNT(*) as total FROM clientes ${empresaFilter}`,
      empresaParam
    );
    const totalClientes = parseInt(totalClientesResult.rows[0].total);

    const contratos7DiasResult = await pool.query(
      `SELECT COUNT(*) as total 
       FROM contratos c
       INNER JOIN clientes cl ON cl.id = c.cliente_id
       WHERE 1=1 ${authUser.empresaId ? "AND cl.empresa_id = $1" : ""}
         AND c.fecha_fin >= $${empresaParam.length + 1}
         AND c.fecha_fin <= $${empresaParam.length + 2}`,
      [...empresaParam, hoyFormateado, fecha7DiasFormateada]
    );
    const contratos7Dias = parseInt(contratos7DiasResult.rows[0].total);

    const contratos15DiasResult = await pool.query(
      `SELECT COUNT(*) as total 
       FROM contratos c
       INNER JOIN clientes cl ON cl.id = c.cliente_id
       WHERE 1=1 ${authUser.empresaId ? "AND cl.empresa_id = $1" : ""}
         AND c.fecha_fin >= $${empresaParam.length + 1}
         AND c.fecha_fin <= $${empresaParam.length + 2}`,
      [...empresaParam, hoyFormateado, fecha15DiasFormateada]
    );
    const contratos15Dias = parseInt(contratos15DiasResult.rows[0].total);

    const contratos30DiasResult = await pool.query(
      `SELECT COUNT(*) as total 
       FROM contratos c
       INNER JOIN clientes cl ON cl.id = c.cliente_id
       WHERE 1=1 ${authUser.empresaId ? "AND cl.empresa_id = $1" : ""}
         AND c.fecha_fin >= $${empresaParam.length + 1}
         AND c.fecha_fin <= $${empresaParam.length + 2}`,
      [...empresaParam, hoyFormateado, fecha30DiasFormateada]
    );
    const contratos30Dias = parseInt(contratos30DiasResult.rows[0].total);

    res.json({
      totalUsuarios,
      totalClientes,
      contratosVencen7Dias: contratos7Dias,
      contratosVencen15Dias: contratos15Dias,
      contratosVencen30Dias: contratos30Dias,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al obtener estadísticas",
      glosa: err.message || err.toString(),
    });
  }
});

// Obtener contrato por id
router.get("/:id", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT id, cliente_id, titulo, descripcion, fecha_inicio, fecha_fin, created_at FROM contratos WHERE id=$1",
      [req.params.id]
    );
    if (result.rows.length === 0)
      return res.status(404).json({ error: "No encontrado" });
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al obtener contrato",
      glosa: err.message || err.toString(),
    });
  }
});

// Actualizar contrato
router.put("/:id", upload.single("file"), async (req, res) => {
  try {
    const authUser = getAuthUser(req);

    if (!authUser) {
      return res.status(401).json({
        error: "No autorizado",
        glosa: "Token inválido",
      });
    }

    const contratoId = req.params.id;

    if (authUser.empresaId) {
      const contratoExistente = await pool.query(
        `SELECT c.id, c.cliente_id, c.titulo
         FROM contratos c
         INNER JOIN clientes cl ON cl.id = c.cliente_id
         WHERE c.id = $1 AND cl.empresa_id = $2`,
        [contratoId, authUser.empresaId]
      );
      if (contratoExistente.rows.length === 0) {
        return res.status(404).json({
          error: "No encontrado",
          glosa: "Contrato no encontrado o no pertenece a tu empresa",
        });
      }
    }

    const { cliente_id, titulo, descripcion, fecha_inicio, fecha_fin } =
      req.body;

    // Construir la consulta dinámicamente solo con los campos que se envían
    const updates = [];
    const values = [];
    let paramIndex = 1;

    if (cliente_id !== undefined) {
      updates.push(`cliente_id = $${paramIndex}`);
      values.push(Number(cliente_id));
      paramIndex++;
    }

    if (titulo !== undefined) {
      updates.push(`titulo = $${paramIndex}`);
      values.push(titulo);
      paramIndex++;
    }

    if (descripcion !== undefined) {
      updates.push(`descripcion = $${paramIndex}`);
      values.push(descripcion);
      paramIndex++;
    }

    if (fecha_inicio !== undefined) {
      updates.push(`fecha_inicio = $${paramIndex}`);
      values.push(fecha_inicio);
      paramIndex++;
    }

    if (fecha_fin !== undefined) {
      updates.push(`fecha_fin = $${paramIndex}`);
      values.push(fecha_fin);
      paramIndex++;
    }

    // Si no hay campos para actualizar, retornar el contrato actual
    if (updates.length === 0) {
      const contratoActual = await pool.query(
        "SELECT * FROM contratos WHERE id = $1",
        [contratoId]
      );
      return res.json(contratoActual.rows[0]);
    }

    // Agregar el ID del contrato al final
    values.push(contratoId);
    const result = await pool.query(
      `UPDATE contratos 
       SET ${updates.join(", ")}
       WHERE id = $${paramIndex}
       RETURNING *`,
      values
    );

    // Si se envía un nuevo archivo, actualizarlo
    if (req.file && req.file.buffer) {
      // Insertar nuevo documento (manteniendo el historial)
      await pool.query(
        "INSERT INTO documentos_contrato (contract_id, document) VALUES ($1, $2)",
        [contratoId, req.file.buffer]
      );
    }

    // Registrar en auditoría
    await pool.query(
      "INSERT INTO auditoria (usuario_id, accion) VALUES ($1, $2)",
      [
        authUser?.nombreUsuario || null,
        `CONTRATOS_ACTUALIZAR: actualizó contrato ID ${contratoId} - ${
          titulo || contratoExistente.rows[0].titulo
        }`,
      ]
    );

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al actualizar contrato",
      glosa: err.message || err.toString(),
    });
  }
});

// Descargar/visualizar el último documento asociado al contrato
router.get("/:id/file", async (req, res) => {
  try {
    const doc = await pool.query(
      "SELECT document FROM documentos_contrato WHERE contract_id=$1 ORDER BY id DESC LIMIT 1",
      [req.params.id]
    );
    if (doc.rows.length === 0)
      return res.status(404).json({ error: "Documento no encontrado" });
    const buffer = doc.rows[0].document;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", 'inline; filename="contrato.pdf"');
    res.send(buffer);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al obtener archivo",
      glosa: err.message || err.toString(),
    });
  }
});

// Eliminar contrato y su archivo
router.delete("/:id", async (req, res) => {
  try {
    await pool.query("DELETE FROM documentos_contrato WHERE contract_id=$1", [
      req.params.id,
    ]);
    const result = await pool.query(
      "DELETE FROM contratos WHERE id=$1 RETURNING *",
      [req.params.id]
    );
    if (result.rows.length === 0)
      return res.status(404).json({ error: "No encontrado" });

    const authUser = getAuthUser(req);
    await pool.query(
      "INSERT INTO auditoria (usuario_id, accion) VALUES ($1, $2)",
      [
        authUser?.nombreUsuario || null,
        `CONTRATOS_ELIMINAR: eliminó contrato ID ${req.params.id}`,
      ]
    );

    res.json({ deleted: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al eliminar contrato",
      glosa: err.message || err.toString(),
    });
  }
});

module.exports = router;
