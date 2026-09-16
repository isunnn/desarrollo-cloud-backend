const express = require("express");
const { pool } = require("../db");
const { getAuthUser } = require("../middleware/validateEntra");

const router = express.Router();

// Crear usuario
router.post("/", async (req, res) => {
  const { nombre_usuario, contrasena_hash, correo } = req.body;
  try {
    const result = await pool.query(
      "INSERT INTO usuarios (nombre_usuario, contrasena_hash, correo) VALUES ($1, $2, $3) RETURNING id, nombre_usuario, correo, created_at",
      [nombre_usuario, contrasena_hash, correo]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al crear usuario",
      glosa: err.message || err.toString(),
    });
  }
});

// Listar usuarios filtrados por empresa del token
router.get("/", async (req, res) => {
  try {
    const authUser = getAuthUser(req);

    if (!authUser) {
      return res.status(401).json({
        error: "No autorizado",
        glosa: "Token inválido",
      });
    }

    const empresaFilter = authUser.empresaId
      ? "INNER JOIN empresa_usuarios eu ON eu.usuario_id = u.id WHERE eu.empresa_id = $1"
      : "";
    const result = await pool.query(
      `SELECT DISTINCT u.id, u.nombre_usuario, u.correo, u.created_at 
       FROM usuarios u
       ${empresaFilter}
       ORDER BY u.id DESC`,
      authUser.empresaId ? [authUser.empresaId] : []
    );

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al listar usuarios",
      glosa: err.message || err.toString(),
    });
  }
});

// Obtener usuario por id
router.get("/:id", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT id, nombre_usuario, correo, created_at FROM usuarios WHERE id = $1",
      [req.params.id]
    );
    if (result.rows.length === 0)
      return res.status(404).json({ error: "No encontrado" });
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al obtener usuario",
      glosa: err.message || err.toString(),
    });
  }
});

// Actualizar usuario
router.put("/:id", async (req, res) => {
  const { nombre_usuario, contrasena_hash, correo } = req.body;
  try {
    const result = await pool.query(
      "UPDATE usuarios SET nombre_usuario=$1, contrasena_hash=$2, correo=$3 WHERE id=$4 RETURNING id, nombre_usuario, correo, created_at",
      [nombre_usuario, contrasena_hash, correo, req.params.id]
    );
    if (result.rows.length === 0)
      return res.status(404).json({ error: "No encontrado" });

    const authUser = getAuthUser(req);
    await pool.query(
      "INSERT INTO auditoria (usuario_id, accion) VALUES ($1, $2)",
      [
        authUser?.nombreUsuario || null,
        `USUARIOS_ACTUALIZAR: actualizó usuario ID ${req.params.id}`,
      ]
    );

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al actualizar usuario",
      glosa: err.message || err.toString(),
    });
  }
});

// Eliminar usuario
router.delete("/:id", async (req, res) => {
  try {
    const result = await pool.query(
      "DELETE FROM usuarios WHERE id=$1 RETURNING *",
      [req.params.id]
    );
    if (result.rows.length === 0)
      return res.status(404).json({ error: "No encontrado" });

    const authUser = getAuthUser(req);
    await pool.query(
      "INSERT INTO auditoria (usuario_id, accion) VALUES ($1, $2)",
      [
        authUser?.nombreUsuario || null,
        `USUARIOS_ELIMINAR: eliminó usuario ID ${req.params.id}`,
      ]
    );

    res.json({ deleted: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Error al eliminar usuario",
      glosa: err.message || err.toString(),
    });
  }
});

module.exports = router;
