const express = require("express");
const { pool } = require("../db");
const { getAuthUser } = require("../middleware/validateEntra");

const router = express.Router();

// Registrar log manualmente
router.post("/", async (req, res) => {
  const { accion, detalles } = req.body;
  try {
    const authUser = getAuthUser(req);
    const actionText = detalles ? `${accion}: ${detalles}` : accion;
    const result = await pool.query(
      "INSERT INTO auditoria (usuario_id, accion) VALUES ($1, $2) RETURNING *",
      [authUser?.nombreUsuario || null, actionText]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Error al registrar log", glosa: err.message || err.toString() });
  }
});

// Listar logs
router.get("/", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM auditoria ORDER BY id DESC");
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Error al listar logs", glosa: err.message || err.toString() });
  }
});

module.exports = router;
