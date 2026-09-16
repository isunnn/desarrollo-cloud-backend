const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const morgan = require("morgan");
const swaggerUi = require("swagger-ui-express");

const { pool } = require("./db");
const { authOptional } = require("./middleware/validateEntra");

const authRoutes = require("./routes/auth");
const userRoutes = require("./routes/users");
const clientRoutes = require("./routes/clients");
const contractRoutes = require("./routes/contracts");
const auditRoutes = require("./routes/audit");
const empresasRoutes = require("./routes/empresas");
const rolesRoutes = require("./routes/roles");
const empresaUsuariosRoutes = require("./routes/empresa_usuarios");
const metricasRoutes = require("./routes/metricas");

const app = express();

app.use(cors({
  origin: process.env.CORS_ORIGINS ? process.env.CORS_ORIGINS.split(",") : true,
  allowedHeaders: ["Content-Type", "Authorization"],
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
}));
app.use(express.json());
app.use(morgan("dev"));
app.use(authOptional);

app.get("/health", (req, res) => {
  res.json({ status: "ok", entra: !!process.env.ENTRA_TENANT_ID });
});
app.get("/auth/me", require("./middleware/validateEntra").authRequired, (req, res) => {
  res.json({ source: req.authSource, claims: req.auth, roles: req.authRoles });
});

const uploadsDir = path.join(__dirname, "..", "uploads", "contracts");
fs.mkdirSync(uploadsDir, { recursive: true });

app.use("/autenticacion", authRoutes);
app.use("/usuarios", userRoutes);
app.use("/clientes", clientRoutes);
app.use("/contratos", contractRoutes);
app.use("/auditoria", auditRoutes);
app.use("/empresas", empresasRoutes);
app.use("/roles", rolesRoutes);
app.use("/empresa-usuarios", empresaUsuariosRoutes);
app.use("/metricas", metricasRoutes);

const openapi = require("./openapi.json");
app.use("/docs", swaggerUi.serve, swaggerUi.setup(openapi));
app.get("/openapi.json", (req, res) => {
  res.json(openapi);
});

app.use("/files/contracts", express.static(uploadsDir));

app.get("/", (req, res) => {
  res.json({ name: "Atlas", status: "ok" });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Atlas API escuchando en puerto ${PORT}`);
});
