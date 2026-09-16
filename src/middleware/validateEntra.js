const jwt = require("jsonwebtoken");
const jwksClient = require("jwks-rsa");
require("dotenv").config();

const JWT_SECRET = process.env.JWT_SECRET || "inseguro";
const TENANT_ID = process.env.ENTRA_TENANT_ID || "";
const AUDIENCE = process.env.ENTRA_AUDIENCE || "";
const ISSUER = TENANT_ID
  ? `https://login.microsoftonline.com/${TENANT_ID}/v2.0`
  : process.env.ENTRA_ISSUER || "";

let jwks = null;
if (TENANT_ID) {
  jwks = jwksClient({
    jwksUri: `https://login.microsoftonline.com/${TENANT_ID}/discovery/v2.0/keys`,
    cache: true,
    rateLimit: true,
  });
}

function getKey(header, cb) {
  jwks.getSigningKey(header.kid, (err, key) => {
    if (err) return cb(err);
    cb(null, key.getPublicKey());
  });
}

function verifyEntra(token) {
  return new Promise((resolve, reject) => {
    const opts = { algorithms: ["RS256"] };
    if (ISSUER) opts.issuer = ISSUER;

    if (AUDIENCE) {
      jwt.verify(token, getKey, opts, (err, decoded) => {
        if (!err) return resolve(decoded);

        // Retry without audience
        const optsNoAud = { algorithms: ["RS256"] };
        if (ISSUER) optsNoAud.issuer = ISSUER;
        jwt.verify(token, getKey, optsNoAud, (err2, decoded2) => {
          if (!err2) return resolve(decoded2);

          // Retry without audience AND without issuer
          const optsMin = { algorithms: ["RS256"] };
          jwt.verify(token, getKey, optsMin, (err3, decoded3) => {
            if (err3) return reject(err3);
            resolve(decoded3);
          });
        });
      });
    } else {
      jwt.verify(token, getKey, opts, (err, decoded) => {
        if (!err) return resolve(decoded);

        // Retry without issuer
        const optsNoIss = { algorithms: ["RS256"] };
        jwt.verify(token, getKey, optsNoIss, (err2, decoded2) => {
          if (err2) return reject(err2);
          resolve(decoded2);
        });
      });
    }
  });
}

function verifyLegacy(token) {
  return jwt.verify(token, JWT_SECRET);
}

function getRoles(decoded) {
  return decoded.roles || decoded.groups || (decoded.scp ? decoded.scp.split(" ") : []);
}

async function authOptional(req, _res, next) {
  const h = req.headers.authorization || "";
  if (!h.startsWith("Bearer ")) return next();
  const token = h.slice(7);
  try {
    if (jwks) {
      try {
        req.auth = await verifyEntra(token);
        req.authSource = "entra";
      } catch (e) {
        console.log("[authOptional] Entra verification failed:", e.message);
        try {
          req.auth = verifyLegacy(token);
          req.authSource = "legacy";
        } catch (e2) {
          console.log("[authOptional] Legacy fallback also failed:", e2.message);
          req.auth = null;
        }
      }
    } else {
      req.auth = verifyLegacy(token);
      req.authSource = "legacy";
    }
    req.authRoles = getRoles(req.auth || {});
  } catch {
    req.auth = null;
  }
  next();
}

async function authRequired(req, res, next) {
  const h = req.headers.authorization || "";
  if (!h.startsWith("Bearer ")) {
    return res.status(401).json({ error: "No autorizado", glosa: "Falta token Bearer" });
  }
  const token = h.slice(7);
  try {
    if (jwks) {
      try {
        req.auth = await verifyEntra(token);
        req.authSource = "entra";
      } catch {
        req.auth = verifyLegacy(token);
        req.authSource = "legacy";
      }
    } else {
      req.auth = verifyLegacy(token);
      req.authSource = "legacy";
    }
    req.authRoles = getRoles(req.auth || {});
    return next();
  } catch (e) {
    return res.status(401).json({ error: "Token inválido", glosa: e.message });
  }
}

function requireRole(...allowed) {
  return (req, res, next) => {
    const roles = req.authRoles || [];
    if (allowed.some((r) => roles.includes(r))) return next();
    if (req.authSource === "legacy") return next();
    return res.status(403).json({ error: "Prohibido", glosa: "Rol insuficiente" });
  };
}

function getAuthUser(req) {
  if (!req.auth) return null;
  if (req.authSource === "entra") {
    return {
      usuarioId: null,
      nombreUsuario: req.auth.preferred_username || req.auth.name || null,
      empresaId: null,
      empresaUsuarioId: null,
      _source: "entra",
    };
  }
  return {
    usuarioId: req.auth.usuarioId || req.auth.usuario_id || null,
    nombreUsuario: req.auth.nombreUsuario || req.auth.nombre_usuario || null,
    empresaId: req.auth.empresaId || req.auth.empresa_id || null,
    empresaUsuarioId: req.auth.empresaUsuarioId || req.auth.empresa_usuario_id || null,
    _source: "legacy",
  };
}

module.exports = { authOptional, authRequired, requireRole, getRoles, getAuthUser };
