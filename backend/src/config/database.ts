require("../bootstrap");

module.exports = {
  define: {
    charset: "utf8mb4",
    collate: "utf8mb4_bin"
  },
  dialect: process.env.DB_DIALECT || "mysql",
  timezone: "-03:00",
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  username: process.env.DB_USER,
  password: process.env.DB_PASS,
  logging: false,
  dialectOptions: { connectTimeout: 20000 },
  pool: { max: 10, min: 0, acquire: 30000, idle: 10000 },
  // Kurze Netz-/Datenbank-Aussetzer wiederholen statt die Anfrage scheitern zu lassen.
  retry: {
    max: 4,
    match: [
      /ETIMEDOUT/,
      /ECONNRESET/,
      /ECONNREFUSED/,
      /EHOSTUNREACH/,
      /PROTOCOL_CONNECTION_LOST/,
      /SequelizeConnectionError/,
      /SequelizeConnectionRefusedError/,
      /SequelizeConnectionAcquireTimeoutError/,
      /SequelizeHostNotFoundError/
    ]
  }
};
