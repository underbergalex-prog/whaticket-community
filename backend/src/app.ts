import "./bootstrap";
import "reflect-metadata";
import "express-async-errors";
import path from "path";
import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import cookieParser from "cookie-parser";
import * as Sentry from "@sentry/node";

import "./database";
import uploadConfig from "./config/upload";
import AppError from "./errors/AppError";
import routes from "./routes";
import { logger } from "./utils/logger";

Sentry.init({ dsn: process.env.SENTRY_DSN });

const app = express();

app.use(
  cors({
    credentials: true,
    origin: process.env.FRONTEND_URL
  })
);
app.use(cookieParser());
app.use(express.json());
app.use(Sentry.Handlers.requestHandler());
// RFC 5987: Prozent-Kodierung fuer den filename*-Parameter in
// Content-Disposition (UTF-8-Dateinamen, die der Browser korrekt anzeigt).
const encodeRFC5987 = (value: string): string =>
  encodeURIComponent(value)
    .replace(/['()]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`)
    .replace(/\*/g, "%2A");

app.use(
  "/public",
  express.static(uploadConfig.directory, {
    dotfiles: "allow",
    setHeaders: (res, filePath) => {
      // Expliziter Content-Disposition-Header mit ASCII-Fallback und
      // RFC-5987-filename*: Downloads von Altbestand-Dateien mit
      // Klammern/Leerzeichen/Umlauten bekommen sonst kaputte Namen.
      const base = path.basename(filePath);
      const asciiFallback =
        base.replace(/[^\x20-\x7e]/g, "_").replace(/["\\]/g, "_") || "file";
      res.setHeader(
        "Content-Disposition",
        `inline; filename="${asciiFallback}"; filename*=UTF-8''${encodeRFC5987(
          base
        )}`
      );
    }
  })
);
app.use(routes);

app.use(Sentry.Handlers.errorHandler());

app.use(async (err: Error, req: Request, res: Response, _: NextFunction) => {
  if (err instanceof AppError) {
    logger.warn(err);
    return res.status(err.statusCode).json({ error: err.message });
  }

  logger.error(err);
  return res.status(500).json({ error: "Internal server error" });
});

export default app;
