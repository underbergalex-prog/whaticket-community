import { readFileSync } from "fs";

import pino from "pino";
import makeWASocket, {
  UserFacingSocketConfig,
  DisconnectReason,
  WASocket,
  AuthenticationCreds,
  initAuthCreds,
  isJidUser,
  isLidUser,
  isJidGroup,
  isJidBroadcast,
  makeCacheableSignalKeyStore,
  BufferJSON,
  WAMessage,
  WAMessageKey,
  downloadMediaMessage,
  getContentType,
  jidNormalizedUser,
  jidDecode,
  makeInMemoryStore,
  SignalDataSet,
  AnyMessageContent,
  proto,
  Browsers,
  fetchLatestWaWebVersion,
  WAVersion,
  ConnectionState,
  DEFAULT_CONNECTION_CONFIG,
  MessageRetryMap
} from "whaileys";
import { LRUCache } from "lru-cache";
import { Boom } from "@hapi/boom";
import { HttpsProxyAgent } from "https-proxy-agent";
import NodeCache from "node-cache";
import { Op } from "sequelize";

import Whatsapp from "../../../models/Whatsapp";
import { getIO } from "../../../libs/socket";
import { logger } from "../../../utils/logger";
import AppError from "../../../errors/AppError";
import StoreWppSessionKeys from "../../../services/WppKeyServices/StoreWppSessionKeys";
import GetWppSessionKeys from "../../../services/WppKeyServices/GetWppSessionKeys";
import { getRedisClient } from "../../../libs/redisStore";
import {
  SendMessageOptions,
  ProviderMessage,
  ProviderMediaInput,
  SendMediaOptions,
  ProviderContact,
  MessageType,
  MessageAck,
  InitOptions
} from "../types";
import { WhatsappProvider } from "../whatsappProvider";
import { sleep } from "../../../utils/sleep";
import {
  handleMessage,
  handleMessageAck,
  ContactPayload,
  MessagePayload,
  MediaPayload,
  WhatsappContextPayload
} from "../../../handlers/handleWhatsappEvents";

type WALogger = NonNullable<Parameters<typeof makeInMemoryStore>[0]["logger"]>;

const whaileyLogger = pino({
  level: process.env.WHAILEYS_LOG_LEVEL || "silent"
}) as unknown as WALogger;

type Store = ReturnType<typeof makeInMemoryStore>;

interface Session extends WASocket {
  id: number;
  store?: Store;
  // Verbindungszustand dieses Sockets laut connection.update. Der Wächter startet
  // einen offenen Socket nie neu und erkennt einen, der seit Minuten geschlossen ist.
  zustand?: "connecting" | "open" | "close" | "qr";
  zustandSeit?: number;
  generation?: number;
}

const sessions = new Map<number, Session>();
const stores = new Map<number, Store>();

// Jede init() und jedes removeSession() erhöht die Generation einer Sitzung.
// Ein Reconnect, der während seiner 3-s-Pause überholt wurde (Session-Reset,
// neuer Start per QR-Knopf), bricht ab, statt mit den ALTEN Zugangsdaten aus
// dem Closure-Objekt zurückzukommen und den frischen QR-Socket abzuschießen.
const sessionGeneration = new Map<number, number>();
const nextGeneration = (sessionId: number): number => {
  const gen = (sessionGeneration.get(sessionId) ?? 0) + 1;
  sessionGeneration.set(sessionId, gen);
  return gen;
};

// Letztes Lebenszeichen je Sitzung (connection.update, creds.update, Nachrichten).
// Der Wächter erkennt daran Schleifen, die still gestorben sind.
const lastActivity = new Map<number, number>();
const touch = (sessionId: number) => lastActivity.set(sessionId, Date.now());
const lastRestart = new Map<number, number>();
// Geplanter Neustart je Sitzung samt der Generation, für die er gilt.
const pendingRestarts = new Map<
  number,
  { timer: NodeJS.Timeout; generation: number }
>();

// Neueste creds je Sitzung: das state.creds-Objekt des letzten init (whaileys schreibt
// es fort) samt der Generation, der es gehört. Neustarts derselben Generation nehmen
// diese statt der DB-Kopie – die kann nach einem gescheiterten Speichern veraltet oder
// leer sein (frische Kopplung nach dem Scan weg, Pre-Key-Zähler springen zurück).
// removeSession (Reset/Abmeldung) verwirft sie.
const liveCreds = new Map<
  number,
  { generation: number; creds: AuthenticationCreds; whatsapp: Whatsapp }
>();

// Bewusst gestoppte Sitzungen (logout, Abmeldung durch WhatsApp) samt Grund. Wächter und
// automatische Neustarts lassen sie liegen, bis init() ausdrücklich aufgerufen wird.
const stoppedSessions = new Map<number, "logout" | "abmeldung">();

// Laufende logout()-Resets je Sitzung. Ein ausdrücklicher Start einer gestoppten Sitzung
// wartet darauf (höchstens RESET_WARTEN_MS).
const resetsLaufend = new Map<number, Promise<void>>();
const RESET_WARTEN_MS = 60_000;

// Unabhängige Kopie (wie aus der DB geladen): der alte Socket berührt den neuen nicht.
const kopiereCreds = (creds: AuthenticationCreds): AuthenticationCreds =>
  JSON.parse(JSON.stringify(creds, BufferJSON.replacer), BufferJSON.reviver);

// Neustart nach einem Fehler, mit wachsendem Abstand (max. 2 Min), solange niemand
// inzwischen neu gestartet hat. Ohne das starb die Schleife bei einem einzigen
// Datenbank-Timeout dauerhaft (Vorfall 13.10.2026, 04:01).
const scheduleRestart = (sessionId: number, generation: number, attempt = 0): void => {
  if (stoppedSessions.has(sessionId)) return;
  const geplant = pendingRestarts.get(sessionId);
  if (geplant) {
    if (geplant.generation >= generation) return;
    // Der geplante Neustart einer überholten Generation täte beim Auslösen nichts (die
    // Generation passt nicht mehr): ersetzen, statt den Wunsch der aktuellen zu verwerfen.
    clearTimeout(geplant.timer);
    pendingRestarts.delete(sessionId);
  }
  const delay = Math.min(5_000 * 2 ** attempt, 120_000);
  const timer = setTimeout(async () => {
    if (pendingRestarts.get(sessionId)?.timer === timer) {
      pendingRestarts.delete(sessionId);
    }
    if (
      sessionGeneration.get(sessionId) !== generation ||
      stoppedSessions.has(sessionId)
    )
      return;
    try {
      const fresh = await Whatsapp.findByPk(sessionId);
      if (!fresh || fresh.status === "DISCONNECTED") return;
      if (
        sessionGeneration.get(sessionId) !== generation ||
        stoppedSessions.has(sessionId)
      )
        return;
      lastRestart.set(sessionId, Date.now());
      // eslint-disable-next-line @typescript-eslint/no-use-before-define
      await init(fresh, { credsVonGeneration: generation });
    } catch (err) {
      logger.error({ info: "Restart after error failed, retrying", sessionId, attempt, err });
      scheduleRestart(sessionId, sessionGeneration.get(sessionId) ?? generation, attempt + 1);
    }
  }, delay);
  pendingRestarts.set(sessionId, { timer, generation });
};

const msgRetryCounterLRU = new LRUCache<string, number>({
  max: 5000,
  ttl: 600 * 1000,
  allowStale: false,
  updateAgeOnGet: true
});

const msgRetryCounterMap = new Proxy<MessageRetryMap>({} as MessageRetryMap, {
  get(target, prop) {
    if (typeof prop === "string") {
      return msgRetryCounterLRU.get(prop);
    }
    return Reflect.get(target, prop);
  },
  set(target, prop, value) {
    if (typeof prop === "string" && typeof value === "number") {
      msgRetryCounterLRU.set(prop, value);
      return true;
    }
    return Reflect.set(target, prop, value);
  },
  deleteProperty(target, prop) {
    if (typeof prop === "string") {
      msgRetryCounterLRU.delete(prop);
      return true;
    }
    return Reflect.deleteProperty(target, prop);
  },
  has(target, prop) {
    if (typeof prop === "string") {
      return msgRetryCounterLRU.has(prop);
    }
    return Reflect.has(target, prop);
  },
  ownKeys() {
    return Array.from(msgRetryCounterLRU.keys());
  },
  getOwnPropertyDescriptor(target, prop) {
    if (typeof prop === "string" && msgRetryCounterLRU.has(prop)) {
      return {
        configurable: true,
        enumerable: true,
        value: msgRetryCounterLRU.get(prop)
      };
    }
    return undefined;
  }
});

const msgCacheLRU = new LRUCache<string, string>({
  max: 5000,
  ttl: 600 * 1000,
  allowStale: false,
  updateAgeOnGet: true
});

const sentMessagesCache = new NodeCache({
  stdTTL: 60,
  useClones: false
});

const normalizeJid = (jid: string): string => {
  if (!jid) return jid;
  if (!jid.includes("@")) return `${jid}@s.whatsapp.net`;
  return jid.replace(/@c\.us$/i, "@s.whatsapp.net");
};

const msgCache = {
  get: (key: WAMessageKey): proto.IMessage | undefined => {
    const { id } = key;
    if (!id) return undefined;
    const data = msgCacheLRU.get(id);
    if (data) {
      try {
        const msg = JSON.parse(data);
        return msg?.message;
      } catch {
        return undefined;
      }
    }
    return undefined;
  },
  save: (msg: WAMessage) => {
    const { id } = msg.key;
    if (!id) return;
    try {
      msgCacheLRU.set(id, JSON.stringify(msg));
    } catch (e) {
      logger.debug({ info: "Error caching message", messageId: id, err: e });
    }
  }
};

const clearSessionKeys = async (sessionId: number): Promise<void> => {
  const client = getRedisClient();
  if (!client) return;

  try {
    const match = `wpp:${sessionId}:*`;

    const scanAndDelete = async (cursor: string): Promise<void> => {
      const [nextCursor, keys] = await client.scan(
        cursor,
        "MATCH",
        match,
        "COUNT",
        100
      );

      if (keys.length > 0) {
        await client.del(keys);
      }

      if (nextCursor !== "0") {
        await scanAndDelete(nextCursor);
      }
    };

    await scanAndDelete("0");

    logger.info({ info: "Cleared Redis session keys", sessionId });
  } catch (err) {
    logger.error({ info: "Error clearing Redis session keys", sessionId, err });
  }
};

const assertUnique = (sessionId: number) => {
  const wbot = sessions.get(sessionId);

  if (wbot) {
    wbot.ev.removeAllListeners("connection.update");
    sessions.delete(sessionId);
    stores.delete(sessionId);

    wbot.end(undefined);
  }
};

// Wartet höchstens ms, danach undefined (der Aufruf läuft im Hintergrund aus).
const mitZeitlimit = <T>(
  promise: Promise<T>,
  ms: number
): Promise<T | undefined> =>
  new Promise(resolve => {
    const timer = setTimeout(() => resolve(undefined), ms);
    promise.then(
      value => {
        clearTimeout(timer);
        resolve(value);
      },
      () => {
        clearTimeout(timer);
        resolve(undefined);
      }
    );
  });

// true = gespeichert. Fehler werden nur geloggt; der Aufrufer merkt sie erneut vor.
// nurSession: nur die creds, ohne status CONNECTED (Neustart vor open).
const saveSessionCreds = async (
  whatsapp: Whatsapp,
  creds: AuthenticationCreds,
  nurSession = false
): Promise<boolean> => {
  try {
    const session = JSON.stringify(creds, BufferJSON.replacer);
    // Statisch und nur, solange die Zeile nicht DISCONNECTED ist: eine verspätet landende
    // Speicherung (hängende Abfrage, Sequelize-Wiederholung desselben SQL) belebt eine
    // zurückgesetzte bzw. abgemeldete Sitzung nicht wieder (status CONNECTED, alte creds).
    // Jeder Start schreibt vorher OPENING bzw. qrcode. Statisch entfällt zudem das
    // Dirty-Flag-Rennen der gemeinsamen Instanz.
    const [anzahl] = await Whatsapp.update(
      nurSession ? { session } : { session, status: "CONNECTED", qrcode: "" },
      { where: { id: whatsapp.id, status: { [Op.ne]: "DISCONNECTED" } } }
    );

    logger.debug({
      info: anzahl
        ? "Creds saved to database"
        : "Creds not saved, session is DISCONNECTED",
      whatsappId: whatsapp.id
    });
    return true;
  } catch (err) {
    logger.error({
      info: "Error saving creds to database",
      whatsappId: whatsapp.id,
      err
    });
    return false;
  }
};

const CREDS_RETRY_MS = 15_000;
// Höchstens so lange auf die vorige Speicherung warten: eine hängende Abfrage (es gibt
// keinen Query-Timeout) blockierte sonst alle folgenden und den Reconnect dahinter.
const CREDS_KETTE_WARTEN_MS = 30_000;
const credsDebounceTimers = new Map<number, NodeJS.Timeout>();
// generation: welcher init die creds gehören. Ein Stand überholter Generationen
// (Reset, Neustart mit DB-creds) wird nicht mehr geschrieben.
const pendingCredsSaves = new Map<
  number,
  {
    whatsapp: Whatsapp;
    creds: AuthenticationCreds;
    generation: number;
    nurSession?: boolean;
  }
>();
// Laufende Speicherung je Sitzung; die nächste wartet auf sie. Parallel konnte ein
// langsames älteres UPDATE nach einem neueren landen (DB behielt den alten Stand),
// oder das fertige ältere setzte die Dirty-Flags der gemeinsamen Instanz zurück und
// das neuere schrieb gar nicht, meldete aber Erfolg. pending.creds ist das Live-Objekt:
// der zuletzt laufende Schreibvorgang trägt damit immer den neuesten Stand.
const credsSaveKette = new Map<number, Promise<void>>();

const armCredsTimer = (sessionId: number, delayMs: number) => {
  const existingTimer = credsDebounceTimers.get(sessionId);
  if (existingTimer) {
    clearTimeout(existingTimer);
  }

  const timer = setTimeout(() => {
    credsDebounceTimers.delete(sessionId);
    // eslint-disable-next-line no-use-before-define
    flushPendingCredsSave(sessionId);
  }, delayMs);

  credsDebounceTimers.set(sessionId, timer);
};

const credsEinmalSpeichern = async (sessionId: number): Promise<void> => {
  const existingTimer = credsDebounceTimers.get(sessionId);
  if (existingTimer) {
    clearTimeout(existingTimer);
    credsDebounceTimers.delete(sessionId);
  }

  const pending = pendingCredsSaves.get(sessionId);
  if (!pending) return;
  pendingCredsSaves.delete(sessionId);
  if (sessionGeneration.get(sessionId) !== pending.generation) return;

  if (
    await saveSessionCreds(pending.whatsapp, pending.creds, pending.nurSession)
  ) {
    return;
  }

  // Gescheitert (z. B. DB-Timeout): vorgemerkt lassen und später erneut versuchen.
  // Früher war der Eintrag schon vor dem await gelöscht – der Stand war für immer weg.
  if (
    pendingCredsSaves.has(sessionId) ||
    sessionGeneration.get(sessionId) !== pending.generation
  ) {
    return;
  }
  pendingCredsSaves.set(sessionId, pending);
  if (!credsDebounceTimers.has(sessionId)) {
    armCredsTimer(sessionId, CREDS_RETRY_MS);
  }
};

// Den Live-Stand der laufenden Generation (erneut) vormerken, nur session.
const liveStandNachziehen = (sessionId: number): void => {
  const live = liveCreds.get(sessionId);
  if (
    !live ||
    live.generation !== sessionGeneration.get(sessionId) ||
    pendingCredsSaves.has(sessionId)
  ) {
    return;
  }
  pendingCredsSaves.set(sessionId, {
    whatsapp: live.whatsapp,
    creds: live.creds,
    generation: live.generation,
    nurSession: true
  });
  armCredsTimer(sessionId, 0);
};

// Hängt sich hinter eine laufende Speicherung derselben Sitzung (credsSaveKette).
const flushPendingCredsSave = (sessionId: number): Promise<void> => {
  const vorige = credsSaveKette.get(sessionId);
  const warten = vorige
    ? mitZeitlimit(
        vorige.then(() => true),
        CREDS_KETTE_WARTEN_MS
      )
    : Promise.resolve(true);
  const lauf = warten
    .then(gewartet => {
      if (!gewartet && vorige) {
        // Zeitlimit: die vorige Abfrage läuft weiter und kann NACH dieser landen (älterer
        // Stand). Sobald sie endet, den Live-Stand noch einmal schreiben – so landet der
        // neueste zuletzt. Gestoppte Sitzungen haben keinen Live-Stand mehr.
        vorige.then(() => liveStandNachziehen(sessionId));
      }
      return credsEinmalSpeichern(sessionId);
    })
    .catch(err => {
      logger.error({ info: "Error flushing creds", sessionId, err });
    });
  credsSaveKette.set(sessionId, lauf);
  lauf.then(() => {
    if (credsSaveKette.get(sessionId) === lauf) {
      credsSaveKette.delete(sessionId);
    }
  });
  return lauf;
};

const debouncedSaveCreds = (
  whatsapp: Whatsapp,
  creds: AuthenticationCreds,
  generation: number,
  nurSession: boolean,
  delayMs = 1000
) => {
  // Spätes creds.update eines ersetzten Sockets verdrängt nicht den aktuellen Eintrag.
  if (sessionGeneration.get(whatsapp.id) !== generation) return;
  pendingCredsSaves.set(whatsapp.id, {
    whatsapp,
    creds,
    generation,
    nurSession
  });
  armCredsTimer(whatsapp.id, delayMs);
};

// Reset/Abmeldung: Vorgemerktes verwerfen, statt es danach doch noch (samt
// status CONNECTED) in die DB zu schreiben und die alte Sitzung wiederzubeleben.
const discardPendingCreds = (sessionId: number): void => {
  const existingTimer = credsDebounceTimers.get(sessionId);
  if (existingTimer) {
    clearTimeout(existingTimer);
    credsDebounceTimers.delete(sessionId);
  }
  pendingCredsSaves.delete(sessionId);
};

const useSessionAuthState = async (
  whatsapp: Whatsapp,
  credsAusSpeicher?: AuthenticationCreds
) => {
  const sessionId = whatsapp.id;

  const creds =
    credsAusSpeicher ??
    (whatsapp.session
      ? JSON.parse(whatsapp.session, BufferJSON.reviver)
      : initAuthCreds());

  return {
    state: {
      creds: creds as AuthenticationCreds,
      keys: {
        get: async (type: string, ids: string[]) => {
          const deviceId = jidDecode(creds?.me?.id)?.device || 1;

          const data = await GetWppSessionKeys({
            connectionId: sessionId,
            deviceId,
            type,
            ids
          });

          return data;
        },
        set: async (data: SignalDataSet) => {
          const deviceId = jidDecode(creds?.me?.id)?.device || 1;

          try {
            const promises: Promise<void>[] = [];

            Object.entries(data).forEach(([category, categoryData]) => {
              if (!categoryData) return;
              Object.entries(categoryData).forEach(([id, value]) => {
                promises.push(
                  StoreWppSessionKeys({
                    connectionId: sessionId,
                    deviceId,
                    type: category,
                    id,
                    value
                  })
                );
              });
            });

            await Promise.all(promises);
          } catch (err) {
            logger.error({
              info: "Error setting keys",
              sessionId,
              err
            });
          }
        }
      }
    }
  };
};

const mapMessageType = (msg: WAMessage): MessageType => {
  const messageType = getContentType(msg.message || undefined);

  if (messageType === "audioMessage" && msg.message?.audioMessage?.ptt) {
    return "ptt";
  }

  const typeMap: Record<string, MessageType> = {
    conversation: "chat",
    extendedTextMessage: "chat",
    imageMessage: "image",
    videoMessage: "video",
    audioMessage: "audio",
    documentMessage: "document",
    stickerMessage: "sticker",
    locationMessage: "location",
    contactMessage: "vcard",
    contactsArrayMessage: "vcard"
  };

  return typeMap[messageType || ""] || "chat";
};

const getMessageBody = (msg: WAMessage): string => {
  try {
    const messageType = getContentType(msg.message || undefined);

    if (messageType === "conversation") {
      return msg.message?.conversation || "";
    }

    if (messageType === "extendedTextMessage") {
      return msg.message?.extendedTextMessage?.text || "";
    }

    if (messageType === "imageMessage") {
      return msg.message?.imageMessage?.caption || "";
    }

    if (messageType === "videoMessage") {
      return msg.message?.videoMessage?.caption || "";
    }

    if (messageType === "documentMessage") {
      return msg.message?.documentMessage?.caption || "";
    }

    if (messageType === "contactMessage") {
      return msg.message?.contactMessage?.vcard || "";
    }

    if (messageType === "contactsArrayMessage") {
      const contacts = msg.message?.contactsArrayMessage?.contacts || [];
      return contacts.map(c => c.vcard).join("\n");
    }

    if (messageType === "locationMessage") {
      const location = msg.message?.locationMessage;
      if (!location) return "";

      const gmapsUrl = `https://maps.google.com/maps?q=${location.degreesLatitude}%2C${location.degreesLongitude}&z=17&hl=pt-BR`;
      const description =
        location.name ||
        `${location.degreesLatitude}, ${location.degreesLongitude}`;

      return `${gmapsUrl}|${description}`;
    }

    return "";
  } catch (err) {
    logger.error({ info: "Error getting message body", err });
    return "";
  }
};

const getQuotedMessageId = (msg: WAMessage): string | undefined => {
  const quotedMessageId =
    msg.message?.extendedTextMessage?.contextInfo?.stanzaId ||
    msg.message?.imageMessage?.contextInfo?.stanzaId ||
    msg.message?.videoMessage?.contextInfo?.stanzaId ||
    msg.message?.documentMessage?.contextInfo?.stanzaId ||
    undefined;

  return quotedMessageId;
};

const hasMedia = (msg: WAMessage): boolean => {
  const messageType = getContentType(msg.message || undefined);
  return [
    "imageMessage",
    "videoMessage",
    "audioMessage",
    "documentMessage",
    "stickerMessage"
  ].includes(messageType || "");
};

const mapMessageAck = (status: number | null | undefined): MessageAck => {
  if (status === null || status === undefined) return 0;
  if (status >= 4) return 4;
  if (status >= 3) return 3;
  if (status >= 2) return 2;
  if (status >= 1) return 1;
  return 0;
};

const shouldHandleMessage = (msg: WAMessage): boolean => {
  const messageType = getContentType(msg.message || undefined);
  const validTypes = [
    "conversation",
    "extendedTextMessage",
    "imageMessage",
    "videoMessage",
    "audioMessage",
    "documentMessage",
    "stickerMessage",
    "locationMessage",
    "contactMessage",
    "contactsArrayMessage"
  ];

  if (!validTypes.includes(messageType || "")) return false;

  const body = getMessageBody(msg);
  if (/\u200e/.test(body[0])) return false;

  if (!msg.key.fromMe) return true;

  const allowedFromMeTypes = [
    "locationMessage",
    "conversation",
    "extendedTextMessage",
    "contactMessage"
  ];

  return hasMedia(msg) || allowedFromMeTypes.includes(messageType || "");
};

const convertToMessagePayload = (msg: WAMessage): MessagePayload => {
  const fromJid = msg.key.remoteJid || "";
  const toJid = msg.key.fromMe ? fromJid : msg.key.participant || fromJid;
  const fromMe = msg.key.fromMe || false;

  return {
    id: msg.key.id || "",
    body: getMessageBody(msg),
    fromMe,
    hasMedia: hasMedia(msg),
    type: mapMessageType(msg),
    timestamp: msg.messageTimestamp ? Number(msg.messageTimestamp) : Date.now(),
    from: fromJid,
    to: toJid,
    hasQuotedMsg: Boolean(getQuotedMessageId(msg)),
    quotedMsgId: getQuotedMessageId(msg),
    ack: fromMe ? 1 : 0
  };
};

type ExtendedKey = WAMessageKey &
  Partial<{
    senderPn: string;
    sender_pn: string;
    participantPn: string;
    participant_pn: string;
    recipientPn: string;
    recipient_pn: string;
    peerRecipientPn: string;
    peer_recipient_pn: string;
    senderLid: string;
    sender_lid: string;
    participantLid: string;
    participant_lid: string;
    recipientLid: string;
    recipient_lid: string;
  }>;

type ExtendedContext = proto.IContextInfo &
  Partial<{
    senderLid: string;
    sender_lid: string;
    participantLid: string;
    participant_lid: string;
    recipientLid: string;
    recipient_lid: string;
    senderPn: string;
    sender_pn: string;
    participantPn: string;
    participant_pn: string;
    peerRecipientPn: string;
    peer_recipient_pn: string;
  }>;

const convertToContactPayload = async (
  jid: string,
  msg: WAMessage,
  wbot: Session
): Promise<ContactPayload> => {
  const keyExt = (msg.key || {}) as ExtendedKey;
  const content = msg.message || {};
  const ctx = (content?.extendedTextMessage?.contextInfo ||
    content?.imageMessage?.contextInfo ||
    content?.videoMessage?.contextInfo ||
    content?.documentMessage?.contextInfo ||
    content?.audioMessage?.contextInfo ||
    content?.stickerMessage?.contextInfo ||
    undefined) as ExtendedContext | undefined;

  let resolvedJid = jid || "";

  const lidCandidates: (string | undefined)[] = [
    keyExt.senderLid,
    keyExt.participantLid,
    keyExt.recipientLid,
    ctx?.senderLid,
    ctx?.participantLid,
    ctx?.recipientLid,
    keyExt.sender_lid,
    keyExt.participant_lid,
    keyExt.recipient_lid,
    ctx?.sender_lid,
    ctx?.participant_lid,
    ctx?.recipient_lid,
    (keyExt.senderPn || keyExt.sender_pn)?.includes("@lid")
      ? keyExt.senderPn || keyExt.sender_pn
      : undefined,
    (keyExt.participantPn || keyExt.participant_pn)?.includes("@lid")
      ? keyExt.participantPn || keyExt.participant_pn
      : undefined,
    (keyExt.peerRecipientPn || keyExt.peer_recipient_pn)?.includes("@lid")
      ? keyExt.peerRecipientPn || keyExt.peer_recipient_pn
      : undefined
  ];

  const lid = lidCandidates.find(
    cand => typeof cand === "string" && cand.includes("@lid")
  );

  const pnCandidates: (string | undefined)[] = [
    keyExt.senderPn || keyExt.sender_pn,
    keyExt.participantPn || keyExt.participant_pn,
    // whaileys setzt bei fromMe-Chats (Nachricht vom eigenen Handy an einen
    // LID-adressierten Kontakt) die echte Nummer in key.recipientPn –
    // ohne diesen Kandidaten entstand ein Pseudo-Nummern-Kontakt.
    keyExt.recipientPn || keyExt.recipient_pn,
    keyExt.peerRecipientPn || keyExt.peer_recipient_pn,
    ctx?.senderPn || ctx?.sender_pn,
    ctx?.participantPn || ctx?.participant_pn
  ];

  const preferPn = pnCandidates.find(
    v => typeof v === "string" && /@s\.whatsapp\.net$/i.test(v)
  );

  if (resolvedJid.endsWith("@lid") && preferPn) {
    resolvedJid = preferPn;
  } else if (
    resolvedJid &&
    !resolvedJid.endsWith("@s.whatsapp.net") &&
    !resolvedJid.endsWith("@g.us") &&
    preferPn
  ) {
    resolvedJid = preferPn;
  }

  const safeNormalized = (value?: string) => {
    if (!value) return "";
    try {
      return jidNormalizedUser(value);
    } catch {
      return value;
    }
  };

  const normalizedJid = safeNormalized(resolvedJid);

  const contactInfo =
    wbot.store?.contacts?.[resolvedJid] ||
    wbot.store?.contacts?.[normalizedJid];

  const chatInfo =
    wbot.store?.chats?.get?.(resolvedJid) ||
    wbot.store?.chats?.get?.(normalizedJid);

  let profilePicUrl: string | undefined;

  if (normalizedJid) {
    try {
      const url = await wbot.profilePictureUrl(normalizedJid, "image");
      profilePicUrl = url || undefined;
    } catch (err) {
      logger.debug({
        info: "Could not get profile picture",
        jid: normalizedJid,
        err
      });
    }
  }

  if (isJidGroup(resolvedJid)) {
    const groupNumber = normalizedJid.split("@")[0];
    const groupName =
      contactInfo?.name ||
      contactInfo?.notify ||
      chatInfo?.name ||
      (chatInfo as { subject?: string } | undefined)?.subject ||
      groupNumber;

    if (!contactInfo && (!groupName || groupName === groupNumber)) {
      try {
        const meta = await wbot.groupMetadata(normalizedJid);
        const metaName = typeof meta?.subject === "string" ? meta.subject : "";
        if (metaName) {
          return {
            name: metaName,
            number: groupNumber,
            isGroup: true,
            profilePicUrl
          };
        }
      } catch {
        /* ignore */
      }
    }

    return {
      name: groupName,
      number: groupNumber,
      isGroup: true,
      profilePicUrl
    };
  }

  const decoded = jidDecode(resolvedJid);

  const sessionPushName = wbot.user?.name?.trim().toLowerCase();
  const incomingPushName = msg.pushName?.trim();
  const pushName =
    incomingPushName &&
    sessionPushName &&
    incomingPushName.toLowerCase() === sessionPushName
      ? undefined
      : incomingPushName;

  // WICHTIG: Für @lid-JIDs ohne aufgelöste echte Nummer darf die LID-Ziffernfolge
  // NIEMALS als Telefonnummer verwendet werden (das erzeugte die Pseudo-Nummern
  // wie "+161340597477621" und damit doppelte Kontakte/Chats). In dem Fall bleibt
  // number leer und der Kontakt wird ausschließlich über die LID identifiziert.
  const number =
    (isJidUser(resolvedJid) && decoded?.user) ||
    jidDecode(preferPn || "")?.user ||
    (isLidUser(resolvedJid) ? "" : normalizedJid.split("@")[0]);

  const lidValue =
    isLidUser(resolvedJid) && decoded?.user ? `${decoded.user}@lid` : lid;

  const name =
    contactInfo?.name ||
    contactInfo?.notify ||
    pushName ||
    number ||
    lidValue ||
    "";

  return {
    name,
    number,
    lid: lidValue,
    isGroup: false,
    profilePicUrl
  };
};

const convertToMediaPayload = async (
  msg: WAMessage,
  wbot: Session
): Promise<MediaPayload | undefined> => {
  if (!hasMedia(msg)) return undefined;

  // TODO save direct to disc using stream
  try {
    const buffer = await downloadMediaMessage(
      msg,
      "buffer",
      {},
      {
        logger: whaileyLogger,
        reuploadRequest: wbot.updateMediaMessage
      }
    );

    const messageType = getContentType(msg.message || undefined);
    const getExtension = (mimetype: string, fallback: string): string =>
      mimetype.split("/")[1]?.split(";")[0] || fallback;

    if (messageType === "imageMessage") {
      const mimetype = msg.message?.imageMessage?.mimetype || "image/jpeg";
      return {
        filename: `image-${Date.now()}.${getExtension(mimetype, "jpg")}`,
        mimetype,
        data: buffer.toString("base64")
      };
    }

    if (messageType === "videoMessage") {
      const mimetype = msg.message?.videoMessage?.mimetype || "video/mp4";
      return {
        filename: `video-${Date.now()}.${getExtension(mimetype, "mp4")}`,
        mimetype,
        data: buffer.toString("base64")
      };
    }

    if (messageType === "audioMessage") {
      const mimetype =
        msg.message?.audioMessage?.mimetype || "audio/ogg; codecs=opus";
      return {
        filename: `audio-${Date.now()}.ogg`,
        mimetype,
        data: buffer.toString("base64")
      };
    }

    if (messageType === "documentMessage") {
      const docMsg = msg.message?.documentMessage;
      const mimetype = docMsg?.mimetype || "application/octet-stream";
      const ext = getExtension(mimetype, "bin");
      return {
        filename: docMsg?.title || `document-${Date.now()}.${ext}`,
        mimetype,
        data: buffer.toString("base64")
      };
    }

    if (messageType === "stickerMessage") {
      const mimetype = msg.message?.stickerMessage?.mimetype || "image/webp";
      return {
        filename: `sticker-${Date.now()}.webp`,
        mimetype,
        data: buffer.toString("base64")
      };
    }

    return {
      filename: "",
      mimetype: "",
      data: buffer.toString("base64")
    };
  } catch (err) {
    logger.error({
      info: "Error downloading media",
      err,
      messageId: msg.key.id
    });

    return undefined;
  }
};

const getMessageData = async (
  msg: WAMessage,
  wbot: Session
): Promise<{
  messagePayload: MessagePayload;
  contactPayload: ContactPayload;
  contextPayload: WhatsappContextPayload;
  mediaPayload: MediaPayload | undefined;
}> => {
  const remoteJid = msg.key.remoteJid || "";
  const isGroup = isJidGroup(remoteJid);

  let contactJid = remoteJid;
  let groupContact;

  if (!msg.key.fromMe && isGroup && msg.key.participant) {
    contactJid = msg.key.participant;
    groupContact = await convertToContactPayload(remoteJid, msg, wbot);
  }

  const contactPayload = await convertToContactPayload(contactJid, msg, wbot);
  const messagePayload = convertToMessagePayload(msg);
  const mediaPayload = await convertToMediaPayload(msg, wbot);

  const contextPayload: WhatsappContextPayload = {
    whatsappId: wbot.id,
    unreadMessages: 0,
    groupContact
  };

  return {
    messagePayload,
    contactPayload,
    contextPayload,
    mediaPayload
  };
};

const getWbot = (sessionId: number): Session => {
  const wbot = sessions.get(sessionId);

  if (!wbot) {
    throw new AppError("ERR_WAPP_NOT_INITIALIZED");
  }

  return wbot;
};

const removeSession = async (whatsappId: number): Promise<void> => {
  nextGeneration(whatsappId);
  // Alle Aufrufer setzen zurück (logout, Abmeldung, Löschen): weder vorgemerkte noch
  // Speicher-creds dürfen die Sitzung danach wiederbeleben ("Alte Sitzung verwerfen"
  // muss einen frischen QR liefern).
  discardPendingCreds(whatsappId);
  liveCreds.delete(whatsappId);

  const wbot = sessions.get(whatsappId);
  if (wbot) {
    wbot.ev.removeAllListeners("connection.update");
    wbot.ev.removeAllListeners("creds.update");
    wbot.ev.removeAllListeners("messages.upsert");
    wbot.ev.removeAllListeners("messages.update");
    wbot.ev.removeAllListeners("message-receipt.update");
    wbot.ev.removeAllListeners("presence.update");
    wbot.ev.removeAllListeners("groups.upsert");
    wbot.ev.removeAllListeners("groups.update");
    wbot.ev.removeAllListeners("group-participants.update");
    wbot.ev.removeAllListeners("contacts.upsert");
    wbot.ev.removeAllListeners("contacts.update");
    wbot.ev.removeAllListeners("chats.upsert");
    wbot.ev.removeAllListeners("chats.update");
    wbot.ev.removeAllListeners("chats.delete");
    wbot.ev.removeAllListeners("blocklist.set");
    wbot.ev.removeAllListeners("blocklist.update");

    try {
      wbot.end(undefined);
    } catch (e) {
      logger.debug({ info: "Error ending wbot", err: e });
    }

    try {
      wbot.ws?.removeAllListeners?.();
      await wbot.ws?.close?.();
    } catch (e) {
      logger.debug({ info: "Error closing websocket", err: e });
    }
  }

  sessions.delete(whatsappId);
  stores.delete(whatsappId);
};

const WA_VERSION_TIMEOUT_MS = 10_000;

const zustandMerken = (
  wbot: Session,
  update: Partial<ConnectionState>
): void => {
  let { zustand } = wbot;
  if (update.connection) zustand = update.connection;
  else if (update.qr) zustand = "qr";
  if (zustand !== wbot.zustand) {
    wbot.zustand = zustand;
    wbot.zustandSeit = Date.now();
  }
};

// Abmeldungen (durch WhatsApp oder bewusst) brauchen einen neuen QR; nach allen
// anderen Schließ-Codes verbindet der close-Handler neu.
const sollNeuVerbinden = (update: Partial<ConnectionState>): boolean => {
  const error = update.lastDisconnect?.error;
  const statusCode = (error as Boom)?.output?.statusCode;
  const errorMessage =
    (error as Boom)?.output?.payload?.message ||
    (error as Error)?.message ||
    "";
  return (
    errorMessage !== "Intentional Logout" &&
    statusCode !== DisconnectReason.loggedOut
  );
};

// DISCONNECTED nach einer Abmeldung bzw. einem logout (dann auch session "") in die DB
// bringen. Scheitert das Schreiben, wird NUR das Schreiben wiederholt – kein init: die
// creds sind ungültig, ein Neustart versuchte sonst alle paar Sekunden einen Login mit
// ihnen. generation = Stand direkt nach removeSession. false = gescheitert (Wiederholung
// läuft), true = geschrieben oder inzwischen hinfällig.
const disconnectedSpeichern = async (
  sessionId: number,
  generation: number | undefined,
  sessionLeeren = false,
  attempt = 0
): Promise<boolean> => {
  // Inzwischen ausdrücklich neu gestartet (QR-Knopf) → dessen Status nicht überschreiben.
  const giltNoch = () =>
    stoppedSessions.has(sessionId) &&
    sessionGeneration.get(sessionId) === generation;
  if (!giltNoch()) return true;
  try {
    // Vorgemerktes ist verworfen (removeSession); eine noch laufende creds-Speicherung soll
    // nicht danach landen (höchstens CREDS_KETTE_WARTEN_MS; landet sie später, verhindert
    // saveSessionCreds das Wiederbeleben).
    await flushPendingCredsSave(sessionId);
    // Das Warten kann lange dauern: erneut prüfen.
    if (!giltNoch()) return true;
    await Whatsapp.update(
      sessionLeeren
        ? { status: "DISCONNECTED", qrcode: "", session: "", retries: 0 }
        : { status: "DISCONNECTED", qrcode: "", retries: 0 },
      { where: { id: sessionId } }
    );
  } catch (err) {
    logger.error({
      info: "Could not persist DISCONNECTED, retrying",
      sessionId,
      attempt,
      err
    });
    setTimeout(() => {
      disconnectedSpeichern(sessionId, generation, sessionLeeren, attempt + 1);
    }, Math.min(5_000 * 2 ** attempt, 120_000));
    return false;
  }
  try {
    const updatedWhatsapp = await Whatsapp.findByPk(sessionId);
    if (updatedWhatsapp) {
      getIO().emit("whatsappSession", {
        action: "update",
        session: updatedWhatsapp
      });
    }
  } catch (err) {
    logger.warn({ info: "Could not emit DISCONNECTED", sessionId, err });
  }
  if (sessionLeeren) await clearSessionKeys(sessionId);
  return true;
};

// Socket offen, DB-Status hängt (z. B. CONNECTED-Schreiben gescheitert). Best effort;
// geändert wird nur, wenn noch der gelesene Status drinsteht (ein logout gewinnt).
const statusReparieren = async (
  w: Whatsapp,
  anlass: "watchdog" | "start"
): Promise<void> => {
  try {
    await flushPendingCredsSave(w.id);
    await Whatsapp.update(
      { status: "CONNECTED", qrcode: "", retries: 0 },
      { where: { id: w.id, status: w.status } }
    );
    const updatedWhatsapp = await Whatsapp.findByPk(w.id);
    if (updatedWhatsapp) {
      getIO().emit("whatsappSession", {
        action: "update",
        session: updatedWhatsapp
      });
    }
    logger.warn({
      info: "Repaired status of open session",
      anlass,
      sessionId: w.id,
      status: w.status
    });
  } catch (err) {
    logger.error({
      info: "Could not repair status of open session",
      anlass,
      sessionId: w.id,
      err
    });
  }
};

interface InitOptionen extends InitOptions {
  // Gesetzt bei automatischen Neustarts (close-Handler, scheduleRestart, Wächter) = die
  // Generation, deren Socket ersetzt wird. Hat sich seitdem nichts getan, laufen deren
  // creds aus dem Speicher weiter statt der evtl. veralteten DB-Kopie.
  credsVonGeneration?: number;
}

const init = async (
  whatsapp: Whatsapp,
  optionen: InitOptionen = {}
): Promise<void> => {
  const { credsVonGeneration, fortsetzen = false } = optionen;
  const sessionId = whatsapp.id;
  const automatisch = credsVonGeneration !== undefined;
  const stoppGrund = stoppedSessions.get(sessionId);
  const gestoppt = stoppGrund !== undefined;
  if (automatisch && gestoppt) {
    logger.info({ info: "Restart of stopped session skipped", sessionId });
    return;
  }
  // Reiner Start (POST /whatsappsession, auch von der CRM-Gesundheitsprüfung bei
  // hängendem DB-Status): eine offene, aktuelle Verbindung bleibt stehen, nur der
  // Status wird repariert. Ein Neustart nähme sonst die creds aus der DB – nach einem
  // gescheiterten Speichern ohne Kopplung bzw. mit zurückgesprungenen Pre-Key-Zählern.
  const sock = sessions.get(sessionId);
  if (
    fortsetzen &&
    !gestoppt &&
    sock?.zustand === "open" &&
    sock.generation === sessionGeneration.get(sessionId)
  ) {
    logger.info({ info: "Start skipped, session already open", sessionId });
    await statusReparieren(whatsapp, "start");
    return;
  }
  // Einen laufenden logout-Reset abwarten (der Stopp bleibt so lange stehen, Wächter und
  // Neustarts warten mit), statt mittendrin zu starten.
  const reset = gestoppt ? resetsLaufend.get(sessionId) : undefined;
  if (reset) await mitZeitlimit(reset, RESET_WARTEN_MS);
  // Nach einem logout ist die Kopplung verworfen (logout schreibt session ""): mit frischen
  // creds starten. Die Zeile des Aufrufers kann VOR dem Reset gelesen sein (Start-POST im
  // logout-Fenster: zweiter Tab, CRM-Gesundheitsprüfung), mit ihren creds lebte die eben
  // verworfene Kopplung wieder auf ("Alte Sitzung verwerfen" ohne frischen QR).
  const frischeCreds = stoppGrund === "logout" ? initAuthCreds() : undefined;
  // Ein ausdrücklicher Start (QR-Knopf, Serverstart) hebt einen Stopp auf.
  stoppedSessions.delete(sessionId);
  const io = getIO();
  // Fortsetzen ohne offenen Socket: die creds der laufenden Generation. Die DB-Kopie ist
  // nie neuer (alles Gespeicherte stammt von hier, removeSession verwirft die
  // Speicher-creds), kann aber älter oder leer sein. PUT (session "") und Starts nach einer
  // Abmeldung nehmen die DB-Kopie, nach einem logout frische creds; der Serverstart setzt
  // fortsetzen (ohne Speicherstand ebenfalls die DB-Kopie, nach einem Wächter-Neustart
  // dessen Stand).
  const vonGeneration =
    fortsetzen && !gestoppt
      ? sessionGeneration.get(sessionId)
      : credsVonGeneration;
  const speicher = liveCreds.get(sessionId);
  const credsAusSpeicher =
    vonGeneration !== undefined &&
    speicher &&
    speicher.generation === vonGeneration &&
    sessionGeneration.get(sessionId) === vonGeneration
      ? kopiereCreds(speicher.creds)
      : undefined;
  const generation = nextGeneration(sessionId);
  touch(sessionId);
  // Ein geplanter Neustart einer Vorgänger-Generation täte nichts mehr; er soll den
  // Wächter nicht blockieren.
  const geplant = pendingRestarts.get(sessionId);
  if (geplant) {
    clearTimeout(geplant.timer);
    pendingRestarts.delete(sessionId);
  }

  const { state } = await useSessionAuthState(
    whatsapp,
    credsAusSpeicher ?? frischeCreds
  );
  if (sessionGeneration.get(sessionId) === generation) {
    liveCreds.set(sessionId, { generation, creds: state.creds, whatsapp });
    if (credsAusSpeicher) {
      logger.info({ info: "Restarting with in-memory creds", sessionId });
      // Vorgemerktes der Vorgänger-Generation wird ab jetzt verworfen: den Stand gleich
      // (mit Wiederholung) sichern statt erst bei open. Sonst bliebe die DB-Kopie bis
      // dahin veraltet, und ein Start mit DB-creds (Serverstart) verlöre die Kopplung.
      // Nur session – verbunden ist erst nach open.
      pendingCredsSaves.set(sessionId, {
        whatsapp,
        creds: state.creds,
        generation,
        nurSession: true
      });
      flushPendingCredsSave(sessionId);
    }
  }
  const store = makeInMemoryStore({ logger: whaileyLogger });
  stores.set(sessionId, store);

  let waVersionToUse: WAVersion | undefined;

  if (process.env.WA_SOCKET_VERSION) {
    try {
      const parsed = JSON.parse(process.env.WA_SOCKET_VERSION) as number[];
      if (Array.isArray(parsed) && parsed.length >= 3) {
        waVersionToUse = parsed as WAVersion;
        logger.info({
          info: "Using WA_SOCKET_VERSION from env",
          version: waVersionToUse.join(".")
        });
      }
    } catch {
      logger.warn({
        info: "Failed to parse WA_SOCKET_VERSION, fetching latest"
      });
    }
  }

  if (!waVersionToUse) {
    // Mit Zeitlimit: axios ohne timeout liess den Reconnect beliebig lange hängen,
    // der geschlossene Socket blieb in der Map und die Oberfläche zeigte "verbunden".
    // fetchLatestWaWebVersion wirft nie: bei Fehlern liefert es die eingebaute Version
    // mit isLatest false und error.
    const fetchedVersionData = await mitZeitlimit(
      fetchLatestWaWebVersion({ timeout: WA_VERSION_TIMEOUT_MS }),
      WA_VERSION_TIMEOUT_MS + 2_000
    );
    if (fetchedVersionData?.isLatest) {
      waVersionToUse = fetchedVersionData.version;
      logger.info({
        info: "Using latest WA Web version",
        version: waVersionToUse.join(".")
      });
    } else {
      // Fehler/Zeitlimit: eingebaute Standardversion. version: undefined würde den
      // Default in makeWASocket überschreiben ({ ...DEFAULT_CONNECTION_CONFIG, ...config }).
      waVersionToUse = DEFAULT_CONNECTION_CONFIG.version;
      logger.warn({
        info: "Failed to fetch latest WA version, using default",
        version: waVersionToUse.join("."),
        grund: fetchedVersionData
          ? (fetchedVersionData.error as { message?: string } | undefined)
              ?.message
          : `Zeitlimit ${WA_VERSION_TIMEOUT_MS + 2_000} ms`
      });
    }
  }

  const connOptions: UserFacingSocketConfig = {
    logger: whaileyLogger,
    browser: Browsers.ubuntu(process.env.WHATSAPP_BROWSER_NAME || "Chrome"),
    emitOwnEvents: true,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(
        state.keys,
        whaileyLogger,
        new NodeCache({
          useClones: false,
          stdTTL: 60 * 60,
          checkperiod: 60 * 5
        })
      )
    },
    shouldSyncHistoryMessage: () => false,
    shouldIgnoreJid: jid => {
      if (typeof jid !== "string") return false;
      return (
        isJidBroadcast(jid) ||
        jid?.endsWith("newsletter") ||
        jid === "status@broadcast"
      );
    },
    syncFullHistory: false,
    version: waVersionToUse,
    msgRetryCounterMap,
    markOnlineOnConnect: false,
    fireInitQueries: true,
    generateHighQualityLinkPreview: true,
    linkPreviewImageThumbnailWidth: 192,
    defaultQueryTimeoutMs: 60_000,
    connectTimeoutMs: 25_000,
    retryRequestDelayMs: 500,
    transactionOpts: { maxCommitRetries: 10, delayBetweenTriesMs: 3000 },
    sentMessagesCache,
    getMessage: async (key: WAMessageKey) => {
      const cached = msgCache.get(key);
      if (cached) return cached;

      const msg = store.messages[key.remoteJid!]?.get(key.id!);
      if (msg?.message) return msg.message;

      return undefined;
    }
  };

  const proxyAddress = process.env.PROXY_ADDRESS || "";
  if (proxyAddress) {
    const proxyAuth = process.env.PROXY_AUTH || "";
    const proxyUrl = proxyAuth
      ? `http://${proxyAuth}@${proxyAddress}`
      : `http://${proxyAddress}`;

    connOptions.agent = new HttpsProxyAgent(proxyUrl);
    connOptions.fetchAgent = new HttpsProxyAgent(proxyUrl);
  }

  // Während der awaits oben kann ein neuerer Start oder ein Reset gelaufen sein.
  if (sessionGeneration.get(sessionId) !== generation) {
    logger.info({ info: "Superseded session init aborted", sessionId });
    return;
  }

  // Mit Speicher-creds: der ersetzte Socket lief während der awaits weiter (z. B. Login mit
  // Pre-Key-Upload; whaileys schreibt seine creds fort, sein creds.update wird ab
  // nextGeneration verworfen). Seinen neuesten Stand übernehmen statt der Kopie vom
  // Anfang – sonst sprängen die Pre-Key-Zähler zurück, und der nächste Upload
  // überschriebe Schlüssel gleicher IDs. Danach wird er sofort beendet.
  if (credsAusSpeicher && speicher) {
    const neuesterStand = JSON.stringify(speicher.creds, BufferJSON.replacer);
    if (neuesterStand !== JSON.stringify(state.creds, BufferJSON.replacer)) {
      Object.assign(state.creds, JSON.parse(neuesterStand, BufferJSON.reviver));
      pendingCredsSaves.set(sessionId, {
        whatsapp,
        creds: state.creds,
        generation,
        nurSession: true
      });
      flushPendingCredsSave(sessionId);
    }
  }

  assertUnique(sessionId);

  const wbot = makeWASocket(connOptions) as Session;
  wbot.id = sessionId;
  wbot.store = store;
  wbot.zustand = "connecting";
  wbot.zustandSeit = Date.now();
  wbot.generation = generation;

  store.bind(wbot.ev);

  sessions.set(sessionId, wbot);

  wbot.ev.on("creds.update", () => {
    touch(sessionId);
    // Nach open nur session (wie früher über die Instanz: CONNECTED stand dort schon):
    // eine verspätet landende Speicherung überschreibt so keinen neueren Status, etwa
    // den QR eines Neustarts. Vor open (Kopplung nach dem Scan) weiter mit CONNECTED.
    debouncedSaveCreds(
      whatsapp,
      state.creds,
      generation,
      wbot.zustand === "open"
    );
  });

  wbot.ev.on("messages.upsert", async ({ messages, type }) => {
    // Ein gesunder Socket sendet kaum connection.update/creds.update – Nachrichten
    // sind sein Lebenszeichen.
    touch(sessionId);
    messages.forEach(msg => {
      msgCache.save(msg);
      logger.debug({
        info: "[RAW] Message received",
        sessionId,
        type,
        key: msg.key,
        messageTimestamp: msg.messageTimestamp,
        pushName: msg.pushName,
        status: msg.status,
        messageType: Object.keys(msg.message || {}),
        rawMessage: JSON.stringify(msg, null, 2)
      });
    });

    const validMessages = messages.filter(msg => {
      if (!msg.message || !shouldHandleMessage(msg)) return false;

      if (type === "notify") return true;

      if (type === "append" && msg.key.fromMe) return true;

      return false;
    });

    if (validMessages.length === 0) return;

    await Promise.all(
      validMessages.map(async msg => {
        try {
          const {
            messagePayload,
            contactPayload,
            contextPayload,
            mediaPayload
          } = await getMessageData(msg, wbot);

          await handleMessage(
            messagePayload,
            contactPayload,
            contextPayload,
            mediaPayload
          );
        } catch (err) {
          logger.error(err, "Error handling message upsert");
        }
      })
    );
  });

  const handleConnectionUpdate = async (update: Partial<ConnectionState>): Promise<void> => {
    const { connection, lastDisconnect, qr } = update;

    if (connection === "close") {
      const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
      const errorMessage =
        (lastDisconnect?.error as Boom)?.output?.payload?.message ||
        (lastDisconnect?.error as Error)?.message ||
        "";

      // Abmeldung: Sitzung ZUERST aus dem Speicher nehmen und als gestoppt markieren.
      // Scheitert danach das DISCONNECTED-Schreiben, wird nur dieses wiederholt –
      // früher plante der Fehler-Fang einen Neustart mit den ungültigen creds.
      if (errorMessage === "Intentional Logout") {
        stoppedSessions.set(sessionId, "logout");
        await removeSession(sessionId);

        logger.info({ info: "Session intentionally logged out", sessionId });

        await disconnectedSpeichern(
          sessionId,
          sessionGeneration.get(sessionId)
        );
        await clearSessionKeys(sessionId);
        return;
      }

      if (statusCode === DisconnectReason.loggedOut) {
        stoppedSessions.set(sessionId, "abmeldung");
        await removeSession(sessionId);
        await disconnectedSpeichern(
          sessionId,
          sessionGeneration.get(sessionId)
        );
        return;
      }

      const shouldReconnect = statusCode !== DisconnectReason.loggedOut; // TODO handle other cases

      if (shouldReconnect) {
        try {
          await flushPendingCredsSave(sessionId);
          await whatsapp.update({ status: "OPENING" });
          io.emit("whatsappSession", {
            action: "update",
            session: whatsapp
          });
        } catch (err) {
          logger.error({ info: "Could not persist OPENING, reconnecting anyway", sessionId, err });
        }
        logger.info({
          info: "Connection closed, reconnecting...",
          sessionId,
          statusCode
        });

        await sleep(3000);

        // Überholt (Reset/Neustart in der Pause) → dieser Reconnect ist hinfällig.
        if (sessionGeneration.get(sessionId) !== generation) {
          logger.info({ info: "Stale reconnect skipped", sessionId });
          return;
        }
        // Frisch aus der DB statt des Closure-Objekts: dessen session-Feld kann
        // noch die inzwischen verworfenen Zugangsdaten enthalten.
        let fresh: Whatsapp | null;
        try {
          fresh = await Whatsapp.findByPk(sessionId);
        } catch (err) {
          logger.error({ info: "Could not load session for reconnect", sessionId, err });
          scheduleRestart(sessionId, generation);
          return;
        }
        if (!fresh || sessionGeneration.get(sessionId) !== generation) return;
        init(fresh, { credsVonGeneration: generation }).catch(err => {
          logger.error({ info: "Reconnect init failed", sessionId, err });
          scheduleRestart(sessionId, sessionGeneration.get(sessionId) ?? generation);
        });
      }
    }

    if (connection === "open") {
      // Mit Speicher-creds gestartet: den aktuellen Stand nachziehen (die Sicherung beim
      // Start kann noch in der Wiederholung hängen).
      if (credsAusSpeicher) {
        pendingCredsSaves.set(sessionId, {
          whatsapp,
          creds: state.creds,
          generation
        });
      }
      await flushPendingCredsSave(sessionId);

      await whatsapp.update({
        status: "CONNECTED",
        qrcode: "",
        retries: 0
      });

      const updatedWhatsapp = await Whatsapp.findByPk(sessionId);
      if (updatedWhatsapp) {
        io.emit("whatsappSession", {
          action: "update",
          session: updatedWhatsapp
        });
      }

      logger.info({ info: "Session connected", sessionId });
    }

    if (qr !== undefined) {
      // Status bei jedem QR mitschreiben: steht in der Instanz schon "qrcode", liesse
      // Sequelize ihn weg, und ein dazwischen per Whatsapp.update geschriebener anderer
      // Status (z. B. ein spätes DISCONNECTED) bliebe stehen, während ein QR läuft.
      whatsapp.changed("status", true);
      await whatsapp.update({
        qrcode: qr,
        status: "qrcode"
      });

      io.emit("whatsappSession", {
        action: "update",
        session: whatsapp
      });

      logger.info({ info: "QR Code generated", sessionId });
    }
  };

  wbot.ev.on("connection.update", async update => {
    touch(sessionId);
    zustandMerken(wbot, update);
    try {
      await handleConnectionUpdate(update);
    } catch (err) {
      // Z. B. Datenbank-Timeout beim Status-Schreiben: nie die Schleife sterben lassen –
      // aber nur nach Schließ-Codes neu starten, nach denen ohnehin neu verbunden wird.
      logger.error({ info: "connection.update failed", sessionId, err });
      if (update.connection === "close" && sollNeuVerbinden(update)) {
        scheduleRestart(sessionId, generation);
      }
    }
  });

  wbot.ev.on("messages.update", async updates => {
    await Promise.all(
      updates.map(async event => {
        try {
          if (!event.update.status || !event.key.id) return;

          const ack = (event.update.status as MessageAck) || 0;
          await handleMessageAck(event.key.id, ack);
        } catch (err) {
          logger.error({
            info: "Error handling message update",
            err,
            messageId: event.key.id
          });
        }
      })
    );
  });

  wbot.ev.on("message-receipt.update", async updates => {
    await Promise.all(
      updates.map(async ({ key, receipt }) => {
        try {
          if (!key.id) return;

          let ack: MessageAck = 2;
          if (receipt.playedTimestamp) {
            ack = 4;
          } else if (receipt.readTimestamp) {
            ack = 3;
          } else if (receipt.receiptTimestamp) {
            ack = 2;
          }

          await handleMessageAck(key.id, ack);

          logger.debug({
            info: "Message receipt update processed",
            messageId: key.id,
            ack,
            sessionId
          });
        } catch (err) {
          logger.error({
            info: "Error processing message receipt",
            err,
            messageId: key.id
          });
        }
      })
    );
  });
};

// Wächter: startet Sitzungen neu, deren Schleife still gestorben ist (kein Socket mehr,
// Socket seit Minuten geschlossen, oder in OPENING/QR seit Minuten kein Ereignis). Ein
// offener Socket wird nie neu gestartet, nur sein DB-Status repariert. DISCONNECTED und
// bewusst gestoppte Sitzungen bleiben unberührt — dort braucht es einen neuen QR-Scan
// bzw. einen bewussten Start.
const WATCHDOG_INTERVAL_MS = 60_000;
const STILL_NACH_MS = 3 * 60_000;
const RESTART_SPERRE_MS = 5 * 60_000;
let watchdogTimer: NodeJS.Timeout | null = null;

type Befund = "ok" | "reparieren" | "neustart";

const sitzungPruefen = (w: Whatsapp, now: number): Befund => {
  // Ein geplanter Neustart zählt nur für die aktuelle Generation (der einer überholten
  // täte beim Auslösen nichts).
  const geplant = pendingRestarts.get(w.id);
  if (
    stoppedSessions.has(w.id) ||
    (geplant && geplant.generation === sessionGeneration.get(w.id))
  ) {
    return "ok";
  }
  const status = String(w.status || "").toUpperCase();
  const sock = sessions.get(w.id);
  // Offener Socket = Verbindung lebt, auch wenn lange kein Ereignis kam: nie neu
  // starten, höchstens den hängengebliebenen DB-Status reparieren (nicht, solange
  // ein neuerer init läuft – der ersetzt diesen Socket gleich).
  if (sock?.zustand === "open") {
    const aktuell = sock.generation === sessionGeneration.get(w.id);
    return status === "CONNECTED" || !aktuell ? "ok" : "reparieren";
  }
  if (status === "DISCONNECTED" || !status) return "ok";
  if (!sock) return "neustart";
  // Seit Minuten geschlossen und kein neuer Socket: die Reconnect-Kette hängt
  // (unabhängig davon, was in der DB steht).
  if (
    sock.zustand === "close" &&
    now - (sock.zustandSeit ?? 0) > STILL_NACH_MS
  ) {
    return "neustart";
  }
  const silentFor = now - (lastActivity.get(w.id) ?? 0);
  return status !== "CONNECTED" && silentFor > STILL_NACH_MS
    ? "neustart"
    : "ok";
};

const watchdogTick = async (): Promise<void> => {
  // Wer seit Tick-Beginn neu gestartet oder zurückgesetzt wurde, bleibt unberührt.
  const generationAmStart = new Map(sessionGeneration);
  let all: Whatsapp[];
  try {
    all = await Whatsapp.findAll();
  } catch (err) {
    logger.error({ info: "Watchdog could not read sessions", err });
    return;
  }
  for (const w of all) {
    if (sitzungPruefen(w, Date.now()) === "ok") continue;
    // Der Stand von findAll() veraltet, während hier nacheinander neu gestartet wird
    // (und logout() nimmt die Sitzung VOR dem DISCONNECTED-Schreiben aus dem Speicher):
    // Zeile frisch lesen und direkt vor dem Neustart alles erneut prüfen.
    let fresh: Whatsapp | null;
    try {
      fresh = await Whatsapp.findByPk(w.id);
    } catch (err) {
      logger.error({
        info: "Watchdog could not read session",
        sessionId: w.id,
        err
      });
      continue;
    }
    if (!fresh) continue;
    const generation = sessionGeneration.get(w.id);
    if (generation !== generationAmStart.get(w.id)) continue;
    const now = Date.now();
    const befund = sitzungPruefen(fresh, now);
    if (befund === "reparieren") {
      await statusReparieren(fresh, "watchdog");
      continue;
    }
    if (befund !== "neustart") continue;
    if (now - (lastRestart.get(w.id) ?? 0) < RESTART_SPERRE_MS) continue;
    lastRestart.set(w.id, now);
    const sock = sessions.get(w.id);
    logger.warn({
      info: "Watchdog restarts stalled session",
      sessionId: w.id,
      status: fresh.status,
      hasSocket: !!sock,
      zustand: sock?.zustand,
      silentFor: now - (lastActivity.get(w.id) ?? 0)
    });
    try {
      await init(fresh, { credsVonGeneration: generation ?? 0 });
    } catch (err) {
      logger.error({ info: "Watchdog restart failed", sessionId: w.id, err });
      scheduleRestart(w.id, sessionGeneration.get(w.id) ?? 0);
    }
  }
};

const startWatchdog = (): void => {
  if (watchdogTimer) return;
  watchdogTimer = setInterval(() => {
    watchdogTick().catch(err => logger.error({ info: "Watchdog tick failed", err }));
  }, WATCHDOG_INTERVAL_MS);
};

const logout = async (sessionId: number): Promise<void> => {
  // Bewusst gestoppt, und zwar VOR removeSession: sonst startet ein Wächter-Tick oder
  // geplanter Neustart die Sitzung zwischen removeSession und dem DISCONNECTED-Schreiben
  // mit den alten creds neu und überholt das frische init danach. Ausdrückliche Starts
  // warten auf diesen Reset (init).
  stoppedSessions.set(sessionId, "logout");
  let resetFertig: () => void = () => undefined;
  const reset = new Promise<void>(resolve => {
    resetFertig = resolve;
  });
  resetsLaufend.set(sessionId, reset);

  try {
    // Vorgemerktes verwerfen statt schreiben: eine volle Speicherung setzte status
    // CONNECTED, und scheiterte der Reset danach, stünde die tote Sitzung dauerhaft als
    // CONNECTED da (Wächter und CRM ignorieren sie dann).
    discardPendingCreds(sessionId);

    const wbot = sessions.get(sessionId);

    if (wbot) {
      await wbot
        .logout()
        .catch(err =>
          logger.error({ info: "Error on logout", sessionId, err })
        );
    }

    await removeSession(sessionId);
    // DISCONNECTED + session "" (wartet auf eine laufende creds-Speicherung, scheitert es,
    // wird es im Hintergrund wiederholt).
    let generation = sessionGeneration.get(sessionId);
    let geschrieben = await disconnectedSpeichern(sessionId, generation, true);
    // Trotzdem dazwischengerutschter Start (init wartet höchstens RESET_WARTEN_MS): der
    // Reset gilt, session "" bleibt maßgeblich.
    if (geschrieben && sessionGeneration.get(sessionId) !== generation) {
      stoppedSessions.set(sessionId, "logout");
      await removeSession(sessionId);
      generation = sessionGeneration.get(sessionId);
      geschrieben = await disconnectedSpeichern(sessionId, generation, true);
    }
    if (!geschrieben) {
      throw new Error("Logout not persisted yet, retrying in background");
    }

    logger.info({ info: "Session logged out", sessionId });
  } finally {
    resetFertig();
    if (resetsLaufend.get(sessionId) === reset) resetsLaufend.delete(sessionId);
  }
};

const sendMessage = async (
  sessionId: number,
  to: string,
  body: string,
  options?: SendMessageOptions
): Promise<ProviderMessage> => {
  const wbot = getWbot(sessionId);
  const toJid = normalizeJid(to);

  const messageContent: AnyMessageContent = options?.quotedMessageId
    ? {
        text: body,
        contextInfo: {
          stanzaId: options.quotedMessageId,
          participant: options.quotedMessageFromMe ? wbot.user?.id : toJid
        }
      }
    : { text: body };

  const sentMsg = await wbot.sendMessage(toJid, messageContent);

  if (!sentMsg?.key.id) {
    throw new AppError("ERR_SENDING_WAPP_MSG");
  }

  logger.debug({
    info: "[RAW] Message sent",
    sessionId,
    to: toJid,
    key: sentMsg.key,
    messageTimestamp: sentMsg.messageTimestamp,
    status: sentMsg.status,
    rawMessage: JSON.stringify(sentMsg, null, 2)
  });

  msgCache.save(sentMsg);

  return {
    id: sentMsg.key.id,
    body,
    fromMe: true,
    hasMedia: false,
    type: "chat",
    timestamp: sentMsg.messageTimestamp
      ? Number(sentMsg.messageTimestamp)
      : Date.now(),
    from: wbot.user?.id || "",
    to,
    ack: 1
  };
};

const sendMedia = async (
  sessionId: number,
  to: string,
  media: ProviderMediaInput,
  options?: SendMediaOptions
): Promise<ProviderMessage> => {
  const wbot = getWbot(sessionId);
  const toJid = normalizeJid(to);

  const mediaBuffer = media.path ? readFileSync(media.path) : media.data;
  if (!mediaBuffer) throw new AppError("ERR_NO_MEDIA_DATA");

  const contextInfo = options?.quotedMessageId
    ? { stanzaId: options.quotedMessageId, participant: toJid }
    : undefined;

  const buildPayload = () => {
    const base = {
      caption: options?.caption,
      mimetype: media.mimetype,
      contextInfo
    };

    if (media.mimetype.startsWith("image/")) {
      return {
        message: { image: mediaBuffer, ...base },
        type: "image" as MessageType
      };
    }

    if (media.mimetype.startsWith("video/")) {
      return {
        message: { video: mediaBuffer, ...base },
        type: "video" as MessageType
      };
    }

    if (media.mimetype.startsWith("audio/")) {
      const ptt = Boolean(options?.sendAudioAsVoice);
      return {
        message: {
          audio: mediaBuffer,
          mimetype: media.mimetype,
          ptt,
          contextInfo
        },
        type: ptt ? "ptt" : ("audio" as MessageType)
      };
    }

    return {
      message: {
        document: mediaBuffer,
        caption: options?.caption,
        mimetype: media.mimetype,
        fileName: media.filename,
        contextInfo
      },
      type: "document" as MessageType
    };
  };

  const { message, type } = buildPayload();

  const sent = await wbot.sendMessage(toJid, message);
  if (!sent?.key?.id) throw new AppError("ERR_SENDING_WAPP_MEDIA_MSG");

  logger.debug({
    info: "[RAW] Media sent",
    sessionId,
    to: toJid,
    mediaType: type,
    mimetype: media.mimetype,
    filename: media.filename,
    key: sent.key,
    messageTimestamp: sent.messageTimestamp,
    status: sent.status,
    rawMessage: JSON.stringify(sent, null, 2)
  });

  msgCache.save(sent);

  return {
    id: sent.key.id,
    body: options?.caption || media.filename,
    fromMe: true,
    hasMedia: true,
    type,
    timestamp: sent.messageTimestamp
      ? Number(sent.messageTimestamp)
      : Date.now(),
    from: wbot.user?.id || "",
    to,
    ack: 1
  };
};

const deleteMessage = async (
  sessionId: number,
  chatId: string,
  messageId: string,
  fromMe: boolean
): Promise<void> => {
  const wbot = getWbot(sessionId);

  const normalizedChatId = normalizeJid(chatId);

  const key = {
    remoteJid: normalizedChatId,
    id: messageId,
    fromMe
  };

  await wbot.sendMessage(normalizedChatId, { delete: key });
};

const checkNumber = async (
  sessionId: number,
  number: string
): Promise<string> => {
  const wbot = getWbot(sessionId);

  const cleanNumber = number.replace(/\D/g, "");

  const [result] = await wbot.onWhatsApp(cleanNumber);

  if (!result?.exists) {
    throw new AppError("ERR_NUMBER_NOT_ON_WHATSAPP", 404);
  }

  return result.jid;
};

const getProfilePicUrl = async (
  sessionId: number,
  number: string
): Promise<string> => {
  const wbot = getWbot(sessionId);

  const jid = number.includes("@") ? number : `${number}@s.whatsapp.net`;

  try {
    const url = await wbot.profilePictureUrl(jid, "image");
    return url || "";
  } catch (err) {
    logger.debug({
      info: "Could not get profile picture",
      number,
      err
    });
    return "";
  }
};

const getContacts = async (sessionId: number): Promise<ProviderContact[]> => {
  const wbot = getWbot(sessionId);

  const contacts: ProviderContact[] = [];

  if (wbot.store?.contacts) {
    Object.values(wbot.store.contacts).forEach(contact => {
      if (contact.id && isJidUser(contact.id)) {
        contacts.push({
          id: contact.id,
          number: jidNormalizedUser(contact.id).replace("@s.whatsapp.net", ""),
          name: contact.name || contact.notify || "",
          pushname: contact.notify || "",
          isGroup: false
        });
      }
    });
  }

  return contacts;
};

const sendSeen = async (sessionId: number, chatId: string): Promise<void> => {
  const wbot = getWbot(sessionId);

  const normalizedChatId = normalizeJid(chatId);

  const lastMessages =
    wbot.store?.messages?.[normalizedChatId]?.array?.slice(-5) || [];

  if (lastMessages.length === 0) {
    return;
  }

  const keys = lastMessages
    .filter(msg => !msg.key.fromMe && msg.key.id)
    .map(msg => ({
      remoteJid: normalizedChatId,
      id: msg.key.id!,
      participant: msg.key.participant
    }));

  if (keys.length > 0) {
    await wbot.readMessages(keys);
  }
};

const fetchChatMessages = async (
  sessionId: number,
  chatId: string,
  limit = 100
): Promise<ProviderMessage[]> => {
  const wbot = getWbot(sessionId);

  const normalizedChatId = normalizeJid(chatId);

  const messagesFromStore =
    wbot.store?.messages?.[normalizedChatId]?.array || [];

  const messages = messagesFromStore.slice(-limit);

  return messages.map(msg => ({
    id: msg.key.id || "",
    body: getMessageBody(msg),
    fromMe: msg.key.fromMe || false,
    hasMedia: hasMedia(msg),
    type: mapMessageType(msg),
    timestamp: msg.messageTimestamp ? Number(msg.messageTimestamp) : Date.now(),
    from: msg.key.participant || msg.key.remoteJid || "",
    to: normalizedChatId,
    ack: mapMessageAck(msg.status)
  }));
};

export const WhaileysProvider: WhatsappProvider = {
  init,
  startWatchdog,
  removeSession,
  logout,
  sendMessage,
  sendMedia,
  deleteMessage,
  checkNumber,
  getProfilePicUrl,
  getContacts,
  sendSeen,
  fetchChatMessages
};
