import ListWhatsAppsService from "../WhatsappService/ListWhatsAppsService";
import { StartWhatsAppSession } from "./StartWhatsAppSession";

export const StartAllWhatsAppsSessions = async (): Promise<void> => {
  const whatsapps = await ListWhatsAppsService();
  if (whatsapps.length > 0) {
    whatsapps.forEach(whatsapp => {
      // fortsetzen: ohne Speicherstand wie bisher die DB-Kopie. Hing das OPENING-Schreiben
      // so lange, dass der Wächter die Sitzung schon gestartet hat, wird dessen Verbindung
      // fortgesetzt statt mit dem veralteten Listen-Stand ersetzt (Pre-Key-Zähler).
      StartWhatsAppSession(whatsapp, { fortsetzen: true });
    });
  }
};
