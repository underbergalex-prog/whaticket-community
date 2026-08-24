import AppError from "../errors/AppError";
import Contact from "../models/Contact";

/**
 * Liefert die WhatsApp-Chat-ID fuer einen Kontakt.
 *
 * Normalfall: echte Nummer -> "<nummer>@c.us" (bzw. "@g.us" fuer Gruppen).
 * LID-only-Kontakte (WhatsApp liefert keine echte Nummer mit, nur die
 * anonyme LID-Adresse): Fallback auf die gespeicherte LID ("<id>@lid"),
 * damit Antworten an solche Kontakte nicht ins Leere laufen.
 */
const GetWaChatId = (contact: Contact, isGroup: boolean): string => {
  if (isGroup) {
    return `${contact.number}@g.us`;
  }

  if (contact.number) {
    return `${contact.number}@c.us`;
  }

  if (contact.lid) {
    return contact.lid.includes("@") ? contact.lid : `${contact.lid}@lid`;
  }

  throw new AppError("ERR_CONTACT_HAS_NO_NUMBER_OR_LID");
};

export default GetWaChatId;
