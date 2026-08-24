import { getIO } from "../../libs/socket";
import Contact from "../../models/Contact";
import Ticket from "../../models/Ticket";
import Message from "../../models/Message";
import { logger } from "../../utils/logger";

interface ExtraInfo {
  name: string;
  value: string;
}

interface Request {
  name: string;
  number: string;
  lid?: string;
  isGroup: boolean;
  email?: string;
  profilePicUrl?: string;
  extraInfo?: ExtraInfo[];
}

const emitContact = (action: "update" | "create", contact: Contact) => {
  const io = getIO();

  io.emit("contact", { action, contact });
};

const CreateOrUpdateContactService = async ({
  name,
  number: rawNumber,
  lid,
  profilePicUrl,
  isGroup,
  email = "",
  extraInfo = []
}: Request): Promise<Contact> => {
  const number = isGroup ? rawNumber : rawNumber.replace(/[^0-9]/g, "");
  if (!number && !lid) throw new Error("Either number or lid must be provided");

  const [contactByNumber, contactByLid] = await Promise.all([
    number ? Contact.findOne({ where: { number } }) : null,
    lid ? Contact.findOne({ where: { lid } }) : null
  ]);

  const shouldMerge =
    contactByNumber && contactByLid && contactByNumber.id !== contactByLid.id;

  if (shouldMerge) {
    await Ticket.update(
      { contactId: contactByNumber.id },
      { where: { contactId: contactByLid.id } }
    );

    // Messages.contactId hat ON DELETE CASCADE – ohne Umhaengen wuerden beim
    // destroy() alle Nachrichten des Doubletten-Kontakts geloescht.
    await Message.update(
      { contactId: contactByNumber.id },
      { where: { contactId: contactByLid.id } }
    );

    await contactByLid.destroy();

    await contactByNumber.update({
      lid: contactByLid.lid,
      profilePicUrl
    });

    logger.info({
      info: "Merged contacts by number and lid",
      primaryContactId: contactByNumber.id,
      mergedContactId: contactByLid.id
    });

    emitContact("update", contactByNumber);

    return contactByNumber;
  }

  if (contactByNumber) {
    await contactByNumber.update({
      lid: lid || contactByNumber.lid,
      profilePicUrl
    });

    emitContact("update", contactByNumber);

    return contactByNumber;
  }

  if (contactByLid) {
    // NIE eine bereits bekannte echte Nummer ueberschreiben: eine spaeter
    // eintreffende LID-only-Nachricht darf die Nummer nicht "wegklobbern"
    // (das machte aus echten Kontakten Pseudo-Nummern-Kontakte und
    // provozierte anschliessend Duplikate).
    await contactByLid.update({
      number: contactByLid.number || number || null,
      profilePicUrl
    });

    emitContact("update", contactByLid);
    return contactByLid;
  }

  const created = await Contact.create({
    name,
    // "" wuerde bei mehreren LID-only-Kontakten den Unique-Index verletzen
    number: number || null,
    lid,
    profilePicUrl,
    email,
    isGroup,
    extraInfo
  });

  emitContact("create", created);
  return created;
};

export default CreateOrUpdateContactService;
