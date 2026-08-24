import fs from "fs";
import AppError from "../../errors/AppError";
import Ticket from "../../models/Ticket";
import { whatsappProvider, ProviderMessage } from "../../providers/WhatsApp";

import formatBody from "../../helpers/Mustache";
import GetWaChatId from "../../helpers/GetWaChatId";

// multer 1.4.2 (busboy 0.x) dekodiert originalname als latin1 – UTF-8-Namen
// mit Umlauten kommen sonst als Mojibake an ("KÃ¼ndigung.pdf" statt
// "Kündigung.pdf"). Fuer reine ASCII-Namen ist die Konvertierung ein No-op.
export const fixLatin1Utf8 = (name: string): string => {
  if (!name) return name;
  try {
    const converted = Buffer.from(name, "latin1").toString("utf8");
    return converted.includes("�") ? name : converted;
  } catch {
    return name;
  }
};

interface Request {
  media: Express.Multer.File;
  ticket: Ticket;
  body?: string;
}

const SendWhatsAppMedia = async ({
  media,
  ticket,
  body
}: Request): Promise<ProviderMessage> => {
  try {
    if (!ticket.whatsappId) {
      throw new AppError("ERR_TICKET_NO_WHATSAPP");
    }

    const chatId = GetWaChatId(ticket.contact, ticket.isGroup);

    const hasBody = body
      ? formatBody(body as string, ticket.contact)
      : undefined;

    // Empfänger soll den ECHTEN Dateinamen sehen (z. B. CV_Max_Mustermann.pdf),
    // nicht den von multer generierten Zeitstempel-Namen. Fallback auf filename,
    // falls originalname ausnahmsweise fehlt.
    const displayName = fixLatin1Utf8(media.originalname) || media.filename;

    const mediaInput = {
      filename: displayName,
      mimetype: media.mimetype,
      path: media.path
    };

    const mediaOptions = {
      caption: hasBody,
      sendAudioAsVoice: true,
      sendMediaAsDocument:
        media.mimetype.startsWith("image/") &&
        !/^.*\.(jpe?g|png|gif)?$/i.exec(media.filename)
    };

    const sentMessage = await whatsappProvider.sendMedia(
      ticket.whatsappId,
      chatId,
      mediaInput,
      mediaOptions
    );

    await ticket.update({ lastMessage: body || displayName });

    fs.unlinkSync(media.path);

    return sentMessage;
  } catch (err) {
    console.log(err);
    throw new AppError("ERR_SENDING_WAPP_MSG");
  }
};

export default SendWhatsAppMedia;
