import AppError from "../../errors/AppError";
import Message from "../../models/Message";
import Ticket from "../../models/Ticket";
import { whatsappProvider, ProviderMessage } from "../../providers/WhatsApp";

import formatBody from "../../helpers/Mustache";
import GetWaChatId from "../../helpers/GetWaChatId";

interface Request {
  body: string;
  ticket: Ticket;
  quotedMsg?: Message;
}

const SendWhatsAppMessage = async ({
  body,
  ticket,
  quotedMsg
}: Request): Promise<ProviderMessage> => {
  if (!ticket.whatsappId) {
    throw new AppError("ERR_TICKET_NO_WHATSAPP");
  }

  const chatId = GetWaChatId(ticket.contact, ticket.isGroup);

  try {
    const sentMessage = await whatsappProvider.sendMessage(
      ticket.whatsappId,
      chatId,
      formatBody(body, ticket.contact),
      {
        quotedMessageId: quotedMsg?.id,
        quotedMessageFromMe: quotedMsg?.fromMe,
        linkPreview: false
      }
    );

    // Gleicher Text → ohne changed() kein UPDATE, updatedAt bliebe stehen (CRM-Poller).
    ticket.changed("lastMessage", true);
    await ticket.update({ lastMessage: body });
    return sentMessage;
  } catch (err) {
    throw new AppError("ERR_SENDING_WAPP_MSG");
  }
};

export default SendWhatsAppMessage;
