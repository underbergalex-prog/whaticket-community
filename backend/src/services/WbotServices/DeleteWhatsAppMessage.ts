import AppError from "../../errors/AppError";
import Message from "../../models/Message";
import Ticket from "../../models/Ticket";
import { whatsappProvider } from "../../providers/WhatsApp";
import GetWaChatId from "../../helpers/GetWaChatId";

const DeleteWhatsAppMessage = async (messageId: string): Promise<Message> => {
  const message = await Message.findByPk(messageId, {
    include: [
      {
        model: Ticket,
        as: "ticket",
        include: ["contact"]
      }
    ]
  });

  if (!message) {
    throw new AppError("No message found with this ID.");
  }

  const { ticket } = message;

  const chatId = GetWaChatId(ticket.contact, ticket.isGroup);

  await whatsappProvider.deleteMessage(
    ticket.whatsappId,
    chatId,
    message.id,
    message.fromMe
  );

  await message.update({ isDeleted: true });

  return message;
};

export default DeleteWhatsAppMessage;
