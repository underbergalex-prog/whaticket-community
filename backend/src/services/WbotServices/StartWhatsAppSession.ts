import Whatsapp from "../../models/Whatsapp";
import { whatsappProvider, InitOptions } from "../../providers/WhatsApp";
import { getIO } from "../../libs/socket";
import { logger } from "../../utils/logger";

export const StartWhatsAppSession = async (
  whatsapp: Whatsapp,
  options?: InitOptions
): Promise<void> => {
  await whatsapp.update({ status: "OPENING" });

  const io = getIO();
  io.emit("whatsappSession", {
    action: "update",
    session: whatsapp
  });

  try {
    console.log("VAI!");
    await whatsappProvider.init(whatsapp, options);
  } catch (err) {
    logger.error(err);
  }
};
