import { Request, Response } from "express";
import { whatsappProvider } from "../providers/WhatsApp";
import ShowWhatsAppService from "../services/WhatsappService/ShowWhatsAppService";
import { StartWhatsAppSession } from "../services/WbotServices/StartWhatsAppSession";
import UpdateWhatsAppService from "../services/WhatsappService/UpdateWhatsAppService";

const store = async (req: Request, res: Response): Promise<Response> => {
  const { whatsappId } = req.params;
  const whatsapp = await ShowWhatsAppService(whatsappId);

  // Reiner Start: eine laufende Sitzung wird fortgesetzt, nicht neu gekoppelt
  // (frischer QR über update bzw. remove + store).
  StartWhatsAppSession(whatsapp, { fortsetzen: true });

  return res.status(200).json({ message: "Starting session." });
};

const update = async (req: Request, res: Response): Promise<Response> => {
  const { whatsappId } = req.params;

  const { whatsapp } = await UpdateWhatsAppService({
    whatsappId,
    whatsappData: { session: "" }
  });

  StartWhatsAppSession(whatsapp);

  return res.status(200).json({ message: "Starting session." });
};

const remove = async (req: Request, res: Response): Promise<Response> => {
  const { whatsappId } = req.params;
  const whatsapp = await ShowWhatsAppService(whatsappId);

  await whatsappProvider.logout(whatsapp.id);

  return res.status(200).json({ message: "Session disconnected." });
};

export default { store, remove, update };
