import Whatsapp from "../../models/Whatsapp";
import {
  ProviderMessage,
  ProviderMediaInput,
  ProviderContact,
  SendMessageOptions,
  SendMediaOptions,
  InitOptions
} from "./types";
import { WhatsappWebJsProvider } from "./Implementations/wwebjs";
import { WhaileysProvider } from "./Implementations/whaileys";

export interface WhatsappProvider {
  init(whatsapp: Whatsapp, options?: InitOptions): Promise<void>;
  startWatchdog?(): void;
  removeSession(whatsappId: number): void;
  logout(sessionId: number): Promise<void>;
  sendMessage(
    sessionId: number,
    to: string,
    body: string,
    options?: SendMessageOptions
  ): Promise<ProviderMessage>;
  sendMedia(
    sessionId: number,
    to: string,
    media: ProviderMediaInput,
    options?: SendMediaOptions
  ): Promise<ProviderMessage>;
  deleteMessage(
    sessionId: number,
    chatId: string,
    messageId: string,
    fromMe: boolean
  ): Promise<void>;
  checkNumber(sessionId: number, number: string): Promise<string>;
  getProfilePicUrl(sessionId: number, number: string): Promise<string>;
  getContacts(sessionId: number): Promise<ProviderContact[]>;
  sendSeen(sessionId: number, chatId: string): Promise<void>;
  fetchChatMessages(
    sessionId: number,
    chatId: string,
    limit: number
  ): Promise<ProviderMessage[]>;
}

const provider = process.env.WHATSAPP_PROVIDER || "wwebjs";

const providersMap: Record<string, WhatsappProvider> = {
  wwebjs: WhatsappWebJsProvider,
  whaileys: WhaileysProvider
};

const whatsappProvider = providersMap[provider];

export { whatsappProvider };
