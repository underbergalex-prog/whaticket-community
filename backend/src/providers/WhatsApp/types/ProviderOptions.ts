export interface SendMessageOptions {
  quotedMessageId?: string;
  quotedMessageFromMe?: boolean;
  linkPreview?: boolean;
}

export interface SendMediaOptions {
  caption?: string;
  sendAudioAsVoice?: boolean;
  sendMediaAsDocument?: boolean;
  quotedMessageId?: string;
}

export interface InitOptions {
  // Reiner Start (POST /whatsappsession, Serverstart): eine laufende Sitzung fortsetzen
  // statt sie mit den creds aus der DB neu aufzubauen. Reset-Pfade setzen es nicht.
  fortsetzen?: boolean;
}
