/** Channel-neutral message and capability model shared by client and server. */

export const CHANNEL_IDS = [
  "feishu",
  "weixin",
  "dingtalk",
  "wecom",
  "wecom-app",
  "qq",
  "slack",
  "telegram",
  "discord",
  "whatsapp",
  "imessage",
  "matrix",
] as const;

export type ChannelId = (typeof CHANNEL_IDS)[number];

export type ProgressMode = "edit" | "stream" | "typing" | "none";

export interface NormalizedMedia {
  kind: "image" | "file";
  path: string;
  mime?: string;
  name?: string;
}

export interface NormalizedMessage {
  channel: ChannelId;
  connectionId: string;
  kind: "dm" | "group";
  chatId: string;
  threadId?: string;
  senderId: string;
  senderName?: string;
  messageId: string;
  timestamp: number;
  text?: string;
  media?: NormalizedMedia[];
  replyTo?: { id?: string; text?: string; sender?: string };
  trigger: { mention: boolean; replyToBot: boolean; command: boolean };
}

export interface GroupMembershipEvent {
  chatId: string;
  title?: string;
  added: boolean;
}

export interface ChannelCapabilities {
  progress: ProgressMode;
  markdown: "native" | "html" | "custom" | "plain";
  uploadImage: boolean;
  uploadFile: boolean;
  reactions: boolean;
  typing: boolean;
  maxChunkChars: number;
  threads: boolean;
}

export interface OutboundText {
  chatId: string;
  threadId?: string;
  text: string;
  replyToId?: string;
}

export interface OutboundMedia {
  chatId: string;
  threadId?: string;
  kind: "image" | "file";
  path: string;
  mime?: string;
  name?: string;
  caption?: string;
  replyToId?: string;
}

export type SendResult = { messageId?: string; uncertain?: boolean };

export function conversationKey(
  message: Pick<NormalizedMessage, "kind" | "chatId" | "threadId">,
): string {
  return `${message.kind}:${message.chatId}${message.threadId ? `:${message.threadId}` : ""}`;
}
