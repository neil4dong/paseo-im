/** The frozen channel adapter contract. Each channel implements one definition;
 * the bridge and manager only ever see these interfaces. */
import type { Connection } from "../../shared/config.ts";
import type {
  ChannelCapabilities,
  ChannelId,
  GroupMembershipEvent,
  NormalizedMessage,
  OutboundMedia,
  OutboundText,
  SendResult,
} from "../../shared/model.ts";
import type { ImSettings } from "../../shared/settings.ts";

export interface AdapterContext<C extends Connection = Connection> {
  connectionId: string;
  config: C;
  secrets: Record<string, string>;
  dataDir: string;
  mediaDir: string;
  signal: AbortSignal;
  settings: () => Promise<ImSettings>;
  log: (level: "info" | "warn" | "error", message: string) => void;
  onMessage: (message: NormalizedMessage) => void;
  onGroups: (events: GroupMembershipEvent[]) => void;
  onError: (error: Error) => void;
  /** Persist server-derived identity fields (e.g. botUserId/username) back to
   * the connection config after a successful handshake. */
  updateConfig?: (fields: Partial<Connection>) => Promise<void>;
}

export interface ChannelAdapter {
  readonly channel: ChannelId;
  readonly capabilities: ChannelCapabilities;
  start(): Promise<void>;
  stop(): Promise<void>;
  sendText(outbound: OutboundText): Promise<SendResult>;
  sendMedia?(outbound: OutboundMedia): Promise<SendResult>;
  editMessage?(messageId: string, chatId: string, text: string): Promise<void>;
  streamStart?(chatId: string, threadId?: string): Promise<{ streamId: string }>;
  streamAppend?(streamId: string, text: string): Promise<void>;
  streamFinish?(streamId: string, text: string): Promise<{ messageId?: string }>;
  sendTyping?(chatId: string): Promise<void>;
  /** Best-effort, channel-native acknowledgement for an accepted inbound
   * message. The adapter chooses reaction, read state, typing, or another
   * platform-specific mechanism. */
  acknowledgeMessage?(messageId: string, chatId: string): Promise<void>;
  addReaction?(messageId: string, chatId: string, emoji: string): Promise<void>;
  leaveChat?(chatId: string): Promise<void>;
  /** Credential/transport sanity check invoked from the settings screen. */
  testConnection?(): Promise<{ ok: boolean; detail?: string }>;
  /** Group administrator check for the "admins" request policy. */
  isAdmin?(chatId: string, senderId: string): Promise<boolean>;
  /** Delivered bot message ids, used by reply-trigger heuristics. */
  remembersOwnMessages?: boolean;
  isOwnMessage?(messageId: string): boolean;
  rememberOwnMessage?(messageId: string): void;
}

export type PairingStatus =
  | { status: "pending"; qrDataUrl?: string; detail?: string }
  | { status: "scanned" }
  | { status: "ok"; fields: Record<string, string>; detail?: string }
  | { status: "expired" }
  | { status: "failed"; detail: string };

export interface PairingSession {
  start(): Promise<void>;
  poll(): Promise<PairingStatus>;
  stop(): Promise<void>;
}

export interface ConfiguredFields {
  config: Record<string, unknown>;
  secrets: Record<string, string>;
}

export interface ChannelDefinition<C extends Connection = Connection> {
  channel: ChannelId;
  create(ctx: AdapterContext<C>): ChannelAdapter;
  /** Validate and split raw connect/pairing fields into config entries and
   * secrets; also derives channel-specific defaults (e.g. callbackSecret). */
  configure(fields: Record<string, string>): ConfiguredFields;
  pairing?(connectionId: string, dataDir: string): PairingSession;
  /** Static platform gate; throws UserFacingError when the host cannot run it. */
  checkPlatform?(): void;
}

export class AdapterStartError extends Error {}
