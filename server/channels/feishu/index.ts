/** 飞书（Lark）渠道适配器 — WebSocket 长连接（@larksuiteoapi/node-sdk）。
 * Protocol facts ported from xmanrui/dsh-im (MIT): im.message.receive_v1
 * flattened event shape（sender/message{chat_id, chat_type, message_id,
 * message_type, content 为 JSON 字符串, mentions[{key, id.open_id}],
 * thread_id/parent_id/create_time(ms 字符串)}）、机器人身份经
 * GET /open-apis/bot/v3/info（bot.open_id）、mention 占位 @_user_N 剥离/替换、
 * 媒体经 im.v1.messageResource.get（type: image|file）有界流式下载，以及
 * cardkit 卡片实体流式进度（POST /cardkit/v1/cards +
 * PUT /cardkit/v1/cards/:card_id，卡片上限 18 000 字节）。 */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import type { Readable } from "node:stream";
import { loadPackage } from "../runtime-loader.ts";

export type LarkSdk = typeof import("@larksuiteoapi/node-sdk");

let cached: LarkSdk | null = null;
async function larkSdk(): Promise<LarkSdk> {
  if (!cached) cached = await loadPackage<LarkSdk>("@larksuiteoapi/node-sdk");
  return cached;
}
import { z } from "zod";
import type { Connection } from "../../../shared/config.ts";
import { CHANNEL_SPECS } from "../../../shared/channels.ts";
import { UserFacingError, userMessage } from "../../../shared/errors.ts";
import type {
  ChannelCapabilities,
  NormalizedMedia,
  NormalizedMessage,
  OutboundMedia,
  OutboundText,
  SendResult,
} from "../../../shared/model.ts";
import { atomicWrite, readJson, WriteChain } from "../../files.ts";
import { splitChunks } from "../../format/chunks.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import type {
  AdapterContext,
  ChannelAdapter,
  ChannelDefinition,
  ConfiguredFields,
} from "../types.ts";

export type FeishuConnection = Extract<Connection, { channel: "feishu" }>;

/** 飞书单条文本消息分块上限（任务规定的 4 000 字，≤ 卡片 18 000 字节预算）。 */
const MAX_CHUNK_CHARS = 4_000;
/** cardkit 卡片实体上限 18 000 字节；留出安全余量给包裹结构。 */
const CARD_MAX_BYTES = 17_000;
/** 媒体上传（图片/文件）单文件上限。 */
const MAX_UPLOAD_BYTES = 30 * 1024 * 1024;
/** 瞬时故障重连退避，直到 ctx.signal abort。 */
const RECONNECT_DELAYS_MS = [1_000, 3_000, 5_000, 10_000, 30_000] as const;
const SEEN_CAP = 1_000;
const STREAMS_CAP = 32;
const PROGRESS_PLACEHOLDER = "⏳ 正在处理…";

const adapterStateSchema = z
  .object({
    version: z.literal(1),
    seen: z.array(z.string().min(1).max(128)).max(SEEN_CAP).default([]),
    own: z.array(z.string().min(1).max(128)).max(SEEN_CAP).default([]),
  })
  .strict();

/** 飞书 Open API 统一响应外壳。 */
export interface LarkEnvelope<T> {
  code?: number;
  msg?: string;
  data?: T;
}

/** im.v1.message.get 返回的消息条目。 */
export interface LarkMessageItem {
  message_id?: string;
  chat_id?: string;
  thread_id?: string;
  msg_type?: string;
  create_time?: string;
  sender?: { id?: string; id_type?: string; sender_type?: string } | null;
  body?: { content?: string } | null;
}

/** 媒体资源下载结果（SDK 返回 writeFile/getReadableStream 双通道）。 */
export interface LarkResourceResult {
  writeFile(filePath: string): Promise<unknown>;
  getReadableStream(): Readable;
  headers: unknown;
}

/** SDK Client 中适配器实际触碰的结构化子集；测试用 fake 实现它。 */
export interface LarkClientLike {
  im: {
    v1: {
      message: {
        create(payload: {
          data: { receive_id: string; msg_type: string; content: string };
          params: { receive_id_type: "open_id" | "union_id" | "chat_id" };
        }): Promise<LarkEnvelope<{ message_id?: string }>>;
        reply(payload: {
          data: { content: string; msg_type: string; reply_in_thread?: boolean };
          path: { message_id: string };
        }): Promise<LarkEnvelope<{ message_id?: string }>>;
        get(payload: {
          path: { message_id: string };
        }): Promise<LarkEnvelope<{ items?: LarkMessageItem[] }>>;
      };
      messageResource: {
        get(payload: {
          params: { type: string };
          path: { message_id: string; file_key: string };
        }): Promise<LarkResourceResult>;
      };
      image: {
        create(payload: {
          data: { image_type: "message"; image: Buffer };
        }): Promise<{ image_key?: string } | null>;
      };
      file: {
        create(payload: {
          data: { file_type: string; file_name: string; file: Buffer };
        }): Promise<{ file_key?: string } | null>;
      };
      messageReaction: {
        create(payload: {
          data: { reaction_type: { emoji_type: string } };
          path: { message_id: string };
        }): Promise<LarkEnvelope<{ reaction_id?: string }>>;
      };
      chat: {
        get(payload: {
          path: { chat_id: string };
        }): Promise<LarkEnvelope<{
          owner_id?: string;
          user_manager_id_list?: string[];
        }>>;
      };
      chatMembers: {
        delete(payload: {
          data?: { id_list?: string[] };
          params?: { member_id_type?: "user_id" | "union_id" | "open_id" | "app_id" };
          path: { chat_id: string };
        }): Promise<LarkEnvelope<{ invalid_id_list?: string[] }>>;
      };
    };
  };
  cardkit: {
    v1: {
      card: {
        create(payload: {
          data: { type: string; data: string };
        }): Promise<LarkEnvelope<{ card_id?: string }>>;
        update(payload: {
          data: { card: { type: "card_json"; data: string }; sequence: number };
          path: { card_id: string };
        }): Promise<LarkEnvelope<unknown>>;
      };
    };
  };
  /** 原生 REST 通道（bot/v3/info 无类型化命名空间，走这里）。 */
  request(payload: { method: string; url: string }): Promise<unknown>;
}

export interface FeishuReceiveEvent {
  sender?: {
    sender_id?: { open_id?: string; union_id?: string; user_id?: string };
    sender_type?: string;
  } | null;
  message?: {
    message_id?: string;
    root_id?: string;
    parent_id?: string;
    create_time?: string;
    chat_id?: string;
    thread_id?: string;
    chat_type?: string;
    message_type?: string;
    content?: string;
    mentions?: Array<{
      key?: string;
      id?: { open_id?: string; union_id?: string; user_id?: string };
      name?: string;
    }>;
  } | null;
}

export interface FeishuEventDispatcherLike {
  register(handles: {
    "im.message.receive_v1"?: (data: FeishuReceiveEvent) => unknown;
  }): unknown;
}

export interface FeishuWSClientLike {
  start(params: { eventDispatcher: FeishuEventDispatcherLike }): Promise<void>;
  close?(params?: { force?: boolean }): void;
}

export interface FeishuCredentials {
  appId: string;
  appSecret: string;
  domain: "feishu" | "lark";
}

export interface FeishuAdapterDeps {
  /** 预建传输（测试注入 fake）；默认按凭据构建 SDK 客户端。 */
  client?: LarkClientLike;
  createClient?: (credentials: FeishuCredentials) => LarkClientLike;
  wsClient?: FeishuWSClientLike;
  createWSClient?: (
    params: FeishuCredentials & { onError(error: Error): void; onReady(): void },
  ) => FeishuWSClientLike;
  dispatcher?: FeishuEventDispatcherLike;
  createDispatcher?: () => FeishuEventDispatcherLike;
  reconnectDelaysMs?: readonly number[];
}

export async function defaultDeps(): Promise<
  Pick<FeishuAdapterDeps, "createClient" | "createWSClient" | "createDispatcher">
> {
  const lark = await larkSdk();
  const domainOf = (value: "feishu" | "lark") =>
    value === "lark" ? lark.Domain.Lark : lark.Domain.Feishu;
  return {
    createClient: (credentials) =>
      new lark.Client({
        appId: credentials.appId,
        appSecret: credentials.appSecret,
        domain: domainOf(credentials.domain),
        loggerLevel: lark.LoggerLevel.warn,
      }) as unknown as LarkClientLike,
    createWSClient: ({ onError, onReady, ...credentials }) =>
      new lark.WSClient({
        appId: credentials.appId,
        appSecret: credentials.appSecret,
        domain: domainOf(credentials.domain),
        loggerLevel: lark.LoggerLevel.warn,
        onError,
        onReady,
      }) as unknown as FeishuWSClientLike,
    createDispatcher: () =>
      new lark.EventDispatcher({}) as unknown as FeishuEventDispatcherLike,
  };
}

/** 测试/管理端入口：注入 deps 即可 mock 全部传输层。 */
export function createAdapter(
  ctx: AdapterContext<FeishuConnection>,
  deps?: FeishuAdapterDeps,
): ChannelAdapter {
  return new FeishuAdapter(ctx, deps);
}

interface MediaTarget {
  kind: "image" | "file";
  fileKey: string;
  type: "image" | "file";
  name?: string;
}

interface StreamRecord {
  chatId: string;
  threadId?: string;
  cardId: string | null;
  messageId?: string;
  sequence: number;
  broken: boolean;
  finished: boolean;
}

function str(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function nonEmpty(value: unknown): string | null {
  const text = str(value).trim();
  return text ? text : null;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function descriptionOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** 权限类错误码（任务约定 40999/99991663/99991661 + 文案匹配）。 */
const PERMISSION_CODES = new Set([40999, 99991663, 99991661, 99991672, 230027, 234043]);

function extractProviderCode(error: unknown): number | null {
  const response = (error as { response?: { data?: unknown } } | null)?.response?.data;
  for (const source of [error, response]) {
    const code = Number((source as { code?: unknown } | null | undefined)?.code);
    if (Number.isFinite(code) && code !== 0) return code;
  }
  return null;
}

function isPermissionFailure(code: number | null, message: string): boolean {
  if (code !== null && PERMISSION_CODES.has(code)) return true;
  return /permission|权限/i.test(message);
}

/** 统一包装成用户可读的中文错误；权限类错误给出补权限指引。 */
function feishuError(operation: string, error: unknown): UserFacingError {
  if (error instanceof UserFacingError) return error;
  const message = descriptionOf(error);
  const code = extractProviderCode(error);
  const codeNote = code !== null ? `（错误码 ${code}）` : "";
  if (isPermissionFailure(code, message)) {
    return new UserFacingError(
      `飞书${operation}失败${codeNote}：${message}。请在飞书开放平台为应用开通所需权限（如 im:message、im:resource、cardkit 卡片读写），并重新发布应用版本。`,
    );
  }
  return new UserFacingError(`飞书${operation}失败${codeNote}：${message}`);
}

/** 校验业务码并返回 data；code != 0 视为失败。 */
function unwrap<T>(operation: string, response: LarkEnvelope<T>): LarkEnvelope<T> {
  const code = Number(response?.code ?? 0);
  if (code !== 0) {
    const message = nonEmpty(response?.msg) ?? `code ${code}`;
    throw feishuError(operation, { code, message });
  }
  return response;
}

function parseJsonContent(raw: unknown): Record<string, unknown> | null {
  const text = str(raw);
  if (!text.trim()) return null;
  try {
    const parsed: unknown = JSON.parse(text);
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

/** 富文本（post）：标题 + 段落文本行，img 元素收集为媒体。 */
function collectPost(parsed: Record<string, unknown> | null): { text: string; targets: MediaTarget[] } {
  if (!parsed) return { text: "", targets: [] };
  const lines: string[] = [];
  const targets: MediaTarget[] = [];
  const title = nonEmpty(parsed.title);
  if (title) lines.push(title);
  const paragraphs = Array.isArray(parsed.content) ? parsed.content : [];
  for (const paragraph of paragraphs) {
    if (!Array.isArray(paragraph)) continue;
    let visible = "";
    for (const element of paragraph) {
      const record = isRecord(element) ? element : null;
      const tag = str(record?.tag).toLowerCase();
      if (tag === "img") {
        const key = nonEmpty(record?.image_key);
        if (key) targets.push({ kind: "image", fileKey: key, type: "image" });
      } else if (tag === "text" || tag === "a" || tag === "link") {
        visible += str(record?.text);
      }
    }
    const line = visible.trim();
    if (line) lines.push(line);
  }
  return { text: lines.join("\n"), targets };
}

/** 按消息类型解析原始文本与媒体目标；sticker/interactive/share_* 等视为噪音。 */
function collectContent(
  messageType: string,
  parsed: Record<string, unknown> | null,
): { text: string; targets: MediaTarget[] } {
  switch (messageType) {
    case "text":
      return { text: str(parsed?.text), targets: [] };
    case "image": {
      const key = nonEmpty(parsed?.image_key);
      return { text: "", targets: key ? [{ kind: "image", fileKey: key, type: "image" }] : [] };
    }
    case "file":
    case "media":
    case "video":
    case "audio": {
      const key = nonEmpty(parsed?.file_key);
      return {
        text: "",
        targets: key
          ? [{ kind: "file", fileKey: key, type: "file", name: nonEmpty(parsed?.file_name) ?? undefined }]
          : [],
      };
    }
    case "post":
      return collectPost(parsed);
    default:
      return { text: "", targets: [] };
  }
}

/** mention 占位处理：机器人自己的 @ 整体剥离，其他成员替换为 @昵称。 */
function applyMentions(
  text: string,
  mentions: NonNullable<NonNullable<FeishuReceiveEvent["message"]>["mentions"]>,
  botOpenId: string | null,
): string {
  let result = text;
  for (const mention of mentions) {
    const key = str(mention.key);
    if (!key || !result.includes(key)) continue;
    const isBot = botOpenId !== null && mention.id?.open_id === botOpenId;
    const name = isBot ? "" : nonEmpty(mention.name);
    result = result.split(key).join(name ? `@${name}` : "");
  }
  return result.trim();
}

function normalizeTimestamp(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : Date.now();
}

const MIME_BY_EXT: Record<string, string> = {
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  png: "image/png",
  gif: "image/gif",
  webp: "image/webp",
  bmp: "image/bmp",
  heic: "image/heic",
  pdf: "application/pdf",
  txt: "text/plain",
  md: "text/markdown",
  csv: "text/csv",
  json: "application/json",
  zip: "application/zip",
  mp3: "audio/mpeg",
  m4a: "audio/mp4",
  opus: "audio/opus",
  wav: "audio/wav",
  mp4: "video/mp4",
  doc: "application/msword",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  xls: "application/vnd.ms-excel",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  ppt: "application/vnd.ms-powerpoint",
  pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
};

function guessMime(name: string, kind: "image" | "file"): string {
  const ext = name.includes(".") ? name.split(".").pop()!.toLowerCase() : "";
  if (ext && MIME_BY_EXT[ext]) return MIME_BY_EXT[ext];
  return kind === "image" ? "image/jpeg" : "application/octet-stream";
}

function sanitizeName(name: string, fallback: string): string {
  const cleaned = name.replace(/[\\/]/g, "_").replace(/[\u0000-\u001f]/g, "").trim().slice(0, 120);
  return cleaned || fallback;
}

/** 常用 emoji → 飞书 emoji_type；已是大写键名的直接透传。 */
const EMOJI_TYPES: Record<string, string> = {
  "👍": "THUMBSUP",
  "👎": "THUMBSDOWN",
  "❤️": "HEART",
  "❤": "HEART",
  "✅": "DONE",
  "😄": "SMILE",
  "😂": "LAUGH",
  "🎉": "PARTY",
};

function emojiTypeOf(emoji: string): string {
  const mapped = EMOJI_TYPES[emoji] ?? EMOJI_TYPES[emoji.trim()];
  if (mapped) return mapped;
  if (/^[A-Z][A-Z0-9_]*$/.test(emoji.trim())) return emoji.trim();
  throw new UserFacingError(`不支持的表情回复：${emoji.trim() || "(空)"}。`);
}

function pushCapped(list: string[], value: string, cap: number): void {
  list.push(value);
  if (list.length > cap) list.splice(0, list.length - cap);
}

/** cardkit 卡片 JSON 2.0：单 markdown 元素，承载进度与最终回复。 */
function progressCard(markdown: string): Record<string, unknown> {
  return {
    schema: "2.0",
    config: { update_multi: true },
    body: { elements: [{ tag: "markdown", content: markdown }] },
  };
}

/** 有界读取媒体流：超限或中止时销毁流并抛错。 */
async function readBounded(stream: Readable, maxBytes: number, signal?: AbortSignal): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  const onAbort = () => stream.destroy(new Error("下载已中止"));
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    for await (const chunk of stream) {
      const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
      size += data.length;
      if (size > maxBytes) {
        stream.destroy();
        throw new UserFacingError(
          `飞书媒体超过 ${Math.round(maxBytes / 1024 / 1024)}MB 下载上限，已跳过。`,
        );
      }
      chunks.push(data);
    }
  } finally {
    signal?.removeEventListener("abort", onAbort);
  }
  return Buffer.concat(chunks);
}

export class FeishuAdapter implements ChannelAdapter {
  readonly channel = "feishu" as const;
  readonly capabilities: ChannelCapabilities = {
    progress: "stream",
    receiptReaction: "GET",
    markdown: "native",
    uploadImage: true,
    uploadFile: true,
    reactions: true,
    typing: false,
    maxChunkChars: MAX_CHUNK_CHARS,
    threads: false,
  };

  /** 适配器会记住自己发出的 message_id，供回复触发判定。 */
  readonly remembersOwnMessages = true;

  readonly #ctx: AdapterContext<FeishuConnection>;
  #deps: FeishuAdapterDeps = {};
  readonly #statePath: string;
  readonly #writeChain = new WriteChain();
  #credentials: FeishuCredentials | null = null;
  #client: LarkClientLike | null = null;
  #dispatcher: FeishuEventDispatcherLike | null = null;
  #ws: FeishuWSClientLike | null = null;
  #botOpenId: string | null = null;
  #seen: string[] = [];
  #own: string[] = [];
  #streams = new Map<string, StreamRecord>();
  #reconnectTimer: NodeJS.Timeout | null = null;
  #reconnectIndex = 0;
  #started = false;
  #stopped = false;
  #onAbort: (() => void) | null = null;

  constructor(ctx: AdapterContext<FeishuConnection>, deps?: FeishuAdapterDeps) {
    this.#ctx = ctx;
    if (deps) this.#deps = deps;
    this.#statePath = join(ctx.dataDir, "feishu-state.json");
  }

  async start(): Promise<void> {
    if (this.#started) throw new UserFacingError("飞书适配器已启动。");
    if (this.#stopped) throw new UserFacingError("飞书适配器已停止，请重新保存连接以重建适配器。");
    if (!this.#deps.createClient || !this.#deps.createWSClient || !this.#deps.createDispatcher) {
      // 默认依赖垫底，注入项（含显式 undefined 的槽位）必须最后展开以胜出。
      this.#deps = { ...(await defaultDeps()), ...this.#deps };
    }
    const appId = nonEmpty(this.#ctx.config.appId);
    const appSecret = nonEmpty(this.#ctx.secrets.appSecret);
    if (!appId || !appSecret) {
      throw new UserFacingError("请在连接设置中填写飞书应用的 App ID，并在密钥中填写 App Secret。");
    }
    this.#credentials = { appId, appSecret, domain: this.#ctx.config.domain ?? "feishu" };
    this.#client = (this.#deps.client ?? this.#deps.createClient?.(this.#credentials)) ?? null;
    if (!this.#client) throw new UserFacingError("飞书客户端创建失败，请检查适配器依赖配置。");

    await this.#loadState();
    // 启动即校验凭据并缓存机器人 open_id（@ 判定与退群依赖它）。
    const bot = await this.#fetchBotInfo(this.#client);
    this.#botOpenId = bot.openId;
    if (!bot.openId) {
      this.#ctx.log("warn", "飞书机器人信息未返回 open_id，群内 @提及 触发判定将不可用。");
    }

    this.#dispatcher = (this.#deps.dispatcher ?? this.#deps.createDispatcher?.()) ?? null;
    if (!this.#dispatcher) throw new UserFacingError("飞书事件分发器创建失败，请检查适配器依赖配置。");
    this.#dispatcher.register({
      "im.message.receive_v1": (event) => this.#handleEvent(event),
    });

    const onAbort = () => void this.stop();
    this.#ctx.signal.addEventListener("abort", onAbort, { once: true });
    this.#onAbort = () => this.#ctx.signal.removeEventListener("abort", onAbort);

    this.#started = true;
    try {
      await this.#connect();
    } catch (error) {
      this.#teardown();
      throw error;
    }
  }

  async stop(): Promise<void> {
    this.#teardown();
    await this.#writeChain.run(async () => undefined);
  }

  async sendText(outbound: OutboundText): Promise<SendResult> {
    const text = outbound.text.trim();
    if (!text) return {};
    const client = this.#requireClient();
    const chunks = splitChunks(text, this.capabilities.maxChunkChars);
    let messageId: string | undefined;
    for (const chunk of chunks) {
      const content = JSON.stringify({ text: chunk });
      const receipt = outbound.replyToId
        ? await client.im.v1.message
            .reply({
              path: { message_id: outbound.replyToId },
              data: {
                msg_type: "text",
                content,
                ...(outbound.threadId ? { reply_in_thread: true } : {}),
              },
            })
            .catch((error: unknown) => {
              throw feishuError("回复消息", error);
            })
        : await client.im.v1.message
            .create({
              params: { receive_id_type: receiveIdType(outbound.chatId) },
              data: { receive_id: outbound.chatId, msg_type: "text", content },
            })
            .catch((error: unknown) => {
              throw feishuError("发送消息", error);
            });
      const id = unwrap(outbound.replyToId ? "回复消息" : "发送消息", receipt).data?.message_id;
      if (id) {
        messageId = id;
        this.#rememberOwn(id);
      }
    }
    await this.#flushState();
    return { messageId };
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const label = outbound.kind === "image" ? "图片" : "文件";
    const bytes = await readFile(outbound.path).catch((error: NodeJS.ErrnoException) => {
      throw new UserFacingError(`无法读取待发送的${label} ${outbound.path}：${error.message}`);
    });
    if (bytes.length === 0) {
      throw new UserFacingError(`无法发送空的${label}，飞书拒绝空媒体。`);
    }
    if (bytes.length > MAX_UPLOAD_BYTES) {
      throw new UserFacingError(`待发送的${label}超过飞书 30MB 媒体上限，未发送。`);
    }
    const client = this.#requireClient();
    let msgType: string;
    let content: string;
    if (outbound.kind === "image") {
      const uploaded = await client.im.v1.image
        .create({ data: { image_type: "message", image: bytes } })
        .catch((error: unknown) => {
          throw feishuError("上传图片", error);
        });
      const key = nonEmpty(uploaded?.image_key);
      if (!key) throw new UserFacingError("飞书图片上传未返回 image_key，发送已取消。");
      msgType = "image";
      content = JSON.stringify({ image_key: key });
    } else {
      const name = sanitizeName(outbound.name || basename(outbound.path), "file.bin");
      const uploaded = await client.im.v1.file
        .create({ data: { file_type: "stream", file_name: name, file: bytes } })
        .catch((error: unknown) => {
          throw feishuError("上传文件", error);
        });
      const key = nonEmpty(uploaded?.file_key);
      if (!key) throw new UserFacingError("飞书文件上传未返回 file_key，发送已取消。");
      msgType = "file";
      content = JSON.stringify({ file_key: key });
    }
    const messageId = await this.#deliver(outbound.chatId, outbound.threadId, undefined, msgType, content);
    if (outbound.caption?.trim()) {
      await this.sendText({
        chatId: outbound.chatId,
        threadId: outbound.threadId,
        text: outbound.caption,
      });
    }
    return { messageId };
  }

  async streamStart(chatId: string, threadId?: string): Promise<{ streamId: string }> {
    const streamId = `card-${crypto.randomUUID()}`;
    let cardId: string | null = null;
    let messageId: string | undefined;
    const client = this.#client;
    if (client) {
      try {
        const created = unwrap(
          "创建进度卡片",
          await client.cardkit.v1.card.create({
            data: { type: "card_json", data: JSON.stringify(progressCard(PROGRESS_PLACEHOLDER)) },
          }),
        );
        cardId = nonEmpty(created.data?.card_id);
        if (!cardId) throw new UserFacingError("飞书进度卡片创建未返回 card_id。");
        messageId = await this.#deliver(
          chatId,
          threadId,
          undefined,
          "card",
          JSON.stringify({ type: "card", data: { card_id: cardId } }),
        );
      } catch (error) {
        cardId = null;
        messageId = undefined;
        this.#ctx.log("warn", `飞书进度卡片创建失败，将直接发送最终回复：${descriptionOf(error)}`);
      }
    }
    if (this.#streams.size >= STREAMS_CAP) {
      const oldest = this.#streams.keys().next().value;
      if (oldest !== undefined) this.#streams.delete(oldest);
    }
    this.#streams.set(streamId, {
      chatId,
      ...(threadId ? { threadId } : {}),
      cardId,
      ...(messageId ? { messageId } : {}),
      sequence: 1,
      broken: cardId === null,
      finished: false,
    });
    return { streamId };
  }

  async streamAppend(streamId: string, text: string): Promise<void> {
    const record = this.#streams.get(streamId);
    if (!record || record.finished) return;
    await this.#updateProgressCard(record, text);
  }

  async streamFinish(streamId: string, text: string): Promise<{ messageId?: string }> {
    const record = this.#streams.get(streamId);
    if (!record) throw new Error(`飞书流式回复记录不存在：${streamId}`);
    this.#streams.delete(streamId);
    record.finished = true;
    const content = text.trim();
    if (record.cardId && !record.broken) {
      if (Buffer.byteLength(content, "utf8") > CARD_MAX_BYTES) {
        // 卡片放不下：卡片提示，完整内容分条发送，避免丢内容。
        await this.#updateProgressCard(record, "回复内容较长，以下分条发送：");
        const sent = await this.sendText({
          chatId: record.chatId,
          threadId: record.threadId,
          text: content,
        });
        return { messageId: sent.messageId ?? record.messageId };
      }
      if (await this.#updateProgressCard(record, content || PROGRESS_PLACEHOLDER)) {
        return { messageId: record.messageId };
      }
    }
    if (!content) return { messageId: record.messageId };
    return this.sendText({ chatId: record.chatId, threadId: record.threadId, text: content });
  }

  async addReaction(messageId: string, _chatId: string, emoji: string): Promise<void> {
    const client = this.#requireClient();
    unwrap(
      "添加表情回复",
      await client.im.v1.messageReaction
        .create({
          path: { message_id: messageId },
          data: { reaction_type: { emoji_type: emojiTypeOf(emoji) } },
        })
        .catch((error: unknown) => {
          throw feishuError("添加表情回复", error);
        }),
    );
  }

  async leaveChat(chatId: string): Promise<void> {
    const client = this.#requireClient();
    const botOpenId = this.#botOpenId;
    if (!botOpenId) throw new UserFacingError("飞书机器人 open_id 未知，无法退出群聊。");
    unwrap(
      "退出群聊",
      await client.im.v1.chatMembers
        .delete({
          path: { chat_id: chatId },
          data: { id_list: [botOpenId] },
          params: { member_id_type: "app_id" },
        })
        .catch((error: unknown) => {
          throw feishuError("退出群聊", error);
        }),
    );
  }

  /** 设置页凭据探测：读取机器人信息，返回中文诊断。启动前后均可调用。 */
  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    const credentials = this.#credentials ?? {
      appId: this.#ctx.config.appId,
      appSecret: this.#ctx.secrets.appSecret ?? "",
      domain: this.#ctx.config.domain ?? "feishu",
    };
    if (!credentials.appId || !credentials.appSecret) {
      return { ok: false, detail: "请先填写 App ID 与 App Secret。" };
    }
    const client = this.#client ?? this.#deps.createClient?.(credentials);
    if (!client) return { ok: false, detail: "飞书客户端不可用，请检查适配器依赖配置。" };
    try {
      const bot = await this.#fetchBotInfo(client);
      const name = bot.name ?? "未知应用";
      if (!bot.openId) return { ok: true, detail: `凭据有效：${name}（未返回 open_id）。` };
      return { ok: true, detail: `凭据有效：${name}（${bot.openId}）。` };
    } catch (error) {
      return { ok: false, detail: userMessage(error) };
    }
  }

  /** 群管理员判定：群主或群管理员列表命中即为管理员。 */
  async isAdmin(chatId: string, senderId: string): Promise<boolean> {
    const client = this.#client;
    if (!client) return false;
    try {
      const response = unwrap("查询群信息", await client.im.v1.chat.get({ path: { chat_id: chatId } }));
      const data = response.data ?? {};
      return data.owner_id === senderId || (data.user_manager_id_list ?? []).includes(senderId);
    } catch {
      return false;
    }
  }

  isOwnMessage(messageId: string): boolean {
    return this.#own.includes(messageId);
  }

  rememberOwnMessage(messageId: string): void {
    const id = nonEmpty(messageId);
    if (!id || this.#own.includes(id)) return;
    pushCapped(this.#own, id, SEEN_CAP);
    void this.#flushState().catch(() => {});
  }

  #requireClient(): LarkClientLike {
    if (!this.#client) throw new UserFacingError("飞书适配器尚未启动。");
    return this.#client;
  }

  async #fetchBotInfo(client: LarkClientLike): Promise<{ openId: string | null; name: string | null }> {
    let body: unknown;
    try {
      body = await client.request({ method: "GET", url: "/open-apis/bot/v3/info" });
    } catch (error) {
      throw new UserFacingError(`无法连接飞书服务：${descriptionOf(error)}。请检查网络后重试。`);
    }
    const record = isRecord(body) ? body : {};
    const code = Number(record.code ?? 0);
    if (code !== 0) {
      const message = nonEmpty(record.msg) ?? `code ${code}`;
      if (isPermissionFailure(code, message)) {
        throw new UserFacingError(
          `飞书凭据校验失败（错误码 ${code}）：${message}。请检查 App ID 与 App Secret 是否正确。`,
        );
      }
      throw new UserFacingError(
        `飞书凭据校验失败（错误码 ${code}）：${message}。请检查 App ID 与 App Secret 是否正确，并确认应用已启用机器人能力。`,
      );
    }
    const bot = isRecord(record.bot) ? record.bot : isRecord(record.data) ? record.data : {};
    return { openId: nonEmpty(bot.open_id), name: nonEmpty(bot.app_name) ?? nonEmpty(bot.bot_name) };
  }

  async #connect(): Promise<void> {
    if (!this.#credentials || !this.#dispatcher) return;
    const ws =
      this.#ws ??
      this.#deps.wsClient ??
      this.#deps.createWSClient?.({
        ...this.#credentials,
        onError: (error) => this.#handleWSError(error),
        onReady: () => {
          this.#reconnectIndex = 0;
        },
      });
    if (!ws) throw new UserFacingError("飞书 WebSocket 客户端创建失败，请检查适配器依赖配置。");
    this.#ws = ws;
    try {
      await ws.start({ eventDispatcher: this.#dispatcher });
    } catch (error) {
      throw feishuError("建立长连接", error);
    }
  }

  #handleWSError(error: Error): void {
    if (this.#stopped || this.#ctx.signal.aborted) return;
    this.#ctx.onError(error);
    this.#scheduleReconnect();
  }

  #scheduleReconnect(): void {
    if (this.#stopped || this.#ctx.signal.aborted) return;
    this.#clearReconnectTimer();
    const delays = this.#deps.reconnectDelaysMs ?? RECONNECT_DELAYS_MS;
    const delay = delays[Math.min(this.#reconnectIndex, delays.length - 1)];
    this.#reconnectIndex += 1;
    this.#ctx.log("warn", `飞书长连接中断，${Math.round(delay / 1000)} 秒后重连。`);
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      if (this.#stopped || this.#ctx.signal.aborted) return;
      // 重建 WSClient 实例：旧实例断开后可能触发 SDK 内部自动重连，复用会形成
      // 新旧双路连接；注入的 wsClient 无重建入口，保持原样复用。
      if (!this.#deps.wsClient) {
        this.#ws?.close?.({});
        this.#ws = null;
      }
      this.#connect().catch((error) => {
        this.#ctx.onError(error instanceof Error ? error : new Error(String(error)));
        this.#scheduleReconnect();
      });
    }, delay);
    this.#reconnectTimer.unref?.();
  }

  #clearReconnectTimer(): void {
    if (this.#reconnectTimer) {
      clearTimeout(this.#reconnectTimer);
      this.#reconnectTimer = null;
    }
  }

  #teardown(): void {
    this.#stopped = true;
    this.#started = false;
    this.#clearReconnectTimer();
    this.#onAbort?.();
    this.#onAbort = null;
    this.#ws?.close?.({});
    this.#ws = null;
    this.#streams.clear();
  }

  async #handleEvent(event: FeishuReceiveEvent): Promise<void> {
    if (this.#stopped) return;
    try {
      await this.#accept(event);
    } catch (error) {
      this.#ctx.onError(error instanceof Error ? error : new Error(String(error)));
    }
  }

  async #accept(event: FeishuReceiveEvent): Promise<void> {
    const message = event?.message ?? null;
    const sender = event?.sender ?? null;
    if (!message || !sender) return;
    // 机器人自身消息不 emit。
    if (sender.sender_type === "app") return;
    const chatType = message.chat_type;
    if (chatType !== "p2p" && chatType !== "group") return;
    const messageId = nonEmpty(message.message_id);
    const senderId =
      nonEmpty(sender.sender_id?.open_id) ??
      nonEmpty(sender.sender_id?.union_id) ??
      nonEmpty(sender.sender_id?.user_id);
    // p2p 用 open_id 作会话标识（发送时 receive_id_type=open_id），群聊用 chat_id。
    const chatId = chatType === "group" ? nonEmpty(message.chat_id) : senderId;
    if (!messageId || !senderId || !chatId) return;
    if (this.#seen.includes(messageId)) return;
    pushCapped(this.#seen, messageId, SEEN_CAP);

    const parsed = parseJsonContent(message.content);
    const { text: rawText, targets } = collectContent(str(message.message_type), parsed);
    const mentions = message.mentions ?? [];
    const text = applyMentions(rawText, mentions, this.#botOpenId);

    let media: NormalizedMedia[] = [];
    if (targets.length > 0) {
      try {
        const settings = await this.#ctx.settings();
        const maxBytes = Math.max(1, settings.imageMaxDownloadMb) * 1024 * 1024;
        media = await this.#downloadMedia(messageId, targets.slice(0, Math.max(1, settings.imageMaxCount)), maxBytes);
      } catch (error) {
        this.#ctx.log("warn", `飞书媒体下载失败，本次消息仅保留文本：${descriptionOf(error)}`);
      }
    }

    // bot open_id 未知时按“有 @ 即算”降级（与 dsh bridge 同款兜底）。
    const mention = this.#botOpenId
      ? mentions.some((entry) => entry.id?.open_id === this.#botOpenId)
      : mentions.length > 0;
    const parentId = nonEmpty(message.parent_id);
    // @命中即无需再查被回复消息；否则 own 集未命中时经 message.get 兜底判定。
    const replyToBot =
      chatType === "group" && parentId !== null
        ? mention || (await this.#isReplyToBot(parentId))
        : false;

    const normalized: NormalizedMessage = {
      channel: "feishu",
      connectionId: this.#ctx.connectionId,
      kind: chatType === "group" ? "group" : "dm",
      chatId,
      ...(message.thread_id ? { threadId: message.thread_id } : {}),
      senderId,
      messageId,
      timestamp: normalizeTimestamp(message.create_time),
      ...(text ? { text } : {}),
      ...(media.length > 0 ? { media } : {}),
      ...(parentId ? { replyTo: { id: parentId } } : {}),
      trigger: { mention, replyToBot, command: text.startsWith("/") },
    };
    await this.#flushState();
    if (!normalized.text && (normalized.media?.length ?? 0) === 0) return; // 噪音
    this.#ctx.onMessage(normalized);
  }

  async #isReplyToBot(parentId: string): Promise<boolean> {
    if (this.#own.includes(parentId)) return true;
    const client = this.#client;
    if (!client) return false;
    try {
      const response = unwrap("查询被回复消息", await client.im.v1.message.get({ path: { message_id: parentId } }));
      const item = response.data?.items?.[0];
      return item?.sender?.sender_type === "app" || (!!this.#botOpenId && item?.sender?.id === this.#botOpenId);
    } catch {
      return false; // 查询失败不阻塞消息投递
    }
  }

  async #downloadMedia(messageId: string, targets: MediaTarget[], maxBytes: number): Promise<NormalizedMedia[]> {
    const client = this.#requireClient();
    const media: NormalizedMedia[] = [];
    await mkdir(this.#ctx.mediaDir, { recursive: true });
    for (const [index, target] of targets.entries()) {
      try {
        const resource = await client.im.v1.messageResource.get({
          path: { message_id: messageId, file_key: target.fileKey },
          params: { type: target.type },
        });
        const buffer = await readBounded(resource.getReadableStream(), maxBytes, this.#ctx.signal);
        if (buffer.length === 0) throw new Error("下载内容为空");
        const fallback = target.kind === "image" ? "image.jpg" : "file.bin";
        const name = sanitizeName(target.name ?? fallback, fallback);
        const path = join(this.#ctx.mediaDir, `${messageId}-${index + 1}-${name}`);
        await writeFile(path, buffer);
        media.push({ kind: target.kind, path, mime: guessMime(name, target.kind), name });
      } catch (error) {
        this.#ctx.log("warn", `飞书媒体下载失败，已跳过：${descriptionOf(error)}`);
      }
    }
    return media;
  }

  async #deliver(
    chatId: string,
    threadId: string | undefined,
    replyToId: string | undefined,
    msgType: string,
    content: string,
  ): Promise<string | undefined> {
    const client = this.#requireClient();
    const receipt = replyToId
      ? await client.im.v1.message
          .reply({
            path: { message_id: replyToId },
            data: { msg_type: msgType, content, ...(threadId ? { reply_in_thread: true } : {}) },
          })
          .catch((error: unknown) => {
            throw feishuError("回复消息", error);
          })
      : await client.im.v1.message
          .create({
            params: { receive_id_type: receiveIdType(chatId) },
            data: { receive_id: chatId, msg_type: msgType, content },
          })
          .catch((error: unknown) => {
            throw feishuError("发送消息", error);
          });
    const id = unwrap(replyToId ? "回复消息" : "发送消息", receipt).data?.message_id;
    if (id) this.#rememberOwn(id);
    return id;
  }

  /** 全量更新卡片 markdown；失败置 broken（后续走直接发送），不向上抛。 */
  async #updateProgressCard(record: StreamRecord, markdown: string): Promise<boolean> {
    if (!record.cardId || record.broken) return false;
    if (Buffer.byteLength(markdown, "utf8") > CARD_MAX_BYTES) return false;
    try {
      const client = this.#requireClient();
      unwrap(
        "更新进度卡片",
        await client.cardkit.v1.card.update({
          path: { card_id: record.cardId },
          data: {
            card: { type: "card_json", data: JSON.stringify(progressCard(markdown)) },
            sequence: record.sequence++,
          },
        }),
      );
      return true;
    } catch (error) {
      record.broken = true;
      this.#ctx.log("warn", `飞书进度卡片更新失败，将在完成时直接发送：${descriptionOf(error)}`);
      return false;
    }
  }

  async #loadState(): Promise<void> {
    try {
      const state = await readJson(this.#statePath, (value) => adapterStateSchema.parse(value));
      this.#seen = state?.seen ?? [];
      this.#own = state?.own ?? [];
    } catch (error) {
      this.#seen = [];
      this.#own = [];
      this.#ctx.log("warn", `飞书适配器状态文件损坏，已重置去重记录：${descriptionOf(error)}`);
    }
  }

  async #flushState(): Promise<void> {
    const seen = [...this.#seen];
    const own = [...this.#own];
    await this.#writeChain.run(() => atomicWrite(this.#statePath, { version: 1, seen, own }));
  }

  #rememberOwn(messageId: string): void {
    if (!messageId || this.#own.includes(messageId)) return;
    pushCapped(this.#own, messageId, SEEN_CAP);
  }
}

/** 会话标识按前缀判型：ou_=open_id，on_=union_id，其余（oc_ 开头的 chat_id）按 chat_id。 */
function receiveIdType(chatId: string): "open_id" | "union_id" | "chat_id" {
  if (chatId.startsWith("ou_")) return "open_id";
  if (chatId.startsWith("on_")) return "union_id";
  return "chat_id";
}

export const definition: ChannelDefinition<FeishuConnection> = {
  channel: "feishu",
  create: (ctx) => createAdapter(ctx),
  configure: (fields: Record<string, string>): ConfiguredFields => {
    requireFields(CHANNEL_SPECS.feishu.fields, fields);
    const domain = (fields.domain ?? "").trim().toLowerCase() || "feishu";
    if (domain !== "feishu" && domain !== "lark") {
      throw new UserFacingError("飞书域名仅支持 feishu 或 lark。");
    }
    const result = defaultConfigure("feishu", fields);
    result.config.domain = domain;
    return result;
  },
};
