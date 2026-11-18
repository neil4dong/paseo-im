/** 飞书渠道适配器测试：全部走注入的 fake 传输（LarkClientLike / WS / dispatcher），
 * 不触真实 SDK 与网络，无真实定时等待——重连用注入的 1ms 退避 + 短 sleep。 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { Readable } from "node:stream";
import type { AdapterContext, ChannelAdapter } from "../../server/channels/types.ts";
import {
  createAdapter,
  definition,
  type FeishuAdapterDeps,
  type FeishuConnection,
  type FeishuEventDispatcherLike,
  type FeishuReceiveEvent,
  type FeishuWSClientLike,
  type LarkClientLike,
  type LarkEnvelope,
  type LarkMessageItem,
  type LarkResourceResult,
} from "../../server/channels/feishu/index.ts";
import { UserFacingError } from "../../shared/errors.ts";
import type { NormalizedMessage } from "../../shared/model.ts";
import type { ImSettings } from "../../shared/settings.ts";

const SETTINGS: ImSettings = {
  imageMaxDownloadMb: 30,
  imageMaxSingleMb: 5,
  imageMaxTotalMb: 20,
  imageMaxCount: 20,
  busyMode: "queue",
  attachmentTtlHours: 168,
  deferTimeoutMinutes: 30,
};

const CONNECTION: FeishuConnection = {
  id: "c1d2e3f4-1111-4222-8333-444455556666",
  name: "飞书测试",
  channel: "feishu",
  enabled: true,
  target: null,
  policy: {
    groupAdmission: "everyone",
    directMessages: "everyone",
    groupUsers: "operators",
    operators: [],
    invocation: "mentions",
  },
  bindings: {},
  revision: 0,
  appId: "cli_a1b2c3d4e5f6a7b8",
  domain: "feishu",
};

const SECRETS = { appSecret: "s3cret-value" };
const BOT_OPEN_ID = "ou_bot111111111111111111111111";

interface CardRecord {
  card_id: string;
  data: Record<string, unknown>;
  sequence: number;
}

/** 读取卡片 JSON 2.0 首个 markdown 元素内容。 */
function cardContent(card: Record<string, unknown> | undefined): string {
  const body = card?.body as { elements?: { content?: string }[] } | undefined;
  return body?.elements?.[0]?.content ?? "";
}

class FakeLark implements LarkClientLike {
  readonly creates: { receive_id: string; receive_id_type: string; msg_type: string; content: string }[] = [];
  readonly replies: { message_id: string; msg_type: string; content: string; reply_in_thread?: boolean }[] = [];
  readonly cardCreates: Record<string, unknown>[] = [];
  readonly cardUpdates: CardRecord[] = [];
  readonly reactions: { message_id: string; emoji_type: string }[] = [];
  readonly memberDeletes: { chat_id: string; id_list: string[]; member_id_type: string }[] = [];
  readonly resourceGets: { message_id: string; file_key: string; type: string }[] = [];
  messageCounter = 0;
  chatInfo: { owner_id?: string; user_manager_id_list?: string[] } = {};
  messageItems: LarkMessageItem[] = [];
  botInfo: unknown = { code: 0, bot: { open_id: BOT_OPEN_ID, app_name: "Paseo 机器人" } };
  botInfoError: Error | null = null;
  cardError: Error | null = null;
  cardUpdateError: Error | null = null;
  resources = new Map<string, Buffer | Error>();

  async request(payload: { method: string; url: string }): Promise<unknown> {
    if (payload.url === "/open-apis/bot/v3/info") {
      if (this.botInfoError) throw this.botInfoError;
      return this.botInfo;
    }
    throw new Error(`意外的 REST 调用：${payload.method} ${payload.url}`);
  }

  readonly im: LarkClientLike["im"] = {
    v1: {
      message: {
        create: async (payload) => {
          this.creates.push({
            receive_id: payload.data.receive_id,
            receive_id_type: payload.params.receive_id_type,
            msg_type: payload.data.msg_type,
            content: payload.data.content,
          });
          this.messageCounter += 1;
          return { code: 0, data: { message_id: `om_out_${this.messageCounter}` } };
        },
        reply: async (payload) => {
          this.replies.push({
            message_id: payload.path.message_id,
            msg_type: payload.data.msg_type,
            content: payload.data.content,
            ...(payload.data.reply_in_thread !== undefined
              ? { reply_in_thread: payload.data.reply_in_thread }
              : {}),
          });
          this.messageCounter += 1;
          return { code: 0, data: { message_id: `om_reply_${this.messageCounter}` } };
        },
        get: async () => ({ code: 0, data: { items: this.messageItems } }),
      },
      messageResource: {
        get: async (payload) => {
          this.resourceGets.push({
            message_id: payload.path.message_id,
            file_key: payload.path.file_key,
            type: payload.params.type,
          });
          const entry = this.resources.get(payload.path.file_key);
          if (entry instanceof Error) throw entry;
          return resourceResult(entry ?? Buffer.from("raw-bytes"));
        },
      },
      image: {
        create: async () => ({ image_key: "img_key_1" }),
      },
      file: {
        create: async () => ({ file_key: "file_key_1" }),
      },
      messageReaction: {
        create: async (payload) => {
          this.reactions.push({
            message_id: payload.path.message_id,
            emoji_type: payload.data.reaction_type.emoji_type,
          });
          return { code: 0, data: { reaction_id: "re_1" } };
        },
      },
      chat: {
        get: async () => ({ code: 0, data: this.chatInfo }),
      },
      chatMembers: {
        delete: async (payload) => {
          this.memberDeletes.push({
            chat_id: payload.path.chat_id,
            id_list: payload.data?.id_list ?? [],
            member_id_type: payload.params?.member_id_type ?? "open_id",
          });
          return { code: 0, data: { invalid_id_list: [] } };
        },
      },
    },
  };

  readonly cardkit: LarkClientLike["cardkit"] = {
    v1: {
      card: {
        create: async (payload) => {
          if (this.cardError) throw this.cardError;
          this.cardCreates.push(JSON.parse(payload.data.data) as Record<string, unknown>);
          return { code: 0, data: { card_id: "card_v1_1" } };
        },
        update: async (payload) => {
          if (this.cardUpdateError) throw this.cardUpdateError;
          this.cardUpdates.push({
            card_id: payload.path.card_id,
            data: JSON.parse(payload.data.card.data) as Record<string, unknown>,
            sequence: payload.data.sequence,
          });
          return { code: 0 };
        },
      },
    },
  };
}

function resourceResult(buffer: Buffer): LarkResourceResult {
  return {
    writeFile: async () => {},
    getReadableStream: () => Readable.from([buffer]),
    headers: {},
  };
}

class FakeDispatcher implements FeishuEventDispatcherLike {
  handler: ((data: FeishuReceiveEvent) => unknown) | null = null;

  register(handles: { "im.message.receive_v1"?: (data: FeishuReceiveEvent) => unknown }): unknown {
    this.handler = handles["im.message.receive_v1"] ?? null;
    return this;
  }

  async emit(event: FeishuReceiveEvent): Promise<void> {
    await this.handler?.(event);
  }
}

class FakeWS implements FeishuWSClientLike {
  starts = 0;
  closes = 0;
  startError: Error | null = null;
  #waiters: { at: number; resolve: () => void }[] = [];

  async start(): Promise<void> {
    this.starts += 1;
    this.#waiters = this.#waiters.filter((waiter) => {
      if (waiter.at > this.starts) return true;
      waiter.resolve();
      return false;
    });
    if (this.startError) throw this.startError;
  }

  close(): void {
    this.closes += 1;
  }

  /** 等待第 at 次 start 发生（真实完成信号，非猜测时长）。 */
  waitForStart(at: number): Promise<void> {
    if (this.starts >= at) return Promise.resolve();
    // ES2023 lib 无 Promise.withResolvers 类型，需在 executor 外持有 resolve。
    return new Promise<void>((resolve) => {
      this.#waiters.push({ at, resolve });
    });
  }
}

interface HarnessOptions {
  secrets?: Record<string, string>;
  dataDir?: string;
  signal?: AbortSignal;
  reconnectDelaysMs?: number[];
  /** 覆盖默认 deps（如注入 createWSClient 以驱动重连路径）。 */
  depsOverride?: Partial<FeishuAdapterDeps>;
}

interface Harness {
  adapter: ChannelAdapter;
  ctx: AdapterContext<FeishuConnection>;
  lark: FakeLark;
  ws: FakeWS;
  dispatcher: FakeDispatcher;
  dataDir: string;
  received: NormalizedMessage[];
  errors: Error[];
  logs: string[];
}

async function createHarness(options: HarnessOptions = {}): Promise<Harness> {
  const dataDir = options.dataDir ?? (await mkdtemp(join(tmpdir(), "feishu-test-")));
  const lark = new FakeLark();
  const ws = new FakeWS();
  const dispatcher = new FakeDispatcher();
  const received: NormalizedMessage[] = [];
  const errors: Error[] = [];
  const logs: string[] = [];
  const ctx: AdapterContext<FeishuConnection> = {
    connectionId: CONNECTION.id,
    config: CONNECTION,
    secrets: options.secrets ?? SECRETS,
    dataDir,
    mediaDir: join(dataDir, "media"),
    signal: options.signal ?? new AbortController().signal,
    settings: async () => SETTINGS,
    log: (level, message) => logs.push(`${level}: ${message}`),
    onMessage: (message) => received.push(message),
    onGroups: () => {},
    onError: (error) => errors.push(error),
  };
  const deps: FeishuAdapterDeps = {
    client: lark,
    createClient: () => lark,
    wsClient: ws,
    dispatcher,
    reconnectDelaysMs: options.reconnectDelaysMs ?? [1, 1],
    ...options.depsOverride,
  };
  const adapter = createAdapter(ctx, deps);
  return { adapter, ctx, lark, ws, dispatcher, dataDir, received, errors, logs };
}

function statePath(dataDir: string): string {
  return join(dataDir, "feishu-state.json");
}

function groupEvent(
  messageId: string,
  messageOverrides: Record<string, unknown> = {},
  senderOverrides: Record<string, unknown> = {},
): FeishuReceiveEvent {
  return {
    sender: { sender_id: { open_id: "ou_alice" }, sender_type: "user", ...senderOverrides },
    message: {
      message_id: messageId,
      chat_id: "oc_group1",
      chat_type: "group",
      message_type: "text",
      content: JSON.stringify({ text: "大家好" }),
      create_time: "1700000000000",
      ...messageOverrides,
    },
  };
}

function dmEvent(
  messageId: string,
  messageOverrides: Record<string, unknown> = {},
  senderOverrides: Record<string, unknown> = {},
): FeishuReceiveEvent {
  return {
    sender: { sender_id: { open_id: "ou_bob" }, sender_type: "user", ...senderOverrides },
    message: {
      message_id: messageId,
      chat_id: "oc_dm_bob",
      chat_type: "p2p",
      message_type: "text",
      content: JSON.stringify({ text: "帮我查天气" }),
      create_time: "1700000000000",
      ...messageOverrides,
    },
  };
}

test("definition：渠道与能力（卡片流式进度、4 000 字分块）", async () => {
  assert.equal(definition.channel, "feishu");
  const harness = await createHarness();
  const adapter = definition.create(harness.ctx);
  assert.deepEqual(adapter.capabilities, {
    progress: "stream",
    markdown: "native",
    uploadImage: true,
    uploadFile: true,
    reactions: true,
    typing: false,
    maxChunkChars: 4000,
    threads: false,
  });
  assert.equal(adapter.remembersOwnMessages, true);
});

test("configure：必填校验、机密拆分、trim 与 domain 默认/校验", () => {
  const configured = definition.configure({
    appId: " cli_x1 ",
    appSecret: " s3cret ",
    domain: " LARK ",
  });
  assert.deepEqual(configured, {
    config: { appId: "cli_x1", domain: "lark" },
    secrets: { appSecret: "s3cret" },
  });
  const defaulted = definition.configure({ appId: "cli_x1", appSecret: "s3cret", domain: "" });
  assert.deepEqual(defaulted.config, { appId: "cli_x1", domain: "feishu" });
  assert.throws(() => definition.configure({ appSecret: "s3cret" }), UserFacingError);
  assert.throws(() => definition.configure({ appId: "cli_x1", domain: "slack" }), UserFacingError);
});

test("start：缺凭据抛中文可执行错误", async () => {
  const harness = await createHarness({ secrets: {} });
  await assert.rejects(harness.adapter.start(), (error: unknown) => {
    assert.ok(error instanceof UserFacingError);
    assert.match(error.message, /App ID/);
    assert.match(error.message, /App Secret/);
    return true;
  });
});

test("start：凭据无效（bot info 业务码非 0）抛凭据错误", async () => {
  const harness = await createHarness();
  harness.lark.botInfo = { code: 99991663, msg: "app secret invalid" };
  await assert.rejects(harness.adapter.start(), (error: unknown) => {
    assert.ok(error instanceof UserFacingError);
    assert.match(error.message, /99991663/);
    assert.match(error.message, /App ID/);
    return true;
  });
});

test("start：bot info 网络失败抛连接错误", async () => {
  const harness = await createHarness();
  harness.lark.botInfoError = new Error("ECONNREFUSED");
  await assert.rejects(harness.adapter.start(), (error: unknown) => {
    assert.ok(error instanceof UserFacingError);
    assert.match(error.message, /无法连接飞书服务/);
    return true;
  });
});

test("start/stop：注册事件处理器并清理 WS；停止后事件不再投递", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  assert.equal(harness.ws.starts, 1);
  assert.ok(harness.dispatcher.handler);
  await harness.adapter.stop();
  assert.equal(harness.ws.closes, 1);
  await harness.dispatcher.emit(dmEvent("om_after_stop"));
  assert.equal(harness.received.length, 0);
  assert.equal(harness.errors.length, 0);
});

test("normalize：p2p 文本 → dm 消息，chatId 为发送者 open_id", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  await harness.dispatcher.emit(dmEvent("om_dm1"));
  assert.equal(harness.received.length, 1);
  const message = harness.received[0]!;
  assert.equal(message.channel, "feishu");
  assert.equal(message.kind, "dm");
  assert.equal(message.chatId, "ou_bob");
  assert.equal(message.senderId, "ou_bob");
  assert.equal(message.text, "帮我查天气");
  assert.equal(message.messageId, "om_dm1");
  assert.equal(message.timestamp, 1700000000000);
  assert.deepEqual(message.trigger, { mention: false, replyToBot: false, command: false });
});

test("normalize：群 @机器人剥离占位、保留其他成员昵称、threadId 透传", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  await harness.dispatcher.emit(
    groupEvent("om_g1", {
      thread_id: "ot_topic1",
      content: JSON.stringify({ text: "@_user_1 @_user_2 帮我总结" }),
      mentions: [
        { key: "@_user_1", id: { open_id: BOT_OPEN_ID }, name: "Paseo" },
        { key: "@_user_2", id: { open_id: "ou_xiaoming" }, name: "小明" },
      ],
    }),
  );
  assert.equal(harness.received.length, 1);
  const message = harness.received[0]!;
  assert.equal(message.kind, "group");
  assert.equal(message.chatId, "oc_group1");
  assert.equal(message.threadId, "ot_topic1");
  assert.equal(message.text, "@小明 帮我总结");
  assert.deepEqual(message.trigger, { mention: true, replyToBot: false, command: false });
});

test("normalize：回复机器人消息 → replyToBot（own 集与 message.get 兜底）", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  // 路径一：自己发出的消息 id 直接命中。
  harness.adapter.rememberOwnMessage?.("om_own_1");
  await harness.dispatcher.emit(groupEvent("om_g2", { parent_id: "om_own_1" }));
  assert.equal(harness.received[0]?.trigger.replyToBot, true);
  // 路径二：不在 own 集，经 message.get 查得 sender_type=app。
  harness.lark.messageItems = [{ message_id: "om_hist_bot", sender: { id: BOT_OPEN_ID, sender_type: "app" } }];
  await harness.dispatcher.emit(groupEvent("om_g3", { parent_id: "om_hist_bot" }));
  assert.equal(harness.received[1]?.trigger.replyToBot, true);
  assert.deepEqual(harness.received[1]?.replyTo, { id: "om_hist_bot" });
  // 路径三：回复的是普通用户消息。
  harness.lark.messageItems = [{ message_id: "om_hist_user", sender: { id: "ou_alice", sender_type: "user" } }];
  await harness.dispatcher.emit(groupEvent("om_g4", { parent_id: "om_hist_user" }));
  assert.equal(harness.received[2]?.trigger.replyToBot, false);
});

test("normalize：同一 message_id 去重只投递一次；重启后状态文件继续去重", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "feishu-test-"));
  const first = await createHarness({ dataDir });
  await first.adapter.start();
  await first.dispatcher.emit(dmEvent("om_dup1"));
  await first.dispatcher.emit(dmEvent("om_dup1"));
  assert.equal(first.received.length, 1);
  await first.adapter.stop();

  const second = await createHarness({ dataDir });
  await second.adapter.start();
  await second.dispatcher.emit(dmEvent("om_dup1"));
  assert.equal(second.received.length, 0);
  await second.dispatcher.emit(dmEvent("om_dup2"));
  assert.equal(second.received.length, 1);
});

test("normalize：机器人自身消息与噪音不投递", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  await harness.dispatcher.emit(dmEvent("om_bot1", {}, { sender_type: "app" }));
  await harness.dispatcher.emit(
    groupEvent("om_share1", { message_type: "share_chat", content: JSON.stringify({ chat_id: "oc_x" }) }),
  );
  await harness.dispatcher.emit(dmEvent("om_bad1", { content: "不是 JSON" }));
  await harness.dispatcher.emit(dmEvent("om_empty1", { content: JSON.stringify({ text: "   " }) }));
  assert.equal(harness.received.length, 0);
});

test("normalize：图片消息下载进 mediaDir 并附媒体信息", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  harness.lark.resources.set("img_key_9", Buffer.from("png-bytes"));
  await harness.dispatcher.emit(
    groupEvent("om_img1", {
      message_type: "image",
      content: JSON.stringify({ image_key: "img_key_9" }),
      mentions: [{ key: "@_user_1", id: { open_id: BOT_OPEN_ID }, name: "Paseo" }],
      content2: undefined,
    }),
  );
  assert.equal(harness.received.length, 1);
  const media = harness.received[0]!.media ?? [];
  assert.equal(media.length, 1);
  assert.equal(media[0]!.kind, "image");
  assert.equal(harness.lark.resourceGets[0]?.type, "image");
  const stored = await readFile(media[0]!.path, "utf8");
  assert.equal(stored, "png-bytes");
  assert.ok(media[0]!.path.startsWith(harness.ctx.mediaDir));
  assert.equal(media[0]!.mime, "image/jpeg"); // 兜底命名 image.jpg
});

test("normalize：文件消息按 file 类型下载，保留原始文件名与 mime", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  harness.lark.resources.set("file_key_7", Buffer.from("%PDF-1.4"));
  await harness.dispatcher.emit(
    dmEvent("om_file1", {
      message_type: "file",
      content: JSON.stringify({ file_key: "file_key_7", file_name: "报告.pdf" }),
    }),
  );
  const media = harness.received[0]!.media ?? [];
  assert.equal(media.length, 1);
  assert.equal(media[0]!.kind, "file");
  assert.equal(media[0]!.name, "报告.pdf");
  assert.equal(media[0]!.mime, "application/pdf");
  assert.equal(harness.lark.resourceGets[0]?.type, "file");
});

test("normalize：富文本 post 提取标题与段落文本，img 元素转媒体", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  harness.lark.resources.set("img_post_1", Buffer.from("post-image"));
  await harness.dispatcher.emit(
    dmEvent("om_post1", {
      message_type: "post",
      content: JSON.stringify({
        title: "周报",
        content: [
          [{ tag: "text", text: "第一段" }, { tag: "a", text: "链接", href: "https://example.com" }],
          [{ tag: "img", image_key: "img_post_1" }],
        ],
      }),
    }),
  );
  const message = harness.received[0]!;
  assert.equal(message.text, "周报\n第一段链接");
  const media = message.media ?? [];
  assert.equal(media.length, 1);
  assert.equal(media[0]!.kind, "image");
  assert.equal(harness.lark.resourceGets[0]?.file_key, "img_post_1");
});

test("sendText：按 4 000 字分块，p2p 用 open_id、群用 chat_id", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  const long = "a".repeat(9000);
  const result = await harness.adapter.sendText({ chatId: "oc_group1", text: long });
  assert.equal(harness.lark.creates.length, 3);
  assert.ok(harness.lark.creates.every((call) => call.receive_id_type === "chat_id"));
  assert.ok(harness.lark.creates.every((call) => call.msg_type === "text"));
  const joined = harness.lark.creates
    .map((call) => JSON.parse(call.content).text as string)
    .join("");
  assert.equal(joined, long);
  assert.ok(harness.lark.creates.every((call) => (JSON.parse(call.content).text as string).length <= 4000));
  assert.match(result.messageId ?? "", /^om_out_/);
  // p2p → open_id
  await harness.adapter.sendText({ chatId: "ou_bob", text: "单聊" });
  assert.equal(harness.lark.creates[3]?.receive_id_type, "open_id");
  assert.equal(harness.lark.creates[3]?.receive_id, "ou_bob");
  // union_id（on_ 前缀）→ union_id；其余（oc_ 等）按 chat_id。
  await harness.adapter.sendText({ chatId: "on_bob_union", text: "union 单聊" });
  assert.equal(harness.lark.creates[4]?.receive_id_type, "union_id");
  assert.equal(harness.lark.creates[4]?.receive_id, "on_bob_union");
});

test("normalize：bot open_id 未知时群 @ 按 mentions.length>0 降级判定", async () => {
  const harness = await createHarness();
  harness.lark.botInfo = { code: 0, bot: { app_name: "Paseo 机器人" } }; // 无 open_id
  await harness.adapter.start();
  assert.ok(harness.logs.some((entry) => entry.includes("open_id")));
  // 有 @ 即算被叫到（无法比对 open_id 时的兜底）。
  await harness.dispatcher.emit(
    groupEvent("om_m1", {
      content: JSON.stringify({ text: "@小明 在吗" }),
      mentions: [{ key: "@_user_1", id: { open_id: "ou_xiaoming" }, name: "小明" }],
    }),
  );
  assert.equal(harness.received[0]?.trigger.mention, true);
  // 无 @ 不触发。
  await harness.dispatcher.emit(groupEvent("om_m2"));
  assert.equal(harness.received[1]?.trigger.mention, false);
});

test("sendText：带 replyToId 走 reply 接口，thread 消息携带 reply_in_thread", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  const result = await harness.adapter.sendText({
    chatId: "oc_group1",
    threadId: "ot_topic1",
    text: "回复内容",
    replyToId: "om_root1",
  });
  assert.equal(harness.lark.creates.length, 0);
  assert.equal(harness.lark.replies.length, 1);
  assert.equal(harness.lark.replies[0]?.message_id, "om_root1");
  assert.equal(harness.lark.replies[0]?.reply_in_thread, true);
  assert.match(result.messageId ?? "", /^om_reply_/);
});

test("sendText：空文本不发送", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  const result = await harness.adapter.sendText({ chatId: "oc_group1", text: "   " });
  assert.deepEqual(result, {});
  assert.equal(harness.lark.creates.length, 0);
});

test("sendText：业务码非 0 抛用户可读错误", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  harness.lark.im.v1.message.create = async () => ({
    code: 99991663,
    msg: "permission denied",
  });
  await assert.rejects(
    harness.adapter.sendText({ chatId: "oc_group1", text: "hi" }),
    (error: unknown) => {
      assert.ok(error instanceof UserFacingError);
      assert.match(error.message, /补齐所需权限|开通所需权限/);
      return true;
    },
  );
});

test("流式进度：cardkit 卡片创建 → 发送卡片消息 → append/finish 更新 markdown", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  const { streamId } = await harness.adapter.streamStart!("oc_group1");
  assert.equal(harness.lark.cardCreates.length, 1);
  assert.equal(cardContent(harness.lark.cardCreates[0]), "⏳ 正在处理…");
  assert.equal(harness.lark.creates[0]?.msg_type, "interactive");
  assert.deepEqual(JSON.parse(harness.lark.creates[0]?.content ?? "{}"), {
    type: "card",
    data: { card_id: "card_v1_1" },
  });
  await harness.adapter.streamAppend!(streamId, "⏳ 8s · 检索中");
  const result = await harness.adapter.streamFinish!(streamId, "## 答案\n正文");
  assert.equal(harness.lark.cardUpdates.length, 2);
  assert.equal(harness.lark.cardUpdates[0]?.sequence, 1);
  assert.equal(harness.lark.cardUpdates[1]?.sequence, 2);
  assert.equal(cardContent(harness.lark.cardUpdates[1]?.data), "## 答案\n正文");
  assert.equal(result.messageId, harness.lark.creates[0] ? "om_out_1" : undefined);
});

test("流式进度：cardkit 无权限 → 警告并回退为直接发送", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  harness.lark.cardError = new Error("permission denied (code 40999)");
  const { streamId } = await harness.adapter.streamStart!("oc_group1");
  assert.match(harness.logs.join("\n"), /进度卡片创建失败/);
  assert.equal(harness.lark.creates.length, 0); // 未发卡片消息
  await harness.adapter.streamAppend!(streamId, "⏳ 1s");
  const result = await harness.adapter.streamFinish!(streamId, "最终答案");
  assert.equal(harness.lark.creates.length, 1);
  assert.equal(harness.lark.creates[0]?.msg_type, "text");
  assert.equal(JSON.parse(harness.lark.creates[0]?.content ?? "{}").text, "最终答案");
  assert.ok(result.messageId);
});

test("流式进度：卡片更新失败 → 置 broken 并在完成时直接发送", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  harness.lark.cardUpdateError = new Error("card update failed (99991663)");
  const { streamId } = await harness.adapter.streamStart!("oc_group1");
  await harness.adapter.streamAppend!(streamId, "⏳ 2s");
  assert.match(harness.logs.join("\n"), /进度卡片更新失败/);
  const result = await harness.adapter.streamFinish!(streamId, "兜底答案");
  const textCreates = harness.lark.creates.filter((call) => call.msg_type === "text");
  assert.equal(textCreates.length, 1);
  assert.equal(JSON.parse(textCreates[0]?.content ?? "{}").text, "兜底答案");
  assert.ok(result.messageId);
});

test("流式进度：超长回复卡片放不下 → 卡片提示 + 分条完整发送", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  const { streamId } = await harness.adapter.streamStart!("oc_group1");
  const big = "字".repeat(6000); // 18 000 字节 > 卡片安全预算
  const result = await harness.adapter.streamFinish!(streamId, big);
  const lastUpdate = harness.lark.cardUpdates.at(-1);
  assert.match(cardContent(lastUpdate?.data), /分条发送/);
  // 卡片消息 1 条 + 文本分条（6000 字 → 4000 + 2000）。
  const textCreates = harness.lark.creates.filter((call) => call.msg_type === "text");
  assert.equal(textCreates.length, 2);
  const joined = textCreates.map((call) => JSON.parse(call.content).text as string).join("");
  assert.equal(joined, big);
  assert.ok(result.messageId);
});

test("sendMedia：图片上传 → image_key → 图片消息；caption 追加文本", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  const imagePath = join(harness.dataDir, "out.png");
  await writeFile(imagePath, Buffer.from("out-png"));
  const result = await harness.adapter.sendMedia!({
    chatId: "oc_group1",
    kind: "image",
    path: imagePath,
    name: "示意图.png",
    caption: "看这张图",
  });
  assert.equal(harness.lark.creates[0]?.msg_type, "image");
  assert.deepEqual(JSON.parse(harness.lark.creates[0]?.content ?? "{}"), { image_key: "img_key_1" });
  assert.equal(harness.lark.creates[1]?.msg_type, "text");
  assert.equal(JSON.parse(harness.lark.creates[1]?.content ?? "{}").text, "看这张图");
  assert.ok(result.messageId);
});

test("sendMedia：文件超 30MB 拒绝发送", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  const bigPath = join(harness.dataDir, "big.bin");
  await writeFile(bigPath, Buffer.alloc(30 * 1024 * 1024 + 1));
  await assert.rejects(
    harness.adapter.sendMedia!({ chatId: "oc_group1", kind: "file", path: bigPath }),
    UserFacingError,
  );
  assert.equal(harness.lark.creates.length, 0);
});

test("addReaction：emoji 映射为飞书 emoji_type", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  await harness.adapter.addReaction!("om_msg1", "oc_group1", "👍");
  await harness.adapter.addReaction!("om_msg2", "oc_group1", "GET");
  assert.deepEqual(harness.lark.reactions, [
    { message_id: "om_msg1", emoji_type: "THUMBSUP" },
    { message_id: "om_msg2", emoji_type: "Get" },
  ]);
  await harness.adapter.acknowledgeMessage!("om_msg3", "oc_group1");
  assert.deepEqual(harness.lark.reactions.at(-1), { message_id: "om_msg3", emoji_type: "Get" });
  await assert.rejects(harness.adapter.addReaction!("om_msg1", "oc_group1", "看不懂"), UserFacingError);
});

test("leaveChat：以 app_id 身份移除机器人", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  await harness.adapter.leaveChat!("oc_group1");
  assert.deepEqual(harness.lark.memberDeletes, [
    { chat_id: "oc_group1", id_list: [BOT_OPEN_ID], member_id_type: "app_id" },
  ]);
});

test("testConnection：凭据有效返回 open_id，失败返回中文诊断", async () => {
  const harness = await createHarness();
  const ok = await harness.adapter.testConnection!();
  assert.equal(ok.ok, true);
  assert.match(ok.detail ?? "", /ou_bot111111111111111111111111/);
  harness.lark.botInfo = { code: 99991663, msg: "invalid" };
  const bad = await harness.adapter.testConnection!();
  assert.equal(bad.ok, false);
  assert.match(bad.detail ?? "", /App ID/);
});

test("isAdmin：群主与管理员名单命中", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  harness.lark.chatInfo = { owner_id: "ou_owner", user_manager_id_list: ["ou_admin"] };
  assert.equal(await harness.adapter.isAdmin!("oc_group1", "ou_admin"), true);
  assert.equal(await harness.adapter.isAdmin!("oc_group1", "ou_owner"), true);
  assert.equal(await harness.adapter.isAdmin!("oc_group1", "ou_other"), false);
});

test("重连：WS onError 走退避重启、重建 WSClient 实例并关闭旧实例，onReady 重置退避", async () => {
  const clients: FakeWS[] = [];
  let hooks: { onError?: (error: Error) => void; onReady?: () => void } = {};
  const harness = await createHarness({
    reconnectDelaysMs: [1],
    depsOverride: {
      wsClient: undefined,
      createWSClient: (params) => {
        hooks = params;
        const ws = new FakeWS();
        clients.push(ws);
        return ws;
      },
    },
  });
  await harness.adapter.start();
  assert.equal(clients.length, 1, `clients=${clients.length} logs=${JSON.stringify(harness.logs)}`);
  assert.equal(clients[0]!.starts, 1);
  hooks.onError?.(new Error("连接已断开"));
  const deadline = Date.now() + 1_000;
  while (clients.length < 2) {
    if (Date.now() > deadline) throw new Error("重连未创建新 WSClient 实例");
    await delay(1);
  }
  await clients[1]!.waitForStart(1);
  assert.equal(clients.length, 2, "重连必须重建 WSClient 实例，避免与 SDK 内部重连叠加双路");
  assert.equal(clients[0]!.closes, 1, "旧实例应显式关闭，阻断其内部自动重连");
  assert.equal(clients[1]!.starts, 1);
  assert.match(harness.logs.join("\n"), /重连/);
  assert.equal(harness.errors.some((error) => error.message === "连接已断开"), true);
  hooks.onReady?.();
});

test("abort：信号中止即清理 WS，后续事件不再投递", async () => {
  const controller = new AbortController();
  const harness = await createHarness({ signal: controller.signal });
  await harness.adapter.start();
  controller.abort();
  // abort 监听同步进入 stop()：teardown 先于第一个 await 执行。
  assert.equal(harness.ws.closes, 1);
  await harness.dispatcher.emit(dmEvent("om_after_abort"));
  assert.equal(harness.received.length, 0);
});

test("状态文件：own 消息 id 持久化，供重启后 replyToBot 判定", async () => {
  const harness = await createHarness();
  await harness.adapter.start();
  await harness.adapter.sendText({ chatId: "oc_group1", text: "会被记住" });
  const state = JSON.parse(await readFile(statePath(harness.dataDir), "utf8")) as {
    seen: string[];
    own: string[];
  };
  assert.equal(state.own.length, 1);
  assert.match(state.own[0] ?? "", /^om_out_/);
  assert.equal(harness.adapter.isOwnMessage!(state.own[0] ?? ""), true);
});
