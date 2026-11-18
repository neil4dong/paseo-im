import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { PaseoApi } from "@getpaseo/client";
import { Bridge, DeferredTimeoutError, type BridgeDeps } from "../server/bridge.ts";
import { Router } from "../server/routing.ts";
import { AgentQueue } from "../server/queue.ts";
import { StateStore } from "../server/state-store.ts";
import { DeferredRegistry } from "../server/deferred.ts";
import { emptyPolicy, connectionSchema, type Connection, type Policy, type Target } from "../shared/config.ts";
import { imSettings } from "../shared/settings.ts";
import type { AdapterContext, ChannelAdapter } from "../server/channels/types.ts";
import type { NormalizedMessage, OutboundText, SendResult } from "../shared/model.ts";

const CONNECTION_ID = "00000000-0000-4000-8000-000000000001";
const AGENT_ID = "agent-1";

interface TestHarness {
  bridge: Bridge;
  queue: AgentQueue;
  state: StateStore;
  deferred: DeferredRegistry;
  sent: (OutboundText & { messageId?: string })[];
  agentCalls: { send: { text: string; options?: unknown }[]; respond: unknown[] };
  deps: BridgeDeps;
  waitResults: unknown[];
  logs: string[];
  events: string[];
}

function makeAdapter(
  sent: (OutboundText & { messageId?: string })[],
  noReceipt = false,
  acknowledgement = false,
  events: string[] = [],
  acknowledgementError = false,
): ChannelAdapter {
  return {
    channel: "telegram",
    capabilities: {
      progress: acknowledgement ? "stream" : "none",
      markdown: "plain",
      uploadImage: false,
      uploadFile: false,
      reactions: false,
      typing: false,
      maxChunkChars: 4000,
      threads: false,
    },
    async acknowledgeMessage(): Promise<void> {
      events.push("acknowledge");
      if (acknowledgementError) throw new Error("acknowledgement unavailable");
    },
    async streamStart(): Promise<{ streamId: string }> {
      events.push("progress:start");
      return { streamId: "stream-1" };
    },
    async streamFinish(_streamId: string, text: string): Promise<{ messageId?: string }> {
      events.push(`progress:finish:${text}`);
      return { messageId: "progress-1" };
    },
    async sendText(outbound: OutboundText): Promise<SendResult> {
      if (noReceipt) {
        // wecom-app 类渠道：sendText 无平台回执 id。
        sent.push({ ...outbound });
        return {};
      }
      const messageId = `out-${sent.length + 1}`;
      sent.push({ ...outbound, messageId });
      return { messageId };
    },
    async stop() {},
    async start() {},
  };
}

function makeApi(agentCalls: { send: { text: string; options?: unknown }[]; respond: unknown[] }, timelineEntries: unknown[] = []) {
  let refetchCount = 0;
  const handle = {
    id: AGENT_ID,
    async refresh() {
      return {
        agent: {
          id: AGENT_ID,
          status: "idle",
          archivedAt: null,
          activeTurn: false,
          workspaceId: "ws-1",
          cwd: "/tmp",
          pendingPermissions: pendingPermission(),
        },
      };
    },
    async send(text: string, options?: unknown) {
      agentCalls.send.push({ text, options });
    },
    waitForFinish: async () => nextWaitResult(),
    async respondToPermission(args: unknown) {
      agentCalls.respond.push(args);
    },
    timeline: {
      refetch: async () => {
        refetchCount += 1;
        const entries =
          timelineEntries.length === 0
            ? []
            : timelineEntries.map((entry, index) => {
                const base = entry as { seqEnd?: number; item?: unknown };
                return index === 0 ? { ...base, seqEnd: 5 + refetchCount } : entry;
              });
        return { epoch: "e1", entries, error: null };
      },
    },
  };
  const api = {
    agents: { ref: (_id: string) => handle },
  } as unknown as PaseoApi;
  return api;
}

let pendingPermission: () => unknown = () => null;
let nextWaitResult: () => unknown = () => ({
  status: "idle",
  final: null,
  error: null,
  lastMessage: "模型回答",
});

function connection(target: Target | null): Connection {
  return connectionSchema.parse({
    id: CONNECTION_ID,
    channel: "telegram",
    name: "测试",
    enabled: true,
    target,
    policy: emptyPolicy(),
    bindings: {},
    revision: 0,
    username: "paseo_bot",
    botUserId: "1",
  }) as Connection;
}

async function harness(options: {
  target?: Target | null;
  policyOperators?: string[];
  policyOverrides?: Partial<Policy>;
  timelineEntries?: unknown[];
  noReceipt?: boolean;
  acknowledgement?: boolean;
  acknowledgementError?: boolean;
}): Promise<TestHarness> {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-bridge-"));
  const sent: (OutboundText & { messageId?: string })[] = [];
  const agentCalls = { send: [] as { text: string; options?: unknown }[], respond: [] as unknown[] };
  const logs: string[] = [];
  const events: string[] = [];
  const state = new StateStore(CONNECTION_ID, root);
  await state.load();
  const queue = new AgentQueue();
  const deferred = new DeferredRegistry({ sweep: async () => false, intervalMs: 60_000 });
  const config = connection(options.target === undefined ? { kind: "agent", agentId: AGENT_ID, sharedHistory: false } : options.target);
  if (options.policyOperators || options.policyOverrides) {
    config.policy = {
      ...config.policy,
      ...(options.policyOperators ? { operators: options.policyOperators } : {}),
      ...(options.policyOverrides ?? {}),
    };
  }
  const deps: BridgeDeps = {
    connectionId: CONNECTION_ID,
    config: () => config,
    adapter: () =>
      makeAdapter(
        sent,
        options.noReceipt === true,
        options.acknowledgement === true,
        events,
        options.acknowledgementError === true,
      ),
    state,
    queue,
    daemon: () => Promise.resolve(makeApi(agentCalls, options.timelineEntries ?? [])),
    settings: async () => imSettings.schema.parse({}),
    deferred: { track: (id) => deferred.track(id), resolve: (id) => deferred.resolve(id) },
    signal: new AbortController().signal,
    log: (level, message) => logs.push(`${level}: ${message}`),
  };
  const bridge = new Bridge(deps, new Router(deps.daemon));
  return { bridge, queue, state, deferred, sent, agentCalls, deps, waitResults: [], logs, events };
}

function dmMessage(overrides: Partial<NormalizedMessage> = {}): NormalizedMessage {
  return {
    channel: "telegram",
    connectionId: CONNECTION_ID,
    kind: "dm",
    chatId: "100",
    senderId: "user-1",
    messageId: "m1",
    timestamp: Date.now(),
    text: "帮我看看构建日志",
    trigger: { mention: false, replyToBot: false, command: false },
    ...overrides,
  };
}

test("私聊消息全链路：派发→等待→分段回复", async () => {
  const h = await harness({});
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.equal(h.agentCalls.send.length, 1);
  assert.ok(h.agentCalls.send[0]!.text.includes("帮我看看构建日志"));
  assert.ok(
    h.sent.some((out) => out.text === "模型回答"),
    `sent=${JSON.stringify(h.sent.map((out) => out.text))} logs=${JSON.stringify(h.logs)} job=${JSON.stringify(h.state.job("m1"))}`,
  );
  const job = h.state.job("m1");
  assert.equal(job?.delivery, "done");
  assert.equal(job?.agentId, AGENT_ID);
  h.deferred.stop();
});

test("适配器实现渠道回执时：先回应原消息，再启动并完成进度流", async () => {
  const h = await harness({ acknowledgement: true });
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.deepEqual(h.events, ["acknowledge", "progress:start", "progress:finish:模型回答"]);
  assert.equal(h.state.job("m1")?.delivery, "done");
  h.deferred.stop();
});

test("消息回执失败只告警，不阻断进度和最终结果", async () => {
  const h = await harness({ acknowledgement: true, acknowledgementError: true });
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.deepEqual(h.events, ["acknowledge", "progress:start", "progress:finish:模型回答"]);
  assert.ok(h.logs.some((line) => line.includes("消息回执失败：acknowledgement unavailable")));
  assert.equal(h.state.job("m1")?.delivery, "done");
  h.deferred.stop();
});

test("同 messageId 幂等去重", async () => {
  const h = await harness({});
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.equal(h.agentCalls.send.length, 1);
  h.deferred.stop();
});

test("未授权私聊不派发", async () => {
  const h = await harness({
    policyOperators: ["someone-else"],
    policyOverrides: { directMessages: "operators" },
  });
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.equal(h.agentCalls.send.length, 0);
  assert.ok(h.sent.length === 0);
  h.deferred.stop();
});

test("未配置去向提示可读错误", async () => {
  const h = await harness({ target: null });
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.ok(
    h.sent.some((out) => out.text.includes("尚未配置消息去向")),
    `sent=${JSON.stringify(h.sent.map((out) => out.text))} logs=${JSON.stringify(h.logs)}`,
  );
  const job = h.state.job("m1");
  assert.ok(job?.failure);
  h.deferred.stop();
});

test(
  "异步提问：通知→回复→代答",
  { timeout: 20_000 },
  async () => {
  const questionRequest = {
    id: "req-1",
    kind: "question",
    name: "request_user_input_async",
    input: { questions: [{ header: "选择", question: "用哪个方案？", options: [{ label: "方案 A" }, { label: "方案 B" }] }] },
  };
  let calls = 0;
  pendingPermission = () => [questionRequest];
  nextWaitResult = () => {
    calls += 1;
    if (calls === 1) {
      return { status: "permission", final: { pendingPermissions: [questionRequest], status: "running" }, error: null, lastMessage: null };
    }
    if (calls === 2) {
      return { status: "permission", final: { pendingPermissions: [questionRequest], status: "running" }, error: null, lastMessage: null };
    }
    return { status: "idle", final: null, error: null, lastMessage: "已按方案 A 继续" };
  };
  // The question reply path reads the timeline for the follow-up turn boundary.
  const timelineEntries = [{ seqEnd: 5, item: { type: "user_message", messageId: "later" } }];
  const h = await harness({ timelineEntries });
  await h.bridge.handle(dmMessage());
  await new Promise<void>((resolve) => {
    queueMicrotask(() => resolve());
  });
  // waitForTask polls at 1s between async-question rounds; two rounds run.
  const pending = h.queue.idle();
  await pending;
  const questionMessage = h.sent.find((out) => out.text.includes("需要澄清"));
  assert.ok(questionMessage, "应向 IM 通知问题");
  assert.equal(questionMessage?.messageId, "out-1");

  await h.bridge.handle(
    dmMessage({
      messageId: "reply-1",
      text: "1",
      replyTo: { id: "out-1" },
    }),
  );
  await h.queue.idle();
  const respond = h.agentCalls.respond[0] as { requestId?: string; response?: { updatedInput?: { answers?: Record<string, string> } } } | undefined;
  assert.ok(
    respond,
    `respond=${JSON.stringify(h.agentCalls.respond)} sent=${JSON.stringify(h.sent.map((out) => ({ text: out.text, id: out.messageId })))} jobs=${JSON.stringify(Object.values(h.state.get().jobs))} logs=${JSON.stringify(h.logs)}`,
  );
  assert.equal(respond?.requestId, "req-1");
  assert.deepEqual(respond?.response?.updatedInput?.answers, { 选择: "方案 A" });
  const followUp = h.sent.find((out) => out.text.includes("已按方案 A 继续"));
  assert.ok(followUp, "应把后续回合的最终回答送达 IM");
  const sourceJob = h.state.job("m1");
  assert.equal(sourceJob?.delivery, "done");
  pendingPermission = () => null;
  nextWaitResult = () => ({ status: "idle", final: null, error: null, lastMessage: "模型回答" });
  h.deferred.stop();
  },
);

test("等待超时进入补发登记", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  nextWaitResult = () => Promise.withResolvers<unknown>().promise;
  const h = await harness({});
  await h.bridge.handle(dmMessage());
  // Flush microtasks so the delivery (and its timeout timer) has started.
  await new Promise<void>((resolve) => {
    setImmediate(() => resolve());
  });
  t.mock.timers.tick(31 * 60_000);
  await h.queue.idle();
  const job = h.state.job("m1");
  assert.ok(job?.deadlineAt, "应登记补发时限");
  assert.equal(h.deferred.count(), 1);
  assert.ok(h.sent.some((out) => out.text.includes("等待模型回复超时")));
  nextWaitResult = () => ({ status: "idle", final: null, error: null, lastMessage: "模型回答" });
  h.deferred.stop();
});

test("DeferredTimeoutError 可识别", () => {
  const error = new DeferredTimeoutError();
  assert.ok(error instanceof DeferredTimeoutError);
});

test(
  "异步提问兜底：无回执 id 渠道回复引用按唯一未答问题代答",
  { timeout: 20_000 },
  async () => {
    const questionRequest = {
      id: "req-1",
      kind: "question",
      name: "request_user_input_async",
      input: { questions: [{ header: "选择", question: "用哪个方案？", options: [{ label: "方案 A" }, { label: "方案 B" }] }] },
    };
    let calls = 0;
    pendingPermission = () => [questionRequest];
    nextWaitResult = () => {
      calls += 1;
      if (calls <= 2) {
        return { status: "permission", final: { pendingPermissions: [questionRequest], status: "running" }, error: null, lastMessage: null };
      }
      return { status: "idle", final: null, error: null, lastMessage: "已按方案 A 继续" };
    };
    const timelineEntries = [{ seqEnd: 5, item: { type: "user_message", messageId: "later" } }];
    // noReceipt：sendText 不返回平台 id → question.messageId 为 null。
    const h = await harness({ timelineEntries, noReceipt: true });
    await h.bridge.handle(dmMessage());
    await h.queue.idle();
    const questionMessage = h.sent.find((out) => out.text.includes("需要澄清"));
    assert.ok(questionMessage, "应向 IM 通知问题");
    assert.equal(questionMessage?.messageId, undefined, "该渠道无平台回执 id");

    // 用户引用回复卡片：平台侧引用 id 不在 jobs 表内 → 直接匹配未命中 → 兜底。
    await h.bridge.handle(
      dmMessage({
        messageId: "reply-nr-1",
        text: "1",
        replyTo: { id: "platform-quote-id-not-in-jobs" },
      }),
    );
    await h.queue.idle();
    const respond = h.agentCalls.respond[0] as { requestId?: string; response?: { updatedInput?: { answers?: Record<string, string> } } } | undefined;
    assert.ok(respond, `兜底应代答唯一未答问题 respond=${JSON.stringify(h.agentCalls.respond)} jobs=${JSON.stringify(Object.values(h.state.get().jobs))}`);
    assert.equal(respond?.requestId, "req-1");
    assert.deepEqual(respond?.response?.updatedInput?.answers, { 选择: "方案 A" });
    pendingPermission = () => null;
    nextWaitResult = () => ({ status: "idle", final: null, error: null, lastMessage: "模型回答" });
    h.deferred.stop();
  },
);

test("异步提问兜底：多于一个未答问题时不猜，按新任务处理", async () => {
  const h = await harness({});
  // 直接放置两个未答问题 job，破坏“同连接唯一未答问题”前置条件。
  h.state.ensureJob("q-job-1", {
    messageId: "q-job-1",
    conversationKey: "dm:100",
    chatId: "100",
    senderId: "user-1",
    createdAt: Date.now() - 1_000,
    agentId: AGENT_ID,
    question: {
      requestId: "req-q1",
      fields: [{ header: "选择", text: "用哪个方案？", options: ["方案 A", "方案 B"] }],
      messageId: null,
      notified: true,
      status: "pending",
    },
  });
  h.state.ensureJob("q-job-2", {
    messageId: "q-job-2",
    conversationKey: "dm:100",
    chatId: "100",
    senderId: "user-1",
    createdAt: Date.now(),
    agentId: AGENT_ID,
    question: {
      requestId: "req-q2",
      fields: [{ header: "选择", text: "用哪个方案？", options: ["方案 A", "方案 B"] }],
      messageId: null,
      notified: true,
      status: "pending",
    },
  });

  await h.bridge.handle(
    dmMessage({
      messageId: "reply-multi-1",
      text: "1",
      replyTo: { id: "platform-quote-id-not-in-jobs" },
    }),
  );
  await h.queue.idle();
  assert.equal(h.agentCalls.respond.length, 0, "多个未答问题不得兜底");
  const job = h.state.job("reply-multi-1");
  assert.ok(job);
  assert.equal(job.questionReply, null, "未命中兜底，按新任务建 job");
  h.deferred.stop();
});
