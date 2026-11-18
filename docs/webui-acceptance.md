# paseo-im WebUI 自测验收清单

前置：daemon 已重启并启用 WebUI（`features.webUi.enabled=true` 已写入 `~/.paseo/config.json`）。
浏览器打开 `http://127.0.0.1:6767/`。

## A. 插件加载与入口
1. 侧栏出现 **IM** 条目（icon: MessagesSquare）。
2. Settings → Plugins → paseo-im 状态 enabled、无 incompatible 提示（client bundle 编译成功才会如此）。
3. ⌘K/Ctrl+K Command Center 搜 "IM 机器人" 可见并跳转。

## B. IM 屏渲染
4. 打开 IM 屏：渠道列表 12 行（飞书/微信/钉钉/企微/企微应用/QQ/Slack/Telegram/Discord/WhatsApp/iMessage/Matrix），各显示 "未接入" 或 "n/m 在线"。
5. 底部 "通用设置" 分区渲染数字字段与忙时行为下拉（读宿主 defineSettings）。
6. 切换深色主题：文字仍用 foreground/foregroundMuted（无黑字黑底）。
7. 窄窗口（compact）：布局回退单列，无横向溢出。

## C. 连接流程（用 Telegram 做冒烟，凭据无效也能走通错误路径）
8. 点开 Telegram → 填连接名称 + 任意非法 token → "连接" → 出现中文可执行错误（如 getMe 校验失败提示），不白屏。
9. 连接卡片状态点为 "需要关注"（红），lastError 文案可见。
10. iMessage：点开渠道 → 尝试连接 → Linux 上报 "仅 macOS 支持" 类错误（平台门禁生效）。
11. WhatsApp/微信/企微机器人：详情页出现 "扫码配对" 分区（按钮可点，无凭据渠道字段为空）。

## D. 持久化与往返
12. 用 `paseo plugin logs paseo-im` 确认无未捕获异常；RPC 错误只出现在业务错误行。
13. `curl`/CLI 环回：`im.list` 返回 12 渠道（见下命令）。

```sh run
# 重启 daemon 以启用 WebUI（会终止当前 agent 会话）
paseo daemon restart --home "$PASEO_HOME"
```

## E. RPC 环回（不依赖浏览器）
```sh run
# 环回验证 im.list / im.choices（插件运行即通）
cd "$(git rev-parse --show-toplevel)" && timeout 60 node --input-type=module -e "
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { readFileSync } from 'node:fs';
import os from 'node:os';
const require_ = createRequire(process.cwd() + '/package.json');
const { DaemonClient } = await import(pathToFileURL(require_.resolve('@getpaseo/client/internal/daemon-client')).href);
const { createPaseoApi } = await import(pathToFileURL(require_.resolve('@getpaseo/client')).href);
const home = process.env.PASEO_HOME ?? `${os.homedir()}/.paseo`;
const lock = JSON.parse(readFileSync(home + '/paseo.pid', 'utf8'));
const url = 'ws://127.0.0.1:' + lock.listen.split(':').pop() + '/ws';
const client = new DaemonClient({ url, clientId: 'paseo-im-verify', clientType: 'cli', appVersion: '0.15.2', connectTimeoutMs: 8000,
  localCredential: () => readFileSync(home + '/local-credential', 'utf8').trim(),
  logger: { debug(){}, info(){}, warn(){}, error(){} } });
await client.connect();
const api = createPaseoApi(client);
const list = await client.invokePluginRpc('paseo-im', 'im.list', {});
console.log('im.list channels:', list.channels.length);
const choices = await client.invokePluginRpc('paseo-im', 'im.choices', {});
console.log('im.choices agents:', choices.agents.length, 'providers:', choices.providers.length);
await api.dispose(); await client.close();
"
```

## F. 飞书消息回执与进度卡（待实机复核）

14. 在已绑定去向的飞书私聊或群聊中向机器人发送一条普通任务消息。
15. 原消息先出现飞书 `GET` 表情回复；若表情接口暂时失败，日志出现「消息回执失败」，任务仍继续。
16. 随后出现一张 CardKit 进度卡；任务运行时同一张卡持续更新耗时/活动信息，不新增多张进度卡。
17. 任务完成后同一张卡更新为最终答案；超长答案继续沿用飞书适配器既有的卡片提示与分条发送行为。
18. 用另一个未实现 `acknowledgeMessage()` 的渠道复测一条消息，确认其原有进度模式和回复方式没有变化。

执行记录：代码与无凭据自动化测试已通过；真实飞书客户端显示效果尚未执行，不能标记实机 PASS。

---

## 验收执行记录（2026-11-07，Playwright headless + Chromium 158，第二 daemon 实例 :6768）

驱动：`docs/webui-acceptance/accept.mjs`（复跑方式：`cd /tmp/webui-accept && node accept.mjs`，playwright 1.65）。

| # | 检查项 | 结果 |
| --- | --- | --- |
| 1 | WebUI 首屏加载（daemon 托管 Expo 应用） | PASS |
| 2 | 侧栏 IM 入口（client 侧栏贡献渲染） | PASS |
| 3 | 渠道列表 12 渠道全部渲染（未接入 状态） | PASS |
| 4 | 通用设置分区 + 忙时行为（defineSettings 读宿主） | PASS |
| 5 | Telegram 非法凭据 → schema 拒绝文案「Bot Token 格式不正确…请从 @BotFather 重新复制完整 Token」 | PASS |
| 6 | 逐级返回渠道列表 | PASS |
| 7 | iMessage 渠道入口 + macOS 平台提示 | PASS |
| 8 | compact 视口（420px）渲染 | PASS |
| 9 | 无未捕获控制台错误 | PASS |

截图：`docs/webui-acceptance/shots/`（boot / im-screen / telegram-error / compact）。

### 验收期真实修复
1. **telegram 网络层错误文案中文化**（`server/channels/telegram/index.ts`）：`fetch failed` 等 DNS/网络错误现为「无法连接 Telegram API（网络错误）。请检查本机网络对 api.telegram.org 的可达性，连接将自动重试。」
2. **lark/baileys 运行时装载**（`server/channels/runtime-loader.ts`）：daemon 以裸函数作用域执行 CJS 包，`__dirname`/`import.meta` 皆不可用——两个 SDK 改为 `import(pathToFileURL(require.resolve(...)))` 运行时装载（锚定 `process.argv[1]` 所在插件目录）。

### 环境说明
- 第二 daemon：`PASEO_HOME=~/.paseo-webui`，端口 **http://127.0.0.1:6768**（WebUI 已开、插件已装、可直接浏览器打开复核）；停止：`PASEO_HOME="$HOME/.paseo-webui" paseo daemon stop`。
- 生产 daemon（:6767）未重启；其 WebUI 需 `paseo daemon restart --home "$PASEO_HOME"` 后在同地址生效。
- app.paseo.sh 托管路径：daemon relay 已启用（`relay.paseo.sh:443`），登录托管应用并配对目标 daemon 实例（serverId/主机名见 `paseo daemon status`）后验收面相同。

---

## QQ 实机验收记录（2026-11-07，生产 daemon :6767，真实凭据）

| 项 | 结果 |
| --- | --- |
| 网关连接（WS 会话恢复） | ✅ 日志「QQ 网关连接就绪」「会话已恢复」 |
| `im.test` / running / ready | ✅ RPC 实测 `{ok:true}` |
| 实机入站：规范化 + msgId 去重 + 落盘 | ✅ state.json job「hi」 |
| 准入（dm=everyone） | ✅ |
| 未绑定去向 → 中文提示回发 QQ | ✅ 用户端可见 |
| 绑定 workspace → 自动建 agent → 回复发回 QQ | ✅ **用户确认对话正常** |

### 实机调试修复（验收期）
1. 归档绑定轮换幂等键：死 agent 复用导致派发必败
2. 入站不预填 agentId：预填会绕过 prepare 轮换
3. `updateJob` 异步落盘：dispatch/failure 标记此前不写盘
4. 会话错误回复带真实 detail + 失败日志打原始堆栈
5. 幂等返回归档实例时随机键重建一次 + Router 全程日志
6. 多段模型 ID 按首斜杠切分：选完模型显示"未选择"

WebUI 侧两处界面级修复见上节（OptionPicker 滚动、choices 过滤）。可复用经验已整理至 `docs/gotchas.md`（daemon 打包约束 / 斜杠约定与选择器 / agent 绑定生命周期 / 调试工作法）。

---

## Telegram 实机验收记录（2026-11-07，生产 daemon :6767，真实凭据）

| 项 | 结果 |
| --- | --- |
| long-poll 连接建立 | ✅ 插件进程与 `149.154.166.110:443`（api.telegram.org IPv4）ESTABLISHED，getUpdates 轮询静默（无错误即健康） |
| 实机入站：bot 私信 | ✅ 用户 DM「hi」→ normalize → 桥接至绑定 agent 会话（sharedHistory）|
| 双通道汇合同一会话 | ✅ QQ 与 Telegram 消息进同一 agent，各自原生回复 |

### 实机调试修复：出站网络 IPv6 黑洞（核心价值记录）

**症状**：连接创建成功但持续「无法连接 Telegram API（网络错误）」自动重试；同机 `curl https://api.telegram.org` 正常——极易误判为凭据或代码问题。

**根因**（完整证据链）：
1. `api.telegram.org` 双栈（A + AAAA）；WSL2 宿主 **IPv6 路由黑洞**：`curl -6` 超时、`curl -4` 0.9s 通。
2. node v24 域名拨号命中 AAAA 后，happy-eyeballs 竞速 ~400ms 即以 ETIMEDOUT **整体失败、不回退 v4**——`fetch(域名)` 必挂。
3. 鉴别实验序列：`curl -4`/`curl -6` 分家族 → `node net.connect(裸 IP)` 通 → `fetch(IP)` TLS 走完（证书名不符为预期）→ 定位「域名→选址」层，非网络非凭据。

**修复**（`index.server.ts` 引导期两行，进程级覆盖全部渠道）：

```ts
dns.setDefaultResultOrder("ipv4first");
net.setDefaultAutoSelectFamily(false);
```

仅有 AAAA 的域名仍走 IPv6，无副作用；slack/matrix 等双栈渠道同机部署一并免疫。完整排查顺序沉淀于 `docs/gotchas.md` §5.5。

### 配对渠道自动进入修复（同日）

微信/WhatsApp/企微创建连接后自动拉起扫码配对（二维码直出）；接入表单「扫码配对」行提示改为准确描述。微信 iLink 配对会话已实测：`start()`→`poll()` 返回 `pending` + 二维码 dataURL（30s 长轮询为协议设计）。
