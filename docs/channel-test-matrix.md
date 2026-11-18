# 无凭证可测项穷举矩阵（渠道自测验收）

原则：**无凭据 ≠ 不可测**。所有渠道的传输层均可注入假实现（Recorder/Fake），协议交互在假边界上穷举；实机联调（真收发）单列，不属于本矩阵验收范围。
用法：新渠道接入照此分类穷举；每项标注覆盖状态 ✅（有断言用例）/ ⚠️（间接或部分覆盖）/ ❌（可补未补）/ —（渠道本身无此能力，标 n/a）。
基线：365+ 用例（node:test），全绿于审查基准提交。

## 通用分类（12 渠道 × 11 类）

| 类 | 无凭证可测项 |
| --- | --- |
| A 凭据校验 | 缺字段/格式错/机密拆分（SECRET_KEYS）/身份不一致/平台门禁 |
| B 传输生命周期 | 握手序列、瞬时错误退避 [1,3,5,10,30]、致命错误终止、断线恢复、stop 幂等与资源清理、signal 中止 |
| C 入站规范化 | dm/group 判定、@提及（多种形态）、回复机器人（含 ring/get 兜底）、媒体下载落盘（上限/白名单）、自身消息与噪音过滤、msgId 去重与重启语义、时间戳回退 |
| D 发送与分块 | fence-aware 分块（maxChunkChars）、markdown 渲染/降级、messageId 返回、回复引用、空文本、媒体回传（上传→发送、大小上限）、失败中文包装 |
| E 进度展示 | 适配器声明的原消息回执 → edit/stream/typing 对应渠道能力的方法序列、降级回退、broken/超长路径；回执失败不阻断任务 |
| F 配对/登录 | QR 状态机（pending→scanned→ok/failed/expired）、凭据产物字段、auth 目录加固 |
| G 权限与群管理 | isAdmin 数据源解析、leaveChat/移除、群成员事件→审批流 |
| H 状态持久化 | cursor/seen/会话绑定/token 绑定（换凭据清空）、重启续跑 |
| I 限速与错误分类 | 429/retry_after、401/403/409 终止、网络错误重试、业务码映射中文 |
| J 协议专项 | 渠道独有特性（见各渠道行） |
| K 实机联调 | 真收发/真凭据——**不在本矩阵范围**，凭据到位后按 `docs/webui-acceptance.md` 清单执行 |

## 各渠道覆盖

### telegram（25 用例）
A✅ B✅（退避/409 终止/stop）C✅（批量去重入库+媒体落盘+callback_query）D✅（分块/HTML 渲染×6/回复）E⚠️（编辑在主链路覆盖，无独立用例）F— G❌（isAdmin/leaveChat/sendChatAction/setMyCommands 兜底未测）H✅（游标+seen）I✅（429/409/401）J✅（HTML 转义/链接协议过滤/语言标注）
**缺口**：sendPhoto/sendDocument multipart 独立断言；sendChatAction；setMyCommands 失败仅告警路径；isAdmin/leaveChat。

### slack（30 用例）
A✅ B✅（退避/断线重连恢复）C✅（过滤/replyToBot/去重落盘）D✅（mrkdwn/fence/thread_ts/messageId/三步上传/markdown 回退/editMessage not_modified）E✅（startStream→append→stop + 回退）F— G— H✅ I✅（429/invalid_auth）J✅（mrkdwn 转换/chunks）
**缺口**：interactive envelope 只 ACK 分支；completeUploadExternal 失败分支。

### discord（42 用例）
A✅ B✅（握手/心跳看门狗/4004/瞬断重连/stop/abort）C✅（dm/thread 创建与路由/reply/lookup 降级/snowflake）D✅（分块/message_reference 首块/空文本）E⚠️（editMessage 已测）F— G✅（leaveChat）H✅（去重）I✅（4004/心跳超时）J✅（原生线程）
**缺口**：isAdmin（power levels 未实现也未测——discord 管理员判定待补实现+测试）；sendMedia 大附件上限；typing 独立断言已有（901 行）记 ⚠️。

### feishu（31 用例）
A✅（含 bot info 业务码/网络失败）B✅（WS 退避/abort 清理）C✅（p2p/群@占位替换/reply 双路径/去重重启/图片/文件/post 富文本）D✅（分块/reply_in_thread/空文本/业务码/sendMedia 30MB）E✅（cardkit 全序列/无权限降级/broken/超长分条）F— G✅（addReaction/leaveChat/isAdmin 群主+管理员）H✅（own 持久化）I⚠️（重连✅，无限速用例）J⚠️（命令菜单注册未测）
新增桥接顺序断言：飞书声明 `receiptReaction: "GET"` 后，通用桥接层先对原消息添加 GET 表情，再创建 CardKit 进度卡；回执失败只告警，仍创建并完成进度卡。其他渠道未声明该能力，行为不变。
**缺口**：菜单注册兜底；媒体下载受 imageMaxDownloadMb 约束的用例；语音占位。

### dingtalk（23 用例）
A✅ B✅（退避/监视器/stop 清理）C✅（私聊/双 robotCode 匹配/isAtAll/回复/富文本/媒体两路下载）D✅（分块/webhook 过期+被拒双回退/无会话错误/sendMedia）E✅（AI Card 全序列/创建失败回退）F— G— H✅（去重+会话记录持久化）I✅ J✅（cardMarkdown/ack/testConnection）
**缺口**：主动接口（oToMessages/groupMessages）参数形状断言；access_token 过期刷新竞态。

### qq（23 用例，实机 ✅）
A✅（testConnection/token endpoint）B✅（4004 停连/瞬时退避）C✅（c2c/群@ 剥离/reply ring/附件白名单/去重重启/自身/robot 成员事件）D✅（markdown 被动+msg_seq/40034090 回退/3800 fence/溢出转主动+提示/主动路由+群配额/typing 仅 c2c）E—（QQ 无编辑）F— G— H✅ I⚠️（4004/瞬时；无 429 概念 ✓）J✅
**缺口**：face 表情标签解析、音频 asr 引用文本。

### wecom（30 用例）
A✅（缺 Secret/Bot ID）B✅ C✅（图片/文件/混排/语音占位+无转写/去重重启/噪音）D✅（双预算分块/markdown/中文包装/sendMedia/空文件与超限拒绝）E✅（缓存帧流/无帧回退/卡过期回退）F⚠️（pairing 已实现，**测试未列**）G— H✅ I⚠️ J⚠️（replyTemplateCard 菜单卡未测）
**缺口**：pairing 状态机用例；菜单模板卡；群/私聊发送路由显式断言。

### wecom-app（16 用例）
A✅（签名不符/畸形载荷）B✅（健康退避/多连接共享监听/stop 路由移除）C✅（XML 解密/图片两路/语音识别/MsgId 重试去重回空响应）D✅（2048 字节码点安全分块/被动首块+主动余块/sendMedia）E✅（被动窗口）F— G— H✅ I⚠️ J✅（加解密往返/token 跨请求缓存）
**缺口**：JSON 明文分支；quote 引用归一；streamEnabled=false 路径。

### whatsapp（20 用例，实机未验证）
A— B✅（loggedOut 终止/瞬时退避重建）C✅（self-chat/媒体降级/回声抑制/LID 双身份）D✅（方言渲染/嵌套围栏保留/fence 分块/edit key/sendMedia/composing+已读）E✅（edit 信封）F✅（QR→scanned→ok 写凭据目录/登出→failed/restartRequired 重建）G— H⚠️（去重；auth 目录 0700/0600 加固未测）I✅ J✅
**缺口**：auth 目录权限位断言；媒体上传失败分支；群准入（@g.us 非 open 模式拒绝）。

### weixin（32 用例）
A✅（缺凭据/baseUrl 白名单/ret -14）B✅（退避/监控恢复/凭据失效终止）C✅（语音转写/图片 AES 落盘/文件 hex aeskey/ref_msg/不受信任地址）D✅（分块+context_token/网络重试同 client_id/拒绝中文/uncertain/上传全流程/文件 item/上传拒绝）E✅（typing 全序列+保活+发送前 status 2）F✅（QR pending+qrDataUrl/scaned→confirmed）G— H✅（context_token 持久化+换凭据清空）I✅ J✅
**缺口**：notifystart/notifystop；二维码 payload 格式。

### matrix（23 用例）
A✅（authMode 推导/缺凭据/whoami 不一致/401）B✅（首次 sync 不回放/密码登录持久化复用/邀请自动加入/瞬时退避/401 停止）C✅（dm/群 @/pill/回复/thread/媒体/自身/噪音）D✅（HTML 白名单/回复引用/16000 fence/sendMedia mxc）E✅（m.replace 流/typing 立即+周期+发送后停）F— G✅（leaveChat/isAdmin power_levels）H✅（会话持久化复用/m.direct）I✅ J✅（splitFenceAware/renderHtml 净化）
**缺口**：多房间 dm 判定边界；invite inviter 校验（authorized 模式未实现 ✓n/a）。

### imessage（26 用例，实机未验证——需 macOS）
A✅（testConnection 三态/非 darwin 门禁）B✅（启动错误三态/瞬时退避）C✅（文本/attributedBody 解码/附件/回环前缀/服务过滤/时间回退）D✅（AppleScript 转义/前缀幂等/校验/权限错误）E— F— G— H✅（cursor/seen/重启续跑）I✅ J✅（typedstream）
**缺口**：同账号多设备 handle 边界；chat.db schema 变更容错。

### 引擎/框架层（27 用例）
✅ files 原子写/WriteChain/sleep、policy 矩阵、queue 串行与中止、fence 分块、registry 乐观锁、state 作业/去重环/问题单、bridge 全链路/提问代答/幂等去重/超时补发/适配器声明回执顺序与失败降级。
**缺口**：`manager.save` 配置往返与 revision 冲突（registry 层已测，manager 层未）；`runtime-loader` 并发首载竞态。~~choices 过滤单测~~ → 已补 `tests/choices.test.ts`（4 用例：过滤不可用/缺 id 丢弃/modes 兜底/label 回退）。

## 汇总

- 已覆盖最强：discord/feishu/weixin/slack（30+ 用例且含降级路径）
- 需补测优先级：P0 = manager.choices 过滤单测（防回归验收期真 bug）；P1 = whatsapp auth 目录权限、wecom pairing 状态机、telegram sendMedia/isAdmin；P2 = 各渠道列出的其余缺口
- 实机联调：QQ ✅；telegram/slack/discord/feishu/dingtalk/wecom/wecom-app/whatsapp/weixin/matrix/imessage 凭据到位后按 `docs/webui-acceptance.md` 清单执行
