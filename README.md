# AG99live Stream Host

这是一个独立的 AstrBot 插件，负责直播场景的主持逻辑和 B 站输入连接，不接管 AG99live Adapter 的 TTS 或 Live2D 动作执行。

当前版本提供三件事：

- 按间隔向 Personal Runtime 提交一次“可以自然开启话题”的机会。最终是否说话由现有 Gate、Policy、Persona 和冷却规则决定。
- 通过 `blivedm` 直接连接 B 站 WebSocket，接收弹幕、礼物、高级留言和进入直播间事件，由 `ag99live_stream_input` 平台适配器直接提交到 AstrBot Core 事件队列；事件继续使用 OLV 的表现上下文来完成 Personal、Persona、TTS 和动作输出。
- 持久化已见过的 B 站 UID/昵称，用于识别新观众；不会保存 Cookie 或原始弹幕历史。
- 用户单独发送 `+` 或 `-` 时，不触发普通对话；当前只记录输入时间、评价值、来源会话和符号，写入 `motion_feedback.jsonl`，动作关联留到后续校准阶段。

AG99live 前端不再需要连接 B 站；真实观众事件经过输入适配器和 Core pipeline。定时话题只提交结构化观察事实，由 Personal 判断是否表达，再经 Persona、TTS 和 AG99 输出；不再伪造一条观众消息。`blivedm` 使用随插件分发的依赖修订构建，来源和构建方法见 [vendor/README.md](vendor/README.md)。安装时从 AstrBot 根目录运行 `uv pip install --python .venv/Scripts/python.exe -r data/plugins/astrbot_plugin_ag99live_stream_host/requirements.txt`；不要重新安装上游 master，它的旧版 aiohttp/Brotli 约束与宿主冲突。

`proactive_mode` 决定输入适配器所绑定的 AG99 表现目标使用哪一种主动话题来源：`daily` 使用原有主动聊天插件，`live` 使用直播空闲机会，`off` 关闭这两类定时话题。默认 `daily`，其他平台会话不受影响。修改配置后重载插件。`live_reply_cooldown_seconds`、`live_no_action_cooldown_seconds`、`live_daily_output_limit` 分别控制直播冷却、静默后评估间隔和每日次数；日常、直播次数分别持久化。

直播输入保留自身来源平台，但通过表现目标绑定配置、人格和角色历史。直播只读取明确标为公开的消息；日常私聊和旧的无标记记录不会自动进入直播提示词。尚未分级的记忆快照不注入直播；记忆和静态扩展槽须声明 `meta.visibility=public`，动态提示词提供者须声明 `prompt_message_visibility=public`，才能进入公开场景。日常模式可接续角色已有历史。历史不会批量迁移或合并。主动表达与普通对话使用相同的人格基础材料和有预算的历史选择，不再固定只取两轮。AG99 主动输出在同一回合发送文本和音频，并等待前端完成回执后结算；前端离线不预生成待补发话题。模式切换撤销待执行机会，已经交接到输出的分段继续完成；用户仍可打断。

配置 `session_targets` 时填写 OLV 的 UMO，例如：

```text
olv_pet_adapter:FriendMessage:desktop-client
```

留空则使用 AstrBot Personal Runtime 已配置的目标会话。

`stream_input_platform_id` 指定直播输入适配器实例；主动话题的实际观察目标由该实例的 `presentation_platform_id` 和 `presentation_session_id` 确定。

启用直接连接时至少配置：

```text
bilibili_live_enabled: true
bilibili_live_room_id: "你的房间号"
```

Cookie 只填写 `SESSDATA` 的值；不填写时仍可连接，但观众昵称和 UID 可能脱敏。

源代码同步到当前 AstrBot 插件目录：

```powershell
.\deploy_stream_host.ps1 -DryRun
.\deploy_stream_host.ps1
```

脚本默认目标是 `C:\Users\Administrator\Downloads\AstrBot\data\plugins`，也可以通过 `-PluginsRoot` 指定其他 AstrBot 实例。

正式输入平台适配器需要在 AstrBot 的平台配置中启用：

```yaml
- type: ag99live_stream_input
  id: ag99live_stream
  enable: true
  room_id: "你的房间号"
  cookie: "可选的 SESSDATA"
  presentation_platform_id: olv_pet_adapter
  presentation_session_id: desktop-client
  max_batch_size: 8
```

`presentation_session_id` 是 OLV 桌宠表现端的客户端会话 ID。平台适配器会把 B 站事件提交到 Core 全局消息队列，输出继续由 OLV Adapter 表现。

空闲时收到事件立即提交，最多合并 `max_batch_size` 条已到达的事件。优先处理礼物和上舰，其次是醒目留言、普通弹幕和进场。AI 生成及桌宠播放期间，礼物和上舰会暂存，空闲后再由角色在回复开头感谢。待感谢事件与其他输入共用 `max_queue_size` 容量，礼物最多保留 5 分钟；容量用尽时优先淘汰非礼物事件。忙碌时收到的弹幕、醒目留言和进场事件直接丢弃。主动话题和礼物感谢都不会打断当前回复。

普通弹幕仅在同批候选超过剩余名额时排序。每位观众保留一条候选，优先名额按本房粉丝牌等级及本次插件运行期间收到的付费礼物、醒目留言贡献综合选择，约四分之一名额从剩余观众中随机抽取。仅剩一个名额时有 25% 概率走随机，其余按优先级选择。贡献只用于选取，不查询历史榜单，重载后重新累计。缺少粉丝牌或贡献记录的观众仍可被抽中。

日志 `live_input.received`、`live_input.batch_ready`、`live_input.submitted`、`live_input.playback_finished` 记录接收、排队、入队及播放结束；`batch_id` 沿用为 Core turn ID。`live_input.ready` 记录忙碌期间丢弃的数量。等待结束回执设有 300 秒故障上限，超时中止该轮次，防止断线或丢失回执导致永久忙碌；正常收到回执即恢复接收。

## 文本共读

1. 启用 `ag99live_stream_input`，让 `stream_input_platform_id` 指向该实例，并绑定已连接的 OLV 角色会话。只做共读时可留空 B 站房间号。
2. 打开 AG99live 控制台的「文本共读」，选择对应角色。粘贴原文或导入 UTF-8 TXT / Markdown，预览并保存。
3. 将「OBS 共读地址」填入同机的现有 OBS 浏览器源，保留地址末尾的 `#reading=` 参数。点击「开始」，等面板显示「OBS 已展示」，即可点击「评论当前段」。
4. 手动模式由主播评论和切段。勾选「自动评论并翻页」后，角色空闲才评论，收到播放回执或明确静默且达到展示停留时间后才切段。可随时暂停，重启后从原段以暂停状态恢复。

原文由 OBS 展示。角色通过 Core 队列进入 Personal 表达，结合已展示的内容分享理解或感受。共读期间，观众互动仍按直播输入的既有规则处理，定时闲聊暂缓。评论失败会停在当前段，主播可以继续并重试。

本功能需要同时更新 AstrBot Core、Stream Host 和 AG99live Adapter。方案见 [文本共读计划书](docs/co-reading-plan.md)。
