# AG99live Stream Host

这是一个独立的 AstrBot 插件，负责直播场景的主持逻辑和 B 站输入连接，不接管 AG99live Adapter 的 TTS 或 Live2D 动作执行。

当前版本提供三件事：

- 按间隔向 Personal Runtime 提交一次“可以自然开启话题”的机会。最终是否说话由现有 Gate、Policy、Persona 和冷却规则决定。
- 通过 `blivedm` 直接连接 B 站 WebSocket，接收弹幕、礼物、高级留言和进入直播间事件，由 `ag99live_stream_input` 平台适配器直接提交到 AstrBot Core 事件队列；事件继续使用 OLV 的表现上下文来完成 Personal、Persona、TTS 和动作输出。
- 持久化已见过的 B 站 UID/昵称，用于识别新观众；不会保存 Cookie 或原始弹幕历史。
- 用户单独发送 `+` 或 `-` 时，不触发普通对话；当前只记录输入时间、评价值、来源会话和符号，写入 `motion_feedback.jsonl`，动作关联留到后续校准阶段。

AG99live 前端不再需要连接 B 站；插件生成的事件会经过 Core pipeline、Personal、Persona、TTS、分段输出和动作链路。主动话题也会优先通过同一个输入适配器提交。旧版插件配置保留了直接调用 OLV `input.text` 的迁移路径。`blivedm` 按其上游仓库的 Git 依赖安装，因为它没有发布可用的 PyPI 版本。

配置 `session_targets` 时填写 OLV 的 UMO，例如：

```text
olv_pet_adapter:FriendMessage:desktop-client
```

留空则使用 AstrBot Personal Runtime 已配置的目标会话。

主动话题默认提交到 `ag99live_stream` 输入适配器；如果平台配置使用了其他实例 ID，设置插件配置项 `stream_input_platform_id`。

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
  response_interval_seconds: 30
  max_batch_size: 8
```

`presentation_session_id` 是 OLV 桌宠表现端的客户端会话 ID。平台适配器会把 B 站事件提交到 Core 全局消息队列，输出继续由 OLV Adapter 表现。
