"""Input-only AstrBot platform adapter for AG99live live events."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from uuid import uuid4

from astrbot.api import logger
from astrbot.api.message_components import Plain
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
    register_platform_adapter,
)

from .bilibili_live import BilibiliLiveRuntime

_PLATFORM_TYPE = "ag99live_stream_input"
_PRESENTATION_PLATFORM_NAME = "olv_pet_adapter"
_PROTOCOL_VERSION = "v2"

_plugin_context: Any | None = None
_viewer_recorder: Callable[[str, str], Awaitable[bool]] | None = None


def set_stream_platform_context(
    context: Any,
    viewer_recorder: Callable[[str, str], Awaitable[bool]] | None = None,
) -> None:
    global _plugin_context, _viewer_recorder
    _plugin_context = context
    _viewer_recorder = viewer_recorder


@register_platform_adapter(
    _PLATFORM_TYPE,
    "AG99live Stream 输入适配器",
    default_config_tmpl={
        "room_id": "",
        "cookie": "",
        "presentation_platform_id": "olv_pet_adapter",
        "presentation_session_id": "desktop-client",
        "response_interval_seconds": 30,
        "max_batch_size": 8,
        "max_queue_size": 80,
    },
    adapter_display_name="AG99live Stream Input",
)
class AG99liveStreamInputPlatform(Platform):
    """Receive live events and commit them to AstrBot's global event queue."""

    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)
        self.config = platform_config
        self.settings = platform_settings or {}
        self.platform_id = str(platform_config.get("id") or _PLATFORM_TYPE).strip()
        self.presentation_platform_id = str(
            platform_config.get("presentation_platform_id") or _PRESENTATION_PLATFORM_NAME
        ).strip()
        self.presentation_session_id = str(
            platform_config.get("presentation_session_id") or "desktop-client"
        ).strip()
        self._runtime: BilibiliLiveRuntime | None = None
        self._seen_viewers: set[str] = set()

    def meta(self) -> PlatformMetadata:
        metadata = PlatformMetadata(
            name=_PRESENTATION_PLATFORM_NAME,
            description=(
                "AG99live Stream 输入平台；输入进入 Core，表现输出交给 OLV。"
            ),
            id=self.platform_id,
            support_proactive_message=False,
            support_personal_runtime=False,
        )
        metadata.adapter_display_name = "AG99live Stream Input"
        return metadata

    async def run(self) -> None:
        room_id = str(self.config.get("room_id") or "").strip()
        if not room_id.isdecimal() or int(room_id) <= 0:
            raise RuntimeError("ag99live_stream_input_room_id_invalid")
        self._runtime = BilibiliLiveRuntime(
            room_id=room_id,
            cookie=str(self.config.get("cookie") or ""),
            response_interval_seconds=self.config.get(
                "response_interval_seconds", 30
            ),
            max_batch_size=self.config.get("max_batch_size", 8),
            max_queue_size=self.config.get("max_queue_size", 80),
            target_resolver=lambda: (self.presentation_session_id,),
            submit_observation=self._commit_live_message,
            record_viewer=self._record_viewer,
        )
        await self._runtime.start()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            raise
        finally:
            runtime, self._runtime = self._runtime, None
            if runtime is not None:
                await runtime.stop()

    async def terminate(self) -> None:
        runtime, self._runtime = self._runtime, None
        if runtime is not None:
            await runtime.stop()

    async def send_by_session(self, session, message_chain) -> None:
        del session, message_chain
        logger.debug("AG99live Stream Input is input-only; output uses OLV.")

    async def submit_live_item(self, item: Mapping[str, object]) -> None:
        """Commit a plugin-generated live event through this input adapter."""
        await self._commit_live_message(self.presentation_session_id, item)

    async def _record_viewer(self, name: str, uid: str) -> bool:
        if _viewer_recorder is not None:
            return await _viewer_recorder(name, uid)
        key = str(uid or "").strip() or f"name:{str(name or '').strip().casefold()}"
        if not key or key in self._seen_viewers:
            return False
        self._seen_viewers.add(key)
        return True

    async def _commit_live_message(
        self, _session: str, item: Mapping[str, object]
    ) -> None:
        payload = item.get("payload")
        if not isinstance(payload, Mapping):
            return
        kind = str(item.get("kind") or "live_danmaku").strip()
        excerpt = str(payload.get("ambient_message_excerpt") or "").strip()
        if not excerpt:
            return
        event_text = _format_live_message(kind, payload)
        message_id = uuid4().hex
        message = AstrBotMessage()
        message.type = MessageType.FRIEND_MESSAGE
        names = payload.get("participant_names")
        first_name = (
            str(names[0]).strip()
            if isinstance(names, (list, tuple)) and names
            else "直播观众"
        )
        ids = payload.get("participant_ids")
        first_uid = (
            str(ids[0]).strip()
            if isinstance(ids, (list, tuple)) and ids
            else "live-viewer"
        )
        message.sender = MessageMember(user_id=first_uid, nickname=first_name)
        message.self_id = self.platform_id
        message.session_id = self.presentation_session_id
        message.message_id = message_id
        message.message = [Plain(text=event_text)]
        message.message_str = event_text
        message.raw_message = {
            "origin": _PLATFORM_TYPE,
            "event_kind": kind,
            "payload": dict(payload),
            "received_at": time.time(),
            "presentation_session_id": self.presentation_session_id,
        }
        olv_adapter = _resolve_olv_adapter(self.presentation_platform_id)
        if olv_adapter is None:
            raise RuntimeError("ag99live_stream_input_olv_adapter_unavailable")
        event = olv_adapter.build_stream_input_event(
            message,
            platform_meta=self.meta(),
            session_id=self.presentation_session_id,
        )
        event.set_extra("live_source_platform_id", self.platform_id)
        event.set_extra("live_event_kind", kind)
        event.set_extra("live_presentation_target", {
            "platform_id": self.presentation_platform_id,
            "session_id": self.presentation_session_id,
        })
        self.commit_event(event)


def _resolve_olv_adapter(platform_id: str) -> Any | None:
    context = _plugin_context
    getter = getattr(context, "get_platform_inst", None)
    if not callable(getter):
        return None
    return getter(platform_id)


def _format_live_message(kind: str, payload: Mapping[str, object]) -> str:
    excerpt = str(payload.get("ambient_message_excerpt") or "").strip()
    names = payload.get("participant_names")
    participant_text = (
        "、".join(str(item).strip() for item in names if str(item).strip())
        if isinstance(names, (list, tuple))
        else ""
    )
    requirement = {
        "live_gift": "这是礼物事件，请自然表达感谢并结合角色动作表现，不要虚构礼物金额。",
        "live_advanced_chat": "这是高优先级直播留言，请优先理解并自然回应，不要逐字复述内部标签。",
        "live_viewer_entered": "这是进入直播间事件；如果适合欢迎新观众，请简短自然地欢迎。",
        "live_topic": "这是主动话题机会，请结合近期上下文自然表达，允许保持安静。",
    }.get(
        kind,
        "这是普通直播消息，请选择有价值的内容简短回应，不要机械复读。",
    )
    return "\n".join(
        (
            "[AG99live 直播消息]",
            f"事件类型：{kind}",
            f"参与用户：{participant_text or '未知'}",
            f"事件内容：{excerpt}",
            f"处理要求：{requirement}",
        )
    )


__all__ = ["AG99liveStreamInputPlatform", "set_stream_platform_context"]
