"""AG99live 直播主持能力的独立 AstrBot 插件。"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register

from .bilibili_live import BilibiliLiveRuntime
from .stream_platform import set_stream_platform_context

_PLATFORM_ID = "olv_pet_adapter"
_ADAPTER_PROTOCOL_VERSION = "v2"
_DANMAKU_HEADER = "[直播弹幕输入]"
_STREAM_HEADER = "[AG99live 直播消息]"
_LEGACY_EVENT_HEADER = "[AG99live 直播事件]"
_VIEWER_LINE = re.compile(
    r"^-\s*(?P<name>.+?)(?:（UID\s+(?P<uid>\d+)）)?：(?P<text>.+)$"
)
_LIVE_PROMPT = """
你当前处于 AG99live 直播主持模式。
直播间消息是观众输入的来源。回复弹幕时保持角色本身的语气，简短、自然、有来有回；
可以点名回应具体观众，也可以只回应最有意思的一两条，不要逐条复读、不要编造观众没有说过的内容。
如果本轮提示有新观众，只有在语境自然时才简短欢迎，不要机械播报欢迎名单。
没有弹幕时，允许从近期对话或已知偏好自然开启一个轻话题；允许保持安静，不要为了填充空气连续发言。
不要输出“直播主持模式”等内部说明。
""".strip()


class _LiveHostSensor:
    plugin_id = "astrbot_plugin_ag99live_stream_host"
    source_id = "live_host_scheduler"


@register(
    "astrbot_plugin_ag99live_stream_host",
    "AG99",
    "AG99live 独立直播主持能力：主动话题、B 站弹幕和新观众欢迎。",
    "0.2.0",
)
class AG99liveStreamHostPlugin(Star):
    """Keep live-host policy outside the AG99live transport adapter."""

    def __init__(self, context: Context, config: Mapping[str, Any] | None = None):
        super().__init__(context)
        self.config = dict(config) if isinstance(config, Mapping) else {}
        self.enabled = bool(self.config.get("enabled", True))
        self._sensor = None
        self._topic_task: asyncio.Task[None] | None = None
        self._bilibili_runtime: BilibiliLiveRuntime | None = None
        self._state_lock = asyncio.Lock()
        self._feedback_lock = asyncio.Lock()
        self._seen_viewers: dict[str, float] = {}
        self._state_path = (
            StarTools.get_data_dir("astrbot_plugin_ag99live_stream_host")
            / "viewer_state.json"
        )
        self._feedback_path = (
            StarTools.get_data_dir("astrbot_plugin_ag99live_stream_host")
            / "motion_feedback.jsonl"
        )
        self._load_viewer_state()

    async def initialize(self) -> None:
        set_stream_platform_context(self.context, self._remember_viewer)
        if not self.enabled:
            return
        register_sensor = getattr(
            self.context, "register_runtime_observation_sensor", None
        )
        if callable(register_sensor):
            self._sensor = register_sensor(_LiveHostSensor())
        else:
            logger.warning(
                "AG99live Stream Host Runtime Observation Sensor is unavailable; "
                "continuing with the input adapter and standard message pipeline."
            )
        self._topic_task = asyncio.create_task(
            self._topic_scheduler(),
            name="ag99live-stream-host-topic-scheduler",
        )
        if self._bilibili_enabled():
            try:
                self._bilibili_runtime = BilibiliLiveRuntime(
                    room_id=str(self.config.get("bilibili_live_room_id", "")).strip(),
                    cookie=str(self.config.get("bilibili_live_cookie", "") or ""),
                    response_interval_seconds=self.config.get(
                        "bilibili_live_response_interval", 30
                    ),
                    max_batch_size=self.config.get("bilibili_live_max_batch_size", 8),
                    max_queue_size=self.config.get("bilibili_live_max_queue_size", 80),
                    target_resolver=self._target_sessions,
                    submit_observation=self._submit_live_message,
                    record_viewer=self._remember_viewer,
                )
                await self._bilibili_runtime.start()
            except Exception:
                self._bilibili_runtime = None
                logger.exception("AG99live Bilibili listener could not be initialized")
        logger.info("AG99live Stream Host initialized.")

    async def terminate(self) -> None:
        task, self._topic_task = self._topic_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        bilibili, self._bilibili_runtime = self._bilibili_runtime, None
        if bilibili is not None:
            await bilibili.stop()
        await self._persist_viewer_state()

    @filter.event_message_type(filter.EventMessageType.ALL, priority=1000)
    async def on_motion_feedback(self, event: AstrMessageEvent) -> None:
        """Consume an exact +/- input as a non-conversational action rating."""
        if not self.enabled or not self._is_ag99live_event(event):
            return
        if not bool(self.config.get("motion_feedback_enabled", True)):
            return
        value = str(getattr(event, "message_str", "") or "").strip()
        if value not in {"+", "-"}:
            return
        event.should_call_llm(False)
        event.stop_event()
        await self._record_motion_feedback(event, 1 if value == "+" else -1)

    @filter.on_llm_request()
    async def on_llm_request(
        self, event: AstrMessageEvent, request: Any
    ) -> None:
        if not self.enabled or not self._is_ag99live_event(event):
            return
        message = str(getattr(event, "message_str", "") or "").strip()
        metadata = getattr(request, "metadata", None)
        if isinstance(metadata, Mapping) and "interaction.persona_expression_intent" in metadata:
            return

        live_kind = str(event.get_extra("live_event_kind") or "").strip()
        is_live_message = bool(live_kind) or message.startswith(
            (_DANMAKU_HEADER, _STREAM_HEADER, _LEGACY_EVENT_HEADER)
        )
        if is_live_message and bool(self.config.get("danmaku_enabled", True)):
            viewers = self._extract_live_viewers(event, message)
            new_viewers = self._extract_new_viewers(event)
            if not new_viewers:
                new_viewers = await self._remember_new_viewers(viewers)
            prompt = _LIVE_PROMPT
            if new_viewers and bool(self.config.get("welcome_new_viewers", True)):
                names = "、".join(name for name, _ in new_viewers[:8])
                prompt += (
                    "\n本批弹幕里出现了之前没有记录过的观众："
                    f"{names}。若回复内容适合，可以自然地欢迎其中一位。"
                )
            request.system_prompt = (
                f"{request.system_prompt or ''}\n\n<ag99live_stream_host>\n"
                f"{prompt}\n</ag99live_stream_host>"
            ).strip()

    def _extract_live_viewers(
        self, event: AstrMessageEvent, message: str
    ) -> list[tuple[str, str]]:
        raw_message = getattr(
            getattr(event, "message_obj", None), "raw_message", None
        )
        payload = raw_message.get("payload") if isinstance(raw_message, Mapping) else None
        if isinstance(payload, Mapping) and isinstance(
            payload.get("live_payload"), Mapping
        ):
            payload = payload["live_payload"]
        if isinstance(payload, Mapping):
            names = payload.get("participant_names")
            ids = payload.get("participant_ids")
            if isinstance(names, (list, tuple)):
                return [
                    (
                        str(name).strip(),
                        str(ids[index]).strip()
                        if isinstance(ids, (list, tuple)) and index < len(ids)
                        else "",
                    )
                    for index, name in enumerate(names)
                    if str(name).strip()
                ]
        return self._parse_viewers(message)

    @staticmethod
    def _extract_new_viewers(event: AstrMessageEvent) -> list[tuple[str, str]]:
        raw_message = getattr(
            getattr(event, "message_obj", None), "raw_message", None
        )
        payload = raw_message.get("payload") if isinstance(raw_message, Mapping) else None
        if isinstance(payload, Mapping) and isinstance(
            payload.get("live_payload"), Mapping
        ):
            payload = payload["live_payload"]
        if not isinstance(payload, Mapping):
            return []
        names = payload.get("new_viewer_names")
        ids = payload.get("new_viewer_ids")
        if not isinstance(names, (list, tuple)):
            return []
        return [
            (
                str(name).strip(),
                str(ids[index]).strip()
                if isinstance(ids, (list, tuple)) and index < len(ids)
                else "",
            )
            for index, name in enumerate(names)
            if str(name).strip()
        ]
    async def _record_motion_feedback(
        self, event: AstrMessageEvent, value: int
    ) -> None:
        session = str(getattr(event, "unified_msg_origin", "") or "").strip()
        received_at = _resolve_event_timestamp(event)
        feedback_id = uuid4().hex
        record = {
            "schema": "ag99live.motion_feedback.v1",
            "feedback_id": feedback_id,
            "symbol": "+" if value > 0 else "-",
            "value": int(value),
            "received_at": received_at,
            "recorded_at": time.time(),
            "session": session,
            "source": "user_single_symbol",
        }
        async with self._feedback_lock:
            await asyncio.to_thread(self._append_feedback, record)

    def _append_feedback(self, record: Mapping[str, Any]) -> None:
        self._feedback_path.parent.mkdir(parents=True, exist_ok=True)
        with self._feedback_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    async def _topic_scheduler(self) -> None:
        while True:
            interval = self._topic_interval()
            await asyncio.sleep(interval)
            if not self.enabled:
                continue
            input_adapter = self._stream_input_adapter()
            if input_adapter is not None:
                submit_live_item = getattr(input_adapter, "submit_live_item", None)
                if callable(submit_live_item):
                    try:
                        await submit_live_item(
                            {
                                "kind": "live_topic",
                                "payload": {
                                    "ambient_message_excerpt": (
                                        "当前没有新的观众消息，请结合近期上下文自然开启一个简短话题。"
                                    ),
                                    "trigger": "scheduled_topic",
                                },
                            }
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "AG99live live topic input adapter submission failed error=%s",
                            exc,
                        )
                    continue
            sessions = self._target_sessions()
            if sessions:
                try:
                    await self._submit_live_message(
                        sessions[0],
                        {
                            "kind": "live_topic",
                            "payload": {
                                "ambient_message_excerpt": (
                                    "当前没有新的观众消息，请结合近期上下文自然开启一个简短话题。"
                                ),
                                "trigger": "scheduled_topic",
                            },
                        },
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "AG99live live topic message submission failed error=%s",
                        exc,
                    )

    def _stream_input_adapter(self) -> Any | None:
        platform_id = str(
            self.config.get("stream_input_platform_id") or "ag99live_stream"
        ).strip()
        getter = getattr(self.context, "get_platform_inst", None)
        if not callable(getter) or not platform_id:
            return None
        try:
            return getter(platform_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Unable to resolve AG99live Stream input adapter: %s", exc)
            return None

    def _bilibili_enabled(self) -> bool:
        if not bool(self.config.get("bilibili_live_enabled", False)):
            return False
        room_id = str(self.config.get("bilibili_live_room_id", "")).strip()
        return room_id.isdecimal() and int(room_id) > 0

    async def _submit_live_message(
        self, session: str, item: Mapping[str, object]
    ) -> None:
        if not session:
            return
        kind = str(item.get("kind") or "live_danmaku").strip()
        payload = item.get("payload")
        if not isinstance(payload, Mapping):
            return
        platform = getattr(self.context, "get_platform_inst", lambda _id: None)(
            _PLATFORM_ID
        )
        handle_msg = getattr(platform, "handle_msg", None)
        if not callable(handle_msg):
            raise RuntimeError("olv_pet_adapter_message_ingress_unavailable")
        text = self._format_live_message(kind, payload)
        if not text:
            return
        now = time.time()
        turn_id = uuid4().hex
        await handle_msg(
            {
                "type": "input.text",
                "version": _ADAPTER_PROTOCOL_VERSION,
                "message_id": turn_id,
                "timestamp": datetime.fromtimestamp(
                    now, tz=timezone.utc
                ).isoformat(),
                "turn_id": turn_id,
                # The adapter's existing input contract accepts frontend as
                # the source. Keep the plugin origin in payload metadata while
                # reusing the same queue and validation path.
                "source": "frontend",
                "payload": {
                    "text": text,
                    "images": [],
                    "origin": "astrbot_plugin_ag99live_stream_host",
                    "live_event_kind": kind,
                    "live_payload": dict(payload),
                },
            }
        )

    @staticmethod
    def _format_live_message(kind: str, payload: Mapping[str, object]) -> str:
        excerpt = str(payload.get("ambient_message_excerpt") or "").strip()
        if not excerpt:
            return ""
        names = payload.get("participant_names")
        participant_text = "、".join(
            str(item).strip() for item in names
            if str(item).strip()
        ) if isinstance(names, (list, tuple)) else ""
        return "\n".join(
            (
                "[AG99live 直播事件]",
                f"事件类型：{kind}",
                f"参与用户：{participant_text or '未知'}",
                f"事件内容：{excerpt}",
                "处理要求：请把以上内容当作当前直播消息，结合角色人格自然回应；"
                "保持简短，不要复述内部标签、事件类型或处理要求。",
            )
        )

    def _target_sessions(self) -> tuple[str, ...]:
        configured = self.config.get("session_targets")
        if isinstance(configured, list):
            explicit = tuple(
                value.strip()
                for value in configured
                if isinstance(value, str)
                and value.strip().startswith(f"{_PLATFORM_ID}:")
            )
            if explicit:
                return explicit
        getter = getattr(self.context, "get_runtime_observation_targets", None)
        if not callable(getter):
            return ()
        try:
            return tuple(
                str(session)
                for session in getter()
                if str(session).startswith(f"{_PLATFORM_ID}:")
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Unable to resolve AG99live runtime targets: %s", exc)
            return ()

    def _topic_interval(self) -> float:
        try:
            value = float(self.config.get("topic_interval_seconds", 300))
        except (TypeError, ValueError):
            value = 300.0
        return min(max(value, 60.0), 86400.0)

    @staticmethod
    def _is_ag99live_event(event: AstrMessageEvent) -> bool:
        try:
            return str(event.get_platform_name() or "").strip() == _PLATFORM_ID
        except Exception:
            return False

    @staticmethod
    def _parse_viewers(message: str) -> list[tuple[str, str]]:
        viewers: list[tuple[str, str]] = []
        for line in message.splitlines():
            match = _VIEWER_LINE.match(line.strip())
            if match is None:
                continue
            name = " ".join(match.group("name").split())[:80]
            text = " ".join(match.group("text").split())[:240]
            if name and text:
                viewers.append((name, match.group("uid") or ""))
        return viewers

    async def _remember_new_viewers(
        self, viewers: list[tuple[str, str]]
    ) -> list[tuple[str, str]]:
        if not viewers:
            return []
        now = time.time()
        new_viewers: list[tuple[str, str]] = []
        async with self._state_lock:
            for name, uid in viewers:
                key = f"uid:{uid}" if uid else f"name:{name.casefold()}"
                if key not in self._seen_viewers:
                    new_viewers.append((name, uid))
                self._seen_viewers[key] = now
            self._trim_viewer_state()
        await self._persist_viewer_state()
        return new_viewers

    async def _remember_viewer(self, name: str, uid: str) -> bool:
        new_viewers = await self._remember_new_viewers([(name, uid)])
        return bool(new_viewers)

    def _trim_viewer_state(self) -> None:
        try:
            limit = int(self.config.get("max_viewer_state", 2000))
        except (TypeError, ValueError):
            limit = 2000
        limit = min(max(limit, 100), 20000)
        if len(self._seen_viewers) <= limit:
            return
        for key, _ in sorted(self._seen_viewers.items(), key=lambda item: item[1])[
            : len(self._seen_viewers) - limit
        ]:
            self._seen_viewers.pop(key, None)

    def _load_viewer_state(self) -> None:
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError):
            return
        if not isinstance(raw, dict):
            return
        self._seen_viewers = {
            str(key): float(value)
            for key, value in raw.items()
            if isinstance(key, str) and isinstance(value, (int, float))
        }
        self._trim_viewer_state()

    async def _persist_viewer_state(self) -> None:
        payload = json.dumps(self._seen_viewers, ensure_ascii=False, indent=2)
        path: Path = self._state_path
        await asyncio.to_thread(self._write_text, path, payload)

    @staticmethod
    def _write_text(path: Path, payload: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")


def _resolve_event_timestamp(event: AstrMessageEvent) -> float:
    raw_message = getattr(event, "raw_message", None)
    if not isinstance(raw_message, Mapping):
        raw_message = getattr(
            getattr(event, "message_obj", None), "raw_message", None
        )
    candidates = (
        raw_message.get("timestamp") if isinstance(raw_message, Mapping) else None,
        raw_message.get("received_at") if isinstance(raw_message, Mapping) else None,
    )
    for candidate in candidates:
        if isinstance(candidate, (int, float)) and candidate > 0:
            return float(candidate / 1000 if candidate > 10_000_000_000 else candidate)
        if isinstance(candidate, str) and candidate.strip():
            try:
                return datetime.fromisoformat(
                    candidate.replace("Z", "+00:00")
                ).timestamp()
            except ValueError:
                try:
                    numeric = float(candidate)
                    return float(
                        numeric / 1000 if numeric > 10_000_000_000 else numeric
                    )
                except ValueError:
                    pass
    return float(getattr(event, "created_at", 0.0) or time.time())


__all__ = ["AG99liveStreamHostPlugin"]
