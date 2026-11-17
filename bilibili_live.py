"""Backend-owned Bilibili live input for the AG99live Stream Host plugin."""

from __future__ import annotations

import asyncio
import http.cookies
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Mapping
from uuid import uuid4

from astrbot.api import logger

try:
    import aiohttp
    import blivedm
    import blivedm.models.web as web_models
except ImportError:  # pragma: no cover - exercised by the optional dependency check.
    aiohttp = None
    blivedm = None
    web_models = None


TargetResolver = Callable[[], tuple[str, ...]]
ObservationSubmitter = Callable[[str, Mapping[str, object]], Awaitable[None]]
ViewerRecorder = Callable[[str, str], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class LiveInput:
    kind: str
    name: str
    uid: str
    text: str = ""
    received_at: float = field(default_factory=time.time)
    received_monotonic: float = field(default_factory=time.monotonic)


class BilibiliLiveRuntime:
    """Own one Bilibili websocket client and turn events into observations."""

    def __init__(
        self,
        *,
        room_id: str,
        cookie: str,
        max_batch_size: int,
        max_queue_size: int,
        target_resolver: TargetResolver,
        submit_observation: ObservationSubmitter,
        record_viewer: ViewerRecorder,
        can_accept_input: Callable[[], bool] = lambda: True,
    ) -> None:
        self.room_id = int(room_id)
        self.cookie = cookie.strip()
        self.max_batch_size = max(1, min(int(max_batch_size), 32))
        self._queue: asyncio.Queue[LiveInput] = asyncio.Queue(
            maxsize=max(16, min(int(max_queue_size), 2048))
        )
        self._target_resolver = target_resolver
        self._submit_observation = submit_observation
        self._record_viewer = record_viewer
        self._can_accept_input = can_accept_input
        self._processing = False
        self._dropped_while_busy = 0
        self._session: aiohttp.ClientSession | None = None
        self._client = None
        self._flush_task: asyncio.Task[None] | None = None
        self._closed = False
        self.last_received_at = time.monotonic()

    @staticmethod
    def dependency_available() -> bool:
        return blivedm is not None and aiohttp is not None and web_models is not None

    async def start(self) -> None:
        if not self.dependency_available():
            raise RuntimeError("blivedm_dependency_missing")
        if self._client is not None:
            return
        self._closed = False
        self._session = aiohttp.ClientSession()
        cookies = http.cookies.SimpleCookie()
        if self.cookie:
            cookies["SESSDATA"] = self.cookie
            cookies["SESSDATA"]["domain"] = "bilibili.com"
            self._session.cookie_jar.update_cookies(cookies)
        self._client = blivedm.BLiveClient(self.room_id, session=self._session)
        self._client.set_handler(_Handler(self))
        self._client.start()
        self._flush_task = asyncio.create_task(
            self._flush_loop(),
            name=f"ag99live-bilibili-flush-{self.room_id}",
        )
        logger.info("AG99live Bilibili listener started room=%s", self.room_id)

    async def stop(self) -> None:
        self._closed = True
        flush_task, self._flush_task = self._flush_task, None
        if flush_task is not None:
            flush_task.cancel()
            await asyncio.gather(flush_task, return_exceptions=True)
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.stop_and_close()
            except Exception:  # noqa: BLE001
                logger.exception("AG99live Bilibili client shutdown failed")
        session, self._session = self._session, None
        if session is not None and not session.closed:
            await session.close()

    def enqueue_danmaku(self, name: str, uid: int, text: str) -> None:
        self._enqueue(
            LiveInput(
                kind="live_danmaku",
                name=_clean_text(name, 80) or "观众",
                uid=str(uid or "").strip(),
                text=_clean_text(text, 220),
            )
        )

    def enqueue_enter(self, name: str, uid: int) -> None:
        self._enqueue(
            LiveInput(
                kind="live_viewer_entered",
                name=_clean_text(name, 80) or "观众",
                uid=str(uid or "").strip(),
            )
        )

    def enqueue_gift(self, name: str, uid: int, text: str) -> None:
        self._enqueue(
            LiveInput(
                kind="live_gift",
                name=_clean_text(name, 80) or "观众",
                uid=str(uid or "").strip(),
                text=_clean_text(text, 220),
            )
        )

    def enqueue_advanced_chat(self, name: str, uid: int, text: str) -> None:
        self._enqueue(
            LiveInput(
                kind="live_advanced_chat",
                name=_clean_text(name, 80) or "观众",
                uid=str(uid or "").strip(),
                text=_clean_text(text, 220),
            )
        )

    def _enqueue(self, item: LiveInput) -> None:
        if self._closed or not item.name:
            return
        self.last_received_at = time.monotonic()
        if self._processing or not self._can_accept_input():
            self._dropped_while_busy += 1
            return
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(item)
            except asyncio.QueueEmpty:
                return
            except asyncio.QueueFull:
                logger.warning("AG99live Bilibili input queue is full; event dropped")
        logger.debug(
            "DIAG live_input.received room=%s kind=%s received_at=%.3f queue_size=%s",
            self.room_id,
            item.kind,
            item.received_at,
            self._queue.qsize(),
        )

    async def _flush_loop(self) -> None:
        while True:
            first = await self._queue.get()
            self._processing = True
            batch = [first]
            try:
                # Take only the already-arrived burst. Never wait to fill it,
                # and never replay unselected or busy-period input afterward.
                while not self._queue.empty():
                    item = self._queue.get_nowait()
                    if len(batch) < self.max_batch_size:
                        batch.append(item)
                if self._can_accept_input():
                    await self._flush_batch(batch)
            finally:
                while not self._queue.empty():
                    self._queue.get_nowait()
                logger.info(
                    "DIAG live_input.ready room=%s dropped_while_busy=%s",
                    self.room_id,
                    self._dropped_while_busy,
                )
                self._dropped_while_busy = 0
                self._processing = False

    @property
    def is_processing(self) -> bool:
        return self._processing

    async def _flush_batch(self, batch: list[LiveInput]) -> None:
        batch_id = uuid4().hex
        started_at = time.monotonic()
        oldest_received = min(item.received_monotonic for item in batch)
        excerpts: list[str] = []
        names: list[str] = []
        ids: list[str] = []
        new_names: list[str] = []
        new_ids: list[str] = []
        kinds: set[str] = set()
        for item in batch:
            is_new = await self._record_viewer(item.name, item.uid)
            prefix = "新观众 " if is_new else ""
            if is_new:
                new_names.append(item.name)
                new_ids.append(item.uid)
            if item.kind == "live_viewer_entered":
                excerpts.append(f"{prefix}{item.name} 进入了直播间。")
            elif item.text:
                excerpts.append(f"{prefix}{item.name}：{item.text}")
            names.append(item.name)
            if item.uid:
                ids.append(item.uid)
            kinds.add(item.kind)
        excerpt = "\n".join(excerpts).strip()[:256]
        if not excerpt:
            return
        payload = {
            "batch_id": batch_id,
            "live_input_received_at": min(item.received_at for item in batch),
            "ambient_message_excerpt": excerpt,
            "participant_names": tuple(dict.fromkeys(names))[:16],
            "participant_ids": tuple(dict.fromkeys(ids))[:16],
            "new_viewer_names": tuple(dict.fromkeys(new_names))[:16],
            "new_viewer_ids": tuple(new_ids)[:16],
            "message_count": len(batch),
            "trigger": "bilibili_live",
            "event_kinds": tuple(sorted(kinds)),
        }
        if "live_advanced_chat" in kinds:
            kind = "live_advanced_chat"
        elif "live_gift" in kinds:
            kind = "live_gift"
        elif kinds == {"live_viewer_entered"}:
            kind = "live_viewer_entered"
        else:
            kind = "live_danmaku"
        logger.info(
            "DIAG live_input.batch_ready room=%s batch_id=%s kind=%s count=%s "
            "oldest_wait_ms=%.1f prepare_ms=%.1f queue_size=%s",
            self.room_id,
            batch_id,
            kind,
            len(batch),
            (time.monotonic() - oldest_received) * 1000,
            (time.monotonic() - started_at) * 1000,
            self._queue.qsize(),
        )
        for session in self._target_resolver():
            submit_started = time.monotonic()
            try:
                await self._submit_observation(
                    session, {"kind": kind, "payload": payload}
                )
                logger.info(
                    "DIAG live_input.turn_finished batch_id=%s session=%s "
                    "turn_ms=%.1f total_ms=%.1f",
                    batch_id,
                    session,
                    (time.monotonic() - submit_started) * 1000,
                    (time.monotonic() - oldest_received) * 1000,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "AG99live Bilibili observation failed session=%s error=%s",
                    session,
                    exc,
                )


if blivedm is not None:

    class _Handler(blivedm.BaseHandler):
        def __init__(self, runtime: BilibiliLiveRuntime) -> None:
            self._runtime = runtime

        def _on_danmaku(self, client, message) -> None:
            del client
            self._runtime.enqueue_danmaku(message.uname, message.uid, message.msg)

        def _on_interact_word_v2(self, client, message) -> None:
            del client
            if message.msg_type == 1:
                self._runtime.enqueue_enter(message.username, message.uid)

        def _on_gift(self, client, message) -> None:
            del client
            self._runtime.enqueue_gift(
                message.uname,
                message.uid,
                f"赠送{message.gift_name} x{message.num}。",
            )

        def _on_buy_guard(self, client, message) -> None:
            del client
            self._runtime.enqueue_gift(
                message.username,
                message.uid,
                f"开通舰队等级 {message.guard_level}。",
            )

        def _on_super_chat(self, client, message) -> None:
            del client
            self._runtime.enqueue_advanced_chat(
                message.uname,
                message.uid,
                message.message,
            )

else:

    class _Handler:  # pragma: no cover - only exists when optional dependency is absent.
        def __init__(self, runtime: BilibiliLiveRuntime) -> None:
            del runtime


def _clean_text(value: object, limit: int) -> str:
    return " ".join(str(value or "").split()).strip()[:limit]


__all__ = ["BilibiliLiveRuntime", "LiveInput"]
