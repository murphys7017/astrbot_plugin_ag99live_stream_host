"""Backend-owned Bilibili live input for the AG99live Stream Host plugin."""

from __future__ import annotations

import asyncio
import http.cookies
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from math import log2
from random import random, sample
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
# False means admission was busy and no event was committed.
ObservationSubmitter = Callable[[str, Mapping[str, object]], Awaitable[bool]]
ViewerRecorder = Callable[[str, str], Awaitable[bool]]
_GIFT_MAX_AGE_SECONDS = 300
_BUSY_RECHECK_SECONDS = 0.25
_CONTRIBUTOR_LIMIT = 4096


@dataclass(frozen=True, slots=True)
class LiveInput:
    kind: str
    name: str
    uid: str
    text: str = ""
    received_at: float = field(default_factory=time.time)
    received_monotonic: float = field(default_factory=time.monotonic)
    fan_level: int = 0


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
        self._queue_limit = max(16, min(int(max_queue_size), 2048))
        self._pending: deque[LiveInput] = deque()
        self._input_ready = asyncio.Event()
        self._contributions: OrderedDict[str, int] = OrderedDict()
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
        self.last_received_at = 0.0

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
        self._pending.clear()
        self._input_ready.clear()
        self._contributions.clear()

    def enqueue_danmaku(
        self, name: str, uid: int, text: str, *, fan_level: int = 0
    ) -> None:
        self._enqueue(
            LiveInput(
                kind="live_danmaku",
                name=_clean_text(name, 80) or "观众",
                uid=str(uid or "").strip(),
                text=_clean_text(text, 220),
                fan_level=fan_level,
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

    def enqueue_gift(
        self, name: str, uid: int, text: str, *, paid_coins: int = 0
    ) -> None:
        self._record_contribution(uid, paid_coins)
        self._enqueue(
            LiveInput(
                kind="live_gift",
                name=_clean_text(name, 80) or "观众",
                uid=str(uid or "").strip(),
                text=_clean_text(text, 220),
            )
        )

    def enqueue_advanced_chat(
        self, name: str, uid: int, text: str, *, paid_coins: int = 0
    ) -> None:
        self._record_contribution(uid, paid_coins)
        self._enqueue(
            LiveInput(
                kind="live_advanced_chat",
                name=_clean_text(name, 80) or "观众",
                uid=str(uid or "").strip(),
                text=_clean_text(text, 220),
            )
        )

    def _record_contribution(self, uid: int, paid_coins: int) -> None:
        if self._closed or not uid or paid_coins <= 0:
            return
        key = str(uid).strip()
        self._contributions[key] = self._contributions.get(key, 0) + paid_coins
        self._contributions.move_to_end(key)
        if len(self._contributions) > _CONTRIBUTOR_LIMIT:
            self._contributions.popitem(last=False)

    def _enqueue(self, item: LiveInput) -> None:
        if self._closed or (item.kind != "live_viewer_entered" and not item.text):
            return
        self.last_received_at = time.monotonic()
        gift = item.kind == "live_gift"
        if not gift and (self._processing or not self._can_accept_input()):
            self._dropped_while_busy += 1
            return
        if len(self._pending) >= self._queue_limit:
            expendable = next(
                (pending for pending in self._pending if pending.kind != "live_gift"),
                None,
            )
            if expendable is not None:
                self._pending.remove(expendable)
            elif not gift:
                return
            else:
                self._pending.popleft()
                logger.warning(
                    "AG99live Bilibili gift queue is full; oldest gift dropped room=%s",
                    self.room_id,
                )
        self._pending.append(item)
        self._input_ready.set()
        logger.debug(
            "DIAG live_input.received room=%s kind=%s received_at=%.3f queue_size=%s",
            self.room_id,
            item.kind,
            item.received_at,
            len(self._pending),
        )

    async def _flush_loop(self) -> None:
        while True:
            await self._input_ready.wait()
            self._discard_expired_gifts()
            if not self._can_accept_input():
                # External playback has no receipt owned by this runtime. Only
                # pending gifts need to check when that playback becomes idle.
                self._pending = deque(
                    item for item in self._pending if item.kind == "live_gift"
                )
                if self._pending:
                    await asyncio.sleep(_BUSY_RECHECK_SECONDS)
                    continue
            if not self._pending:
                self._input_ready.clear()
                continue
            batch = self._select_batch(list(self._pending))
            selected_gifts = {id(item) for item in batch if item.kind == "live_gift"}
            self._pending = deque(
                item
                for item in self._pending
                if item.kind == "live_gift" and id(item) not in selected_gifts
            )
            self._processing = True
            try:
                if not await self._flush_batch(batch):
                    # Admission may become busy while recording viewers. No
                    # committed event is replayed, and ordinary chat stays dropped.
                    retry = [item for item in batch if item.kind == "live_gift"]
                    if retry:
                        self._pending.extendleft(reversed(retry))
                        while len(self._pending) > self._queue_limit:
                            self._pending.popleft()
                        await asyncio.sleep(_BUSY_RECHECK_SECONDS)
            finally:
                logger.info(
                    "DIAG live_input.ready room=%s dropped_while_busy=%s",
                    self.room_id,
                    self._dropped_while_busy,
                )
                self._dropped_while_busy = 0
                self._processing = False

    def _discard_expired_gifts(self) -> None:
        oldest = time.monotonic() - _GIFT_MAX_AGE_SECONDS
        self._pending = deque(
            item
            for item in self._pending
            if item.kind != "live_gift" or item.received_monotonic >= oldest
        )

    def _select_batch(self, burst: list[LiveInput]) -> list[LiveInput]:
        gifts = [item for item in burst if item.kind == "live_gift"]
        advanced = [item for item in burst if item.kind == "live_advanced_chat"]
        selected = (gifts + advanced)[: self.max_batch_size]
        slots = self.max_batch_size - len(selected)
        if not slots:
            return selected

        # One ordinary candidate per viewer prevents repeated posts filling a batch.
        chats = {
            item.uid or f"name:{item.name.casefold()}": item
            for item in burst
            if item.kind == "live_danmaku"
        }
        candidates = list(chats.values())
        if len(candidates) > slots:
            random_slots = max(1, slots // 4) if slots > 1 else int(random() < 0.25)
            candidates.sort(key=self._chat_priority, reverse=True)
            ranked = slots - random_slots
            candidates = candidates[:ranked] + sample(candidates[ranked:], random_slots)
        selected.extend(candidates)
        remaining = self.max_batch_size - len(selected)
        if remaining:
            selected.extend(
                [item for item in burst if item.kind == "live_viewer_entered"][
                    :remaining
                ]
            )
        return selected

    def _chat_priority(self, item: LiveInput) -> float:
        paid_coins = self._contributions.get(item.uid, 0)
        # Diminishing returns let both fan level and contribution affect selection.
        return min(item.fan_level, 50) + 4 * log2(1 + paid_coins / 1000)

    @property
    def is_processing(self) -> bool:
        return self._processing

    @property
    def has_pending_input(self) -> bool:
        return self._processing or bool(self._pending)

    async def _flush_batch(self, batch: list[LiveInput]) -> bool:
        batch_id = uuid4().hex
        started_at = time.monotonic()
        oldest_received = min(item.received_monotonic for item in batch)
        excerpts: list[str] = []
        participants: dict[tuple[str, str], None] = {}
        new_viewers: dict[tuple[str, str], None] = {}
        kinds: set[str] = set()
        for item in batch:
            try:
                is_new = await self._record_viewer(item.name, item.uid)
            except OSError:
                logger.exception(
                    "Unable to save Bilibili viewer state room=%s", self.room_id
                )
                is_new = False
            prefix = "新观众 " if is_new else ""
            if is_new:
                new_viewers[item.name, item.uid] = None
            if item.kind == "live_viewer_entered":
                excerpts.append(f"{prefix}{item.name} 进入了直播间。")
            elif item.kind == "live_gift":
                excerpts.append(f"【礼物】{item.name}：{item.text}")
            elif item.kind == "live_advanced_chat":
                excerpts.append(f"【醒目留言】{item.name}：{item.text}")
            elif item.text:
                excerpts.append(f"{prefix}{item.name}：{item.text[:120]}")
            participants[item.name, item.uid] = None
            kinds.add(item.kind)
        excerpt = "\n".join(excerpts).strip()
        if not excerpt:
            return True
        viewers = list(participants)[:16]
        newcomers = list(new_viewers)[:16]
        payload = {
            "batch_id": batch_id,
            "live_input_received_at": min(item.received_at for item in batch),
            "ambient_message_excerpt": excerpt,
            "participant_names": tuple(name for name, _ in viewers),
            "participant_ids": tuple(uid for _, uid in viewers),
            "new_viewer_names": tuple(name for name, _ in newcomers),
            "new_viewer_ids": tuple(uid for _, uid in newcomers),
            "message_count": len(batch),
            "trigger": "bilibili_live",
            "event_kinds": tuple(sorted(kinds)),
        }
        if "live_gift" in kinds:
            kind = "live_gift"
        elif "live_advanced_chat" in kinds:
            kind = "live_advanced_chat"
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
            len(self._pending),
        )
        retry = False
        for session in self._target_resolver():
            submit_started = time.monotonic()
            try:
                submitted = await self._submit_observation(
                    session, {"kind": kind, "payload": payload}
                )
                if not submitted:
                    retry = True
                    continue
                logger.info(
                    "DIAG live_input.turn_finished batch_id=%s session=%s "
                    "turn_ms=%.1f total_ms=%.1f",
                    batch_id,
                    session,
                    (time.monotonic() - submit_started) * 1000,
                    (time.monotonic() - oldest_received) * 1000,
                )
                # One batch maps to one Core turn even if legacy settings
                # provide multiple candidates for the same presentation target.
                return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                # The output may already have started. Do not risk a double thank-you.
                logger.warning(
                    "AG99live Bilibili observation failed session=%s error=%s",
                    session,
                    exc,
                )
                return True
        return not retry


if blivedm is not None:

    class _Handler(blivedm.BaseHandler):
        def __init__(self, runtime: BilibiliLiveRuntime) -> None:
            self._runtime = runtime

        def _on_danmaku(self, client, message) -> None:
            fan_level = (
                max(0, int(message.medal_level or 0))
                if message.medal_room_id in {client.room_id, self._runtime.room_id}
                else 0
            )
            self._runtime.enqueue_danmaku(
                message.uname,
                message.uid,
                message.msg,
                fan_level=fan_level,
            )

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
                paid_coins=message.total_coin if message.coin_type == "gold" else 0,
            )

        def _on_buy_guard(self, client, message) -> None:
            del client
            guard_name = {1: "总督", 2: "提督", 3: "舰长"}.get(
                message.guard_level, "大航海"
            )
            self._runtime.enqueue_gift(
                message.username,
                message.uid,
                f"开通{guard_name}。",
            )

        def _on_super_chat(self, client, message) -> None:
            del client
            self._runtime.enqueue_advanced_chat(
                message.uname,
                message.uid,
                message.message,
                paid_coins=message.price * 1000,
            )

else:

    class _Handler:  # pragma: no cover - only exists when optional dependency is absent.
        def __init__(self, runtime: BilibiliLiveRuntime) -> None:
            del runtime


def _clean_text(value: object, limit: int) -> str:
    return " ".join(str(value or "").split()).strip()[:limit]


__all__ = ["BilibiliLiveRuntime", "LiveInput"]
