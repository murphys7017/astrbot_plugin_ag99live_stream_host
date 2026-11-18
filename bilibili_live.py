"""Backend-owned Bilibili live input for the AG99live Stream Host plugin."""

from __future__ import annotations

import asyncio
import http.cookies
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from hashlib import blake2s
from math import log2
from random import random, sample
from typing import Awaitable, Callable, Literal, Mapping
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
ViewerRecorder = Callable[[str, str], Awaitable[bool]]
_PRIORITY_KINDS = frozenset({"live_gift", "live_advanced_chat"})
_PRIORITY_MAX_AGE_SECONDS = 300
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
    sent_at: float | None = None
    event_id: str = ""


@dataclass(frozen=True, slots=True)
class LiveTurnResult:
    status: Literal["not_admitted", "played", "failed", "unknown"]
    reason: str | None = None


ObservationSubmitter = Callable[[str, Mapping[str, object]], Awaitable[LiveTurnResult]]
FeedbackRecorder = Callable[[int, LiveInput], bool]


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
        record_feedback: FeedbackRecorder | None = None,
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
        self._record_feedback = record_feedback
        self._selection_key = uuid4().bytes
        self._selection_streak: dict[str, int] = {}
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
        pending_ids = tuple(item.event_id for item in self._pending if item.event_id)
        if pending_ids:
            self._log_priority("cancelled", pending_ids, reason="listener_stopped")
        self._pending.clear()
        self._input_ready.clear()
        self._contributions.clear()
        self._selection_streak.clear()

    def enqueue_danmaku(
        self,
        name: str,
        uid: int,
        text: str,
        *,
        fan_level: int = 0,
        timestamp_ms: int = 0,
    ) -> None:
        if self._closed:
            return
        item = LiveInput(
            kind="live_danmaku",
            name=_clean_text(name, 80) or "观众",
            uid=str(uid or "").strip(),
            text=_clean_text(text, 220),
            fan_level=fan_level,
            sent_at=timestamp_ms / 1000 if timestamp_ms and timestamp_ms > 0 else None,
        )
        if item.text in {"+", "-"} and self._record_feedback is not None:
            if self._record_feedback(self.room_id, item):
                self.last_received_at = item.received_monotonic
                return
        self._enqueue(item)

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
                event_id=uuid4().hex,
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
                event_id=uuid4().hex,
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
        priority = item.kind in _PRIORITY_KINDS
        if not priority and (self._processing or not self._can_accept_input()):
            self._dropped_while_busy += 1
            return
        if len(self._pending) >= self._queue_limit:
            expendable = next(
                (
                    pending
                    for pending in self._pending
                    if pending.kind not in _PRIORITY_KINDS
                ),
                None,
            )
            if expendable is not None:
                self._pending.remove(expendable)
            elif not priority:
                return
            else:
                evicted = self._pending.popleft()
                self._log_priority("overflow", (evicted.event_id,))
        self._pending.append(item)
        if priority:
            self._log_priority("queued", (item.event_id,), kind=item.kind)
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
            self._discard_expired_priority()
            if not self._can_accept_input():
                # External playback has no receipt owned by this runtime. Only
                # priority events need to check when that playback becomes idle.
                previous_size = len(self._pending)
                self._pending = deque(
                    item for item in self._pending if item.kind in _PRIORITY_KINDS
                )
                self._dropped_while_busy += previous_size - len(self._pending)
                if self._pending:
                    await asyncio.sleep(_BUSY_RECHECK_SECONDS)
                    continue
            if not self._pending:
                self._input_ready.clear()
                continue
            batch, chat_candidates, chat_routes = self._select_batch(
                list(self._pending)
            )
            selected_priority = {item.event_id for item in batch if item.event_id}
            self._pending = deque(
                item
                for item in self._pending
                if item.kind in _PRIORITY_KINDS
                and item.event_id not in selected_priority
            )
            self._processing = True
            try:
                if not await self._flush_batch(batch, chat_candidates, chat_routes):
                    # Only an explicit non-admission can be retried.
                    retry = [item for item in batch if item.kind in _PRIORITY_KINDS]
                    self._dropped_while_busy += len(batch) - len(retry)
                    if retry:
                        self._pending.extendleft(reversed(retry))
                        while len(self._pending) > self._queue_limit:
                            evicted = self._pending.popleft()
                            self._log_priority("overflow", (evicted.event_id,))
                        await asyncio.sleep(_BUSY_RECHECK_SECONDS)
            finally:
                logger.info(
                    "DIAG live_input.ready room=%s dropped_while_busy=%s",
                    self.room_id,
                    self._dropped_while_busy,
                )
                self._dropped_while_busy = 0
                self._processing = False

    def _discard_expired_priority(self) -> None:
        oldest = time.monotonic() - _PRIORITY_MAX_AGE_SECONDS
        remaining = deque()
        expired = []
        for item in self._pending:
            if item.kind in _PRIORITY_KINDS and item.received_monotonic < oldest:
                expired.append(item.event_id)
            else:
                remaining.append(item)
        self._pending = remaining
        if expired:
            self._log_priority("expired", tuple(expired))

    def _select_batch(
        self, burst: list[LiveInput]
    ) -> tuple[list[LiveInput], int, list[tuple[LiveInput, str]]]:
        gifts = [item for item in burst if item.kind == "live_gift"]
        advanced = [item for item in burst if item.kind == "live_advanced_chat"]
        selected = (gifts + advanced)[: self.max_batch_size]
        slots = self.max_batch_size - len(selected)
        # One ordinary candidate per viewer prevents repeated posts filling a batch.
        chats = {
            item.uid or f"name:{item.name.casefold()}": item
            for item in burst
            if item.kind == "live_danmaku"
        }
        candidates = list(chats.values())
        if not slots:
            return selected, len(candidates), []
        if len(candidates) > slots:
            random_slots = max(1, slots // 4) if slots > 1 else int(random() < 0.25)
            candidates.sort(key=self._chat_priority, reverse=True)
            ranked = slots - random_slots
            chat_routes = [(item, "ranked") for item in candidates[:ranked]]
            chat_routes.extend(
                (item, "random") for item in sample(candidates[ranked:], random_slots)
            )
        else:
            chat_routes = [(item, "all") for item in candidates]
        selected.extend(item for item, _ in chat_routes)
        remaining = self.max_batch_size - len(selected)
        if remaining:
            selected.extend(
                [item for item in burst if item.kind == "live_viewer_entered"][
                    :remaining
                ]
            )
        return selected, len(candidates), chat_routes

    def _log_chat_selection(
        self, batch_id: str, candidates: int, routes: list[tuple[LiveInput, str]]
    ) -> None:
        streak = {}
        selections = []
        for item, route in routes:
            viewer = item.uid or f"name:{item.name.casefold()}"
            key = blake2s(
                viewer.encode(), key=self._selection_key, digest_size=8
            ).hexdigest()
            streak[key] = self._selection_streak.get(key, 0) + 1
            selections.append((key, route, streak[key]))
        self._selection_streak = streak
        if candidates:
            logger.info(
                "DIAG live_input.chat_selection batch_id=%s candidates=%s "
                "ranked=%s random=%s selected=%s",
                batch_id,
                candidates,
                sum(route == "ranked" for _, route in routes),
                sum(route == "random" for _, route in routes),
                selections,
            )

    def _log_priority(
        self,
        state: str,
        event_ids: tuple[str, ...],
        *,
        batch_id: str = "",
        kind: str = "",
        reason: str | None = None,
    ) -> None:
        logger.info(
            "DIAG live_input.priority room=%s state=%s kind=%s event_ids=%s batch_id=%s reason=%s",
            self.room_id,
            state,
            kind,
            event_ids,
            batch_id,
            reason,
        )

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

    async def _flush_batch(
        self,
        batch: list[LiveInput],
        chat_candidates: int,
        chat_routes: list[tuple[LiveInput, str]],
    ) -> bool:
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
        priority_ids = tuple(item.event_id for item in batch if item.event_id)
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
            "priority_event_ids": priority_ids,
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
        for session in self._target_resolver():
            submit_started = time.monotonic()
            try:
                result = await self._submit_observation(
                    session, {"kind": kind, "payload": payload}
                )
                if result.status == "not_admitted":
                    continue
                self._log_chat_selection(batch_id, chat_candidates, chat_routes)
                if priority_ids:
                    self._log_priority(
                        result.status,
                        priority_ids,
                        batch_id=batch_id,
                        reason=result.reason,
                    )
                logger.info(
                    "DIAG live_input.turn_finished batch_id=%s session=%s "
                    "status=%s turn_ms=%.1f total_ms=%.1f",
                    batch_id,
                    session,
                    result.status,
                    (time.monotonic() - submit_started) * 1000,
                    (time.monotonic() - oldest_received) * 1000,
                )
                # One batch maps to one Core turn even if legacy settings
                # provide multiple candidates for the same presentation target.
                return True
            except asyncio.CancelledError:
                if priority_ids:
                    self._log_priority(
                        "unknown",
                        priority_ids,
                        batch_id=batch_id,
                        reason="listener_stopped",
                    )
                raise
            except Exception as exc:  # noqa: BLE001
                # The output may already have started. Do not risk a double thank-you.
                logger.warning(
                    "AG99live Bilibili observation failed session=%s error=%s",
                    session,
                    exc,
                )
                if priority_ids:
                    self._log_priority(
                        "unknown",
                        priority_ids,
                        batch_id=batch_id,
                        reason=type(exc).__name__,
                    )
                return True
        return False


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
                timestamp_ms=message.timestamp,
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
