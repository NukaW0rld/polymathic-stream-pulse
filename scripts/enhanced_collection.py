"""Bounded Helix work for chatter presence and raid-source context.

The coordinator remains the sole database writer. This worker performs exactly
one network request at a time and returns validated, content-safe records. A
presence snapshot is paginated one job per page so viewer polling and EventSub
setup can acquire the shared TokenManager lock between pages.
"""

from dataclasses import dataclass, field
from datetime import datetime
import threading

from scripts.twitch_auth import TwitchError


PRESENCE_PAGE_SIZE = 1000
PRESENCE_MAX_PAGES = 20
PRESENCE_SECONDS = 300
PRESENCE_MAX_ATTEMPT_SECONDS = 120
RAID_CONTEXT_QUEUE_LIMIT = 8


@dataclass(frozen=True)
class PresencePageJob:
    snapshot_id: int
    run_id: int
    stream_id: str = field(repr=False)
    broadcaster_id: str = field(repr=False)
    moderator_id: str = field(repr=False)
    requested_at: datetime
    generation: int
    page_number: int = 1
    after: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class RaidContextJob:
    eventsub_message_id: str = field(repr=False)
    broadcaster_id: str = field(repr=False)
    requested_at: datetime
    attempt: int = 1


@dataclass(frozen=True)
class EnhancedResult:
    job: object = field(repr=False)
    completed_at: datetime
    members: tuple = field(default=(), repr=False)
    reported_total: int | None = None
    after: str | None = field(default=None, repr=False)
    metadata: dict | None = field(default=None, repr=False)
    not_found: bool = False
    error: str | None = None
    fatal: bool = False


def _text(value):
    return isinstance(value, str) and bool(value)


def parse_presence_page(response):
    if not isinstance(response, dict):
        raise ValueError
    data = response.get("data")
    total = response.get("total")
    pagination = response.get("pagination")
    if (not isinstance(data, list) or type(total) is not int or total < 0
            or not isinstance(pagination, dict)):
        raise ValueError
    members = []
    for item in data:
        if not isinstance(item, dict) or not _text(item.get("user_id")):
            raise ValueError
        members.append(item["user_id"])
    cursor = pagination.get("cursor")
    if cursor is not None and not _text(cursor):
        raise ValueError
    return tuple(members), total, cursor


def parse_channel_information(response, broadcaster_id):
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, list) or len(data) > 1:
        raise ValueError
    if not data:
        return None
    row = data[0]
    if (not isinstance(row, dict) or row.get("broadcaster_id") != broadcaster_id
            or not all(isinstance(row.get(key), str)
                       for key in ("game_id", "game_name", "title", "broadcaster_language"))
            or not isinstance(row.get("tags"), list)
            or not all(_text(tag) for tag in row["tags"])):
        raise ValueError
    return {
        "category_id": row["game_id"],
        "category_name": row["game_name"],
        "title": row["title"],
        "language": row["broadcaster_language"],
        "tags": sorted(set(row["tags"])),
    }


class EnhancedWorker:
    """One in-flight request; results cross the thread as fixed safe fields."""

    def __init__(self, auth, *, clock):
        self.auth = auth
        self.clock = clock
        self._thread = None
        self._result = None

    @property
    def busy(self):
        return self._thread is not None

    def start(self, job):
        if self.busy:
            raise RuntimeError("enhanced_worker_overlap")
        self._result = None
        self._thread = threading.Thread(
            target=self._run, args=(job,), name="enhanced-helix", daemon=False,
        )
        self._thread.start()

    def _run(self, job):
        try:
            if isinstance(job, PresencePageJob):
                params = {
                    "broadcaster_id": job.broadcaster_id,
                    "moderator_id": job.moderator_id,
                    "first": PRESENCE_PAGE_SIZE,
                }
                if job.after is not None:
                    params["after"] = job.after
                response = self.auth.helix_get("chat/chatters", params)
                members, total, after = parse_presence_page(response)
                self._result = EnhancedResult(
                    job=job, completed_at=self.clock().utc, members=members,
                    reported_total=total, after=after,
                )
                return
            if isinstance(job, RaidContextJob):
                response = self.auth.helix_get(
                    "channels", {"broadcaster_id": job.broadcaster_id},
                )
                metadata = parse_channel_information(response, job.broadcaster_id)
                self._result = EnhancedResult(
                    job=job, completed_at=self.clock().utc, metadata=metadata,
                    not_found=metadata is None,
                )
                return
            raise ValueError
        except TwitchError as error:
            reason = ({403: "access_denied", 429: "rate_limited"}.get(
                error.status, "request_failed"
            ))
            self._result = EnhancedResult(
                job=job, completed_at=self.clock().utc, error=reason, fatal=error.fatal,
            )
        except Exception:
            self._result = EnhancedResult(
                job=job, completed_at=self.clock().utc, error="request_failed",
            )

    def take(self):
        if not self.busy or self._thread.is_alive():
            return None
        self._thread.join()
        self._thread = None
        if self._result is None:
            raise RuntimeError("enhanced_worker_result_missing")
        result, self._result = self._result, None
        return result

    def finish(self):
        if not self.busy:
            return None
        self._thread.join()
        self._thread = None
        result, self._result = self._result, None
        return result
