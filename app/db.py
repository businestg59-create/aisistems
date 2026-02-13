from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg


@dataclass(slots=True)
class ConnectionInfo:
    business_connection_id: str
    owner_user_id: int
    owner_user_chat_id: int | None
    can_reply: bool
    updated_at: str


@dataclass(slots=True)
class LeadInfo:
    business_connection_id: str
    client_chat_id: int
    step: int
    need: str | None
    budget: str | None
    deadline: str | None
    contact_method: str | None
    phone: str | None
    call_time: str | None
    summary_json: str | None
    escalation_open: bool
    escalation_last_at: str | None
    last_client_message: str | None
    rag_sources_json: str | None
    urgency: str | None
    created_at: str
    updated_at: str


class Database:
    def __init__(self, database_url: str, *, min_size: int = 1, max_size: int = 10) -> None:
        self.database_url = database_url
        self.min_size = min_size
        self.max_size = max_size
        self.pool: asyncpg.Pool | None = None

    async def create_pool(self) -> None:
        if self.pool is not None:
            return
        self.pool = await asyncpg.create_pool(
            dsn=self.database_url,
            min_size=self.min_size,
            max_size=self.max_size,
            command_timeout=10,
        )

    async def init(self) -> None:
        await self.create_pool()

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def ping(self, timeout_seconds: float = 1.0) -> bool:
        try:
            conn = await asyncio.wait_for(self._acquire(), timeout=timeout_seconds)
        except Exception:
            return False

        try:
            result = await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=timeout_seconds)
            return result == 1
        except Exception:
            return False
        finally:
            await self._release(conn)

    async def upsert_connection(
        self,
        business_connection_id: str,
        owner_user_id: int,
        owner_user_chat_id: int | None,
        can_reply: bool,
    ) -> None:
        conn = await self._acquire()
        try:
            await conn.execute(
                """
                INSERT INTO connections (
                    business_connection_id,
                    owner_user_id,
                    owner_user_chat_id,
                    can_reply,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (business_connection_id) DO UPDATE SET
                    owner_user_id = EXCLUDED.owner_user_id,
                    owner_user_chat_id = EXCLUDED.owner_user_chat_id,
                    can_reply = EXCLUDED.can_reply,
                    updated_at = NOW()
                """,
                business_connection_id,
                owner_user_id,
                owner_user_chat_id,
                can_reply,
            )
        finally:
            await self._release(conn)

    async def get_connection(self, business_connection_id: str) -> ConnectionInfo | None:
        conn = await self._acquire()
        try:
            row = await conn.fetchrow(
                """
                SELECT business_connection_id, owner_user_id, owner_user_chat_id, can_reply, updated_at
                FROM connections
                WHERE business_connection_id = $1
                """,
                business_connection_id,
            )
        finally:
            await self._release(conn)

        if not row:
            return None

        return ConnectionInfo(
            business_connection_id=row["business_connection_id"],
            owner_user_id=row["owner_user_id"],
            owner_user_chat_id=row["owner_user_chat_id"],
            can_reply=bool(row["can_reply"]),
            updated_at=_as_iso(row["updated_at"]) or "",
        )

    async def upsert_client(
        self,
        business_connection_id: str,
        client_chat_id: int,
        username: str | None,
        full_name: str | None,
    ) -> bool:
        conn = await self._acquire()
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO clients (
                    business_connection_id,
                    client_chat_id,
                    first_seen_at,
                    last_seen_at,
                    username,
                    full_name,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, NOW(), NOW(), $3, $4, NOW(), NOW())
                ON CONFLICT (business_connection_id, client_chat_id) DO UPDATE SET
                    last_seen_at = NOW(),
                    username = EXCLUDED.username,
                    full_name = EXCLUDED.full_name,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted
                """,
                business_connection_id,
                client_chat_id,
                username,
                full_name,
            )
            return bool(row["inserted"]) if row else False
        finally:
            await self._release(conn)

    async def touch_client(
        self,
        business_connection_id: str,
        client_chat_id: int,
        username: str | None,
        full_name: str | None,
    ) -> bool:
        return await self.upsert_client(business_connection_id, client_chat_id, username, full_name)

    async def get_lead(self, business_connection_id: str, client_chat_id: int) -> LeadInfo | None:
        conn = await self._acquire()
        try:
            row = await conn.fetchrow(
                """
                SELECT
                    business_connection_id,
                    client_chat_id,
                    step,
                    need,
                    budget,
                    deadline,
                    contact_method,
                    phone,
                    call_time,
                    summary_json,
                    escalation_open,
                    escalation_last_at,
                    last_client_message,
                    rag_sources_json,
                    urgency,
                    created_at,
                    updated_at
                FROM leads
                WHERE business_connection_id = $1 AND client_chat_id = $2
                """,
                business_connection_id,
                client_chat_id,
            )
        finally:
            await self._release(conn)

        if not row:
            return None

        return LeadInfo(
            business_connection_id=row["business_connection_id"],
            client_chat_id=row["client_chat_id"],
            step=row["step"],
            need=row["need"],
            budget=row["budget"],
            deadline=row["deadline"],
            contact_method=row["contact_method"],
            phone=row["phone"],
            call_time=row["call_time"],
            summary_json=row["summary_json"],
            escalation_open=bool(row["escalation_open"]),
            escalation_last_at=_as_iso(row["escalation_last_at"]),
            last_client_message=row["last_client_message"],
            rag_sources_json=row["rag_sources_json"],
            urgency=row["urgency"],
            created_at=_as_iso(row["created_at"]) or "",
            updated_at=_as_iso(row["updated_at"]) or "",
        )

    async def get_or_create_lead(self, business_connection_id: str, client_chat_id: int) -> LeadInfo:
        lead = await self.get_lead(business_connection_id, client_chat_id)
        if lead:
            return lead

        conn = await self._acquire()
        try:
            await conn.execute(
                """
                INSERT INTO leads (
                    business_connection_id,
                    client_chat_id,
                    step,
                    escalation_open,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, 0, FALSE, NOW(), NOW())
                ON CONFLICT (business_connection_id, client_chat_id) DO NOTHING
                """,
                business_connection_id,
                client_chat_id,
            )
        finally:
            await self._release(conn)

        created = await self.get_lead(business_connection_id, client_chat_id)
        if created is None:
            raise RuntimeError("Lead creation failed")
        return created

    async def create_or_reset_lead(self, business_connection_id: str, client_chat_id: int) -> LeadInfo:
        conn = await self._acquire()
        try:
            await conn.execute(
                """
                INSERT INTO leads (
                    business_connection_id,
                    client_chat_id,
                    step,
                    need,
                    budget,
                    deadline,
                    contact_method,
                    phone,
                    call_time,
                    summary_json,
                    escalation_open,
                    escalation_last_at,
                    last_client_message,
                    rag_sources_json,
                    urgency,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NOW(), NOW())
                ON CONFLICT (business_connection_id, client_chat_id) DO UPDATE SET
                    step = 0,
                    need = NULL,
                    budget = NULL,
                    deadline = NULL,
                    contact_method = NULL,
                    phone = NULL,
                    call_time = NULL,
                    summary_json = NULL,
                    escalation_open = FALSE,
                    escalation_last_at = NULL,
                    last_client_message = NULL,
                    rag_sources_json = NULL,
                    urgency = NULL,
                    updated_at = NOW()
                """,
                business_connection_id,
                client_chat_id,
            )
        finally:
            await self._release(conn)

        lead = await self.get_lead(business_connection_id, client_chat_id)
        if lead is None:
            raise RuntimeError("Lead reset failed")
        return lead

    async def update_lead_step(self, business_connection_id: str, client_chat_id: int, step: int) -> None:
        await self.update_lead_fields(business_connection_id, client_chat_id, step=step)

    async def save_lead_fields(
        self,
        business_connection_id: str,
        client_chat_id: int,
        **fields: Any,
    ) -> None:
        await self.update_lead_fields(business_connection_id, client_chat_id, **fields)

    async def update_lead_fields(
        self,
        business_connection_id: str,
        client_chat_id: int,
        *,
        step: int | None = None,
        need: str | None = None,
        budget: str | None = None,
        deadline: str | None = None,
        contact_method: str | None = None,
        phone: str | None = None,
        call_time: str | None = None,
        summary: dict[str, Any] | None = None,
        escalation_open: bool | None = None,
        escalation_last_at: str | None = None,
        last_client_message: str | None = None,
        rag_sources: list[str] | None = None,
        urgency: str | None = None,
    ) -> None:
        sets: list[str] = []
        values: list[Any] = []

        if step is not None:
            sets.append(f"step = ${len(values) + 1}")
            values.append(step)
        if need is not None:
            sets.append(f"need = ${len(values) + 1}")
            values.append(need)
        if budget is not None:
            sets.append(f"budget = ${len(values) + 1}")
            values.append(budget)
        if deadline is not None:
            sets.append(f"deadline = ${len(values) + 1}")
            values.append(deadline)
        if contact_method is not None:
            sets.append(f"contact_method = ${len(values) + 1}")
            values.append(contact_method)
        if phone is not None:
            sets.append(f"phone = ${len(values) + 1}")
            values.append(phone)
        if call_time is not None:
            sets.append(f"call_time = ${len(values) + 1}")
            values.append(call_time)
        if summary is not None:
            sets.append(f"summary_json = ${len(values) + 1}")
            values.append(json.dumps(summary, ensure_ascii=False))
        if escalation_open is not None:
            sets.append(f"escalation_open = ${len(values) + 1}")
            values.append(escalation_open)
        if escalation_last_at is not None:
            sets.append(f"escalation_last_at = ${len(values) + 1}")
            values.append(escalation_last_at)
        if last_client_message is not None:
            sets.append(f"last_client_message = ${len(values) + 1}")
            values.append(last_client_message)
        if rag_sources is not None:
            sets.append(f"rag_sources_json = ${len(values) + 1}")
            values.append(json.dumps(rag_sources, ensure_ascii=False))
        if urgency is not None:
            sets.append(f"urgency = ${len(values) + 1}")
            values.append(urgency)

        sets.append("updated_at = NOW()")
        values.extend([business_connection_id, client_chat_id])

        query = (
            f"UPDATE leads SET {', '.join(sets)} "
            f"WHERE business_connection_id = ${len(values) - 1} AND client_chat_id = ${len(values)}"
        )

        conn = await self._acquire()
        try:
            await conn.execute(query, *values)
        finally:
            await self._release(conn)

    async def mark_escalation(
        self,
        business_connection_id: str,
        client_chat_id: int,
        *,
        reason: str,
        urgency: str,
        last_message: str,
        cooldown_minutes: int = 10,
    ) -> bool:
        now = datetime.now(timezone.utc)
        should_alert = True

        conn = await self._acquire()
        try:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT last_alert_at
                    FROM escalations
                    WHERE business_connection_id = $1 AND client_chat_id = $2
                    """,
                    business_connection_id,
                    client_chat_id,
                )

                if row and row["last_alert_at"]:
                    prev = row["last_alert_at"]
                    if isinstance(prev, datetime) and (now - prev) < timedelta(minutes=cooldown_minutes):
                        should_alert = False

                await conn.execute(
                    """
                    INSERT INTO escalations (
                        business_connection_id,
                        client_chat_id,
                        escalation_open,
                        last_alert_at,
                        reason,
                        urgency,
                        last_message,
                        created_at,
                        updated_at
                    )
                    VALUES ($1, $2, TRUE, $3, $4, $5, $6, NOW(), NOW())
                    ON CONFLICT (business_connection_id, client_chat_id) DO UPDATE SET
                        escalation_open = TRUE,
                        last_alert_at = CASE WHEN $7 THEN EXCLUDED.last_alert_at ELSE escalations.last_alert_at END,
                        reason = EXCLUDED.reason,
                        urgency = EXCLUDED.urgency,
                        last_message = EXCLUDED.last_message,
                        updated_at = NOW()
                    """,
                    business_connection_id,
                    client_chat_id,
                    now,
                    reason,
                    urgency,
                    last_message,
                    should_alert,
                )
        finally:
            await self._release(conn)

        lead = await self.get_lead(business_connection_id, client_chat_id)
        if lead is None:
            await self.create_or_reset_lead(business_connection_id, client_chat_id)

        await self.update_lead_fields(
            business_connection_id,
            client_chat_id,
            escalation_open=True,
            escalation_last_at=now.isoformat() if should_alert else (lead.escalation_last_at if lead else now.isoformat()),
            urgency=urgency,
        )
        return should_alert

    async def close_escalation(self, business_connection_id: str, client_chat_id: int) -> None:
        conn = await self._acquire()
        try:
            await conn.execute(
                """
                UPDATE escalations
                SET escalation_open = FALSE,
                    updated_at = NOW()
                WHERE business_connection_id = $1 AND client_chat_id = $2
                """,
                business_connection_id,
                client_chat_id,
            )
        finally:
            await self._release(conn)

        await self.update_lead_fields(
            business_connection_id,
            client_chat_id,
            escalation_open=False,
        )

    async def set_admin_chat_id(self, chat_id: int) -> None:
        conn = await self._acquire()
        try:
            await conn.execute(
                """
                INSERT INTO settings(key, value, created_at, updated_at)
                VALUES ('admin_chat_id', $1, NOW(), NOW())
                ON CONFLICT(key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """,
                str(chat_id),
            )
        finally:
            await self._release(conn)

    async def get_admin_chat_id(self) -> int | None:
        conn = await self._acquire()
        try:
            row = await conn.fetchrow("SELECT value FROM settings WHERE key = 'admin_chat_id'")
        finally:
            await self._release(conn)

        if not row:
            return None
        return int(row["value"])

    async def resolve_admin_chat_id(
        self,
        business_connection_id: str | None,
        env_admin_chat_id: int | None,
    ) -> int | None:
        if business_connection_id:
            connection = await self.get_connection(business_connection_id)
            if connection and connection.owner_user_chat_id:
                return connection.owner_user_chat_id

        saved_admin_chat_id = await self.get_admin_chat_id()
        if saved_admin_chat_id:
            return saved_admin_chat_id

        return env_admin_chat_id

    async def _acquire(self) -> asyncpg.Connection:
        if self.pool is None:
            await self.create_pool()
        assert self.pool is not None
        return await self.pool.acquire()

    async def _release(self, conn: asyncpg.Connection) -> None:
        if self.pool is not None:
            await self.pool.release(conn)


def _as_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
