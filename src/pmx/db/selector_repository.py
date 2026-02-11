from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb


@dataclass(frozen=True, slots=True)
class CandidateScoreRow:
    market_id: str
    token_id: str
    screen_score: float
    deep_score: float
    components_json: dict[str, Any]
    deep_components_json: dict[str, Any]
    flags_json: dict[str, Any]
    ttr_bucket: str
    category: str
    group_id: str


class SelectorRepository:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection
        self._candidate_table_checked = False
        self._candidate_table_exists = False
        self._candidate_deep_columns_checked = False
        self._candidate_deep_columns_exists = False
        self._candidate_fallback: dict[int, list[dict[str, Any]]] = {}

    def insert_run(
        self,
        *,
        run_id: UUID,
        run_type: str,
        decision_ts: datetime,
        ingest_epsilon_seconds: int,
        code_version: str,
        config_hash: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO runs (
                    run_id,
                    run_type,
                    decision_ts,
                    ingest_epsilon_seconds,
                    code_version,
                    config_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    run_type,
                    _as_utc_datetime(decision_ts),
                    ingest_epsilon_seconds,
                    code_version,
                    config_hash,
                ),
            )

    def create_selection_run(
        self,
        *,
        run_id: UUID,
        stage: str,
        universe_size: int,
        selected_size: int,
        selector_version: str,
        params: dict[str, Any],
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO selection_runs (
                    run_id,
                    stage,
                    universe_size,
                    selected_size,
                    selector_version,
                    params
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                RETURNING selection_run_id
                """,
                (
                    run_id,
                    stage,
                    universe_size,
                    selected_size,
                    selector_version,
                    Jsonb(params),
                ),
            )
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError("Unable to create selection_run")
        return int(row[0])

    def upsert_candidate_scores(
        self,
        *,
        selection_run_id: int,
        market_id: str,
        token_id: str,
        screen_score: float,
        deep_score: float,
        components_json: dict[str, Any],
        deep_components_json: dict[str, Any],
        flags_json: dict[str, Any],
        ttr_bucket: str,
        category: str,
        group_id: str,
    ) -> None:
        if self._has_candidate_table():
            if self._has_candidate_deep_columns():
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO selector_candidates (
                            selection_run_id,
                            market_id,
                            token_id,
                            screen_score,
                            deep_score,
                            components,
                            deep_components,
                            flags,
                            ttr_bucket,
                            category,
                            group_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s)
                        ON CONFLICT (selection_run_id, market_id) DO UPDATE
                        SET token_id = EXCLUDED.token_id,
                            screen_score = EXCLUDED.screen_score,
                            deep_score = EXCLUDED.deep_score,
                            components = EXCLUDED.components,
                            deep_components = EXCLUDED.deep_components,
                            flags = EXCLUDED.flags,
                            ttr_bucket = EXCLUDED.ttr_bucket,
                            category = EXCLUDED.category,
                            group_id = EXCLUDED.group_id
                        """,
                        (
                            selection_run_id,
                            market_id,
                            token_id,
                            screen_score,
                            deep_score,
                            Jsonb(components_json),
                            Jsonb(deep_components_json),
                            Jsonb(flags_json),
                            ttr_bucket,
                            category,
                            group_id,
                        ),
                    )
                return

            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO selector_candidates (
                        selection_run_id,
                        market_id,
                        token_id,
                        screen_score,
                        components,
                        flags,
                        ttr_bucket,
                        category,
                        group_id
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
                    ON CONFLICT (selection_run_id, market_id) DO UPDATE
                    SET token_id = EXCLUDED.token_id,
                        screen_score = EXCLUDED.screen_score,
                        components = EXCLUDED.components,
                        flags = EXCLUDED.flags,
                        ttr_bucket = EXCLUDED.ttr_bucket,
                        category = EXCLUDED.category,
                        group_id = EXCLUDED.group_id
                    """,
                    (
                        selection_run_id,
                        market_id,
                        token_id,
                        screen_score,
                        Jsonb(components_json),
                        Jsonb(flags_json),
                        ttr_bucket,
                        category,
                        group_id,
                    ),
                )
            return

        fallback = self._candidate_fallback.setdefault(selection_run_id, [])
        fallback.append(
            {
                "selection_run_id": selection_run_id,
                "market_id": market_id,
                "token_id": token_id,
                "screen_score": round(screen_score, 6),
                "deep_score": round(deep_score, 6),
                "components_json": components_json,
                "deep_components_json": deep_components_json,
                "flags_json": flags_json,
                "ttr_bucket": ttr_bucket,
                "category": category,
                "group_id": group_id,
            }
        )

    def insert_selected(
        self,
        *,
        selection_run_id: int,
        selector_version: str,
        rank: int,
        market_id: str,
        token_id: str,
        score: float,
        reason_json: dict[str, Any],
    ) -> None:
        reason_payload = dict(reason_json)
        reason_payload["selector_version"] = selector_version
        reason_payload["token_id"] = token_id

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO selection_items (
                    selection_run_id,
                    market_id,
                    score,
                    rank,
                    reason
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (selection_run_id, market_id) DO UPDATE
                SET score = EXCLUDED.score,
                    rank = EXCLUDED.rank,
                    reason = EXCLUDED.reason
                """,
                (
                    selection_run_id,
                    market_id,
                    score,
                    rank,
                    Jsonb(reason_payload),
                ),
            )

    def write_candidate_fallback_artifact(self, *, run_id: str, artifacts_root: str) -> Path | None:
        if self._candidate_table_exists:
            return None
        root = Path(artifacts_root)
        root.mkdir(parents=True, exist_ok=True)
        output_path = root / f"{run_id}.json"
        payload = {
            "run_id": run_id,
            "candidate_scores": {
                str(selection_run_id): rows
                for selection_run_id, rows in sorted(self._candidate_fallback.items())
            },
        }
        output_path.write_text(
            json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
            encoding="utf-8",
        )
        return output_path

    def _has_candidate_table(self) -> bool:
        if self._candidate_table_checked:
            return self._candidate_table_exists

        with self.connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.selector_candidates')")
            row = cursor.fetchone()
        self._candidate_table_exists = bool(row and row[0] is not None)
        self._candidate_table_checked = True
        return self._candidate_table_exists

    def _has_candidate_deep_columns(self) -> bool:
        if self._candidate_deep_columns_checked:
            return self._candidate_deep_columns_exists
        if not self._has_candidate_table():
            self._candidate_deep_columns_checked = True
            self._candidate_deep_columns_exists = False
            return False

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'selector_candidates'
                  AND column_name IN ('deep_score', 'deep_components')
                """
            )
            rows = cursor.fetchall()
        columns = {str(row[0]) for row in rows}
        self._candidate_deep_columns_exists = {
            "deep_score",
            "deep_components",
        }.issubset(columns)
        self._candidate_deep_columns_checked = True
        return self._candidate_deep_columns_exists


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
