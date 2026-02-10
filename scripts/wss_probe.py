from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_CLOB_WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/"
DEFAULT_MAX_MESSAGES = 200
DEFAULT_TIMEOUT_SECONDS = 20
SEQ_LIKE_FIELDS = ("seq", "sequence", "offset", "event_id", "msg_id")
SEQ_SUPPORT_FIELDS = {"seq", "sequence", "offset"}


@dataclass(frozen=True, slots=True)
class ProbeSummary:
    message_count: int
    message_types: tuple[str, ...]
    seq_like_fields: tuple[str, ...]
    supports_seq: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "message_count": self.message_count,
            "message_types": list(self.message_types),
            "seq_like_fields": list(self.seq_like_fields),
            "supports_seq": self.supports_seq,
        }


def analyze_messages(messages: list[Any]) -> ProbeSummary:
    message_types: set[str] = set()
    seq_like_fields: set[str] = set()

    for message in messages:
        message_types.add(detect_message_type(message))
        seq_like_fields.update(collect_seq_like_fields(message))

    supports_seq = any(field in SEQ_SUPPORT_FIELDS for field in seq_like_fields)
    return ProbeSummary(
        message_count=len(messages),
        message_types=tuple(sorted(message_types)),
        seq_like_fields=tuple(sorted(seq_like_fields)),
        supports_seq=supports_seq,
    )


def detect_message_type(message: Any) -> str:
    if not isinstance(message, dict):
        return "non_json"

    for key in ("type", "event", "channel"):
        value = message.get(key)
        if value is None:
            continue
        text = str(value).strip().lower()
        if text:
            return text
    return "unknown"


def collect_seq_like_fields(message: Any) -> set[str]:
    found: set[str] = set()
    _walk_fields(message, found)
    return found


def _walk_fields(value: Any, found: set[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            if key_text in SEQ_LIKE_FIELDS and _has_value(child):
                found.add(key_text)
            _walk_fields(child, found)
        return

    if isinstance(value, list):
        for child in value:
            _walk_fields(child, found)


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def parse_token_ids(raw: str | None) -> list[str]:
    if raw is None:
        return []
    items = [token.strip() for token in raw.split(",")]
    return sorted({token for token in items if token})


def load_subscribe_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload_path = args.subscribe_payload_json or os.getenv("CLOB_WSS_SUBSCRIBE_PAYLOAD_JSON")
    if payload_path:
        payload = _load_json_file(Path(payload_path))
        if not isinstance(payload, dict):
            raise ValueError("Subscribe payload JSON must be an object.")
        return payload

    token_ids = parse_token_ids(args.token_ids or os.getenv("TOKEN_IDS"))
    if not token_ids:
        raise ValueError(
            "Missing token ids. Set --token-ids or TOKEN_IDS, "
            "or provide --subscribe-payload-json."
        )

    return {
        "type": "subscribe",
        "token_ids": token_ids,
    }


def _load_json_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def run_probe(args: argparse.Namespace) -> ProbeSummary:
    try:
        import websocket  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "Missing optional dependency 'websocket-client'. "
            "Install it to run scripts/wss_probe.py."
        ) from exc

    subscribe_payload = load_subscribe_payload(args)
    max_messages = args.max_messages
    timeout_seconds = args.timeout_seconds
    url = args.wss_url
    out_path = Path(args.out) if args.out else None

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)

    messages: list[Any] = []
    ws = websocket.create_connection(url, timeout=timeout_seconds)
    out_handle = out_path.open("w", encoding="utf-8") if out_path is not None else None

    try:
        encoded_subscribe = json.dumps(
            subscribe_payload,
            separators=(",", ":"),
            sort_keys=True,
            ensure_ascii=True,
        )
        ws.send(encoded_subscribe)
        print(f"Connected to {url}")
        print(f"Subscribe payload: {encoded_subscribe}")

        for index in range(max_messages):
            raw_message = ws.recv()
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")

            parsed: Any
            try:
                parsed = json.loads(raw_message)
            except json.JSONDecodeError:
                parsed = {
                    "_non_json": True,
                    "_raw": raw_message,
                }

            messages.append(parsed)
            rendered = json.dumps(parsed, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
            print(f"[{index + 1}] {rendered}")
            if out_handle is not None:
                out_handle.write(rendered + "\n")
    except KeyboardInterrupt:
        print("Interrupted by user.")
    except Exception as exc:
        print(f"WSS probe stopped early: {type(exc).__name__}: {exc}")
    finally:
        if out_handle is not None:
            out_handle.close()
        ws.close()

    summary = analyze_messages(messages)
    print("Summary:")
    print(json.dumps(summary.as_dict(), indent=2, sort_keys=True, ensure_ascii=True))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe CLOB WSS message shape and seq support.")
    parser.add_argument(
        "--wss-url",
        default=os.getenv("CLOB_WSS_URL", DEFAULT_CLOB_WSS_URL),
        help="WSS endpoint URL.",
    )
    parser.add_argument(
        "--token-ids",
        default=None,
        help="Comma-separated token ids. Fallback to TOKEN_IDS env var.",
    )
    parser.add_argument(
        "--subscribe-payload-json",
        default=None,
        help="Path to JSON file used as raw subscribe payload.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=DEFAULT_MAX_MESSAGES,
        help="Maximum number of messages to capture.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.getenv("CLOB_WSS_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)),
        help="Socket timeout in seconds.",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional JSONL output path for captured messages.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.max_messages <= 0:
        parser.error("--max-messages must be > 0")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0")

    try:
        run_probe(args)
    except ValueError as exc:
        print(f"Invalid probe configuration: {exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
