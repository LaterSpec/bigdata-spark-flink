import csv
import json
import re
import glob
from pathlib import Path

MIN_CHARS = 3
KEEP_SHORT = {"jp", "fp", "jne", "onpe", "no", "si", "sí"}

OUTPUT_CSV = "youtube_lake.csv"

def extract_video_id(filename):
    match = re.search(r"\[([A-Za-z0-9_-]{8,})\]\.live_chat\.json$", filename)
    return match.group(1) if match else ""

def clean_text(text):
    if not text:
        return ""

    text = text.replace("\u200b", " ")
    text = text.replace("\ufeff", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def runs_to_text(runs):
    parts = []

    for run in runs or []:
        if "text" in run:
            parts.append(run["text"])
        elif "emoji" in run:
            emoji = run["emoji"]
            shortcuts = emoji.get("shortcuts") or []
            if shortcuts:
                parts.append(shortcuts[0])
            else:
                parts.append(emoji.get("image", {}).get("accessibility", {}).get("accessibilityData", {}).get("label", ""))

    return clean_text("".join(parts))

def get_badges(renderer):
    badges = []

    for badge in renderer.get("authorBadges", []) or []:
        badge_renderer = badge.get("liveChatAuthorBadgeRenderer", {})
        tooltip = badge_renderer.get("tooltip")
        if tooltip:
            badges.append(tooltip)

    return "|".join(badges)

def should_keep_message(message):
    normalized = clean_text(message)
    compact = re.sub(r"\s+", "", normalized).lower()

    if not compact:
        return False

    if compact in KEEP_SHORT:
        return True

    if len(compact) < MIN_CHARS:
        return False

    return True

def parse_renderer(item):
    if "liveChatTextMessageRenderer" in item:
        r = item["liveChatTextMessageRenderer"]
        return {
            "message_type": "text",
            "message": runs_to_text(r.get("message", {}).get("runs", [])),
            "amount": "",
            "currency": "",
            "purchase_text": "",
            "author": r.get("authorName", {}).get("simpleText", ""),
            "author_channel_id": r.get("authorExternalChannelId", ""),
            "timestamp_text": r.get("timestampText", {}).get("simpleText", ""),
            "timestamp_usec": r.get("timestampUsec", ""),
            "badges": get_badges(r),
        }

    if "liveChatPaidMessageRenderer" in item:
        r = item["liveChatPaidMessageRenderer"]
        return {
            "message_type": "superchat",
            "message": runs_to_text(r.get("message", {}).get("runs", [])),
            "amount": r.get("purchaseAmountText", {}).get("simpleText", ""),
            "currency": "",
            "purchase_text": r.get("purchaseAmountText", {}).get("simpleText", ""),
            "author": r.get("authorName", {}).get("simpleText", ""),
            "author_channel_id": r.get("authorExternalChannelId", ""),
            "timestamp_text": r.get("timestampText", {}).get("simpleText", ""),
            "timestamp_usec": r.get("timestampUsec", ""),
            "badges": get_badges(r),
        }

    if "liveChatPaidStickerRenderer" in item:
        r = item["liveChatPaidStickerRenderer"]
        sticker_label = (
            r.get("sticker", {})
             .get("accessibility", {})
             .get("accessibilityData", {})
             .get("label", "")
        )

        return {
            "message_type": "supersticker",
            "message": clean_text(sticker_label),
            "amount": r.get("purchaseAmountText", {}).get("simpleText", ""),
            "currency": "",
            "purchase_text": r.get("purchaseAmountText", {}).get("simpleText", ""),
            "author": r.get("authorName", {}).get("simpleText", ""),
            "author_channel_id": r.get("authorExternalChannelId", ""),
            "timestamp_text": r.get("timestampText", {}).get("simpleText", ""),
            "timestamp_usec": r.get("timestampUsec", ""),
            "badges": get_badges(r),
        }

    if "liveChatMembershipItemRenderer" in item:
        r = item["liveChatMembershipItemRenderer"]
        header = runs_to_text(r.get("headerSubtext", {}).get("runs", []))
        message = runs_to_text(r.get("message", {}).get("runs", []))

        return {
            "message_type": "membership",
            "message": clean_text(f"{header} {message}"),
            "amount": "",
            "currency": "",
            "purchase_text": "",
            "author": r.get("authorName", {}).get("simpleText", ""),
            "author_channel_id": r.get("authorExternalChannelId", ""),
            "timestamp_text": r.get("timestampText", {}).get("simpleText", ""),
            "timestamp_usec": r.get("timestampUsec", ""),
            "badges": get_badges(r),
        }

    return None

def main():
    files = sorted(glob.glob("*.live_chat.json"))

    if not files:
        print("No encontré archivos .live_chat.json en esta carpeta.")
        return

    rows = []
    seen = set()

    for file_path in files:
        path = Path(file_path)
        video_id = extract_video_id(path.name)

        print(f"Procesando: {path.name}")

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue

                replay = item.get("replayChatItemAction", {})
                actions = replay.get("actions", [])

                for action in actions:
                    chat_item = (
                        action.get("addChatItemAction", {})
                              .get("item", {})
                    )

                    parsed = parse_renderer(chat_item)
                    if not parsed:
                        continue

                    message = clean_text(parsed["message"])

                    if parsed["message_type"] == "text" and not should_keep_message(message):
                        continue

                    unique_key = (
                        video_id,
                        parsed["timestamp_usec"],
                        parsed["author_channel_id"],
                        message,
                        parsed["message_type"],
                        parsed["purchase_text"]
                    )

                    if unique_key in seen:
                        continue

                    seen.add(unique_key)

                    rows.append({
                        "source_file": path.name,
                        "video_id": video_id,
                        "message_type": parsed["message_type"],
                        "timestamp_text": parsed["timestamp_text"],
                        "timestamp_usec": parsed["timestamp_usec"],
                        "video_offset_msec": replay.get("videoOffsetTimeMsec", ""),
                        "author": parsed["author"],
                        "author_channel_id": parsed["author_channel_id"],
                        "badges": parsed["badges"],
                        "purchase_text": parsed["purchase_text"],
                        "message": message,
                        "message_length": len(message),
                    })

    # Conserva el bloque original arriba y agrega una copia completa abajo.
    rows = rows + [row.copy() for row in rows]

    fieldnames = [
        "source_file",
        "video_id",
        "message_type",
        "timestamp_text",
        "timestamp_usec",
        "video_offset_msec",
        "author",
        "author_channel_id",
        "badges",
        "purchase_text",
        "message",
        "message_length",
    ]

    with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    counts = {}
    for row in rows:
        counts[row["message_type"]] = counts.get(row["message_type"], 0) + 1

    print()
    print(f"Archivo generado: {OUTPUT_CSV}")
    print(f"Total filas: {len(rows)}")
    print("Conteo por tipo:")
    for key, value in sorted(counts.items()):
        print(f"- {key}: {value}")

if __name__ == "__main__":
    main()
