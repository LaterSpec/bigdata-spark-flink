import json
import csv
import glob
from pathlib import Path

input_files = glob.glob("*.live_chat.json")

if not input_files:
    raise FileNotFoundError("No se encontró ningún archivo .live_chat.json")

input_path = input_files[0]
output_path = Path(input_path).with_suffix("").with_suffix(".csv")

rows = []

with open(input_path, "r", encoding="utf-8") as f:
    for line in f:
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue

        actions = (
            item.get("replayChatItemAction", {})
                .get("actions", [])
        )

        for action in actions:
            renderer = (
                action.get("addChatItemAction", {})
                      .get("item", {})
                      .get("liveChatTextMessageRenderer")
            )

            if not renderer:
                continue

            message_runs = renderer.get("message", {}).get("runs", [])
            message = "".join(run.get("text", "") for run in message_runs)

            author = renderer.get("authorName", {}).get("simpleText", "")
            timestamp_text = renderer.get("timestampText", {}).get("simpleText", "")
            timestamp_usec = renderer.get("timestampUsec", "")
            author_channel_id = (
                renderer.get("authorExternalChannelId", "")
            )

            rows.append({
                "video_id": "e8vV8tkfO4c",
                "timestamp_text": timestamp_text,
                "timestamp_usec": timestamp_usec,
                "author": author,
                "author_channel_id": author_channel_id,
                "message": message
            })

with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "video_id",
            "timestamp_text",
            "timestamp_usec",
            "author",
            "author_channel_id",
            "message"
        ]
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Archivo generado: {output_path}")
print(f"Mensajes extraídos: {len(rows)}")