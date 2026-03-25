#!/usr/bin/env python3
"""
Daily incremental WhatsApp message dump.
Reads wa-groups-list.txt from each analysis folder, pulls new messages
from the bridge SQLite DB, and appends to markdown files.
Rotates files on the 1st of each month.

Usage: python3 daily-dump.py
"""

import sqlite3
import os
import re
import sys
from datetime import datetime
from pathlib import Path

BRIDGE_DB = os.path.expanduser("~/code/whatsapp-mcp/whatsapp-bridge/store/messages.db")
NOTES_DIR = os.path.expanduser("~/notes")

# All analysis folders to process
ANALYSIS_FOLDERS = [
    os.path.join(NOTES_DIR, "health-conversation-analysis"),
    os.path.join(NOTES_DIR, "ai-conversation-analysis"),
]

LOG_FILE = "/tmp/wa-daily-dump.log"


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def parse_groups_list(folder):
    """Read wa-groups-list.txt and return list of (jid, filename) tuples."""
    groups_file = os.path.join(folder, "wa-groups-list.txt")
    if not os.path.exists(groups_file):
        log(f"  SKIP {folder} (no wa-groups-list.txt)")
        return []

    groups = []
    with open(groups_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|", 1)
            if len(parts) == 2:
                groups.append((parts[0].strip(), parts[1].strip()))
    return groups


def get_last_timestamp(filepath):
    """Extract the last message timestamp from an existing markdown file."""
    if not os.path.exists(filepath):
        return None

    last_ts = None
    with open(filepath) as f:
        for line in f:
            # Match lines like **sender** (HH:MM): content
            # Look for date headers to track current date
            if line.startswith("### "):
                current_date = line[4:].strip()
            # We need the actual timestamp from the file, but we only store HH:MM
            # So we'll use the date header + time
            pass

    # Simpler: read the "Last Updated" line and use file mtime as proxy
    # But better: store last message ID in a state file
    return None


def get_last_message_id(state_file, jid):
    """Get the last dumped message ID for a group from state file."""
    if not os.path.exists(state_file):
        return None
    with open(state_file) as f:
        for line in f:
            if line.startswith(jid + "|"):
                return line.strip().split("|", 1)[1]
    return None


def update_last_message_id(state_file, jid, msg_id):
    """Update the last dumped message ID for a group."""
    lines = []
    found = False
    if os.path.exists(state_file):
        with open(state_file) as f:
            for line in f:
                if line.startswith(jid + "|"):
                    lines.append(f"{jid}|{msg_id}\n")
                    found = True
                else:
                    lines.append(line)
    if not found:
        lines.append(f"{jid}|{msg_id}\n")
    with open(state_file, "w") as f:
        f.writelines(lines)


def should_rotate(filepath):
    """Check if today is the 1st and the file has content from a previous month."""
    if datetime.now().day != 1:
        return False
    if not os.path.exists(filepath):
        return False
    # Only rotate files larger than 1KB
    if os.path.getsize(filepath) < 1024:
        return False
    return True


def rotate_file(filepath):
    """Move current file to archive/ subfolder with month suffix, start fresh."""
    now = datetime.now()
    if now.month == 1:
        prev_month = f"{now.year - 1}-12"
    else:
        prev_month = f"{now.year}-{now.month - 1:02d}"

    folder = os.path.dirname(filepath)
    archive_dir = os.path.join(folder, "archive")
    os.makedirs(archive_dir, exist_ok=True)

    basename = os.path.basename(filepath)
    base, ext = os.path.splitext(basename)
    archive_path = os.path.join(archive_dir, f"{base}-{prev_month}{ext}")

    if os.path.exists(filepath):
        os.rename(filepath, archive_path)
        log(f"  ROTATED {basename} -> archive/{base}-{prev_month}{ext}")
    return archive_path


def write_new_file_header(filepath, chat_name, msg_count=0):
    """Write the header for a new/rotated markdown file."""
    now = datetime.now().strftime("%Y-%m-%d")
    with open(filepath, "w") as f:
        f.write(f"# {chat_name}\n")
        f.write(f"Source: WhatsApp Group\n")
        f.write(f"Last Updated: {now}\n")
        f.write(f"Message Count: {msg_count}\n")
        f.write(f"\n## Messages\n\n")


def append_messages(filepath, messages, chat_name):
    """Append new messages to an existing markdown file."""
    if not messages:
        return 0

    # If file doesn't exist, create with header
    if not os.path.exists(filepath):
        write_new_file_header(filepath, chat_name)

    # Read existing file to find last date header
    with open(filepath) as f:
        content = f.read()

    # Find the last date in the file
    last_date = None
    for line in content.split("\n"):
        if line.startswith("### "):
            last_date = line[4:].strip()

    # Append new messages
    lines = []
    current_date = last_date
    count = 0

    for msg in messages:
        ts, sender, cname, msg_content, is_from_me, media_type = msg
        if not msg_content.strip():
            continue

        try:
            dt = datetime.fromisoformat(ts)
            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M")
        except:
            continue

        if date_str != current_date:
            current_date = date_str
            lines.append(f"\n### {date_str}\n")

        s = "Me" if is_from_me else (sender or "Unknown")
        lines.append(f"**{s}** ({time_str}): {msg_content.strip()}\n")
        count += 1

    if lines:
        with open(filepath, "a") as f:
            f.write("\n".join(lines))

        # Update the message count and last updated in header
        now = datetime.now().strftime("%Y-%m-%d")
        with open(filepath) as f:
            content = f.read()
        # Count actual message lines
        total = content.count("** (")
        content = re.sub(r"Message Count: \d+", f"Message Count: {total}", content)
        content = re.sub(r"Last Updated: [\d-]+", f"Last Updated: {now}", content)
        with open(filepath, "w") as f:
            f.write(content)

    return count


def process_folder(folder, conn):
    """Process all groups in one analysis folder."""
    folder_name = os.path.basename(folder)
    log(f"Processing {folder_name}...")

    groups = parse_groups_list(folder)
    if not groups:
        return

    state_file = os.path.join(folder, ".dump-state")
    cursor = conn.cursor()
    total_new = 0

    for jid, fname in groups:
        filepath = os.path.join(folder, fname + ".md")

        # Monthly rotation
        if should_rotate(filepath):
            rotate_file(filepath)

        # Get last message ID we dumped
        last_id = get_last_message_id(state_file, jid)

        # Query for new messages
        if last_id:
            cursor.execute("""
                SELECT m.timestamp, m.sender, c.name, m.content, m.is_from_me, m.media_type
                FROM messages m
                JOIN chats c ON m.chat_jid = c.jid
                WHERE m.chat_jid = ? AND m.content != '' AND m.id > ?
                ORDER BY m.timestamp ASC
            """, (jid, last_id))
        else:
            # First run after rotation or new group -- get everything
            cursor.execute("""
                SELECT m.timestamp, m.sender, c.name, m.content, m.is_from_me, m.media_type
                FROM messages m
                JOIN chats c ON m.chat_jid = c.jid
                WHERE m.chat_jid = ? AND m.content != ''
                ORDER BY m.timestamp ASC
            """, (jid,))

        rows = cursor.fetchall()

        if rows:
            chat_name = rows[0][2] or fname
            count = append_messages(filepath, rows, chat_name)
            total_new += count
            if count > 0:
                log(f"  {fname}: +{count} messages")

        # Get the latest message ID for state tracking
        cursor.execute("""
            SELECT m.id FROM messages m
            WHERE m.chat_jid = ?
            ORDER BY m.timestamp DESC LIMIT 1
        """, (jid,))
        latest = cursor.fetchone()
        if latest:
            update_last_message_id(state_file, jid, latest[0])

    log(f"  {folder_name} total: +{total_new} new messages")


def main():
    log("=== WhatsApp Daily Dump ===")

    if not os.path.exists(BRIDGE_DB):
        log(f"ERROR: Bridge DB not found at {BRIDGE_DB}")
        sys.exit(1)

    conn = sqlite3.connect(BRIDGE_DB)

    for folder in ANALYSIS_FOLDERS:
        if os.path.exists(folder):
            process_folder(folder, conn)
        else:
            log(f"SKIP {folder} (not found)")

    conn.close()
    log("=== Done ===\n")


if __name__ == "__main__":
    main()
