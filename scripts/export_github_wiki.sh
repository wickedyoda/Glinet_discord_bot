#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 /path/to/wiki-checkout" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_WIKI_DIR="$ROOT_DIR/wiki"
TARGET_DIR="$1"
REPO_BLOB_BASE="https://github.com/wickedyoda/Glinet_discord_bot/blob/main"
REPO_RAW_BASE="https://raw.githubusercontent.com/wickedyoda/Glinet_discord_bot/main"

if [[ ! -d "$SOURCE_WIKI_DIR" ]]; then
  echo "Source wiki directory not found: $SOURCE_WIKI_DIR" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR"
find "$TARGET_DIR" -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} +
cp "$SOURCE_WIKI_DIR"/*.md "$TARGET_DIR"/

for file in "$TARGET_DIR"/*.md; do
  perl -0pi -e 's#\.\./assets/images/glinet-bot-round\.png#'"$REPO_RAW_BASE"'/assets/images/glinet-bot-round.png#g' "$file"
  perl -0pi -e 's#\.\./assets/images/glinet-bot-full\.png#'"$REPO_RAW_BASE"'/assets/images/glinet-bot-full.png#g' "$file"
  perl -0pi -e 's#\((\.\./README\.md)\)#('"$REPO_BLOB_BASE"'/README.md)#g' "$file"
  perl -0pi -e 's#\((\.\./bot\.py)\)#('"$REPO_BLOB_BASE"'/bot.py)#g' "$file"
  perl -0pi -e 's#\((\.\./web_admin\.py)\)#('"$REPO_BLOB_BASE"'/web_admin.py)#g' "$file"
  perl -0pi -e 's#\((\.\./\.env\.example)\)#('"$REPO_BLOB_BASE"'/.env.example)#g' "$file"
  perl -0pi -e 's@\]\(([A-Za-z0-9._/-]+)\.md(\#[^)]+)?\)@]($1$2)@g' "$file"
done

echo "Exported wiki markdown to $TARGET_DIR"
