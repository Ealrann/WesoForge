#!/bin/sh
set -eu

CONFIG_HOME="${XDG_CONFIG_HOME:-$HOME/.config}"
CONFIG_DIR="${CONFIG_HOME}/bbr-client"
CONFIG_FILE="${CONFIG_DIR}/config.json"

trim() {
  printf '%s' "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

json_string_or_null() {
  value="$(trim "$1")"
  if [ -z "$value" ]; then
    printf 'null'
    return
  fi
  escaped="$(printf '%s' "$value" | tr '\r\n' '  ' | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')"
  printf '"%s"' "$escaped"
}

reward_address="${BBR_REWARD_ADDRESS:-${BBR_SUBMITTER_REWARD_ADDRESS:-}}"
submitter_name="${BBR_SUBMITTER_NAME:-${BBR_NAME:-}}"

if [ -n "${BBR_SUBMITTER_CONFIG_JSON:-}" ]; then
  mkdir -p "$CONFIG_DIR"
  printf '%s\n' "$BBR_SUBMITTER_CONFIG_JSON" > "$CONFIG_FILE"
elif [ -n "${reward_address}" ] || [ -n "${submitter_name}" ]; then
  mkdir -p "$CONFIG_DIR"
  reward_json="$(json_string_or_null "$reward_address")"
  name_json="$(json_string_or_null "$submitter_name")"
  cat > "$CONFIG_FILE" <<EOF
{
  "reward_address": $reward_json,
  "name": $name_json
}
EOF
fi

if [ "$#" -eq 0 ]; then
  set -- wesoforge
fi
if [ "${1#-}" != "$1" ]; then
  set -- wesoforge "$@"
fi

exec "$@"
