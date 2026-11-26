#!/usr/bin/env bash
# Adds shaledb's Alchemy Stack to the pi Cloudflare deployment guard's allowlist, so agents can
# deploy and test the preview stages. Run it yourself; the guard blocks agents from editing the policy.
#
# Usage: scripts/allowlist-shaledb-deploys.sh
#
# Idempotent: it replaces any existing entry for this project, backs up the policy first, and
# validates the result before installing it. `alchemy destroy` stays blocked by the guard whatever the
# allowlist says; tear preview stages down yourself.
set -euo pipefail

POLICY_LINK="${HOME}/.pi/agent/cloudflare-deployment-allowlist.json"
# Resolved from this script's location, so it works wherever the repo is cloned.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
PROJECT="${REPO_ROOT}/infra/shaledb/alchemy.run.ts"
STACK="Shaledb"
# Reusable preview stages: redeployed in place rather than destroyed, so at most three exist.
STAGES='["dev","preview-1","preview-2","preview-3"]'

command -v jq >/dev/null || { echo "jq is required (brew install jq)." >&2; exit 1; }
[[ -e "${POLICY_LINK}" ]] || { echo "Policy not found at ${POLICY_LINK}." >&2; exit 1; }
# The policy is usually a symlink into ~/.dotfiles: edit the real file so the link survives.
POLICY="$(cd "$(dirname "${POLICY_LINK}")" && realpath "$(basename "${POLICY_LINK}")")"
echo "Editing ${POLICY}"
[[ -f "${PROJECT}" ]] || { echo "Alchemy entrypoint not found at ${PROJECT}." >&2; exit 1; }

version="$(jq -r '.version' "${POLICY}")"
[[ "${version}" == "2" ]] || { echo "Expected policy version 2, found ${version}." >&2; exit 1; }

backup="${POLICY}.bak.$(date +%Y%m%d-%H%M%S)"
cp "${POLICY}" "${backup}"

tmp="$(mktemp "${POLICY}.XXXXXX")"
trap 'rm -f "${tmp}"' EXIT

jq --arg project "${PROJECT}" --arg stack "${STACK}" --argjson stages "${STAGES}" '
  .alchemy = ([.alchemy[] | select(.project != $project)]
              + [{ project: $project, stages: $stages, stack: $stack }])
' "${POLICY}" > "${tmp}"

# Validate the shape the guard expects before installing it.
jq -e --arg project "${PROJECT}" '
  .version == 2
  and (.alchemy | map(select(.project == $project)) | length == 1)
  and (.alchemy | map(.project) | length == (unique | length))
' "${tmp}" >/dev/null || { echo "Validation failed; policy left unchanged (backup: ${backup})." >&2; exit 1; }

chmod "$(stat -f '%Lp' "${POLICY}" 2>/dev/null || echo 644)" "${tmp}"
mv "${tmp}" "${POLICY}"
trap - EXIT

echo "Allowlisted ${STACK} (${PROJECT}) for stages ${STAGES}."
echo "Backup: ${backup}"
echo
echo "The guard records the policy at session start: running pi sessions ask you to confirm the change,"
echo "or restart them to pick it up without a prompt."
jq --arg project "${PROJECT}" '.alchemy[] | select(.project == $project)' "${POLICY}"
