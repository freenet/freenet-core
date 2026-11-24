#!/usr/bin/env bash
# Shared lookups for the release scripts. Sourced, not run:
#   scripts/resolve_release_ship.sh, scripts/check_release_preflight.sh
#
# "The latest release" here is the highest SEMVER among published (non-draft,
# non-prerelease) releases. Not GitHub's /releases/latest: that marker can be
# moved by hand, and a recovery un-draft of an older tag deliberately leaves it
# where it is. Callers need the version order, not the marker.
#
# Every function prints nothing and returns non-zero when it cannot answer, so
# callers can fail closed.
#
# Needs REPO (owner/name) and a GH_TOKEN for `gh`.
# Tests: scripts/check_release_preflight_test.sh, scripts/resolve_release_ship_test.sh

# release_rows: one "tag<TAB>draft<TAB>prerelease" line per release, all pages.
# A token that cannot see drafts simply omits them; callers that must see
# unfinished releases use the TAGS (rel_remote_tags), which every cut pushes
# before it creates its draft.
rel_release_rows() {
    gh api --paginate "repos/$REPO/releases?per_page=100" \
        --jq '.[] | "\(.tag_name)\t\(.draft)\t\(.prerelease)"'
}

# rel_is_semver_tag TAG: vX.Y.Z exactly.
rel_is_semver_tag() {
    [[ "$1" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

# rel_version_gt A B: A > B in version order (X.Y.Z, no leading v).
rel_version_gt() {
    [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" = "$1" ]
}

# rel_max_version: the highest X.Y.Z on stdin (one per line), or nothing.
rel_max_version() {
    # `|| true`: grep exits 1 on no match, which under a caller's pipefail and
    # `set -e` would abort a bare `x=$(... | rel_max_version)` silently.
    { grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' || true; } | sort -V | tail -1
}

# rel_latest_published ROWS: the highest published, non-prerelease vX.Y.Z tag
# in ROWS (from rel_release_rows), or return 1 if there is none.
rel_latest_published() {
    local v
    v=$(awk -F'\t' '$2 == "false" && $3 == "false" {print $1}' <<< "$1" \
        | sed -n 's/^v//p' | rel_max_version)
    [ -n "$v" ] || return 1
    printf 'v%s\n' "$v"
}

# rel_remote_tags: every vX.Y.Z tag on origin, one per line, without the v.
rel_remote_tags() {
    local refs
    refs=$(git ls-remote --tags "https://github.com/$REPO.git" 'v*') || return 1
    awk '{print $2}' <<< "$refs" | sed -n 's|^refs/tags/v\([0-9]*\.[0-9]*\.[0-9]*\)$|\1|p' | sort -uV
}

# rel_remote_release_branches: every release/vX.Y.Z branch on origin, one
# version per line.
rel_remote_release_branches() {
    local refs
    refs=$(git ls-remote --heads "https://github.com/$REPO.git" 'release/v*') || return 1
    awk '{print $2}' <<< "$refs" | sed -n 's|^refs/heads/release/v\([0-9]*\.[0-9]*\.[0-9]*\)$|\1|p' | sort -uV
}
