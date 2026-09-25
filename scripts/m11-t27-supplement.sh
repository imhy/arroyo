#!/usr/bin/env bash
#
# M11.T27g — the supplemental D96 runner (plan M11.P64b; design M11.D96, Round-64 extension).
#
# T26's runner (scripts/m11-d39-matrix.sh) and its 37-row registry are T26's immutable record: this
# script never edits, patches or re-implements them. It runs the rows T27 adds (row 38, from
# scripts/m11-t27-supplement-registry.json) to T26's honesty rules, and with --with-aggregate first
# runs T26's runner unchanged, then combines both reports into the current lifecycle sign-off.
#
#   0. Immutability guard, before anything runs: T26's registry has exactly 37 rows, ids 1..37; it
#      and T26's runner are byte-identical to their blobs at base_pin; the supplement's ids are
#      disjoint from T26's and contiguous after them; no name is registered twice in the supplement;
#      both registries agree on `packages` and `topologies`. Any violation is FATAL and nothing runs.
#   1. Resolution proof, as strict as T26's (each name resolves to exactly one test across the six
#      packages' `--list`, in its declared crate), plus: no name may also be registered by T26.
#   2. Both topologies as two processes (ARROYO__JOB_CONTROLLER per cargo run), tests `--exact`.
#   3. Report: did-not-run or ignored is ERROR, failed FAIL, else PASS; GREEN only if all cells pass.
#
# Usage:  bash scripts/m11-t27-supplement.sh [--no-build] [--with-aggregate]
#
#   M11_D39_JOBS          cargo -j for both runners: default 1 here, passed on (T26's default is 4).
#   M11_T27_TEST_THREADS  --test-threads (default 1). T26's runner passes none, so its tests would
#                         take libtest's default; --with-aggregate exports RUST_TEST_THREADS from
#                         this value before invoking it (an environment variable, not an edit).
#   M11_T27_REGISTRY      the supplement registry (absolute or repo-relative). For negative
#                         controls: point it at an edited copy, never edit the checked-in one.
#                         T26's registry path is deliberately fixed.
#   M11_T27_REPORT, M11_D39_REPORT (T26's, under --with-aggregate), M11_T27_SIGNOFF: output paths,
#   default target/m11-t27-supplement-report.json, m11-d39-report.json, m11-lifecycle-signoff.json.
#
# Needs jq, cargo, git and the build environment T26's runner needs. Sourcing this file only
# defines its functions (how `combine_signoff` is exercised on hand-written reports).

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
T26_REGISTRY="${REPO_ROOT}/scripts/m11-d39-registry.json"
T26_RUNNER="${REPO_ROOT}/scripts/m11-d39-matrix.sh"
REGISTRY="${M11_T27_REGISTRY:-${REPO_ROOT}/scripts/m11-t27-supplement-registry.json}"
REPORT="${M11_T27_REPORT:-${REPO_ROOT}/target/m11-t27-supplement-report.json}"
T26_REPORT="${M11_D39_REPORT:-${REPO_ROOT}/target/m11-d39-report.json}"
SIGNOFF="${M11_T27_SIGNOFF:-${REPO_ROOT}/target/m11-lifecycle-signoff.json}"
JOBS="${M11_D39_JOBS:-1}"
TEST_THREADS="${M11_T27_TEST_THREADS:-1}"
T26_ROWS=37 # T26's historical aggregate (M11.D96): immutable, so a literal is the point
BUILD=1

die() { printf '\nm11-t27-supplement: FATAL: %s\n' "$*" >&2; exit 2; }
count_lines() { wc -l < "$1" | tr -d ' '; }

immutability_guard() {
    local problems f pinned
    problems="$(jq -rn --slurpfile t "${T26_REGISTRY}" --slurpfile s "${REGISTRY}" --argjson n "${T26_ROWS}" '
      $t[0] as $t | $s[0] as $s | [$t.rows[].id] as $tid | [$s.rows[].id] as $sid
      | [range($n + 1; $n + 1 + ($sid | length))] as $next | ($sid - ($sid - $tid)) as $both
      | [$s.rows[].tests[].name] as $names
      | [$names | group_by(.)[] | select(length > 1) | .[0]] as $twice
      | if ($t.rows | length) != $n then "T26 registry has \($t.rows | length) rows, not \($n)" else empty end,
        if $tid != [range(1; $n + 1)] then "T26 row ids are not exactly 1..\($n) in order" else empty end,
        if [$s.supplements | .registry, .rows] != ["scripts/m11-d39-registry.json", $n]
        then "the supplement does not name scripts/m11-d39-registry.json and its \($n) rows" else empty end,
        if $both != [] then "supplement ids \($both) collide with T26 row ids" else empty end,
        if $sid == [] or $sid != $next then "supplement ids \($sid) are not \($next)" else empty end,
        if $twice != [] then "the supplement registers \($twice) more than once" else empty end,
        if $s.schema != $t.schema then "the registries have different schemas" else empty end,
        if $s.packages != $t.packages then "the registries disagree on packages" else empty end,
        if $s.topologies != $t.topologies then "the registries disagree on topologies" else empty end
    ')" || die "the registries are not readable JSON in the expected shape"
    for f in scripts/m11-d39-registry.json scripts/m11-d39-matrix.sh; do
        pinned="$(git -C "${REPO_ROOT}" rev-parse -q --verify "$(jq -r .base_pin "${REGISTRY}"):${f}")" \
            || die "cannot read ${f} at the registry's base_pin"
        [ "$(git -C "${REPO_ROOT}" hash-object "${f}")" = "${pinned}" ] \
            || problems+=$'\n'"${f} is not byte-identical to its blob at base_pin"
    done
    if [ -n "$(printf '%s' "${problems}" | tr -d '\n')" ]; then
        printf 'IMMUTABILITY GUARD FAILED:\n%s\n' "$(printf '%s\n' "${problems}" | sed '/^$/d; s/^/  /')" >&2
        die "T26's record and the supplement are not in the shape M11.P64b requires; nothing was run"
    fi
    printf 'immutability guard passed: T26 rows 1..%s and runner at base_pin; supplement ids %s\n\n' \
        "${T26_ROWS}" "$(jq -c '[.rows[].id]' "${REGISTRY}")"
}

resolution_proof() { # writes ${WORK}/resolved.tsv: row, package, test path, name, kind
    local pkg row crate name kind matches count t26_rows failures=0
    refuse() { printf '  row %-2s  %-60s  %s\n' "${row}" "${name}" "$1"; failures=$((failures + 1)); }
    : > "${WORK}/listed.tsv"
    for pkg in "${PACKAGES[@]}"; do
        cargo test -p "${pkg}" --all-features -j "${JOBS}" -- --list > "${WORK}/list-${pkg}.log" 2>&1 \
            || { tail -30 "${WORK}/list-${pkg}.log" >&2; die "could not enumerate ${pkg}'s tests"; }
        sed -n 's/^\(.*\): test$/\1/p' "${WORK}/list-${pkg}.log" \
            | awk -v p="${pkg}" '{ print p "\t" $0 }' >> "${WORK}/listed.tsv"
    done
    printf 'enumerated %s tests across %s packages\n' "$(count_lines "${WORK}/listed.tsv")" "${#PACKAGES[@]}"
    jq -r '.rows[] as $r | $r.tests[] | [($r.id|tostring), .crate, .name, .kind] | @tsv' "${REGISTRY}" \
        > "${WORK}/registered.tsv"
    jq -r '.rows[] as $r | $r.tests[] | [.name, ($r.id|tostring)] | @tsv' "${T26_REGISTRY}" > "${WORK}/t26.tsv"
    : > "${WORK}/resolved.tsv"
    while IFS=$'\t' read -r row crate name kind; do
        t26_rows="$(awk -F'\t' -v n="${name}" '$1 == n { print $2 }' "${WORK}/t26.tsv" | paste -sd, -)"
        [ -n "${t26_rows}" ] && { refuse "ALSO REGISTERED BY T26 (row ${t26_rows})"; continue; }
        # Every occurrence of the bare name in all six packages, not only the declared crate: a
        # duplicate in a sibling crate is exactly the drift this proof exists to catch.
        matches="$(awk -F'\t' -v n="${name}" '{ p = $2; sub(/^.*::/, "", p) } p == n || $2 == n' \
            "${WORK}/listed.tsv")"
        count="$(printf '%s' "${matches}" | grep -c .)"
        if [ "${count}" -ne 1 ]; then
            refuse "UNRESOLVED (${count} matches)"
            [ "${count}" -gt 0 ] && printf '%s\n' "${matches}" | sed 's/^/            /'
            continue
        fi
        [ "${matches%%$'\t'*}" = "${crate}" ] \
            || { refuse "WRONG CRATE (registry says ${crate}, resolved in ${matches%%$'\t'*})"; continue; }
        printf '%s\t%s\t%s\t%s\n' "${row}" "${matches}" "${name}" "${kind}" >> "${WORK}/resolved.tsv"
    done < "${WORK}/registered.tsv"
    if [ "${failures}" -ne 0 ]; then
        printf '\nRESOLUTION PROOF FAILED: %s of %s supplement names do not resolve to exactly one test in\n' \
            "${failures}" "$(count_lines "${WORK}/registered.tsv")"
        printf 'their declared crate, or are also registered by T26. No cell was executed.\n'
        exit 1
    fi
    printf 'RESOLUTION PROOF PASSED: all %s names resolve to exactly one test each; none is T26'"'"'s.\n\n' \
        "$(count_lines "${WORK}/registered.tsv")"
}

execute() { # writes ${WORK}/results.tsv: topology, package, test path, outcome
    local topology key val pkg code paths=()
    : > "${WORK}/results.tsv"
    for topology in "${TOPOLOGIES[@]}"; do
        key="$(jq -r --arg t "${topology}" '.topologies[] | select(.id == $t) | .env | keys[0]' "${REGISTRY}")"
        val="$(jq -r --arg t "${topology}" '.topologies[] | select(.id == $t) | .env[]' "${REGISTRY}")"
        printf -- '--- 2. executing topology %s (%s=%s) ---\n' "${topology}" "${key}" "${val}"
        for pkg in "${PACKAGES[@]}"; do
            mapfile -t paths < <(awk -F'\t' -v p="${pkg}" '$2 == p { print $3 }' "${WORK}/resolved.tsv" | sort -u)
            [ "${#paths[@]}" -eq 0 ] && continue
            env "${key}=${val}" cargo test -p "${pkg}" --all-features -j "${JOBS}" --no-fail-fast -- \
                --exact --test-threads="${TEST_THREADS}" "${paths[@]}" > "${WORK}/run-${topology}-${pkg}.log" 2>&1
            code=$?
            sed -n 's/^test \([A-Za-z0-9_:]*\) \.\.\. \(ok\|FAILED\|ignored\).*$/\1\t\2/p' \
                "${WORK}/run-${topology}-${pkg}.log" \
                | awk -v t="${topology}" -v p="${pkg}" '{ print t "\t" p "\t" $0 }' >> "${WORK}/results.tsv"
            awk -F'\t' -v t="${topology}" -v p="${pkg}" -v n="${#paths[@]}" -v c="${code}" '
                $1 == t && $2 == p { if ($4 == "ok") ok++; else bad++ }
                END { printf "  %-22s %3s tests ... %d ok, %d not ok (cargo exit %s)\n", p, n, ok, bad, c }
            ' "${WORK}/results.tsv"
        done
    done
}

write_report() {
    mkdir -p "$(dirname "${REPORT}")"
    jq -n --slurpfile registry "${REGISTRY}" --rawfile resolved "${WORK}/resolved.tsv" \
        --rawfile results "${WORK}/results.tsv" \
        --arg generated "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg path "${REGISTRY#"${REPO_ROOT}/"}" \
        --argjson listed "$(count_lines "${WORK}/listed.tsv")" --argjson threads "${TEST_THREADS}" '
      def tsv: [splits("\n")] | map(select(length > 0) | split("\t"));
      def count(f): [f] | length;
      $registry[0] as $reg
      | ($resolved | tsv | map({key: "\(.[0]) \(.[3])", value: {pkg: .[1], path: .[2]}}) | from_entries) as $at
      | ($results | tsv | map({key: "\(.[0]) \(.[1]) \(.[2])", value: .[3]}) | from_entries) as $outcome
      | { schema: "m11-t27-supplement-report/1", generated_utc: $generated, registry: $path,
          registry_schema: $reg.schema, milestone: $reg.milestone, base_pin: $reg.base_pin,
          supplements: $reg.supplements, enumerated_tests: $listed, test_threads: $threads,
          topologies: [$reg.topologies[].id],
          rows: [ $reg.rows[] as $row
            | ($row | {id, owner, requirement, topology, fixture, distinct_path, note}) + {
                cells: [ $reg.topologies[] as $t
                  | { topology: $t.id, env: $t.env,
                      tests: [ $row.tests[] | ($at["\($row.id) \(.name)"] // {}) as $w
                        | {name, crate, kind, path: $w.path,
                           outcome: ($outcome["\($t.id) \($w.pkg) \($w.path)"] // "did-not-run")} ] }
                  | . + {status: (if   any(.tests[]; .outcome == "did-not-run") then "ERROR"
                                  elif any(.tests[]; .outcome == "FAILED")      then "FAIL"
                                  elif any(.tests[]; .outcome == "ignored")     then "ERROR"
                                  else "PASS" end)} ] }
            | . + {status: (if all(.cells[]; .status == "PASS") then "GREEN" else "RED" end)} ] }
      | . + { summary: ({
          rows_total: count(.rows[]), rows_green: count(.rows[] | select(.status == "GREEN")),
          cells_total: count(.rows[].cells[]), cells_pass: count(.rows[].cells[] | select(.status == "PASS")),
          cells_fail: count(.rows[].cells[] | select(.status == "FAIL")),
          cells_error: count(.rows[].cells[] | select(.status == "ERROR")),
          rows_topology_dependent: count(.rows[] | select(.topology == "dependent")),
          rows_fixture_bound: count(.rows[] | select(.fixture != "portable")) }
          | .rows_red = .rows_total - .rows_green) }
    ' > "${REPORT}" || die "could not assemble the report"
}

print_summary() {
    printf -- '\n--- 3. the report ---\n'
    jq -r '.rows[]
        | "row \(.id)  owner \(.owner)  \(if .topology == "dependent" then "DEPENDENT" else "independent" end)"
          + "  fixture \(.fixture)  \(.status)\n  \(.requirement)",
          (.cells[] | "  cell \(.topology): \(.status)  (\(.tests | map(select(.outcome == "ok")) | length)"
                      + "/\(.tests | length) tests ok)"),
          (.cells[] | select(.status != "PASS") | .topology as $t | .tests[] | select(.outcome != "ok")
                    | "    \($t): \(.crate)::\(.path // .name) -> \(.outcome)")' "${REPORT}"
    jq -r '.summary | "\nrows:  \(.rows_green)/\(.rows_total) green   (a row is green only when both cells pass)",
        "cells: \(.cells_pass)/\(.cells_total) pass, \(.cells_fail) fail, \(.cells_error) could not run"' "${REPORT}"
    cat <<'CAVEAT'

What a PASS here proves, and what it does not:
  * Row 38 is topology-DEPENDENT through one test, arroyo-controller's
    an_already_running_adoption_leaves_the_old_generation_unfenced_only_under_a_worker_leader: it
    checks config().job_controller against ARROYO__JOB_CONTROLLER, then asserts that topology's
    route. Worker-leader: the row stays Running and the leader route sends the inherited generation
    nothing (the window opens). Controller: the row becomes Compiling and the preamble's superseding
    discharge sends one advance under the adopted fence (it never opens). Two routes, not one twice.
  * The five arroyo-worker tests (the guard half) read no topology: their second cell is execution
    evidence, not coverage. The other two controller tests build through StateMachine::new, whose
    start reads the knob, but assert nothing that depends on it.
  * Not a live cluster: a production WorkerServer behind in-process tonic, test-double schedulers
    and workers, migrated SQLite, and a program that does not load, so the controller test runs
    each route's steps itself and pins them to their production callers by source text.
  * Nothing here covers T26's 37 rows; --with-aggregate runs those through T26's runner, unchanged.
CAVEAT
    printf '\nmachine-readable report: %s\n' "${REPORT}"
}

run_supplement() { # exits 0 only when every supplement row is green
    local pkg
    mapfile -t PACKAGES < <(jq -r '.packages[]' "${REGISTRY}")
    mapfile -t TOPOLOGIES < <(jq -r '.topologies[].id' "${REGISTRY}")
    if [ "${BUILD}" -eq 1 ]; then # per package, as the listing and the runs invoke cargo: same artifacts
        for pkg in "${PACKAGES[@]}"; do
            cargo test -p "${pkg}" --all-features -j "${JOBS}" --no-run >> "${WORK}/build.log" 2>&1 \
                || { tail -40 "${WORK}/build.log" >&2; die "${pkg}'s test binaries do not build; nothing ran"; }
        done
    fi
    printf -- '--- 1. resolution proof (-j %s, --test-threads=%s) ---\n' "${JOBS}" "${TEST_THREADS}"
    resolution_proof
    execute
    write_report
    print_summary
    jq -r '.summary | if .rows_red == 0
        then "RESULT: all \(.rows_total) supplement rows green in both controller topologies"
             + " (\(.cells_pass)/\(.cells_total) cells pass)."
        else "RESULT: \(.rows_red) of \(.rows_total) supplement rows are NOT green. See above." end' "${REPORT}"
    [ "$(jq -r .summary.rows_red "${REPORT}")" = "0" ] && exit 0
    exit 1
}

report_half() { # <report> <schema> <expected-ids-json>: counts computed from the rows, and every "why not"
    local tops
    tops="$(jq -c '[.topologies[].id]' "${REGISTRY}")" || return 1
    [ -s "$1" ] || { jq -nc --arg r "$1" '{ok: false, why: "no report at \($r)"}'; return 0; }
    jq -c --arg schema "$2" --argjson ids "$3" --argjson tops "${tops}" '
      . as $r
      | { rows: (.rows | length), cells: ([.rows[].cells[]] | length),
          rows_green: ([.rows[] | select(.status == "GREEN")] | length),
          cells_pass: ([.rows[].cells[] | select(.status == "PASS")] | length) } as $c
      | [ if $r.schema != $schema then "schema is \($r.schema), not \($schema)" else empty end,
          if [$r.rows[].id] != $ids then "rows are \([$r.rows[].id]), not \($ids)" else empty end,
          if $r.topologies != $tops or any($r.rows[]; [.cells[].topology] != $tops)
          then "cells are not one per topology \($tops)" else empty end,
          if $c.rows_green != $c.rows then "\($c.rows_green)/\($c.rows) rows green" else empty end,
          if $c.cells_pass != $c.cells then "\($c.cells_pass)/\($c.cells) cells pass" else empty end
        ] as $why
      | $c + {ok: ($why == []), why: ($why | join("; "))}
    ' "$1" 2> /dev/null || jq -nc --arg r "$1" '{ok: false, why: "\($r) is not a readable report"}'
}

# The current lifecycle sign-off from T26's report and the supplement's: both counts kept apart, the
# sum computed from them. Returns non-zero naming each failed half, and then writes no sign-off.
combine_signoff() { # <t26-report> <supplement-report>
    local t26 t27 head failed=0
    rm -f "${SIGNOFF}"
    t26="$(report_half "$1" m11-d39-report/1 "$(jq -nc --argjson n "${T26_ROWS}" '[range(1; $n + 1)]')")" || return 2
    t27="$(report_half "$2" m11-t27-supplement-report/1 "$(jq -c '[.rows[].id]' "${REGISTRY}")")" || return 2
    jq -r 'select(.ok != true) | "SIGN-OFF REFUSED: the immutable T26 half failed: \(.why)"' <<< "${t26}" \
        | grep . && failed=1
    jq -r 'select(.ok != true) | "SIGN-OFF REFUSED: the T27 supplement half failed: \(.why)"' <<< "${t27}" \
        | grep . && failed=1
    [ "${failed}" -eq 0 ] || return 1
    head="$(git -C "${REPO_ROOT}" rev-parse HEAD)" || { echo 'SIGN-OFF REFUSED: no arroyo HEAD'; return 1; }
    mkdir -p "$(dirname "${SIGNOFF}")"
    jq -n --argjson a "${t26}" --argjson b "${t27}" --arg ra "$1" --arg rb "$2" --arg head "${head}" \
        --argjson dirty "$(git -C "${REPO_ROOT}" status --porcelain -uno | wc -l)" \
        --arg generated "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '
      { schema: "m11-lifecycle-signoff/1", generated_utc: $generated,
        arroyo_head: $head, uncommitted_tracked_changes: $dirty,
        immutable_t26: {report: $ra, rows: $a.rows, cells: $a.cells},
        t27_supplement: {report: $rb, rows: $b.rows, cells: $b.cells},
        current: {checks: ($a.rows + $b.rows), results: ($a.cells + $b.cells)} }
    ' > "${SIGNOFF}" || { echo "SIGN-OFF REFUSED: could not write ${SIGNOFF}"; return 1; }
    jq -r '"immutable T26 aggregate: \(.immutable_t26.rows) rows / \(.immutable_t26.cells) results",
        "T27 supplement:          \(.t27_supplement.rows) rows / \(.t27_supplement.cells) results",
        "arroyo HEAD \(.arroyo_head); \(.uncommitted_tracked_changes) uncommitted tracked changes",
        "RESULT: current M11 lifecycle sign-off \(.current.checks) checks / \(.current.results)"
        + " topology-specific results"' "${SIGNOFF}"
}

main() {
    local aggregate=0 t26_args=() t26_exit=0 supp_exit arg tool
    for arg in "$@"; do
        case "${arg}" in
            --no-build) BUILD=0; t26_args+=(--no-build) ;;
            --with-aggregate) aggregate=1 ;;
            *) die "unknown argument ${arg}; usage: $0 [--no-build] [--with-aggregate]" ;;
        esac
    done
    for tool in jq cargo git; do command -v "${tool}" > /dev/null 2>&1 || die "${tool} is required"; done
    cd "${REPO_ROOT}" || die "cannot enter ${REPO_ROOT}"
    [ -f "${REGISTRY}" ] || die "supplement registry not found at ${REGISTRY}"
    { [ -f "${T26_REGISTRY}" ] && [ -f "${T26_RUNNER}" ]; } || die "T26's registry or runner is missing"
    WORK="$(mktemp -d)"
    trap 'rm -rf "${WORK}"' EXIT
    printf 'M11.T27g supplemental D96 runner: %s (schema %s), base pin %s\n\n' \
        "${REGISTRY#"${REPO_ROOT}/"}" "$(jq -r .schema "${REGISTRY}")" "$(jq -r .base_pin "${REGISTRY}")"
    immutability_guard
    if [ "${aggregate}" -eq 1 ]; then
        rm -f "${T26_REPORT}" "${SIGNOFF}"
        printf -- '--- aggregate first: T26'"'"'s runner, unchanged (M11_D39_JOBS=%s RUST_TEST_THREADS=%s)\n' \
            "${JOBS}" "${TEST_THREADS}"
        M11_D39_JOBS="${JOBS}" M11_D39_REPORT="${T26_REPORT}" RUST_TEST_THREADS="${TEST_THREADS}" \
            bash "${T26_RUNNER}" "${t26_args[@]}"
        t26_exit=$?
        printf -- '--- T26'"'"'s runner exited %s ---\n\n' "${t26_exit}"
    fi
    rm -f "${REPORT}"
    ( run_supplement ); supp_exit=$?
    [ "${aggregate}" -eq 1 ] || exit "${supp_exit}"
    printf -- '\n--- current M11 lifecycle sign-off ---\n'
    [ "${t26_exit}" -eq 0 ] || echo "SIGN-OFF REFUSED: the immutable T26 half failed: its runner exited ${t26_exit}"
    [ "${supp_exit}" -eq 0 ] || echo "SIGN-OFF REFUSED: the T27 supplement half failed: it exited ${supp_exit}"
    { [ "${t26_exit}" -eq 0 ] && [ "${supp_exit}" -eq 0 ]; } || exit 1
    combine_signoff "${T26_REPORT}" "${REPORT}" || exit 1
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then main "$@"; fi
