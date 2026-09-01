#!/usr/bin/env bash
#
# M11.T26g — the aggregate D96 matrix runner.
#
# Produces the 37-row x 2-controller-topology report M11.D96 and DoD M11.T26y are stated in
# terms of, from the machine-readable registry in `scripts/m11-d39-registry.json`.
#
#   1. Resolution proof. Every registered test name must resolve to exactly one test in the
#      built binaries. This is what stops the registry drifting from the source: a test renamed,
#      deleted, or duplicated under a second module makes its row unresolvable and the run stops
#      before anything is executed. Nothing is inferred from the file paths the registry records
#      — those are for a human, and the proof is against `--list`.
#
#   2. Both topologies, as two processes. `config().job_controller` is a process-global knob
#      (`arroyo-rpc/src/config.rs`), loaded by figment from `ARROYO__JOB_CONTROLLER`. The
#      codebase forbids flipping it inside a suite — see `PhaseContext::run_as_leader_on` — so
#      the runner sets it in the environment and executes the registered tests twice, in two
#      genuinely different process configurations. It is not a re-labelled single run.
#
#   3. A row is green only when both cells pass. Multi-test cells pass as a group.
#
# Honesty rules this script is built to (M11.T26g):
#
#   * a cell that could not run is reported as `ERROR`, never omitted and never counted green;
#   * a row whose fixture is bound to one topology is reported with that binding printed, so a
#     red cell is never read as a mechanism failure nor a green one as a coverage claim;
#   * the report distinguishes topology-DEPENDENT rows (which assert the distinct path was
#     reached) from topology-INDEPENDENT ones (which merely execute under both). Executing a
#     test twice proves execution, not coverage, and the report says so in as many words.
#
# Usage:  bash scripts/m11-d39-matrix.sh [--no-build]
#
# Requires the workspace's build environment to be active already (protoc, a live PostgreSQL for
# the cornucopia build scripts, and the linker shims); this script builds nothing that the
# ordinary `cargo test` for these packages does not.
#
# Environment:
#   M11_D39_REPORT   where the machine-readable report is written
#                    (default: target/m11-d39-report.json)
#   M11_D39_JOBS     value passed to cargo's -j (default: 4)

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REGISTRY="${REPO_ROOT}/scripts/m11-d39-registry.json"
REPORT="${M11_D39_REPORT:-${REPO_ROOT}/target/m11-d39-report.json}"
JOBS="${M11_D39_JOBS:-4}"
BUILD=1
[ "${1:-}" = "--no-build" ] && BUILD=0

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

die() { printf '\nm11-d39-matrix: FATAL: %s\n' "$*" >&2; exit 2; }

command -v jq >/dev/null 2>&1 || die "jq is required to read the registry and write the report"
command -v cargo >/dev/null 2>&1 || die "cargo is required"
[ -f "${REGISTRY}" ] || die "registry not found at ${REGISTRY}"

cd "${REPO_ROOT}" || die "cannot enter ${REPO_ROOT}"

mapfile -t PACKAGES < <(jq -r '.packages[]' "${REGISTRY}")
mapfile -t TOPOLOGIES < <(jq -r '.topologies[].id' "${REGISTRY}")
ROW_COUNT="$(jq -r '.rows | length' "${REGISTRY}")"

printf '=======================================================================\n'
printf 'M11.D96 aggregate matrix — %s rows x %s controller topologies\n' \
    "${ROW_COUNT}" "${#TOPOLOGIES[@]}"
printf 'registry: scripts/m11-d39-registry.json (schema %s)\n' "$(jq -r .schema "${REGISTRY}")"
printf 'base pin: %s\n' "$(jq -r .base_pin "${REGISTRY}")"
printf '=======================================================================\n\n'

# ---------------------------------------------------------------------------------------------
# 0. Build the test binaries once. Both topologies run the same binaries: the knob is read at
#    runtime, so a rebuild between them would be a rebuild of identical code.
# ---------------------------------------------------------------------------------------------

PKG_ARGS=()
for pkg in "${PACKAGES[@]}"; do PKG_ARGS+=(-p "${pkg}"); done

if [ "${BUILD}" -eq 1 ]; then
    printf -- '--- building test binaries (%s packages, -j %s) ---\n' "${#PACKAGES[@]}" "${JOBS}"
    if ! cargo test "${PKG_ARGS[@]}" --all-features -j "${JOBS}" --no-run > "${WORK}/build.log" 2>&1; then
        tail -40 "${WORK}/build.log" >&2
        die "the test binaries do not build; nothing was run"
    fi
    printf 'built.\n\n'
fi

# ---------------------------------------------------------------------------------------------
# 1. Resolution proof.
# ---------------------------------------------------------------------------------------------

printf -- '--- 1. resolution proof: every registered name resolves to exactly one test ---\n'

: > "${WORK}/listed.tsv"
for pkg in "${PACKAGES[@]}"; do
    if ! cargo test -p "${pkg}" --all-features -j "${JOBS}" -- --list \
            > "${WORK}/list-${pkg}.log" 2>&1; then
        tail -30 "${WORK}/list-${pkg}.log" >&2
        die "could not enumerate ${pkg}'s tests; the registry cannot be proven against it"
    fi
    # `<module::path::name>: test` is libtest's listing line.
    sed -n 's/^\(.*\): test$/\1/p' "${WORK}/list-${pkg}.log" \
        | while IFS= read -r path; do printf '%s\t%s\n' "${pkg}" "${path}"; done \
        >> "${WORK}/listed.tsv"
done

TOTAL_LISTED="$(wc -l < "${WORK}/listed.tsv" | tr -d ' ')"
printf 'enumerated %s tests across %s packages\n' "${TOTAL_LISTED}" "${#PACKAGES[@]}"

jq -r '.rows[] as $r | $r.tests[] | [($r.id|tostring), .crate, .name, .kind] | @tsv' \
    "${REGISTRY}" > "${WORK}/registered.tsv"

RESOLUTION_FAILURES=0
: > "${WORK}/resolved.tsv"
while IFS=$'\t' read -r row crate name kind; do
    # Every occurrence of this bare name anywhere in the registry's packages. Scoping the proof
    # to the declared crate would let a duplicate in a sibling crate pass unseen, and an
    # ambiguous name is exactly the drift this step exists to catch.
    matches="$(awk -F'\t' -v n="${name}" '
        { p = $2; sub(/^.*::/, "", p); if (p == n || $2 == n) print $1 "\t" $2 }
    ' "${WORK}/listed.tsv")"
    count="$(printf '%s' "${matches}" | grep -c . )"
    if [ "${count}" -ne 1 ]; then
        printf '  row %-2s  %-60s  UNRESOLVED (%s matches)\n' "${row}" "${name}" "${count}"
        [ "${count}" -gt 0 ] && printf '%s\n' "${matches}" | sed 's/^/            /'
        RESOLUTION_FAILURES=$((RESOLUTION_FAILURES + 1))
        continue
    fi
    found_pkg="$(printf '%s' "${matches}" | cut -f1)"
    found_path="$(printf '%s' "${matches}" | cut -f2)"
    if [ "${found_pkg}" != "${crate}" ]; then
        printf '  row %-2s  %-60s  WRONG CRATE (registry says %s, resolved in %s)\n' \
            "${row}" "${name}" "${crate}" "${found_pkg}"
        RESOLUTION_FAILURES=$((RESOLUTION_FAILURES + 1))
        continue
    fi
    printf '%s\t%s\t%s\t%s\t%s\n' "${row}" "${found_pkg}" "${found_path}" "${name}" "${kind}" \
        >> "${WORK}/resolved.tsv"
done < "${WORK}/registered.tsv"

REGISTERED_COUNT="$(wc -l < "${WORK}/registered.tsv" | tr -d ' ')"
UNIQUE_NAMES="$(cut -f3 "${WORK}/registered.tsv" | sort -u | wc -l | tr -d ' ')"
printf 'registry declares %s test references (%s distinct names) across %s rows\n' \
    "${REGISTERED_COUNT}" "${UNIQUE_NAMES}" "${ROW_COUNT}"

if [ "${RESOLUTION_FAILURES}" -ne 0 ]; then
    printf '\nRESOLUTION PROOF FAILED: %s of %s registered names do not resolve to exactly one\n' \
        "${RESOLUTION_FAILURES}" "${REGISTERED_COUNT}"
    printf 'test. No cell was executed — the registry and the source disagree.\n'
    exit 1
fi
printf 'RESOLUTION PROOF PASSED: all %s registered names resolve to exactly one test each.\n\n' \
    "${REGISTERED_COUNT}"

# ---------------------------------------------------------------------------------------------
# 2. Execute every registered test once per topology.
# ---------------------------------------------------------------------------------------------

for topology in "${TOPOLOGIES[@]}"; do
    env_key="$(jq -r --arg t "${topology}" '.topologies[]|select(.id==$t)|.env|keys[0]' "${REGISTRY}")"
    env_val="$(jq -r --arg t "${topology}" '.topologies[]|select(.id==$t)|.env|to_entries[0].value' "${REGISTRY}")"
    printf -- '--- 2. executing topology %s (%s=%s) ---\n' "${topology}" "${env_key}" "${env_val}"

    : > "${WORK}/results-${topology}.tsv"
    for pkg in "${PACKAGES[@]}"; do
        mapfile -t paths < <(awk -F'\t' -v p="${pkg}" '$2==p {print $3}' "${WORK}/resolved.tsv" | sort -u)
        [ "${#paths[@]}" -eq 0 ] && continue
        printf '  %-22s %3s tests ... ' "${pkg}" "${#paths[@]}"
        # `--exact` with several filters runs exactly the named tests and nothing else.
        env "${env_key}=${env_val}" \
            cargo test -p "${pkg}" --all-features -j "${JOBS}" --no-fail-fast -- \
            --exact "${paths[@]}" > "${WORK}/run-${topology}-${pkg}.log" 2>&1
        sed -n 's/^test \([A-Za-z0-9_:]*\) \.\.\. \(ok\|FAILED\|ignored\).*$/\1\t\2/p' \
            "${WORK}/run-${topology}-${pkg}.log" \
            | while IFS= read -r line; do printf '%s\t%s\n' "${pkg}" "${line}"; done \
            >> "${WORK}/results-${topology}.tsv"
        ok="$(awk -F'\t' -v p="${pkg}" '$1==p && $3=="ok"' "${WORK}/results-${topology}.tsv" | wc -l)"
        bad="$(awk -F'\t' -v p="${pkg}" '$1==p && $3!="ok"' "${WORK}/results-${topology}.tsv" | wc -l)"
        printf '%s ok, %s not ok\n' "${ok}" "${bad}"
    done
    printf '\n'
done

# ---------------------------------------------------------------------------------------------
# 3. The report.
# ---------------------------------------------------------------------------------------------

mkdir -p "$(dirname "${REPORT}")"

jq -n \
  --slurpfile registry "${REGISTRY}" \
  --rawfile resolved "${WORK}/resolved.tsv" \
  --rawfile controller "${WORK}/results-controller.tsv" \
  --rawfile leader "${WORK}/results-worker-leader.tsv" \
  --arg generated "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg listed "${TOTAL_LISTED}" \
'
def tsv: [splits("\n")] | map(select(length>0)) | map(split("\t"));
def outcomes($raw): ($raw|tsv) | map({key: .[1], value: .[2]}) | from_entries;

($registry[0]) as $reg
| ($resolved|tsv) as $res
| ($res | map({key: (.[0] + " " + .[3]), value: .[2]}) | from_entries) as $path_of
| {controller: outcomes($controller), "worker-leader": outcomes($leader)} as $outcome
| {
    schema: "m11-d39-report/1",
    generated_utc: $generated,
    registry_schema: $reg.schema,
    base_pin: $reg.base_pin,
    enumerated_tests: ($listed|tonumber),
    topologies: [$reg.topologies[].id],
    rows: [
      $reg.rows[] as $row
      | {
          id: $row.id,
          owner: $row.owner,
          requirement: $row.requirement,
          topology: $row.topology,
          fixture: $row.fixture,
          distinct_path: ($row.distinct_path // null),
          note: ($row.note // null),
          cells: [
            $reg.topologies[] as $t
            | {
                topology: $t.id,
                tests: [
                  $row.tests[]
                  | ($path_of[($row.id|tostring) + " " + .name]) as $p
                  | {
                      name: .name,
                      crate: .crate,
                      kind: .kind,
                      path: $p,
                      outcome: ($outcome[$t.id][$p] // "did-not-run")
                    }
                ]
              }
            | . + {status: (
                if   (.tests | map(.outcome) | any(. == "did-not-run")) then "ERROR"
                elif (.tests | map(.outcome) | any(. == "FAILED"))      then "FAIL"
                elif (.tests | map(.outcome) | any(. == "ignored"))     then "ERROR"
                else "PASS" end)}
          ]
        }
      | . + {status: (if (.cells | map(.status) | all(. == "PASS")) then "GREEN" else "RED" end)}
    ]
  }
| . + {
    summary: {
      rows_total: (.rows|length),
      rows_green: (.rows | map(select(.status=="GREEN")) | length),
      rows_red:   (.rows | map(select(.status!="GREEN")) | length),
      cells_total: (.rows | map(.cells|length) | add),
      cells_pass:  (.rows | map(.cells | map(select(.status=="PASS")) | length) | add),
      cells_fail:  (.rows | map(.cells | map(select(.status=="FAIL")) | length) | add),
      cells_error: (.rows | map(.cells | map(select(.status=="ERROR")) | length) | add),
      rows_topology_dependent: (.rows | map(select(.topology=="dependent")) | length),
      rows_topology_independent: (.rows | map(select(.topology=="independent")) | length),
      rows_fixture_bound: (.rows | map(select(.fixture!="portable")) | length)
    }
  }
' > "${REPORT}" || die "could not assemble the report"

# ---------------------------------------------------------------------------------------------
# 4. The human-readable summary.
# ---------------------------------------------------------------------------------------------

printf -- '--- 3. the %s-cell report ---\n\n' \
    "$(jq -r '.summary.cells_total' "${REPORT}")"

printf '%-4s %-12s %-11s %-16s %-9s %-14s %s\n' \
    "row" "owner" "topology" "fixture" "controller" "worker-leader" "requirement"
printf -- '%s\n' "-----------------------------------------------------------------------------------------------------------------------"
jq -r '
  .rows[]
  | [ (.id|tostring),
      .owner,
      (if .topology=="dependent" then "DEPENDENT" else "independent" end),
      .fixture,
      (.cells[] | select(.topology=="controller") | .status),
      (.cells[] | select(.topology=="worker-leader") | .status),
      .requirement ]
  | @tsv' "${REPORT}" \
| while IFS=$'\t' read -r id owner topo fixture ctrl leader req; do
    printf '%-4s %-12s %-11s %-16s %-9s %-14s %s\n' \
        "${id}" "${owner}" "${topo}" "${fixture}" "${ctrl}" "${leader}" "${req}"
  done

printf '\n'
jq -r '
  .summary
  | "rows:  \(.rows_green)/\(.rows_total) green   (a row is green only when both cells pass)",
    "cells: \(.cells_pass)/\(.cells_total) pass, \(.cells_fail) fail, \(.cells_error) could not run",
    "topology-dependent rows: \(.rows_topology_dependent)  — these read config().job_controller and assert the distinct path was reached",
    "topology-independent rows: \(.rows_topology_independent)  — these read no topology at all",
    "fixture-bound rows: \(.rows_fixture_bound)  — claim topology-independent, fixture models one deployment end to end"
' "${REPORT}"

cat <<'CAVEAT'

What these counts do NOT answer:
  * A PASS in both cells means the test executed and passed in two differently configured
    processes. Only the topology-DEPENDENT rows above assert that the distinct path was
    reached; every other row's second cell is weaker evidence than that, in one of two ways:
      - most topology-independent rows exercise code that reads no topology at all, so the
        second cell re-ran what the first ran — execution evidence, not coverage;
      - a few (rows 20, 26 and 27) drive code that DOES branch on the topology — the handover
        to a worker leader — without asserting which branch ran. Their worker-leader cell
        proves that branch executed and did not break the row's claim. It does not say what
        the branch did; that is rows 21 and 24's.
  * A green matrix says every registered row passed. It says nothing about requirements that
    have no row, about whether a row's test establishes the D96 requirement it is filed under
    rather than merely bearing the right name, or about faults outside M11.D39g's declared
    model (Byzantine workers and a falsely-reported generation termination are outside it).
  * Rows 12 and 13 are compile checks: they assert rustc's diagnostics over fixtures. They are
    topology-independent by construction and their PASS is about the type system, not about a
    running controller.
  * The matrix runs only the registered tests. The arroyo-controller binary has other tests
    that assert the process default and still fail under ARROYO__JOB_CONTROLLER=worker; plain
    `cargo test` under that environment is not green and no cell above depends on it.
CAVEAT

# Every non-green row, with the reason, in full. Nothing is summarised away.
if [ "$(jq -r '.summary.rows_red' "${REPORT}")" != "0" ]; then
    printf '\n--- rows that are not green ---\n'
    jq -r '
      .rows[] | select(.status!="GREEN")
      | "\nrow \(.id) — \(.requirement)"
      + "\n  owner: \(.owner)   topology: \(.topology)   fixture: \(.fixture)"
      + (if .note then "\n  note: \(.note)" else "" end)
      + ( [ .cells[] | select(.status!="PASS")
            | "\n  cell \(.topology): \(.status)"
              + ( [ .tests[] | select(.outcome != "ok")
                    | "\n    \(.crate)::\(.path) -> \(.outcome)" ] | add // "" ) ] | add // "" )
    ' "${REPORT}"
fi

printf '\nmachine-readable report: %s\n' "${REPORT}"

if [ "$(jq -r '.summary.rows_red' "${REPORT}")" = "0" ]; then
    printf 'RESULT: all %s rows green across both controller topologies.\n' "${ROW_COUNT}"
    exit 0
fi
printf 'RESULT: %s of %s rows are NOT green. See above.\n' \
    "$(jq -r '.summary.rows_red' "${REPORT}")" "${ROW_COUNT}"
exit 1
