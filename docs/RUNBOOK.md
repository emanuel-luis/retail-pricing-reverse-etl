# Runbook

How to run the loop, follow a cycle, and recover when something fails. Commands are Make targets;
Git Bash on Windows has no make, so run the recipe lines from the Makefile by hand (`uv run ...`).
Every host port is bound to 127.0.0.1 and comes from `.env`.

## Start, stop, reset

| Task | Command |
| --- | --- |
| First run, everything | `make demo SF=0.01` (or `SF=1`) |
| Start the core services | `make up` (copies `.env.example` to `.env` when missing) |
| Add the workload | `make workload` (builds the app image first) |
| Stop everything, delete volumes | `make down` |
| Service state, logs | `make ps`, `make logs`, `make runner-logs` |

`make demo` stops early with a list when `.env` lacks a variable that `.env.example` has (an
`.env` from an older checkout): copy the missing lines over, or delete `.env`.

A schema change in the OLTP, the control DB or ClickHouse means a reset: `make down`, then the
setup steps again (`load`, `control-init`, `ch-init`, `s3-init`, `deploy`). There are no
migrations by design (DECISIONS 14).

## Deploy the NiFi flow

```bash
make flows                 # regenerate nifi/flows/retail-pricing.json from the builder
make deploy                # upload, set the secrets from env, enable, start; record group ids
```

The deploy refuses a flow with any invalid component and lists each validation error. It fails
when it cannot write the process group ids into `registry.job` (the control DB must be up and
`control-init` done); `--no-registry` skips that step for test flows.

A fresh deploy starts a scheduled cycle within a minute if the registry schedule is on:

```bash
uv run retail-pricing control schedule --off            # demos and tests trigger by hand
uv run retail-pricing control schedule --every '10 minutes'
```

## Run and follow a cycle

```bash
make cycle MODE=full                                    # first cycle, no watermarks yet
make cycle MODE=incremental                             # every cycle after that
uv run retail-pricing cycle status                      # latest cycle, its runs and stages
uv run retail-pricing cycle wait --cycle 7              # exit 0 succeeded, 1 failed, 2 timeout
uv run retail-pricing cycle wait-stage --stage transform --cycle 7
uv run retail-pricing check --cycles 1-3                # the loop's invariants on finished cycles
make report                                             # timings for the latest run under runs/
```

A trigger answers 409 while another cycle runs; only one cycle runs at a time.

What to read in the control DB (127.0.0.1:25433, database `control`):

| Question | Table or view |
| --- | --- |
| Did the cycle succeed, where did it fail | `audit.cycle` (`status`, `failed_stage`, `error`) |
| How long each stage took | `audit.v_cycle_stage_duration` |
| Which items failed or never started | `audit.step` (`status`, `error`), `audit.job_run.error` |
| What the reverse load did | `audit.reverse_batch`, `audit.v_reverse_totals` |
| Did reconcile pass | `audit.reconcile_result` |
| dbt nodes, guardrails | `audit.dbt_node_result` (guardrails are `test.*.g01_*` to `g10_*`) |
| Open alerts | `audit.alert` where `resolved_at` is null |

## When a cycle fails

1. `retail-pricing cycle status` names the failed stage and the failed steps; `audit.step.error`
   has the message (a NiFi bulletin reference when the processor gave no text).
2. Fix the cause, then rerun the same cycle id. It continues with the same extract window and
   replays every reverse batch that was already applied:

   ```bash
   uv run retail-pricing cycle trigger --mode incremental --rerun 7 --wait
   ```

3. By stage:
   - **extract or load**: watermarks did not move, so nothing is lost. A missing table item
     ("planned item(s) never started") usually means the control DB was unreachable.
   - **transform**: a dbt error or a guardrail error (`audit.dbt_node_result`). Nothing was sent
     to the OLTP. The runner returns a cycle's earlier succeeded build on a rerun; after a code
     change, rebuild the runner image and remove its state volume
     (`docker compose rm -sf dbt-runner`, `docker volume rm retail-pricing-reverse-etl_dbt_runner_state`).
   - **reverse**: `apply_batch` is idempotent per (cycle, target, batch), so the rerun replays the
     batches that ran and applies the rest.
   - **reconcile**: compare `audit.reconcile_expected` and `audit.reconcile_actual` for the failed
     check; staged rows that were never applied make the transit check fail.

## Conflicts

A conflict is a row a person changed after the extract: `apply_batch` keeps the human price and
records the engine's proposal in `pricing_apply.conflict` (OLTP) and `audit.reverse_batch`. The
next cycle's extract brings it to ClickHouse (`marts.mart_conflicts`), and the next cycle prices
the row again from the human price. Nothing has to be resolved by hand.

## NiFi recovery

| Symptom | Fix |
| --- | --- |
| Deploy answers 409 "queue not empty" with millions of FlowFiles | Remove the NiFi container and its `nifi_*` volumes, start it again, deploy |
| Every POST/PUT/DELETE answers 403 | A client sent the login cookie back; the repo client sends only the bearer token |
| The healthcheck never turns healthy | NiFi listens on the container hostname, not 127.0.0.1 (DECISIONS 96) |
| An extract plans billions of empty pages | A row with a huge cycle id in the `pricing_apply` tables; delete it (DECISIONS 124) |

The NiFi UI is https://127.0.0.1:28443/nifi (self-signed certificate; credentials from `.env`).

## Monitoring

| Task | Command or place |
| --- | --- |
| Start or add the profile | `make monitoring` (`make demo` starts it too) |
| Dashboards | Grafana http://127.0.0.1:23000 (anonymous read; admin from `.env`): pipeline overview, pricing outcomes, platform |
| Alert rules | Grafana, Alerting: nine rules; the contact point is email with no SMTP, so a firing rule shows in Grafana and its log only |
| Raw metrics | Prometheus http://127.0.0.1:29090, Status, Targets |
| Change a dashboard | edit `scripts/gen_dashboards.py`, run `uv run python scripts/gen_dashboards.py`, commit the JSON with it |

NiFi's metrics need its JWT: the `nifi-token` sidecar refreshes it every 6 h into a volume
Prometheus reads. If the `nifi` target shows a TLS or 400 "Invalid SNI" error after an upgrade,
the `nifi_conf` volume still holds a certificate issued before `hostname: nifi` existed: run
`make down` once (DECISIONS 134). A Postgres target can be up while its database is down; the
`TargetDown` rule also reads `pg_up`.

## Memory

The default compose limits total about 7.8 GiB with every profile. On the owner's 31 GB laptop
the Docker VM is capped near 12 GB; if the host is short of memory, Claude Code and Windows may
stop processes. Run SF1 for routine work and check free memory before a heavy run
(`make heavy SF=10`); never raise the VM limit from inside this repo.
