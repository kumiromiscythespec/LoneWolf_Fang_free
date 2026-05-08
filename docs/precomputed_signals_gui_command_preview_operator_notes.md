# Free Phase 11 Precomputed Signal Command Preview Operator Notes

Free Phase 11 covers copy UX accessibility / layout smoke / operator docs for
the GUI command preview. GUI command preview is preview-only. GUI does not
execute commands. GUI does not execute commands from this panel.

Free Phase 14 keeps this as GUI smoke checklist / docs-only hardening. Command
preview is preview-only, copy UX is clipboard-only, APP_VERSION unchanged, and
package/release not touched. The GUI still does not auto-run
producer/backtest/runner/inventory and does not connect to LIVE/PAPER/order.

The precomputed signal tape panel is a read-only operator aid. It can show safe
backtest and replay preview command text and can copy that text to the
clipboard. The copy buttons do not start or schedule work.

Operator-visible invariants:

- `preview_only=true`
- `execution_enabled=false`
- `live_command_available=false`
- `paper_command_available=false`
- `Preview only`
- `Execution disabled`
- `This panel does not execute commands`
- `Not selectable for LIVE/PAPER`
- `Backtest/replay only`
- `Run this command manually in a terminal if needed`

Copy controls:

- `Copy backtest command` copies only the safe backtest preview command text.
- `Copy replay command` copies only the safe replay preview command text.
- `Copied` is UI state only after a user-triggered clipboard write.
- Disabled copy state shows a safe disabled reason for invalid preview,
  disabled preview, empty command text, unsafe command text, live command
  availability, or paper command availability.

The GUI does not automatically run producer, backtest, runner, inventory,
selection, adapter, command-preview, or command-copy commands. It does not add
or use `subprocess`, `os.system`, `QProcess`, `Popen`, `startDetached`,
multiprocessing, threading, scheduler, or background worker execution for the
preview/copy panel.

Live and paper safety:

- live / paper command text is not generated.
- LIVE / PAPER / order execution buttons and paths are not connected to
  precomputed signal tape selection, command preview, or copy UX state.
- The GUI does not connect to order creation, order fetch, order submit,
  balance fetch, MEXC API, exchange clients, or `ccxt`.

Manual workflow:

1. Select a safe precomputed signal tape in the GUI.
2. Review the read-only preview text.
3. Copy the backtest or replay preview command if needed.
4. Review the copied command.
5. Run it manually in an external terminal only when the operator explicitly
   chooses to do so. The GUI is not the execution surface.

Data exclusion:

- Do not include raw `trades.csv` rows.
- Do not include row-level `entry_exec`, `exit_exec`, `qty`, exact trade ids,
  order ids, raw order payloads, balance snapshots, raw billing payloads, raw
  market data, raw OHLCV, generated command preview output, generated clipboard
  output, generated inventory output, generated selection output, generated GUI
  adapter output, or generated real signal tape bodies.
- Do not include API keys, secrets, tokens, authorization headers, private keys,
  or other credentials.

Phase 11 does not change `APP_VERSION`, package, exe, setup, installer, signing,
release assets, strategy, indicators, exchange, risk, order runtime logic,
entry timing, exit timing, fee logic, quantity logic, PnL formulas, signal
timing, or DD calculation.

Future execution button is not part of Phase 14. Any future GUI execution phase
must be explicit, local-only or dry-run first, and separated from LIVE/PAPER/
order paths.
