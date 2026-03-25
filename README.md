# LoneWolf Fang Free

This repository contains the free edition of LoneWolf Fang.

## Included in Free

- GUI launcher and desktop-shortcut installer flow
- `PAPER`
- `REPLAY`
- `BACKTEST`

## Not Included in Free

- `LIVE`
- standard online installer flow
- bundled datasets

## Install

Run `Install_LoneWolf_Fang_Free.cmd` from the package root.

The local installer:

- validates the local payload
- probes Python locally
- runs GUI import checks
- creates a desktop shortcut for `LoneWolf Fang Free GUI`
- writes `.install_receipt.json`

## Launch

Use the desktop shortcut created by the installer, or run `Launch_LoneWolf_Fang_Free_GUI.vbs`.

## Notes

- The free edition keeps `configs/config_standard_BTCJPY.py` because the current config loader still expects `config_standard_<symbol>.py`.
- Standard includes additional capabilities beyond the free edition.
