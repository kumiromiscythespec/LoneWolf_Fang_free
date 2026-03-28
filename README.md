# LoneWolf Fang Free

Free edition of LoneWolf Fang with GUI support for **PAPER**, **REPLAY**, and **BACKTEST**.  
**LIVE is not included** in the free edition.

LoneWolf Fang Free は、**PAPER / REPLAY / BACKTEST** に対応した GUI 付き無料版です。  
**LIVE は free には含まれません。**

---

## English

### Overview

LoneWolf Fang Free is the free edition of LoneWolf Fang.

It is designed for users who want to:

- explore the GUI
- test workflows safely with paper trading
- verify behavior with replay
- run backtests locally

The free edition is intentionally limited to research and evaluation use.  
It does **not** include LIVE trading.

### Included in Free

- GUI launcher
- PAPER
- REPLAY
- BACKTEST
- Local installer
- Desktop shortcut creation for the GUI launcher

### Not Included in Free

- LIVE
- Standard online installer flow
- Standard-only distribution flow
- Bundled datasets
- Advanced sweep tooling in the package

### Installation

Run the following file from the package root:

`Install_LoneWolf_Fang_Free.cmd`

The local installer performs the following steps:

- validates the local package contents
- checks for a usable Python environment
- runs a GUI import check
- creates a desktop shortcut named `LoneWolf Fang Free GUI`
- writes `.install_receipt.json`

### Launching the GUI

After installation, launch the app from the desktop shortcut:

`LoneWolf Fang Free GUI`

You can also launch it manually from the package root:

- `Launch_LoneWolf_Fang_Free_GUI.cmd`
- `Launch_LoneWolf_Fang_Free_GUI.vbs`

### Supported Modes

The free edition supports the following modes:

- `PAPER`
- `REPLAY`
- `BACKTEST`

`LIVE` is not available in the free edition.

### Notes

- The free build is intended for evaluation and research workflows.
- Log level is fixed to the minimum level in Free.
- The GUI title is `LoneWolf Fang Free`.
- The package does not bundle market datasets.
- Runtime folders are included as skeleton directories only.

### Directory Notes

This package may include:

- core runtime files
- GUI application code
- packaging scripts for the local installer
- runtime skeleton directories
- minimal config files for initial use

### Standard vs Free

Free is intended for evaluation and non-LIVE usage.

Free includes:

- GUI access
- PAPER
- REPLAY
- BACKTEST

Standard provides additional capabilities beyond Free.

### Local Packaging

This repository is structured so the Free package can be built locally as a zip package.

The Free package is a **local-installer zip package**, not an online installer package.

### Important

This repository does not use LIVE in the free edition.  
Do not expect LIVE behavior or standard-only distribution behavior from this package.

---

## 日本語

### 概要

LoneWolf Fang Free は、LoneWolf Fang の無料版です。

主に次の用途を想定しています。

- GUI を試す
- paper trading で安全に操作確認する
- replay で挙動を検証する
- backtest をローカルで実行する

free 版は、検証・研究・体験用として意図的に制限されています。  
**LIVE は含まれません。**

### Free に含まれるもの

- GUI ランチャー
- PAPER
- REPLAY
- BACKTEST
- ローカルインストーラー
- GUI 起動用デスクトップショートカット作成

### Free に含まれないもの

- LIVE
- standard の online installer 導線
- standard 専用の配布導線
- データセット同梱
- パッケージ内の高度な sweep ツール一式

### インストール方法

パッケージのルートで、次のファイルを実行してください。

`Install_LoneWolf_Fang_Free.cmd`

ローカルインストーラーは以下を行います。

- ローカル package 内容の検証
- 利用可能な Python 環境の確認
- GUI import check
- `LoneWolf Fang Free GUI` という名前のデスクトップショートカット作成
- `.install_receipt.json` の出力

### GUI の起動方法

インストール後は、デスクトップに作成された次のショートカットから起動してください。

`LoneWolf Fang Free GUI`

パッケージルートから手動で起動することもできます。

- `Launch_LoneWolf_Fang_Free_GUI.cmd`
- `Launch_LoneWolf_Fang_Free_GUI.vbs`

### 対応モード

free 版で利用できるモードは次の3つです。

- `PAPER`
- `REPLAY`
- `BACKTEST`

`LIVE` は free 版では利用できません。

### 補足

- free build は、検証・研究・体験用途を想定しています。
- free では log level は最小値に固定されています。
- GUI タイトルは `LoneWolf Fang Free` です。
- この package には market dataset は同梱されません。
- runtime フォルダは skeleton のみを含みます。

### ディレクトリについて

この package には主に以下が含まれます。

- 実行に必要な core runtime files
- GUI アプリ本体
- ローカルインストーラー用 packaging scripts
- runtime skeleton directories
- 初期利用のための最小 config files

### standard と free の違い

free は、評価・検証・非LIVE用途向けです。

free に含まれるもの:

- GUI
- PAPER
- REPLAY
- BACKTEST

standard には、free には含まれない追加機能があります。

### ローカル package について

このリポジトリは、Free package をローカルで zip として生成できる構成になっています。

Free package は **local-installer zip package** であり、online installer package ではありません。

### 重要事項

このリポジトリの free 版には LIVE は含まれません。  
standard 専用の LIVE 動作や配布導線を前提にしないでください。

---

## Quick Start

### English

1. Extract the package
2. Run `Install_LoneWolf_Fang_Free.cmd`
3. Use the desktop shortcut `LoneWolf Fang Free GUI`
4. Choose `PAPER`, `REPLAY`, or `BACKTEST`

### 日本語

1. package を展開する
2. `Install_LoneWolf_Fang_Free.cmd` を実行する
3. デスクトップの `LoneWolf Fang Free GUI` を使って起動する
4. `PAPER` / `REPLAY` / `BACKTEST` を選ぶ

---

## Repository Notes

### English

This repository is intended to keep the Free edition packaging and local installer flow separate from standard distribution.

### 日本語

このリポジトリは、free 版の packaging と local installer 導線を、standard の配布導線から分離して管理することを目的としています。

---

## License

License terms will be provided separately.

ライセンス条件は別途提供予定です。
### Non-Live Tools

- Chart / result panel / snapshots / Save PNG / Open Folder remain available in PAPER / REPLAY / BACKTEST.
