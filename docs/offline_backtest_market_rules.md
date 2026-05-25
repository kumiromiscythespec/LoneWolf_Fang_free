# Offline Backtest Market Rules

Phase 3 execution was blocked because `backtest.py` could import or instantiate the exchange path before or around local CSV use. That path can reach `ExchangeClient`, `ccxt`, `load_markets`, market fee refresh, order book reads, or other exchange-backed market metadata behavior.

Phase 3A adds an explicit offline market-rules path for preparation only. It does not approve or run backtest execution.

## Flag

Use the flag only in a separately approved execution task:

```powershell
python backtest.py --offline-market-rules C:\path\to\market_rules.json ...
```

When the flag is present, `backtest.py` loads local JSON rules with `helper.backtest_offline_market_rules` and passes a local adapter into the backtest sizing path. The adapter provides:

- `get_market_rules(symbol)`
- `market_amount_rules(symbol)`
- `amount_to_precision(symbol, amount)`
- `price_to_precision(symbol, price)`

The adapter is backtest-only. It does not read API keys, credentials, balances, orders, private payloads, or network data.

## JSON Shape

Schema file:

```text
schema/backtest_offline_market_rules.schema.json
```

Required top-level fields:

```json
{
  "schema_version": 1,
  "markets": [
    {
      "symbol": "BTC/USDT",
      "aliases": ["BTCUSDT"],
      "amount_precision": 6,
      "amount_step": 0.000001,
      "price_precision": 2,
      "price_step": 0.01,
      "min_amount": 0.0,
      "min_cost": 0.0
    }
  ]
}
```

The example above shows shape only. It is not an execution-ready BTC/USDT fixture. Execution-ready values must come from an owner-approved public/static source before Phase 3B.

## Fail-Closed Behavior

The loader fails closed when:

- the file is missing
- JSON is invalid
- `schema_version` is not `1`
- `markets` is missing or empty
- the requested symbol is missing
- amount precision/step is missing
- price precision/step is missing
- minimum amount or minimum cost is missing
- values are non-numeric, negative where not allowed, non-finite, or duplicate aliases conflict
- private/runtime fields such as API keys, secrets, orders, balances, or raw order payloads appear

`BTC/USDT` and `BTCUSDT` aliases are normalized deterministically.

## Safety Boundary

Phase 3A is source-only preparation. It does not approve:

- backtest execution
- replay, sweep, or Monte Carlo
- runtime import or dispatch
- PAPER or LIVE trading
- order placement, cancel, fetch balance, or private API use
- deploy, cloud mutation, billing, or push

Phase 3B execution still requires separate owner approval with a concrete command, dataset root, output root, symbol, window, timebox, and execution-ready market rules file.
