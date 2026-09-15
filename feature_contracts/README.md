# Feature contracts

`market_features.json` is the only human-edited registry for market features.
It owns feature names and types, rolling-window sizes, version membership,
offline table bindings, and the ordered input subset for each model horizon.

After changing the manifest, run from the repository root:

```bash
python tools/generate_feature_contracts.py
python tools/generate_feature_contracts.py --check
```

The generator writes service-local modules required by the existing Docker
build contexts. Never edit those generated modules directly. CI runs `--check`
and rejects a change when the manifest and generated modules differ.

Feature formulas are intentionally not generated. A new calculated feature
still needs matching offline SQL and live Python implementations plus parity
coverage; the manifest removes the remaining registration and subset edits.
