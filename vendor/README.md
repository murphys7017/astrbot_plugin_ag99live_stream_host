# blivedm dependency revision

Upstream: https://github.com/xfgryujk/blivedm

Base commit: `3bf17fe862b8c2fc6bc04c81c3f0c5db95de93d6` (1.1.7).

The MIT license is included in the wheel. Application code is unchanged; only
the package version and dependency constraints are revised:

- version: `1.1.7+ag99.1`
- aiohttp: `>=3.13.3,<3.14`
- Brotli: `>=1.2.0,<1.3`
- yarl: `>=1.23.0,<2`
- pure-protobuf: upstream constraint unchanged

Wheel SHA-256: `13fbd82a16e9bd062cae52c7a654368fc48f0d6a112c7653af7f559010090e93`.

Rebuild from the base commit after applying these changes to `pyproject.toml`
and `blivedm/__init__.py`, then run `uv build --wheel --out-dir <plugin>/vendor`.
The companion patch `blivedm-dependencies.patch` records the exact changes.
Apply this zero-context patch with `git apply --unidiff-zero <path-to-patch>`.

Install requirements from the AstrBot root. The relative `--find-links` path
points to the deployed plugin's vendor directory, so no private remote branch
or machine-specific absolute source path is required. Keep the wheel with the
plugin during packaging/deployment.

Validated with Python 3.12, aiohttp 3.13.3, Brotli 1.2.0 and yarl 1.23.0:
HTTP Brotli JSON decoding, room initialization, WebSocket authentication,
Brotli business messages, reconnect, and clean dependency resolution against
a loopback Bilibili protocol server. This does not prove live Bilibili access.
Run `python scripts/verify_blivedm_compat.py` from an environment with the wheel
installed to repeat the loopback acceptance check. It sends no requests to Bilibili.
