# Snapshot Merger

Utility for composing a new snapshot that keeps the target ledger's genesis/validators while importing the latest mainnet-beta state (minus mainnet validator accounts).

## Build

```bash
cargo build --release
```

The binary will be at `target/release/snapshot-merger`

## Usage

```bash
./target/release/snapshot-merger \
  --mainnet-ledger /path/to/mainnet-ledger \
  --ledger-to-merge /path/to/ledger-to-merge \
  --output-directory /path/to/output
```

### Arguments

- `--mainnet-ledger` – directory containing the source (mainnet-beta) ledger and snapshots.
- `--ledger-to-merge` – ledger whose validators/genesis should be preserved in the merged snapshot.
- `--output-directory` – destination directory for the merged snapshot archive and copied genesis (`genesis.bin`).
- `--warp-slot` *(optional)* – warp the merged bank to a specific slot after merging.

### With Warp Slot

```bash
./target/release/snapshot-merger \
  --mainnet-ledger /path/to/mainnet-ledger \
  --ledger-to-merge /path/to/ledger-to-merge \
  --output-directory /path/to/output \
  --warp-slot 300000000
```

## What It Does

1. Loads the mainnet-beta snapshot and counts all accounts.
2. Loads the target ledger snapshot (validators/genesis to keep).
3. Filters mainnet vote & stake accounts so mainnet validators are excluded.
4. Copies every remaining mainnet account into the target ledger bank.
5. Re-applies the target ledger's system accounts (validator identities, etc.).
6. Recalculates capitalization and optionally warps to the requested slot.
7. Emits a full snapshot archive (`snapshot-<slot>.tar.zst`) and the target ledger's `genesis.bin` in the output directory.

**Account batching:** accounts are appended with a 4 GiB per-slot byte ceiling to stay below the AppendVec limit and handle very large datasets safely.

Result: Target ledger validators and genesis + mainnet state (without mainnet validators).

## Requirements

- Rust 1.70+
- 128+ GB RAM (for mainnet snapshots)
- 300+ GB disk space

## Help

```bash
./target/release/snapshot-merger --help
```

## Logging

Use `RUST_LOG` for detailed output:

```bash
RUST_LOG=info ./target/release/snapshot-merger ...
RUST_LOG=debug ./target/release/snapshot-merger ...
```
