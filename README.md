# Snapshot Merger

Tool to merge Solana mainnet-beta snapshot with custom validator set.

## Build

```bash
cargo build --release
```

The binary will be at `target/release/snapshot-merger`

## Usage

```bash
./target/release/snapshot-merger \
  --mainnet-ledger /path/to/mainnet-ledger \
  --tim-cluster-ledger /path/to/tim-cluster-ledger \
  --output-directory /path/to/output
```

### With Warp Slot

```bash
./target/release/snapshot-merger \
  --mainnet-ledger /path/to/mainnet-ledger \
  --tim-cluster-ledger /path/to/tim-cluster-ledger \
  --output-directory /path/to/output \
  --warp-slot 300000000
```

## What It Does

1. Loads mainnet-beta snapshot (all accounts)
2. Loads tim cluster snapshot  
3. Removes all vote and stake accounts from mainnet
4. Adds all vote and stake accounts from tim cluster
5. Recalculates capitalization
6. Creates merged snapshot

Result: Mainnet state with tim cluster validators.

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


