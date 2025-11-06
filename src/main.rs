// Snapshot Merger - Merge mainnet state into another ledger's snapshot
//
// This tool merges two Solana snapshots:
// - Starts with the ledger-to-merge snapshot (keeps its genesis and validators)
// - Extracts all mainnet accounts EXCEPT vote and stake accounts
// - Copies those mainnet accounts into the ledger-to-merge bank
//
// Result: Ledger-to-merge's genesis and validators + mainnet's state (excluding mainnet validators)

use {
    clap::{crate_description, crate_name, value_t, value_t_or_exit, App, Arg},
    log::*,
    solana_accounts_db::{
        accounts_db::AccountsDbConfig,
        hardened_unpack::open_genesis_config,
    },
    solana_clock::Slot,
    solana_genesis_config::GenesisConfig,
    solana_ledger::{
        bank_forks_utils,
        blockstore::{Blockstore, BlockstoreError},
        blockstore_options::{AccessType, BlockstoreOptions},
        blockstore_processor::ProcessOptions,
    },
    solana_runtime::{
        bank::Bank,
        snapshot_archive_info::SnapshotArchiveInfoGetter,
        snapshot_bank_utils,
        snapshot_config::{SnapshotConfig, SnapshotUsage},
        snapshot_utils::{ArchiveFormat, SnapshotVersion, ZstdConfig},
    },
    std::{
        path::{Path, PathBuf},
        process::exit,
        sync::Arc,
    },
};
use snapshot_merger::merge::functions;

#[derive(Debug)]
struct MergeStats {
    mainnet_total_accounts: usize,
    merge_total_accounts: usize,
    mainnet_vote_accounts_excluded: usize,
    mainnet_stake_accounts_excluded: usize,
    mainnet_accounts_copied: usize,
    final_total_accounts: usize,
    capitalization_before: u64,
    capitalization_after: u64,
    snapshot_path: String,
}

fn open_blockstore(ledger_path: &Path) -> Result<Blockstore, BlockstoreError> {
    info!("Opening blockstore at {:?}", ledger_path);
    Blockstore::open_with_options(
        ledger_path,
        BlockstoreOptions {
            access_type: AccessType::Secondary,
            enforce_ulimit_nofile: false,
            ..BlockstoreOptions::default()
        },
    )
}

fn load_bank_from_snapshot(
    ledger_path: &Path,
    genesis_config: &GenesisConfig,
) -> Result<Arc<Bank>, String> {
    info!("Loading snapshot from {:?}", ledger_path);

    let blockstore = Arc::new(
        open_blockstore(ledger_path)
            .map_err(|e| format!("Failed to open blockstore: {:?}", e))?
    );

    let snapshot_config = SnapshotConfig {
        usage: SnapshotUsage::LoadOnly,
        full_snapshot_archives_dir: ledger_path.to_path_buf(),
        incremental_snapshot_archives_dir: ledger_path.to_path_buf(),
        bank_snapshots_dir: ledger_path.join("bank_snapshots"),
        ..SnapshotConfig::default()
    };

    // Use minimal accounts DB config for loading
    let accounts_db_config = Some(AccountsDbConfig::default());
    let process_options = ProcessOptions {
        accounts_db_config,
        ..ProcessOptions::default()
    };

    let (bank_forks, _leader_schedule_cache, _starting_snapshot_hashes, ..) =
        bank_forks_utils::load_bank_forks(
            genesis_config,
            &*blockstore,
            vec![ledger_path.join("accounts")],
            &snapshot_config,
            &process_options,
            None,
            None,
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .map_err(|e| format!("Failed to load bank forks: {:?}", e))?;

    let bank = bank_forks.read().unwrap().working_bank();
    info!("Loaded bank at slot {}", bank.slot());

    Ok(bank)
}

fn create_snapshot_from_bank(
    bank: &Bank,
    output_dir: &Path,
) -> Result<String, String> {
    info!("Preparing bank for snapshot at slot {}", bank.slot());

    // Ensure bank is complete by filling it with ticks if needed
    if !bank.is_complete() {
        info!("Bank is not complete, filling with ticks...");
        bank.fill_bank_with_ticks_for_tests();
        info!("Bank now complete (tick_height: {} / max_tick_height: {})",
              bank.tick_height(), bank.max_tick_height());
    }

    // Create necessary subdirectories
    let bank_snapshots_dir = output_dir.join("bank_snapshots");
    std::fs::create_dir_all(&bank_snapshots_dir)
        .map_err(|e| format!("Failed to create bank snapshots directory: {:?}", e))?;

    info!("Creating full snapshot archive...");
    let archive_format = ArchiveFormat::TarZstd {
        config: ZstdConfig::default(),
    };
    let snapshot_archive_info = snapshot_bank_utils::bank_to_full_snapshot_archive(
        &bank_snapshots_dir,
        bank,
        Some(SnapshotVersion::default()),
        output_dir,
        output_dir,
        archive_format,
    ).map_err(|e| format!("Failed to create snapshot archive: {:?}", e))?;

    let snapshot_path = snapshot_archive_info.path().to_string_lossy().to_string();
    info!("Successfully created snapshot archive: {}", snapshot_path);

    Ok(snapshot_path)
}

fn merge_snapshots(
    mainnet_ledger: &Path,
    ledger_to_merge: &Path,
    output_snapshot_dir: &Path,
    warp_slot: Option<Slot>,
) -> Result<MergeStats, String> {
    info!("=== Starting Snapshot Merge ===");
    info!("Mainnet ledger: {:?}", mainnet_ledger);
    info!("Ledger to merge: {:?}", ledger_to_merge);
    info!("Output directory: {:?}", output_snapshot_dir);

    // Load genesis configs
    info!("\n=== Step 1: Loading Genesis Configs ===");
    let mainnet_genesis_config = open_genesis_config(mainnet_ledger, 10485760)
        .map_err(|e| format!("Failed to open mainnet genesis config: {:?}", e))?;
    let merge_genesis_config = open_genesis_config(ledger_to_merge, 10485760)
        .map_err(|e| format!("Failed to open ledger genesis config: {:?}", e))?;
    info!("Loaded both genesis configs successfully");

    // Load mainnet snapshot
    info!("\n=== Step 2: Loading Mainnet Snapshot ===");
    let mainnet_bank = load_bank_from_snapshot(mainnet_ledger, &mainnet_genesis_config)?;
    let mainnet_total_accounts = functions::count_total_accounts(&mainnet_bank)?;
    info!("Mainnet bank loaded with {} total accounts", mainnet_total_accounts);

    // Load merge ledger snapshot (this will be our base)
    info!("\n=== Step 3: Loading Ledger to Merge ===");
    let merge_bank = load_bank_from_snapshot(ledger_to_merge, &merge_genesis_config)?;
    let merge_total_accounts = functions::count_total_accounts(&merge_bank)?;
    info!("Merge ledger loaded with {} total accounts", merge_total_accounts);

    // Extract mainnet vote and stake accounts (to filter them out)
    info!("\n=== Step 4: Extracting Mainnet Validators (to exclude) ===");
    let mainnet_vote_accounts = functions::extract_vote_accounts(&mainnet_bank)?;
    let mainnet_stake_accounts = functions::extract_stake_accounts(&mainnet_bank)?;
    info!("Found {} vote and {} stake accounts in mainnet to exclude",
          mainnet_vote_accounts.len(), mainnet_stake_accounts.len());

    // Get ALL mainnet accounts and filter out vote/stake
    info!("\n=== Step 5: Extracting Mainnet Accounts (excluding validators) ===");
    let all_mainnet_accounts = mainnet_bank.get_all_accounts(false)
        .map_err(|e| format!("Failed to get all mainnet accounts: {:?}", e))?;

    let mut mainnet_accounts_to_copy = std::collections::HashMap::new();
    let mut filtered_vote_count = 0;
    let mut filtered_stake_count = 0;

    for (pubkey, account, _slot) in all_mainnet_accounts {
        if mainnet_vote_accounts.contains_key(&pubkey) {
            filtered_vote_count += 1;
            continue;
        }
        if mainnet_stake_accounts.contains_key(&pubkey) {
            filtered_stake_count += 1;
            continue;
        }
        mainnet_accounts_to_copy.insert(pubkey, account);
    }

    info!("Prepared {} mainnet accounts to copy (excluded {} vote, {} stake accounts)",
          mainnet_accounts_to_copy.len(), filtered_vote_count, filtered_stake_count);

    // Create child bank from merge ledger (this keeps merge ledger genesis and validators)
    info!("\n=== Step 6: Creating Child Bank from Merge Ledger ===");
    let merged_bank = Bank::new_from_parent(
        merge_bank.clone(),
        merge_bank.collector_id(),
        merge_bank.slot() + 1,
    );
    info!("Created child bank at slot {}", merged_bank.slot());

    let capitalization_before = merged_bank.capitalization();

    // Add all non-validator accounts from mainnet
    info!("\n=== Step 7: Adding Mainnet Accounts (excluding validators) ===");
    functions::add_accounts(&merged_bank, &mainnet_accounts_to_copy, "mainnet")?;

    // Recalculate capitalization
    info!("\n=== Step 8: Recalculating Capitalization ===");
    let new_capitalization = merged_bank.calculate_capitalization_for_tests();
    merged_bank.set_capitalization_for_tests(new_capitalization);
    let capitalization_after = merged_bank.capitalization();

    info!(
        "Capitalization changed from {} to {} ({:+})",
        capitalization_before,
        capitalization_after,
        capitalization_after as i128 - capitalization_before as i128
    );

    // Warp if requested
    let final_bank = if let Some(warp_slot) = warp_slot {
        info!("\n=== Step 9: Warping to Slot {} ===", warp_slot);
        merged_bank.squash();
        merged_bank.force_flush_accounts_cache();
        let merged_bank_arc = Arc::new(merged_bank);
        let collector_id = merged_bank_arc.collector_id();
        Arc::new(Bank::warp_from_parent(
            merged_bank_arc.clone(),
            collector_id,
            warp_slot,
        ))
    } else {
        Arc::new(merged_bank)
    };

    let final_total_accounts = functions::count_total_accounts(&final_bank)?;

    // Create snapshot
    info!("\n=== Step 10: Creating Merged Snapshot ===");
    std::fs::create_dir_all(output_snapshot_dir)
        .map_err(|e| format!("Failed to create output directory: {:?}", e))?;

    let snapshot_path = create_snapshot_from_bank(&final_bank, output_snapshot_dir)?;

    // Write the merge ledger genesis config to the output directory
    info!("Writing merge ledger genesis config to output directory...");
    let genesis_path = output_snapshot_dir.join("genesis.bin");
    let genesis_file = std::fs::File::create(&genesis_path)
        .map_err(|e| format!("Failed to create genesis file: {:?}", e))?;
    bincode::serialize_into(genesis_file, &merge_genesis_config)
        .map_err(|e| format!("Failed to serialize genesis config: {:?}", e))?;
    info!("Genesis config saved to: {:?}", genesis_path);

    let stats = MergeStats {
        mainnet_total_accounts,
        merge_total_accounts,
        mainnet_vote_accounts_excluded: filtered_vote_count,
        mainnet_stake_accounts_excluded: filtered_stake_count,
        mainnet_accounts_copied: mainnet_accounts_to_copy.len(),
        final_total_accounts,
        capitalization_before,
        capitalization_after,
        snapshot_path,
    };

    info!("\n=== Merge Complete ===");
    info!("Statistics:");
    info!("  Mainnet total accounts: {}", stats.mainnet_total_accounts);
    info!("  Merge ledger total accounts: {}", stats.merge_total_accounts);
    info!("  Mainnet vote accounts excluded: {}", stats.mainnet_vote_accounts_excluded);
    info!("  Mainnet stake accounts excluded: {}", stats.mainnet_stake_accounts_excluded);
    info!("  Mainnet accounts copied: {}", stats.mainnet_accounts_copied);
    info!("  Final total accounts: {}", stats.final_total_accounts);
    info!("  Capitalization before: {} lamports", stats.capitalization_before);
    info!("  Capitalization after: {} lamports", stats.capitalization_after);

    Ok(stats)
}

fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("mainnet_ledger")
                .long("mainnet-ledger")
                .value_name("PATH")
                .takes_value(true)
                .required(true)
                .help("Path to mainnet-beta ledger directory"),
        )
        .arg(
            Arg::with_name("ledger_to_merge")
                .long("ledger-to-merge")
                .value_name("PATH")
                .takes_value(true)
                .required(true)
                .help("Path to ledger directory whose validators should be merged"),
        )
        .arg(
            Arg::with_name("output_directory")
                .long("output-directory")
                .short("o")
                .value_name("PATH")
                .takes_value(true)
                .required(true)
                .help("Directory where merged snapshot will be created"),
        )
        .arg(
            Arg::with_name("warp_slot")
                .long("warp-slot")
                .value_name("SLOT")
                .takes_value(true)
                .help("Optionally warp the merged bank to this slot"),
        )
        .get_matches();

    let mainnet_ledger = PathBuf::from(value_t_or_exit!(matches, "mainnet_ledger", String));
    let ledger_to_merge = PathBuf::from(value_t_or_exit!(matches, "ledger_to_merge", String));
    let output_directory = PathBuf::from(value_t_or_exit!(matches, "output_directory", String));
    let warp_slot = value_t!(matches, "warp_slot", Slot).ok();

    match merge_snapshots(&mainnet_ledger, &ledger_to_merge, &output_directory, warp_slot) {
        Ok(stats) => {
            println!("\n✅ Snapshot merge completed successfully!");
            println!("\nSummary:");
            println!("  • Started with {} accounts from merge ledger", stats.merge_total_accounts);
            println!("  • Mainnet had {} total accounts", stats.mainnet_total_accounts);
            println!("  • Excluded {} vote accounts and {} stake accounts from mainnet",
                     stats.mainnet_vote_accounts_excluded,
                     stats.mainnet_stake_accounts_excluded);
            println!("  • Copied {} mainnet accounts to merge ledger",
                     stats.mainnet_accounts_copied);
            println!("  • Final snapshot has {} accounts", stats.final_total_accounts);
            println!("  • Capitalization: {} -> {} lamports",
                     stats.capitalization_before,
                     stats.capitalization_after);
            println!("\nSnapshot archive created: {}", stats.snapshot_path);
            println!("Result: Merge ledger validators + mainnet state (excluding mainnet validators)");
        }
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            exit(1);
        }
    }
}
