// Snapshot Merger - Merge mainnet-beta snapshot with custom cluster validators
//
// This tool merges two Solana snapshots:
// - Takes all accounts from mainnet-beta snapshot
// - Removes all vote and stake accounts from mainnet
// - Adds all vote and stake accounts from custom cluster
//
// Result: Mainnet state with custom cluster's validators

use {
    clap::{crate_description, crate_name, value_t, value_t_or_exit, App, Arg},
    log::*,
    solana_account::{AccountSharedData, WritableAccount},
    solana_accounts_db::{
        accounts_db::AccountsDbConfig,
        accounts_index::ScanConfig,
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
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank,
        snapshot_archive_info::SnapshotArchiveInfoGetter,
        snapshot_bank_utils,
        snapshot_config::{SnapshotConfig, SnapshotUsage},
        snapshot_utils::{ArchiveFormat, SnapshotVersion, ZstdConfig},
    },
    solana_stake_program,
    solana_vote_program,
    std::{
        collections::HashMap,
        path::{Path, PathBuf},
        process::exit,
        sync::Arc,
    },
};

#[derive(Debug)]
struct MergeStats {
    mainnet_total_accounts: usize,
    mainnet_vote_accounts_removed: usize,
    mainnet_stake_accounts_removed: usize,
    tim_vote_accounts_added: usize,
    tim_stake_accounts_added: usize,
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

fn extract_vote_accounts(bank: &Bank) -> Result<HashMap<Pubkey, AccountSharedData>, String> {
    info!("Extracting vote accounts...");
    let vote_program_id = solana_vote_program::id();

    let accounts = bank
        .get_program_accounts(&vote_program_id, &ScanConfig::default())
        .map_err(|e| format!("Failed to get vote accounts: {:?}", e))?;

    info!("Found {} vote accounts", accounts.len());
    Ok(accounts.into_iter().collect())
}

fn extract_stake_accounts(bank: &Bank) -> Result<HashMap<Pubkey, AccountSharedData>, String> {
    info!("Extracting stake accounts...");
    let stake_program_id = solana_stake_program::id();

    let accounts = bank
        .get_program_accounts(&stake_program_id, &ScanConfig::default())
        .map_err(|e| format!("Failed to get stake accounts: {:?}", e))?;

    info!("Found {} stake accounts", accounts.len());
    Ok(accounts.into_iter().collect())
}

fn remove_vote_accounts(bank: &Bank) -> Result<usize, String> {
    info!("Removing vote accounts from mainnet bank...");
    let vote_program_id = solana_vote_program::id();

    let accounts = bank
        .get_program_accounts(&vote_program_id, &ScanConfig::default())
        .map_err(|e| format!("Failed to get vote accounts: {:?}", e))?;

    let count = accounts.len();
    for (pubkey, mut account) in accounts {
        account.set_lamports(0);
        bank.store_account(&pubkey, &account);
    }

    info!("Removed {} vote accounts", count);
    Ok(count)
}

fn remove_stake_accounts(bank: &Bank) -> Result<usize, String> {
    info!("Removing stake accounts from mainnet bank...");
    let stake_program_id = solana_stake_program::id();

    let accounts = bank
        .get_program_accounts(&stake_program_id, &ScanConfig::default())
        .map_err(|e| format!("Failed to get stake accounts: {:?}", e))?;

    let count = accounts.len();
    for (pubkey, mut account) in accounts {
        account.set_lamports(0);
        bank.store_account(&pubkey, &account);
    }

    info!("Removed {} stake accounts", count);
    Ok(count)
}

fn add_accounts(
    bank: &Bank,
    accounts: &HashMap<Pubkey, AccountSharedData>,
    account_type: &str,
) -> Result<(), String> {
    info!("Adding {} {} accounts to merged bank...", accounts.len(), account_type);

    for (pubkey, account) in accounts {
        bank.store_account(pubkey, account);
    }

    info!("Added {} {} accounts", accounts.len(), account_type);
    Ok(())
}

fn count_total_accounts(bank: &Bank) -> Result<usize, String> {
    let mut count = 0;
    bank.scan_all_accounts(|_| { count += 1; }, true)
        .map_err(|e| format!("Failed to scan accounts: {:?}", e))?;
    Ok(count)
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
    tim_cluster_ledger: &Path,
    output_snapshot_dir: &Path,
    warp_slot: Option<Slot>,
) -> Result<MergeStats, String> {
    info!("=== Starting Snapshot Merge ===");
    info!("Mainnet ledger: {:?}", mainnet_ledger);
    info!("Tim cluster ledger: {:?}", tim_cluster_ledger);
    info!("Output directory: {:?}", output_snapshot_dir);

    // Load genesis config from mainnet (we want mainnet's genesis)
    info!("\n=== Step 1: Loading Genesis Config ===");
    let genesis_config = open_genesis_config(mainnet_ledger, 10485760)
        .map_err(|e| format!("Failed to open mainnet genesis config: {:?}", e))?;
    info!("Loaded genesis config successfully");

    // Load mainnet snapshot
    info!("\n=== Step 2: Loading Mainnet Snapshot ===");
    let mainnet_bank = load_bank_from_snapshot(mainnet_ledger, &genesis_config)?;
    let mainnet_total_accounts = count_total_accounts(&mainnet_bank)?;
    info!("Mainnet bank loaded with {} total accounts", mainnet_total_accounts);

    // Load tim cluster snapshot (with its own genesis - we just need the accounts)
    info!("\n=== Step 3: Loading Tim Cluster Snapshot ===");
    let tim_genesis_config = open_genesis_config(tim_cluster_ledger, 10485760)
        .map_err(|e| format!("Failed to open tim cluster genesis config: {:?}", e))?;
    let tim_cluster_bank = load_bank_from_snapshot(tim_cluster_ledger, &tim_genesis_config)?;

    // Extract vote and stake accounts from tim cluster
    info!("\n=== Step 4: Extracting Tim Cluster Validators ===");
    let tim_vote_accounts = extract_vote_accounts(&tim_cluster_bank)?;
    let tim_stake_accounts = extract_stake_accounts(&tim_cluster_bank)?;

    // Create child bank from mainnet for modifications
    info!("\n=== Step 5: Creating Child Bank for Modifications ===");
    let merged_bank = Bank::new_from_parent(
        mainnet_bank.clone(),
        mainnet_bank.collector_id(),
        mainnet_bank.slot() + 1,
    );
    info!("Created child bank at slot {}", merged_bank.slot());

    let capitalization_before = merged_bank.capitalization();

    // Remove mainnet vote and stake accounts
    info!("\n=== Step 6: Removing Mainnet Validators ===");
    let mainnet_vote_accounts_removed = remove_vote_accounts(&merged_bank)?;
    let mainnet_stake_accounts_removed = remove_stake_accounts(&merged_bank)?;

    // Add tim cluster vote and stake accounts
    info!("\n=== Step 7: Adding Tim Cluster Validators ===");
    add_accounts(&merged_bank, &tim_vote_accounts, "vote")?;
    add_accounts(&merged_bank, &tim_stake_accounts, "stake")?;

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

    let final_total_accounts = count_total_accounts(&final_bank)?;

    // Create snapshot
    info!("\n=== Step 10: Creating Merged Snapshot ===");
    std::fs::create_dir_all(output_snapshot_dir)
        .map_err(|e| format!("Failed to create output directory: {:?}", e))?;

    let snapshot_path = create_snapshot_from_bank(&final_bank, output_snapshot_dir)?;

    let stats = MergeStats {
        mainnet_total_accounts,
        mainnet_vote_accounts_removed,
        mainnet_stake_accounts_removed,
        tim_vote_accounts_added: tim_vote_accounts.len(),
        tim_stake_accounts_added: tim_stake_accounts.len(),
        final_total_accounts,
        capitalization_before,
        capitalization_after,
        snapshot_path,
    };

    info!("\n=== Merge Complete ===");
    info!("Statistics:");
    info!("  Mainnet total accounts: {}", stats.mainnet_total_accounts);
    info!("  Mainnet vote accounts removed: {}", stats.mainnet_vote_accounts_removed);
    info!("  Mainnet stake accounts removed: {}", stats.mainnet_stake_accounts_removed);
    info!("  Tim cluster vote accounts added: {}", stats.tim_vote_accounts_added);
    info!("  Tim cluster stake accounts added: {}", stats.tim_stake_accounts_added);
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
            Arg::with_name("tim_cluster_ledger")
                .long("tim-cluster-ledger")
                .value_name("PATH")
                .takes_value(true)
                .required(true)
                .help("Path to tim cluster ledger directory"),
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
    let tim_cluster_ledger = PathBuf::from(value_t_or_exit!(matches, "tim_cluster_ledger", String));
    let output_directory = PathBuf::from(value_t_or_exit!(matches, "output_directory", String));
    let warp_slot = value_t!(matches, "warp_slot", Slot).ok();

    match merge_snapshots(&mainnet_ledger, &tim_cluster_ledger, &output_directory, warp_slot) {
        Ok(stats) => {
            println!("\n✅ Snapshot merge completed successfully!");
            println!("\nSummary:");
            println!("  • Started with {} mainnet accounts", stats.mainnet_total_accounts);
            println!("  • Removed {} vote accounts and {} stake accounts from mainnet",
                     stats.mainnet_vote_accounts_removed,
                     stats.mainnet_stake_accounts_removed);
            println!("  • Added {} vote accounts and {} stake accounts from tim cluster",
                     stats.tim_vote_accounts_added,
                     stats.tim_stake_accounts_added);
            println!("  • Final snapshot has {} accounts", stats.final_total_accounts);
            println!("  • Capitalization: {} -> {} lamports",
                     stats.capitalization_before,
                     stats.capitalization_after);
            println!("\nSnapshot archive created: {}", stats.snapshot_path);
            println!("You can use this snapshot to start a validator with the merged state.");
        }
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            exit(1);
        }
    }
}
