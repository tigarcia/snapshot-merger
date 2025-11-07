// Snapshot merging functionality
pub mod functions {
    use solana_account::{AccountSharedData, ReadableAccount, WritableAccount};
    use solana_pubkey::Pubkey;
    use solana_runtime::bank::Bank;
    use solana_stake_program;
    use solana_vote_program;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;

    pub fn extract_vote_accounts(
        bank: &Bank,
    ) -> Result<HashMap<Pubkey, AccountSharedData>, String> {
        log::info!("Extracting vote accounts...");
        let vote_program_id = solana_vote_program::id();

        let accounts = bank
            .get_program_accounts(
                &vote_program_id,
                &solana_accounts_db::accounts_index::ScanConfig::default(),
            )
            .map_err(|e| format!("Failed to get vote accounts: {:?}", e))?;

        log::info!("Found {} vote accounts", accounts.len());
        Ok(accounts.into_iter().collect())
    }

    pub fn extract_stake_accounts(
        bank: &Bank,
    ) -> Result<HashMap<Pubkey, AccountSharedData>, String> {
        log::info!("Extracting stake accounts...");
        let stake_program_id = solana_stake_program::id();

        let accounts = bank
            .get_program_accounts(
                &stake_program_id,
                &solana_accounts_db::accounts_index::ScanConfig::default(),
            )
            .map_err(|e| format!("Failed to get stake accounts: {:?}", e))?;

        log::info!("Found {} stake accounts", accounts.len());
        Ok(accounts.into_iter().collect())
    }

    pub fn extract_system_accounts(
        bank: &Bank,
    ) -> Result<HashMap<Pubkey, AccountSharedData>, String> {
        log::info!("Extracting system accounts (owned by System Program)...");
        // System Program ID: 11111111111111111111111111111111
        let system_program_id = Pubkey::from_str("11111111111111111111111111111111")
            .map_err(|e| format!("Failed to parse system program ID: {:?}", e))?;

        let accounts = bank
            .get_program_accounts(
                &system_program_id,
                &solana_accounts_db::accounts_index::ScanConfig::default(),
            )
            .map_err(|e| format!("Failed to get system accounts: {:?}", e))?;

        log::info!("Found {} system accounts", accounts.len());
        Ok(accounts.into_iter().collect())
    }

    pub fn remove_vote_accounts(bank: &Bank) -> Result<usize, String> {
        log::info!("Removing vote accounts from mainnet bank...");
        let vote_program_id = solana_vote_program::id();

        let accounts = bank
            .get_program_accounts(
                &vote_program_id,
                &solana_accounts_db::accounts_index::ScanConfig::default(),
            )
            .map_err(|e| format!("Failed to get vote accounts: {:?}", e))?;

        let count = accounts.len();
        for (pubkey, mut account) in accounts {
            account.set_lamports(0);
            bank.store_account(&pubkey, &account);
        }

        log::info!("Removed {} vote accounts", count);
        Ok(count)
    }

    pub fn remove_stake_accounts(bank: &Bank) -> Result<usize, String> {
        log::info!("Removing stake accounts from mainnet bank...");
        let stake_program_id = solana_stake_program::id();

        let accounts = bank
            .get_program_accounts(
                &stake_program_id,
                &solana_accounts_db::accounts_index::ScanConfig::default(),
            )
            .map_err(|e| format!("Failed to get stake accounts: {:?}", e))?;

        let count = accounts.len();
        for (pubkey, mut account) in accounts {
            account.set_lamports(0);
            bank.store_account(&pubkey, &account);
        }

        log::info!("Removed {} stake accounts", count);
        Ok(count)
    }

    pub fn add_accounts(
        starting_bank: Arc<Bank>,
        accounts: &HashMap<Pubkey, AccountSharedData>,
        account_type: &str,
        slot_byte_limit: u64,
    ) -> Result<Arc<Bank>, String> {
        log::info!(
            "Adding {} {} accounts to merged bank...",
            accounts.len(),
            account_type
        );

        const FLUSH_INTERVAL_ACCOUNTS: usize = 250_000;
        let mut current_bank = starting_bank;
        let mut count_since_flush = 0usize;
        let mut bytes_in_current_slot: u64 = 0;

        const ACCOUNT_STORAGE_OVERHEAD: u64 = 512;

        for (pubkey, account) in accounts {
            current_bank.store_account(pubkey, account);
            count_since_flush += 1;
            let approx_bytes = account.data().len() as u64 + ACCOUNT_STORAGE_OVERHEAD;
            bytes_in_current_slot += approx_bytes;

            if count_since_flush % FLUSH_INTERVAL_ACCOUNTS == 0 {
                log::info!(
                    "Progress: {} {} accounts added in slot {} ({} bytes)",
                    count_since_flush,
                    account_type,
                    current_bank.slot(),
                    bytes_in_current_slot
                );
                current_bank.force_flush_accounts_cache();
            }

            if bytes_in_current_slot >= slot_byte_limit {
                log::info!(
                    "Reached byte limit ({}) for slot {}, squashing and advancing to next slot",
                    bytes_in_current_slot,
                    current_bank.slot()
                );
                current_bank.force_flush_accounts_cache();
                current_bank.squash();

                let parent = Arc::clone(&current_bank);
                let next_slot = parent.slot() + 1;
                let collector_id = parent.collector_id().clone();
                current_bank = Arc::new(Bank::new_from_parent(parent, &collector_id, next_slot));
                count_since_flush = 0;
                bytes_in_current_slot = 0;
            }
        }

        log::info!(
            "Final flush after adding {} {} accounts in slot {} ({} bytes)",
            count_since_flush,
            account_type,
            current_bank.slot(),
            bytes_in_current_slot
        );
        current_bank.force_flush_accounts_cache();

        log::info!("Added {} {} accounts", accounts.len(), account_type);
        Ok(current_bank)
    }

    /// Counts total accounts in the bank
    pub fn count_total_accounts(bank: &Bank) -> Result<usize, String> {
        let mut count = 0;
        bank.scan_all_accounts(
            |_| {
                count += 1;
            },
            true,
        )
        .map_err(|e| format!("Failed to scan accounts: {:?}", e))?;
        Ok(count)
    }
}
