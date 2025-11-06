// Snapshot merging functionality
pub mod functions {
    use std::collections::HashMap;
    use solana_account::{AccountSharedData, WritableAccount};
    use solana_pubkey::Pubkey;
    use solana_runtime::bank::Bank;
    use solana_stake_program;
    use solana_vote_program;

    pub fn extract_vote_accounts(bank: &Bank) -> Result<HashMap<Pubkey, AccountSharedData>, String> {
        log::info!("Extracting vote accounts...");
        let vote_program_id = solana_vote_program::id();

        let accounts = bank
            .get_program_accounts(&vote_program_id, &solana_accounts_db::accounts_index::ScanConfig::default())
            .map_err(|e| format!("Failed to get vote accounts: {:?}", e))?;

        log::info!("Found {} vote accounts", accounts.len());
        Ok(accounts.into_iter().collect())
    }

    pub fn extract_stake_accounts(bank: &Bank) -> Result<HashMap<Pubkey, AccountSharedData>, String> {
        log::info!("Extracting stake accounts...");
        let stake_program_id = solana_stake_program::id();

        let accounts = bank
            .get_program_accounts(&stake_program_id, &solana_accounts_db::accounts_index::ScanConfig::default())
            .map_err(|e| format!("Failed to get stake accounts: {:?}", e))?;

        log::info!("Found {} stake accounts", accounts.len());
        Ok(accounts.into_iter().collect())
    }

    pub fn remove_vote_accounts(bank: &Bank) -> Result<usize, String> {
        log::info!("Removing vote accounts from mainnet bank...");
        let vote_program_id = solana_vote_program::id();

        let accounts = bank
            .get_program_accounts(&vote_program_id, &solana_accounts_db::accounts_index::ScanConfig::default())
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
            .get_program_accounts(&stake_program_id, &solana_accounts_db::accounts_index::ScanConfig::default())
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
        bank: &Bank,
        accounts: &HashMap<Pubkey, AccountSharedData>,
        account_type: &str,
    ) -> Result<(), String> {
        log::info!("Adding {} {} accounts to merged bank...", accounts.len(), account_type);
        for (pubkey, account) in accounts {
            bank.store_account(pubkey, account);
        }
        log::info!("Added {} {} accounts", accounts.len(), account_type);
        Ok(())
    }

    /// Counts total accounts in the bank
    pub fn count_total_accounts(bank: &Bank) -> Result<usize, String> {
        let mut count = 0;
        bank.scan_all_accounts(|_| { count += 1; }, true)
            .map_err(|e| format!("Failed to scan accounts: {:?}", e))?;
        Ok(count)
    }
}
