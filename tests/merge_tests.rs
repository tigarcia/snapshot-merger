#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use solana_account::{Account, AccountSharedData};
    use solana_genesis_config::GenesisConfig;
    use solana_keypair::{Keypair, Signer};
    use solana_pubkey::Pubkey;
    use solana_runtime::bank::Bank;
    use snapshot_merger::merge::functions;

    // Helper function to create a minimal bank for testing
    fn create_test_bank() -> Arc<Bank> {
        let genesis_config = GenesisConfig::default();
        Arc::new(Bank::new_for_tests(&genesis_config))
    }

    #[test]
    fn test_count_total_accounts() {
        let bank = create_test_bank();
        let count = functions::count_total_accounts(&bank).unwrap();
        // A fresh bank should have at least some accounts (system program, etc.)
        assert!(count > 0);
    }

    #[test]
    fn test_inspect_bank_contents() {
        let bank = create_test_bank();

        // Print total accounts
        let count = functions::count_total_accounts(&bank).unwrap();
        println!("Total accounts in test bank: {}", count);

        // Check if any vote/stake accounts exist
        let vote_accounts = functions::extract_vote_accounts(&bank).unwrap();
        let stake_accounts = functions::extract_stake_accounts(&bank).unwrap();

        println!("Vote accounts: {}", vote_accounts.len());
        println!("Stake accounts: {}", stake_accounts.len());

        assert_eq!(vote_accounts.len(), 0);
        assert_eq!(stake_accounts.len(), 0);
    }

    #[test]
    fn test_extract_vote_accounts() {
        let bank = create_test_bank();
        let accounts = functions::extract_vote_accounts(&bank).unwrap();
        // A fresh test bank has no vote accounts by default
        assert_eq!(accounts.len(), 0);
    }

    #[test]
    fn test_extract_stake_accounts() {
        let bank = create_test_bank();
        let accounts = functions::extract_stake_accounts(&bank).unwrap();
        // A fresh test bank has no stake accounts by default
        assert_eq!(accounts.len(), 0);
    }

    #[test]
    fn test_remove_vote_accounts_with_no_accounts() {
        let bank = create_test_bank();
        let count = functions::remove_vote_accounts(&bank).unwrap();
        // Should return 0 when there are no vote accounts to remove
        assert_eq!(count, 0);
    }

    #[test]
    fn test_remove_stake_accounts_with_no_accounts() {
        let bank = create_test_bank();
        let count = functions::remove_stake_accounts(&bank).unwrap();
        // Should return 0 when there are no stake accounts to remove
        assert_eq!(count, 0);
    }

    #[test]
    fn test_add_accounts() {
        let bank = create_test_bank();
        let mut accounts = HashMap::new();

        // Create a dummy account
        let keypair = Keypair::new();
        let account = AccountSharedData::from(Account {
            lamports: 1000,
            data: vec![0; 100],
            owner: Pubkey::default(),
            executable: false,
            rent_epoch: 0,
        });

        accounts.insert(keypair.pubkey(), account);

        // Adding accounts should not fail
        let result = functions::add_accounts(&bank, &accounts, "test");
        assert!(result.is_ok());
    }
}
