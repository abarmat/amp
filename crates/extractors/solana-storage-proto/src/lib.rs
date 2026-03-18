pub mod proto;

use std::str::FromStr;

pub use proto::solana::storage::*;
use serde::{Deserialize, Serialize};
use solana_account_decoder::{
    StringAmount,
    parse_token::{UiTokenAmount, real_number_string_trimmed},
};
use solana_message::v0::LoadedAddresses;
use solana_reward_info::RewardType;
use solana_serde::default_on_eof;
use solana_transaction_context::TransactionReturnData;
use solana_transaction_error::{TransactionError, TransactionResult as Result};
use solana_transaction_status_client_types::{
    InnerInstructions, Reward, TransactionStatusMeta, TransactionTokenBalance,
};

pub type StoredExtendedRewards = Vec<StoredExtendedReward>;

#[derive(Serialize, Deserialize)]
pub struct StoredExtendedReward {
    pub pubkey: String,
    pub lamports: i64,
    #[serde(deserialize_with = "default_on_eof")]
    pub post_balance: u64,
    #[serde(deserialize_with = "default_on_eof")]
    pub reward_type: Option<RewardType>,
    #[serde(deserialize_with = "default_on_eof")]
    pub commission: Option<u8>,
}

impl From<StoredExtendedReward> for Reward {
    fn from(value: StoredExtendedReward) -> Self {
        let StoredExtendedReward {
            pubkey,
            lamports,
            post_balance,
            reward_type,
            commission,
        } = value;
        Self {
            pubkey,
            lamports,
            post_balance,
            reward_type,
            commission,
        }
    }
}

impl From<Reward> for StoredExtendedReward {
    fn from(value: Reward) -> Self {
        let Reward {
            pubkey,
            lamports,
            post_balance,
            reward_type,
            commission,
        } = value;
        Self {
            pubkey,
            lamports,
            post_balance,
            reward_type,
            commission,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct StoredTokenAmount {
    pub ui_amount: Option<f64>,
    pub decimals: u8,
    pub amount: StringAmount,
}

impl From<StoredTokenAmount> for UiTokenAmount {
    fn from(value: StoredTokenAmount) -> Self {
        let StoredTokenAmount {
            ui_amount,
            decimals,
            amount,
        } = value;
        let ui_amount_string =
            real_number_string_trimmed(u64::from_str(&amount).unwrap_or(0), decimals);
        Self {
            ui_amount,
            decimals,
            amount,
            ui_amount_string,
        }
    }
}

impl From<UiTokenAmount> for StoredTokenAmount {
    fn from(value: UiTokenAmount) -> Self {
        let UiTokenAmount {
            ui_amount,
            decimals,
            amount,
            ..
        } = value;
        Self {
            ui_amount,
            decimals,
            amount,
        }
    }
}

struct StoredTransactionError(Vec<u8>);

impl From<StoredTransactionError> for TransactionError {
    fn from(value: StoredTransactionError) -> Self {
        let bytes = value.0;
        bincode::deserialize(&bytes).expect("transaction error to deserialize from bytes")
    }
}

impl From<TransactionError> for StoredTransactionError {
    fn from(value: TransactionError) -> Self {
        let bytes = bincode::serialize(&value).expect("transaction error to serialize to bytes");
        StoredTransactionError(bytes)
    }
}

#[derive(Serialize, Deserialize)]
pub struct StoredTransactionTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: StoredTokenAmount,
    #[serde(deserialize_with = "default_on_eof")]
    pub owner: String,
    #[serde(deserialize_with = "default_on_eof")]
    pub program_id: String,
}

impl From<StoredTransactionTokenBalance> for TransactionTokenBalance {
    fn from(value: StoredTransactionTokenBalance) -> Self {
        let StoredTransactionTokenBalance {
            account_index,
            mint,
            ui_token_amount,
            owner,
            program_id,
        } = value;
        Self {
            account_index,
            mint,
            ui_token_amount: ui_token_amount.into(),
            owner,
            program_id,
        }
    }
}

impl From<TransactionTokenBalance> for StoredTransactionTokenBalance {
    fn from(value: TransactionTokenBalance) -> Self {
        let TransactionTokenBalance {
            account_index,
            mint,
            ui_token_amount,
            owner,
            program_id,
        } = value;
        Self {
            account_index,
            mint,
            ui_token_amount: ui_token_amount.into(),
            owner,
            program_id,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct StoredTransactionStatusMeta {
    pub status: Result<()>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    #[serde(deserialize_with = "default_on_eof")]
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    #[serde(deserialize_with = "default_on_eof")]
    pub log_messages: Option<Vec<String>>,
    #[serde(deserialize_with = "default_on_eof")]
    pub pre_token_balances: Option<Vec<StoredTransactionTokenBalance>>,
    #[serde(deserialize_with = "default_on_eof")]
    pub post_token_balances: Option<Vec<StoredTransactionTokenBalance>>,
    #[serde(deserialize_with = "default_on_eof")]
    pub rewards: Option<Vec<StoredExtendedReward>>,
    #[serde(deserialize_with = "default_on_eof")]
    pub return_data: Option<TransactionReturnData>,
    #[serde(deserialize_with = "default_on_eof")]
    pub compute_units_consumed: Option<u64>,
    #[serde(deserialize_with = "default_on_eof")]
    pub cost_units: Option<u64>,
}

impl From<StoredTransactionStatusMeta> for TransactionStatusMeta {
    fn from(value: StoredTransactionStatusMeta) -> Self {
        let StoredTransactionStatusMeta {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions,
            log_messages,
            pre_token_balances,
            post_token_balances,
            rewards,
            return_data,
            compute_units_consumed,
            cost_units,
        } = value;
        Self {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions,
            log_messages,
            pre_token_balances: pre_token_balances
                .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
            post_token_balances: post_token_balances
                .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
            rewards: rewards
                .map(|rewards| rewards.into_iter().map(|reward| reward.into()).collect()),
            loaded_addresses: LoadedAddresses::default(),
            return_data,
            compute_units_consumed,
            cost_units,
        }
    }
}

impl TryFrom<TransactionStatusMeta> for StoredTransactionStatusMeta {
    type Error = bincode::Error;
    fn try_from(value: TransactionStatusMeta) -> std::result::Result<Self, Self::Error> {
        let TransactionStatusMeta {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions,
            log_messages,
            pre_token_balances,
            post_token_balances,
            rewards,
            loaded_addresses,
            return_data,
            compute_units_consumed,
            cost_units,
        } = value;

        if !loaded_addresses.is_empty() {
            // Deprecated bincode serialized status metadata doesn't support
            // loaded addresses.
            return Err(
                bincode::ErrorKind::Custom("Bincode serialization is deprecated".into()).into(),
            );
        }

        Ok(Self {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions,
            log_messages,
            pre_token_balances: pre_token_balances
                .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
            post_token_balances: post_token_balances
                .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
            rewards: rewards
                .map(|rewards| rewards.into_iter().map(|reward| reward.into()).collect()),
            return_data,
            compute_units_consumed,
            cost_units,
        })
    }
}

impl From<legacy_v1::StoredExtendedReward> for StoredExtendedReward {
    fn from(value: legacy_v1::StoredExtendedReward) -> Self {
        let legacy_v1::StoredExtendedReward {
            pubkey,
            lamports,
            post_balance,
            reward_type,
            commission,
        } = value;
        Self {
            pubkey,
            lamports,
            post_balance,
            reward_type,
            commission,
        }
    }
}

impl From<legacy_v1::StoredTokenAmount> for StoredTokenAmount {
    fn from(value: legacy_v1::StoredTokenAmount) -> Self {
        let legacy_v1::StoredTokenAmount {
            ui_amount,
            decimals,
            amount,
            ..
        } = value;
        Self {
            ui_amount: Some(ui_amount),
            decimals,
            amount,
        }
    }
}

impl From<legacy_v1::StoredTransactionTokenBalance> for StoredTransactionTokenBalance {
    fn from(value: legacy_v1::StoredTransactionTokenBalance) -> Self {
        let legacy_v1::StoredTransactionTokenBalance {
            account_index,
            mint,
            ui_token_amount,
            owner,
            program_id,
        } = value;
        Self {
            account_index,
            mint,
            ui_token_amount: ui_token_amount.into(),
            owner,
            program_id,
        }
    }
}

impl From<legacy_v1::StoredTransactionStatusMeta> for StoredTransactionStatusMeta {
    fn from(value: legacy_v1::StoredTransactionStatusMeta) -> Self {
        let legacy_v1::StoredTransactionStatusMeta {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions,
            log_messages,
            pre_token_balances,
            post_token_balances,
            rewards,
            return_data,
            compute_units_consumed,
            cost_units,
        } = value;
        Self {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions: inner_instructions.map(|instructions| {
                instructions
                    .into_iter()
                    .map(|instruction| instruction.into())
                    .collect()
            }),
            log_messages,
            pre_token_balances: pre_token_balances
                .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
            post_token_balances: post_token_balances
                .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
            rewards: rewards
                .map(|rewards| rewards.into_iter().map(|reward| reward.into()).collect()),
            return_data,
            compute_units_consumed,
            cost_units,
        }
    }
}

impl From<legacy_v2::StoredTokenAmount> for StoredTokenAmount {
    fn from(value: legacy_v2::StoredTokenAmount) -> Self {
        let legacy_v2::StoredTokenAmount {
            ui_amount,
            decimals,
            amount,
            ..
        } = value;
        Self {
            ui_amount,
            decimals,
            amount,
        }
    }
}

impl From<legacy_v2::StoredTransactionTokenBalance> for StoredTransactionTokenBalance {
    fn from(value: legacy_v2::StoredTransactionTokenBalance) -> Self {
        let legacy_v2::StoredTransactionTokenBalance {
            account_index,
            mint,
            ui_token_amount,
            owner,
            program_id,
        } = value;
        Self {
            account_index,
            mint,
            ui_token_amount: ui_token_amount.into(),
            owner,
            program_id,
        }
    }
}

impl From<legacy_v2::StoredTransactionStatusMeta> for StoredTransactionStatusMeta {
    fn from(value: legacy_v2::StoredTransactionStatusMeta) -> Self {
        let legacy_v2::StoredTransactionStatusMeta {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions,
            log_messages,
            pre_token_balances,
            post_token_balances,
            rewards,
            return_data,
            compute_units_consumed,
            cost_units,
        } = value;
        Self {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions: inner_instructions.map(|instructions| {
                instructions
                    .into_iter()
                    .map(|instruction| instruction.into())
                    .collect()
            }),
            log_messages,
            pre_token_balances: pre_token_balances
                .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
            post_token_balances: post_token_balances
                .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
            rewards: rewards
                .map(|rewards| rewards.into_iter().map(|reward| reward.into()).collect()),
            return_data,
            compute_units_consumed,
            cost_units,
        }
    }
}

/// Versioned enum that handles deserialization of all bincode formats of transaction status
/// metadata. The binary layout differs across Solana epochs, so this enum tries the current
/// format first, then legacy v2, then legacy v1.
pub enum StoredTransactionStatusMetaVersioned {
    Current(StoredTransactionStatusMeta),
    LegacyV2(legacy_v2::StoredTransactionStatusMeta),
    LegacyV1(legacy_v1::StoredTransactionStatusMeta),
}

impl StoredTransactionStatusMetaVersioned {
    /// Try to deserialize from bincode-encoded bytes, attempting current, legacy v2,
    /// and legacy v1 formats in order.
    pub fn from_bincode(data: &[u8]) -> std::result::Result<Self, BincodeDecodeAllError> {
        match bincode::deserialize::<StoredTransactionStatusMeta>(data) {
            Ok(meta) => Ok(Self::Current(meta)),
            Err(current_err) => {
                match bincode::deserialize::<legacy_v2::StoredTransactionStatusMeta>(data) {
                    Ok(meta) => Ok(Self::LegacyV2(meta)),
                    Err(legacy_v2_err) => {
                        match bincode::deserialize::<legacy_v1::StoredTransactionStatusMeta>(data) {
                            Ok(meta) => Ok(Self::LegacyV1(meta)),
                            Err(legacy_v1_err) => Err(BincodeDecodeAllError {
                                current_err,
                                legacy_v2_err,
                                legacy_v1_err,
                            }),
                        }
                    }
                }
            }
        }
    }
}

impl From<StoredTransactionStatusMetaVersioned> for StoredTransactionStatusMeta {
    fn from(value: StoredTransactionStatusMetaVersioned) -> Self {
        match value {
            StoredTransactionStatusMetaVersioned::Current(meta) => meta,
            StoredTransactionStatusMetaVersioned::LegacyV2(meta) => meta.into(),
            StoredTransactionStatusMetaVersioned::LegacyV1(meta) => meta.into(),
        }
    }
}

impl From<StoredTransactionStatusMetaVersioned> for TransactionStatusMeta {
    fn from(value: StoredTransactionStatusMetaVersioned) -> Self {
        StoredTransactionStatusMeta::from(value).into()
    }
}

#[derive(Debug)]
pub struct BincodeDecodeAllError {
    pub current_err: bincode::Error,
    pub legacy_v2_err: bincode::Error,
    pub legacy_v1_err: bincode::Error,
}

impl std::fmt::Display for BincodeDecodeAllError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bincode: {}; legacy v2 bincode: {}; legacy v1 bincode: {}",
            self.current_err, self.legacy_v2_err, self.legacy_v1_err
        )
    }
}

impl std::error::Error for BincodeDecodeAllError {}

/// Legacy v2 format used in bincode (de)serialization. This format added some fields compared to
/// legacy v1, but is still missing some fields that were added in later versions, so those fields
/// are marked with `#[serde(skip)]` to allow deserialization to succeed even when those fields
/// are missing.
pub mod legacy_v2 {
    use serde::{Deserialize, Serialize};
    use solana_account_decoder::{StringAmount, parse_token::UiTokenAmount};
    use solana_message::v0::LoadedAddresses;
    use solana_serde::default_on_eof;
    use solana_transaction_context::TransactionReturnData;
    use solana_transaction_error::TransactionResult as Result;
    use solana_transaction_status_client_types::{TransactionStatusMeta, TransactionTokenBalance};

    // These types are identical to legacy_v1.
    pub use super::legacy_v1::{
        StoredExtendedReward, StoredExtendedRewards, StoredInnerInstruction,
        StoredInnerInstructions,
    };

    #[derive(Serialize, Deserialize)]
    pub struct StoredTokenAmount {
        // This field changed from `f64` in legacy v1 to `Option<f64>` in legacy v2.
        pub ui_amount: Option<f64>,
        pub decimals: u8,
        pub amount: StringAmount,
        // This field was added in legacy v2 to avoid having to recompute the UI amount
        // string from the raw amount and `decimals` when deserializing.
        pub ui_amount_string: StringAmount,
    }

    impl From<StoredTokenAmount> for UiTokenAmount {
        fn from(value: StoredTokenAmount) -> Self {
            let StoredTokenAmount {
                ui_amount,
                decimals,
                amount,
                ui_amount_string,
            } = value;
            Self {
                ui_amount,
                decimals,
                amount,
                ui_amount_string,
            }
        }
    }

    impl From<UiTokenAmount> for StoredTokenAmount {
        fn from(value: UiTokenAmount) -> Self {
            let UiTokenAmount {
                ui_amount,
                decimals,
                amount,
                ui_amount_string,
            } = value;
            Self {
                ui_amount,
                decimals,
                amount,
                ui_amount_string,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct StoredTransactionTokenBalance {
        pub account_index: u8,
        pub mint: String,
        pub ui_token_amount: StoredTokenAmount,
        // These fields are not present in legacy bincode serialized data.
        #[serde(skip)]
        pub owner: String,
        #[serde(skip)]
        pub program_id: String,
    }

    impl From<StoredTransactionTokenBalance> for TransactionTokenBalance {
        fn from(value: StoredTransactionTokenBalance) -> Self {
            let StoredTransactionTokenBalance {
                account_index,
                mint,
                ui_token_amount,
                owner,
                program_id,
            } = value;
            Self {
                account_index,
                mint,
                ui_token_amount: ui_token_amount.into(),
                owner,
                program_id,
            }
        }
    }

    impl From<TransactionTokenBalance> for StoredTransactionTokenBalance {
        fn from(value: TransactionTokenBalance) -> Self {
            let TransactionTokenBalance {
                account_index,
                mint,
                ui_token_amount,
                owner,
                program_id,
            } = value;
            Self {
                account_index,
                mint,
                ui_token_amount: ui_token_amount.into(),
                owner,
                program_id,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct StoredTransactionStatusMeta {
        pub status: Result<()>,
        pub fee: u64,
        pub pre_balances: Vec<u64>,
        pub post_balances: Vec<u64>,
        #[serde(deserialize_with = "default_on_eof")]
        pub inner_instructions: Option<Vec<StoredInnerInstructions>>,
        #[serde(deserialize_with = "default_on_eof")]
        pub log_messages: Option<Vec<String>>,
        #[serde(deserialize_with = "default_on_eof")]
        pub pre_token_balances: Option<Vec<StoredTransactionTokenBalance>>,
        #[serde(deserialize_with = "default_on_eof")]
        pub post_token_balances: Option<Vec<StoredTransactionTokenBalance>>,
        #[serde(deserialize_with = "default_on_eof")]
        pub rewards: Option<Vec<StoredExtendedReward>>,
        // These fields are not present in legacy bincode serialized data.
        #[serde(skip)]
        pub return_data: Option<TransactionReturnData>,
        #[serde(skip)]
        pub compute_units_consumed: Option<u64>,
        #[serde(skip)]
        pub cost_units: Option<u64>,
    }

    impl From<StoredTransactionStatusMeta> for TransactionStatusMeta {
        fn from(value: StoredTransactionStatusMeta) -> Self {
            let StoredTransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
                return_data,
                compute_units_consumed,
                cost_units,
            } = value;
            Self {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions: inner_instructions.map(|instructions| {
                    instructions
                        .into_iter()
                        .map(|instruction| instruction.into())
                        .collect()
                }),
                log_messages,
                pre_token_balances: pre_token_balances
                    .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
                post_token_balances: post_token_balances
                    .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
                rewards: rewards
                    .map(|rewards| rewards.into_iter().map(|reward| reward.into()).collect()),
                loaded_addresses: LoadedAddresses::default(),
                return_data,
                compute_units_consumed,
                cost_units,
            }
        }
    }

    impl TryFrom<TransactionStatusMeta> for StoredTransactionStatusMeta {
        type Error = bincode::Error;
        fn try_from(value: TransactionStatusMeta) -> std::result::Result<Self, Self::Error> {
            let TransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
                loaded_addresses,
                return_data,
                compute_units_consumed,
                cost_units,
            } = value;

            if !loaded_addresses.is_empty() {
                // Deprecated bincode serialized status metadata doesn't support
                // loaded addresses.
                return Err(bincode::ErrorKind::Custom(
                    "Bincode serialization is deprecated".into(),
                )
                .into());
            }

            Ok(Self {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions: inner_instructions.map(|instructions| {
                    instructions
                        .into_iter()
                        .map(|instruction| instruction.into())
                        .collect()
                }),
                log_messages,
                pre_token_balances: pre_token_balances
                    .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
                post_token_balances: post_token_balances
                    .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
                rewards: rewards
                    .map(|rewards| rewards.into_iter().map(|reward| reward.into()).collect()),
                return_data,
                compute_units_consumed,
                cost_units,
            })
        }
    }
}

/// Legacy v1 format used in bincode (de)serialization. This format is missing some fields that
/// were added in later versions, so those fields are marked with `#[serde(skip)]` to allow
/// deserialization to succeed even when those fields are missing.
pub mod legacy_v1 {
    use std::str::FromStr;

    use serde::{Deserialize, Serialize};
    use solana_account_decoder::{
        StringAmount,
        parse_token::{UiTokenAmount, real_number_string_trimmed},
    };
    use solana_message::v0::LoadedAddresses;
    use solana_reward_info::RewardType;
    use solana_serde::default_on_eof;
    use solana_transaction_context::TransactionReturnData;
    use solana_transaction_error::TransactionResult as Result;
    use solana_transaction_status_client_types::{
        InnerInstruction, InnerInstructions, Reward, TransactionStatusMeta, TransactionTokenBalance,
    };

    pub type StoredExtendedRewards = Vec<StoredExtendedReward>;

    #[derive(Serialize, Deserialize)]
    pub struct StoredExtendedReward {
        pub pubkey: String,
        pub lamports: i64,
        // These fields are not present in legacy bincode serialized data.
        #[serde(skip)]
        pub post_balance: u64,
        #[serde(skip)]
        pub reward_type: Option<RewardType>,
        #[serde(skip)]
        pub commission: Option<u8>,
    }

    impl From<StoredExtendedReward> for Reward {
        fn from(value: StoredExtendedReward) -> Self {
            let StoredExtendedReward {
                pubkey,
                lamports,
                post_balance,
                reward_type,
                commission,
            } = value;
            Self {
                pubkey,
                lamports,
                post_balance,
                reward_type,
                commission,
            }
        }
    }

    impl From<Reward> for StoredExtendedReward {
        fn from(value: Reward) -> Self {
            let Reward {
                pubkey,
                lamports,
                post_balance,
                reward_type,
                commission,
            } = value;
            Self {
                pubkey,
                lamports,
                post_balance,
                reward_type,
                commission,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct StoredTokenAmount {
        pub ui_amount: f64,
        pub decimals: u8,
        pub amount: StringAmount,
    }

    impl From<StoredTokenAmount> for UiTokenAmount {
        fn from(value: StoredTokenAmount) -> Self {
            let StoredTokenAmount {
                ui_amount,
                decimals,
                amount,
            } = value;
            let ui_amount_string =
                real_number_string_trimmed(u64::from_str(&amount).unwrap_or(0), decimals);
            Self {
                ui_amount: Some(ui_amount),
                decimals,
                amount,
                ui_amount_string,
            }
        }
    }

    impl From<UiTokenAmount> for StoredTokenAmount {
        fn from(value: UiTokenAmount) -> Self {
            let UiTokenAmount {
                ui_amount,
                decimals,
                amount,
                ..
            } = value;
            Self {
                ui_amount: ui_amount.unwrap_or(0.0),
                decimals,
                amount,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct StoredInnerInstruction {
        pub instruction: solana_message::compiled_instruction::CompiledInstruction,
        // This field is not present in legacy bincode serialized data.
        #[serde(skip)]
        pub stack_height: Option<u32>,
    }

    impl From<StoredInnerInstruction> for InnerInstruction {
        fn from(value: StoredInnerInstruction) -> Self {
            let StoredInnerInstruction {
                instruction,
                stack_height,
            } = value;
            Self {
                instruction,
                stack_height,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct StoredInnerInstructions {
        pub index: u8,
        pub instructions: Vec<StoredInnerInstruction>,
    }

    impl From<StoredInnerInstructions> for InnerInstructions {
        fn from(value: StoredInnerInstructions) -> Self {
            let StoredInnerInstructions {
                index,
                instructions,
            } = value;
            Self {
                index,
                instructions: instructions
                    .into_iter()
                    .map(|instruction| instruction.into())
                    .collect(),
            }
        }
    }

    impl From<InnerInstructions> for StoredInnerInstructions {
        fn from(value: InnerInstructions) -> Self {
            let InnerInstructions {
                index,
                instructions,
            } = value;
            Self {
                index,
                instructions: instructions
                    .into_iter()
                    .map(|instruction| instruction.into())
                    .collect(),
            }
        }
    }

    impl From<InnerInstruction> for StoredInnerInstruction {
        fn from(value: InnerInstruction) -> Self {
            let InnerInstruction {
                instruction,
                stack_height,
            } = value;
            Self {
                instruction,
                stack_height,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct StoredTransactionTokenBalance {
        pub account_index: u8,
        pub mint: String,
        pub ui_token_amount: StoredTokenAmount,
        // These fields are not present in legacy bincode serialized data.
        #[serde(skip)]
        pub owner: String,
        #[serde(skip)]
        pub program_id: String,
    }

    impl From<StoredTransactionTokenBalance> for TransactionTokenBalance {
        fn from(value: StoredTransactionTokenBalance) -> Self {
            let StoredTransactionTokenBalance {
                account_index,
                mint,
                ui_token_amount,
                owner,
                program_id,
            } = value;
            Self {
                account_index,
                mint,
                ui_token_amount: ui_token_amount.into(),
                owner,
                program_id,
            }
        }
    }

    impl From<TransactionTokenBalance> for StoredTransactionTokenBalance {
        fn from(value: TransactionTokenBalance) -> Self {
            let TransactionTokenBalance {
                account_index,
                mint,
                ui_token_amount,
                owner,
                program_id,
            } = value;
            Self {
                account_index,
                mint,
                ui_token_amount: ui_token_amount.into(),
                owner,
                program_id,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct StoredTransactionStatusMeta {
        pub status: Result<()>,
        pub fee: u64,
        pub pre_balances: Vec<u64>,
        pub post_balances: Vec<u64>,
        #[serde(deserialize_with = "default_on_eof")]
        pub inner_instructions: Option<Vec<StoredInnerInstructions>>,
        #[serde(deserialize_with = "default_on_eof")]
        pub log_messages: Option<Vec<String>>,
        #[serde(deserialize_with = "default_on_eof")]
        pub pre_token_balances: Option<Vec<StoredTransactionTokenBalance>>,
        #[serde(deserialize_with = "default_on_eof")]
        pub post_token_balances: Option<Vec<StoredTransactionTokenBalance>>,
        #[serde(deserialize_with = "default_on_eof")]
        pub rewards: Option<Vec<StoredExtendedReward>>,
        // These fields are not present in legacy bincode serialized data.
        #[serde(skip)]
        pub return_data: Option<TransactionReturnData>,
        #[serde(skip)]
        pub compute_units_consumed: Option<u64>,
        #[serde(skip)]
        pub cost_units: Option<u64>,
    }

    impl From<StoredTransactionStatusMeta> for TransactionStatusMeta {
        fn from(value: StoredTransactionStatusMeta) -> Self {
            let StoredTransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
                return_data,
                compute_units_consumed,
                cost_units,
            } = value;
            Self {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions: inner_instructions.map(|instructions| {
                    instructions
                        .into_iter()
                        .map(|instruction| instruction.into())
                        .collect()
                }),
                log_messages,
                pre_token_balances: pre_token_balances
                    .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
                post_token_balances: post_token_balances
                    .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
                rewards: rewards
                    .map(|rewards| rewards.into_iter().map(|reward| reward.into()).collect()),
                loaded_addresses: LoadedAddresses::default(),
                return_data,
                compute_units_consumed,
                cost_units,
            }
        }
    }

    impl TryFrom<TransactionStatusMeta> for StoredTransactionStatusMeta {
        type Error = bincode::Error;
        fn try_from(value: TransactionStatusMeta) -> std::result::Result<Self, Self::Error> {
            let TransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
                loaded_addresses,
                return_data,
                compute_units_consumed,
                cost_units,
            } = value;

            if !loaded_addresses.is_empty() {
                // Deprecated bincode serialized status metadata doesn't support
                // loaded addresses.
                return Err(bincode::ErrorKind::Custom(
                    "Bincode serialization is deprecated".into(),
                )
                .into());
            }

            Ok(Self {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions: inner_instructions.map(|instructions| {
                    instructions
                        .into_iter()
                        .map(|instruction| instruction.into())
                        .collect()
                }),
                log_messages,
                pre_token_balances: pre_token_balances
                    .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
                post_token_balances: post_token_balances
                    .map(|balances| balances.into_iter().map(|balance| balance.into()).collect()),
                rewards: rewards
                    .map(|rewards| rewards.into_iter().map(|reward| reward.into()).collect()),
                return_data,
                compute_units_consumed,
                cost_units,
            })
        }
    }
}
