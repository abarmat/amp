use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, network_id::NetworkId,
};

use crate::{
    Timestamp,
    arrow::{
        ArrayRef, BinaryBuilder, BooleanBuilder, DataType, Field, Fields, FixedSizeBinaryBuilder,
        Int32Builder, ListBuilder, Schema, SchemaRef, StringBuilder, StructBuilder,
        TimestampArrayBuilder, UInt32Builder, UInt64Builder,
    },
    dataset::Table,
    rows::{TableRowError, TableRows},
    tempo::{
        BYTES32_TYPE, Bytes32, Bytes32ArrayBuilder, EVM_ADDRESS_TYPE as ADDRESS_TYPE,
        EVM_CURRENCY_TYPE, EvmAddress as Address, EvmAddressArrayBuilder, EvmCurrency,
        EvmCurrencyArrayBuilder,
    },
    timestamp_type,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["block_num".to_string(), "timestamp".to_string()],
    )
}

pub const TABLE_NAME: &str = "transactions";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    let special_block_num = Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false);
    let block_hash = Field::new("block_hash", BYTES32_TYPE, false);
    let block_num = Field::new("block_num", DataType::UInt64, false);
    let timestamp = Field::new("timestamp", timestamp_type(), false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let to = Field::new("to", ADDRESS_TYPE, true);
    let from = Field::new("from", ADDRESS_TYPE, false);
    let nonce = Field::new("nonce", DataType::UInt64, false);
    let chain_id = Field::new("chain_id", DataType::UInt64, false);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let gas_used = Field::new("gas_used", DataType::UInt64, false);
    let r#type = Field::new("type", DataType::Int32, false);
    let max_fee_per_gas = Field::new("max_fee_per_gas", EVM_CURRENCY_TYPE, false);
    let max_priority_fee_per_gas = Field::new("max_priority_fee_per_gas", EVM_CURRENCY_TYPE, false);
    let gas_price = Field::new("gas_price", EVM_CURRENCY_TYPE, true);
    let status = Field::new("status", DataType::Boolean, false);
    let value = Field::new("value", DataType::Utf8, true); // null for type 0x76 (moved to calls)
    let input = Field::new("input", DataType::Binary, true); // null for type 0x76 (moved to calls)
    let r = Field::new("r", BYTES32_TYPE, false);
    let s = Field::new("s", BYTES32_TYPE, false);
    let v_parity = Field::new("v_parity", DataType::Boolean, false);

    // Tempo-specific fields
    let fee_token = Field::new("fee_token", ADDRESS_TYPE, true);
    let nonce_key = Field::new("nonce_key", BYTES32_TYPE, false);
    let calls = Field::new(
        "calls",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("to", ADDRESS_TYPE, true),
                Field::new("value", DataType::Utf8, false),
                Field::new("input", DataType::Binary, false),
            ])),
            false,
        ))),
        false,
    );
    let fee_payer_signature = Field::new(
        "fee_payer_signature",
        DataType::Struct(Fields::from(vec![
            Field::new("r", BYTES32_TYPE, false),
            Field::new("s", BYTES32_TYPE, false),
            Field::new("y_parity", DataType::Boolean, false),
        ])),
        true, // null when not sponsored
    );
    let key_authorization = Field::new(
        "key_authorization",
        DataType::Struct(Fields::from(vec![
            Field::new("chain_id", DataType::UInt64, false),
            Field::new("key_type", DataType::Utf8, false),
            Field::new("key_id", ADDRESS_TYPE, false),
            Field::new("expiry", DataType::UInt64, true),
            Field::new(
                "limits",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("token", ADDRESS_TYPE, false),
                        Field::new("limit", DataType::Utf8, false),
                    ])),
                    false,
                ))),
                true, // null means unlimited spending
            ),
            Field::new("r", BYTES32_TYPE, false),
            Field::new("s", BYTES32_TYPE, false),
            Field::new("y_parity", DataType::Boolean, false),
        ])),
        true, // null when not present
    );
    let aa_authorization_list = Field::new(
        "aa_authorization_list",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("chain_id", DataType::UInt64, false),
                Field::new("address", ADDRESS_TYPE, false),
                Field::new("nonce", DataType::UInt64, false),
                Field::new("r", BYTES32_TYPE, false),
                Field::new("s", BYTES32_TYPE, false),
                Field::new("y_parity", DataType::Boolean, false),
            ])),
            false,
        ))),
        true, // null when empty
    );
    let valid_before = Field::new("valid_before", DataType::UInt64, true);
    let valid_after = Field::new("valid_after", DataType::UInt64, true);

    let access_list = Field::new(
        "access_list",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("address", ADDRESS_TYPE, false),
                Field::new(
                    "storage_keys",
                    DataType::List(Arc::new(Field::new("item", BYTES32_TYPE, false))),
                    false,
                ),
            ])),
            false,
        ))),
        true,
    );

    let fields = vec![
        special_block_num,
        block_hash,
        block_num,
        timestamp,
        tx_index,
        tx_hash,
        to,
        from,
        nonce,
        chain_id,
        gas_limit,
        gas_used,
        r#type,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        gas_price,
        status,
        value,
        input,
        r,
        s,
        v_parity,
        fee_token,
        nonce_key,
        calls,
        fee_payer_signature,
        key_authorization,
        aa_authorization_list,
        valid_before,
        valid_after,
        access_list,
    ];

    Schema::new(fields)
}

#[derive(Debug, Default)]
pub struct Transaction {
    pub block_hash: Bytes32,
    pub block_num: u64,
    pub timestamp: Timestamp,
    pub tx_index: u32,
    pub tx_hash: Bytes32,

    pub to: Option<Address>,
    pub from: Address,
    pub nonce: u64,
    pub chain_id: u64,

    pub gas_limit: u64,
    pub gas_used: u64,

    pub r#type: i32,
    pub max_fee_per_gas: EvmCurrency,
    pub max_priority_fee_per_gas: EvmCurrency,
    pub gas_price: Option<EvmCurrency>,

    pub status: bool,

    /// String representation of the total value transferred. None for type 0x76 (see `calls`).
    pub value: Option<String>,

    /// Input data. None for type 0x76 (see `calls`).
    pub input: Option<Vec<u8>>,

    // Signature fields.
    pub r: Bytes32,
    pub s: Bytes32,
    pub v_parity: bool,

    // Tempo-specific fields
    /// TIP-20 fee token address. None means native token.
    pub fee_token: Option<Address>,

    /// 2D nonce key (U256 stored as 32 bytes).
    pub nonce_key: Bytes32,

    /// Batched calls in this transaction. Each call has a to, value, and input.
    pub calls: Vec<Call>,

    /// Fee payer signature. None when not sponsored.
    pub fee_payer_signature: Option<FeePayerSignature>,

    /// Key authorization for provisioning a new access key. None when not present.
    pub key_authorization: Option<KeyAuthorizationRow>,

    /// EIP-7702 style authorization list with Tempo signatures.
    pub aa_authorization_list: Option<Vec<AAAuthorizationRow>>,

    /// Transaction is invalid after this timestamp.
    pub valid_before: Option<u64>,

    /// Transaction is invalid before this timestamp.
    pub valid_after: Option<u64>,

    /// EIP-2930 / EIP-1559 access list.
    pub access_list: Option<Vec<AccessListTuple>>,
}

/// EIP-2930 / EIP-1559 access list tuple: (address, storage_keys)
type AccessListTuple = (Address, Vec<[u8; 32]>);

/// A single call within a Tempo batched transaction.
#[derive(Debug, Default)]
pub struct Call {
    /// Target address. None for contract creation.
    pub to: Option<Address>,
    /// Value transferred (string representation).
    pub value: String,
    /// Calldata.
    pub input: Vec<u8>,
}

/// Fee payer signature for sponsored Tempo transactions.
#[derive(Debug, Default)]
pub struct FeePayerSignature {
    pub r: Bytes32,
    pub s: Bytes32,
    pub y_parity: bool,
}

/// Cryptographic key type for Tempo access key authorization.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    #[default]
    Secp256k1,
    P256,
    WebAuthn,
}

impl KeyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Secp256k1 => "secp256k1",
            Self::P256 => "p256",
            Self::WebAuthn => "webAuthn",
        }
    }
}

/// Key authorization for provisioning access keys in Tempo transactions.
#[derive(Debug, Default)]
pub struct KeyAuthorizationRow {
    pub chain_id: u64,
    pub key_type: KeyType,
    pub key_id: Address,
    pub expiry: Option<u64>,
    /// Token spending limits. None means unlimited.
    pub limits: Option<Vec<TokenLimitRow>>,
    pub r: Bytes32,
    pub s: Bytes32,
    pub y_parity: bool,
}

/// Token spending limit for an access key.
#[derive(Debug, Default)]
pub struct TokenLimitRow {
    pub token: Address,
    /// String representation of the limit amount (U256).
    pub limit: String,
}

/// EIP-7702 style authorization with Tempo signature.
#[derive(Debug, Default)]
pub struct AAAuthorizationRow {
    pub chain_id: u64,
    pub address: Address,
    pub nonce: u64,
    pub r: Bytes32,
    pub s: Bytes32,
    pub y_parity: bool,
}

pub struct TransactionRowsBuilder {
    special_block_num: UInt64Builder,
    block_hash: Bytes32ArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    tx_index: UInt32Builder,
    tx_hash: Bytes32ArrayBuilder,
    to: EvmAddressArrayBuilder,
    from: EvmAddressArrayBuilder,
    nonce: UInt64Builder,
    chain_id: UInt64Builder,
    gas_limit: UInt64Builder,
    gas_used: UInt64Builder,
    r#type: Int32Builder,
    max_fee_per_gas: EvmCurrencyArrayBuilder,
    max_priority_fee_per_gas: EvmCurrencyArrayBuilder,
    gas_price: EvmCurrencyArrayBuilder,
    status: BooleanBuilder,
    value: StringBuilder,
    input: BinaryBuilder,
    r: Bytes32ArrayBuilder,
    s: Bytes32ArrayBuilder,
    v_parity: BooleanBuilder,
    fee_token: EvmAddressArrayBuilder,
    nonce_key: Bytes32ArrayBuilder,
    calls: ListBuilder<StructBuilder>,
    fee_payer_signature: StructBuilder,
    key_authorization: StructBuilder,
    aa_authorization_list: ListBuilder<StructBuilder>,
    valid_before: UInt64Builder,
    valid_after: UInt64Builder,
    access_list: ListBuilder<StructBuilder>,
}

impl TransactionRowsBuilder {
    pub fn with_capacity(count: usize, total_input_size: usize) -> Self {
        let calls_fields = Fields::from(vec![
            Field::new("to", ADDRESS_TYPE, true),
            Field::new("value", DataType::Utf8, false),
            Field::new("input", DataType::Binary, false),
        ]);
        let access_list_fields = Fields::from(vec![
            Field::new("address", ADDRESS_TYPE, false),
            Field::new(
                "storage_keys",
                DataType::List(Arc::new(Field::new("item", BYTES32_TYPE, false))),
                false,
            ),
        ]);
        Self {
            special_block_num: UInt64Builder::with_capacity(count),
            block_hash: Bytes32ArrayBuilder::with_capacity(count),
            block_num: UInt64Builder::with_capacity(count),
            timestamp: TimestampArrayBuilder::with_capacity(count),
            tx_index: UInt32Builder::with_capacity(count),
            tx_hash: Bytes32ArrayBuilder::with_capacity(count),
            to: EvmAddressArrayBuilder::with_capacity(count),
            from: EvmAddressArrayBuilder::with_capacity(count),
            nonce: UInt64Builder::with_capacity(count),
            chain_id: UInt64Builder::with_capacity(count),
            gas_limit: UInt64Builder::with_capacity(count),
            gas_used: UInt64Builder::with_capacity(count),
            r#type: Int32Builder::with_capacity(count),
            max_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(count),
            max_priority_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(count),
            gas_price: EvmCurrencyArrayBuilder::with_capacity(count),
            status: BooleanBuilder::with_capacity(count),
            value: StringBuilder::new(),
            input: BinaryBuilder::with_capacity(count, total_input_size),
            r: Bytes32ArrayBuilder::with_capacity(count),
            s: Bytes32ArrayBuilder::with_capacity(count),
            v_parity: BooleanBuilder::with_capacity(count),
            fee_token: EvmAddressArrayBuilder::with_capacity(count),
            nonce_key: Bytes32ArrayBuilder::with_capacity(count),
            calls: ListBuilder::with_capacity(
                StructBuilder::new(
                    calls_fields.clone(),
                    vec![
                        Box::new(FixedSizeBinaryBuilder::with_capacity(0, 20)),
                        Box::new(StringBuilder::new()),
                        Box::new(BinaryBuilder::with_capacity(0, 0)),
                    ],
                ),
                count,
            )
            .with_field(Field::new("item", DataType::Struct(calls_fields), false)),
            fee_payer_signature: StructBuilder::new(
                Fields::from(vec![
                    Field::new("r", BYTES32_TYPE, false),
                    Field::new("s", BYTES32_TYPE, false),
                    Field::new("y_parity", DataType::Boolean, false),
                ]),
                vec![
                    Box::new(FixedSizeBinaryBuilder::with_capacity(0, 32)),
                    Box::new(FixedSizeBinaryBuilder::with_capacity(0, 32)),
                    Box::new(BooleanBuilder::with_capacity(0)),
                ],
            ),
            key_authorization: {
                let limits_fields = Fields::from(vec![
                    Field::new("token", ADDRESS_TYPE, false),
                    Field::new("limit", DataType::Utf8, false),
                ]);
                let ka_fields = Fields::from(vec![
                    Field::new("chain_id", DataType::UInt64, false),
                    Field::new("key_type", DataType::Utf8, false),
                    Field::new("key_id", ADDRESS_TYPE, false),
                    Field::new("expiry", DataType::UInt64, true),
                    Field::new(
                        "limits",
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::Struct(limits_fields.clone()),
                            false,
                        ))),
                        true,
                    ),
                    Field::new("r", BYTES32_TYPE, false),
                    Field::new("s", BYTES32_TYPE, false),
                    Field::new("y_parity", DataType::Boolean, false),
                ]);
                StructBuilder::new(
                    ka_fields,
                    vec![
                        Box::new(UInt64Builder::with_capacity(0)),
                        Box::new(StringBuilder::new()),
                        Box::new(FixedSizeBinaryBuilder::with_capacity(0, 20)),
                        Box::new(UInt64Builder::with_capacity(0)),
                        Box::new(
                            ListBuilder::with_capacity(
                                StructBuilder::new(
                                    limits_fields.clone(),
                                    vec![
                                        Box::new(FixedSizeBinaryBuilder::with_capacity(0, 20)),
                                        Box::new(StringBuilder::new()),
                                    ],
                                ),
                                0,
                            )
                            .with_field(Field::new(
                                "item",
                                DataType::Struct(limits_fields),
                                false,
                            )),
                        ),
                        Box::new(FixedSizeBinaryBuilder::with_capacity(0, 32)),
                        Box::new(FixedSizeBinaryBuilder::with_capacity(0, 32)),
                        Box::new(BooleanBuilder::with_capacity(0)),
                    ],
                )
            },
            aa_authorization_list: {
                let aa_fields = Fields::from(vec![
                    Field::new("chain_id", DataType::UInt64, false),
                    Field::new("address", ADDRESS_TYPE, false),
                    Field::new("nonce", DataType::UInt64, false),
                    Field::new("r", BYTES32_TYPE, false),
                    Field::new("s", BYTES32_TYPE, false),
                    Field::new("y_parity", DataType::Boolean, false),
                ]);
                ListBuilder::with_capacity(
                    StructBuilder::new(
                        aa_fields.clone(),
                        vec![
                            Box::new(UInt64Builder::with_capacity(0)),
                            Box::new(FixedSizeBinaryBuilder::with_capacity(0, 20)),
                            Box::new(UInt64Builder::with_capacity(0)),
                            Box::new(FixedSizeBinaryBuilder::with_capacity(0, 32)),
                            Box::new(FixedSizeBinaryBuilder::with_capacity(0, 32)),
                            Box::new(BooleanBuilder::with_capacity(0)),
                        ],
                    ),
                    count,
                )
                .with_field(Field::new("item", DataType::Struct(aa_fields), false))
            },
            valid_before: UInt64Builder::with_capacity(count),
            valid_after: UInt64Builder::with_capacity(count),
            access_list: {
                ListBuilder::with_capacity(
                    StructBuilder::new(
                        access_list_fields.clone(),
                        vec![
                            Box::new(FixedSizeBinaryBuilder::with_capacity(0, 20)),
                            Box::new(
                                ListBuilder::new(FixedSizeBinaryBuilder::with_capacity(0, 32))
                                    .with_field(Field::new("item", BYTES32_TYPE, false)),
                            ),
                        ],
                    ),
                    count,
                )
                .with_field(Field::new(
                    "item",
                    DataType::Struct(access_list_fields),
                    false,
                ))
            },
        }
    }

    pub fn append(&mut self, tx: &Transaction) {
        let Transaction {
            block_hash,
            block_num,
            timestamp,
            tx_index,
            tx_hash,
            to,
            from,
            nonce,
            chain_id,
            gas_limit,
            gas_used,
            r#type,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            gas_price,
            status,
            value,
            input,
            r,
            s,
            v_parity,
            fee_token,
            nonce_key,
            calls,
            fee_payer_signature,
            key_authorization,
            aa_authorization_list,
            valid_before,
            valid_after,
            access_list,
        } = tx;

        self.special_block_num.append_value(*block_num);
        self.block_hash.append_value(*block_hash);
        self.block_num.append_value(*block_num);
        self.timestamp.append_value(*timestamp);
        self.tx_index.append_value(*tx_index);
        self.tx_hash.append_value(*tx_hash);
        self.to.append_option(*to);
        self.from.append_value(*from);
        self.nonce.append_value(*nonce);
        self.chain_id.append_value(*chain_id);
        self.gas_limit.append_value(*gas_limit);
        self.gas_used.append_value(*gas_used);
        self.r#type.append_value(*r#type);
        self.max_fee_per_gas.append_value(*max_fee_per_gas);
        self.max_priority_fee_per_gas
            .append_value(*max_priority_fee_per_gas);
        self.gas_price.append_option(*gas_price);
        self.status.append_value(*status);
        match value {
            Some(v) => self.value.append_value(v),
            None => self.value.append_null(),
        }
        match input {
            Some(i) => self.input.append_value(i),
            None => self.input.append_null(),
        }
        self.r.append_value(*r);
        self.s.append_value(*s);
        self.v_parity.append_value(*v_parity);
        self.fee_token.append_option(*fee_token);
        self.nonce_key.append_value(*nonce_key);
        self.append_calls(calls);
        self.append_fee_payer_signature(fee_payer_signature.as_ref());
        self.append_key_authorization(key_authorization.as_ref());
        self.append_aa_authorization_list(aa_authorization_list.as_deref());
        self.valid_before.append_option(*valid_before);
        self.valid_after.append_option(*valid_after);
        self.append_access_list(access_list.as_deref());
    }

    fn append_calls(&mut self, calls: &[Call]) {
        for call in calls {
            let struct_builder = self.calls.values();

            // Field 0: to (nullable address)
            let to_builder = struct_builder
                .field_builder::<FixedSizeBinaryBuilder>(0)
                .unwrap();
            match call.to {
                Some(addr) => to_builder.append_value(addr).unwrap(),
                None => to_builder.append_null(),
            }

            // Field 1: value
            struct_builder
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&call.value);

            // Field 2: input
            struct_builder
                .field_builder::<BinaryBuilder>(2)
                .unwrap()
                .append_value(&call.input);

            struct_builder.append(true);
        }
        self.calls.append(true);
    }

    fn append_fee_payer_signature(&mut self, fps: Option<&FeePayerSignature>) {
        if let Some(fps) = fps {
            self.fee_payer_signature
                .field_builder::<FixedSizeBinaryBuilder>(0)
                .unwrap()
                .append_value(fps.r)
                .unwrap();
            self.fee_payer_signature
                .field_builder::<FixedSizeBinaryBuilder>(1)
                .unwrap()
                .append_value(fps.s)
                .unwrap();
            self.fee_payer_signature
                .field_builder::<BooleanBuilder>(2)
                .unwrap()
                .append_value(fps.y_parity);
            self.fee_payer_signature.append(true);
        } else {
            self.fee_payer_signature
                .field_builder::<FixedSizeBinaryBuilder>(0)
                .unwrap()
                .append_null();
            self.fee_payer_signature
                .field_builder::<FixedSizeBinaryBuilder>(1)
                .unwrap()
                .append_null();
            self.fee_payer_signature
                .field_builder::<BooleanBuilder>(2)
                .unwrap()
                .append_null();
            self.fee_payer_signature.append_null();
        }
    }

    fn append_key_authorization(&mut self, ka: Option<&KeyAuthorizationRow>) {
        if let Some(ka) = ka {
            self.key_authorization
                .field_builder::<UInt64Builder>(0)
                .unwrap()
                .append_value(ka.chain_id);
            self.key_authorization
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(ka.key_type.as_str());
            self.key_authorization
                .field_builder::<FixedSizeBinaryBuilder>(2)
                .unwrap()
                .append_value(ka.key_id)
                .unwrap();
            self.key_authorization
                .field_builder::<UInt64Builder>(3)
                .unwrap()
                .append_option(ka.expiry);
            // Field 4: limits (nullable list)
            let limits_builder = self
                .key_authorization
                .field_builder::<ListBuilder<StructBuilder>>(4)
                .unwrap();
            if let Some(limits) = &ka.limits {
                for tl in limits {
                    let sb = limits_builder.values();
                    sb.field_builder::<FixedSizeBinaryBuilder>(0)
                        .unwrap()
                        .append_value(tl.token)
                        .unwrap();
                    sb.field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_value(&tl.limit);
                    sb.append(true);
                }
                limits_builder.append(true);
            } else {
                limits_builder.append(false);
            }
            self.key_authorization
                .field_builder::<FixedSizeBinaryBuilder>(5)
                .unwrap()
                .append_value(ka.r)
                .unwrap();
            self.key_authorization
                .field_builder::<FixedSizeBinaryBuilder>(6)
                .unwrap()
                .append_value(ka.s)
                .unwrap();
            self.key_authorization
                .field_builder::<BooleanBuilder>(7)
                .unwrap()
                .append_value(ka.y_parity);
            self.key_authorization.append(true);
        } else {
            // Struct is null — append default values for non-nullable children.
            self.key_authorization
                .field_builder::<UInt64Builder>(0)
                .unwrap()
                .append_value(0);
            self.key_authorization
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value("");
            self.key_authorization
                .field_builder::<FixedSizeBinaryBuilder>(2)
                .unwrap()
                .append_value([0u8; 20])
                .unwrap();
            self.key_authorization
                .field_builder::<UInt64Builder>(3)
                .unwrap()
                .append_null();
            self.key_authorization
                .field_builder::<ListBuilder<StructBuilder>>(4)
                .unwrap()
                .append(false);
            self.key_authorization
                .field_builder::<FixedSizeBinaryBuilder>(5)
                .unwrap()
                .append_value([0u8; 32])
                .unwrap();
            self.key_authorization
                .field_builder::<FixedSizeBinaryBuilder>(6)
                .unwrap()
                .append_value([0u8; 32])
                .unwrap();
            self.key_authorization
                .field_builder::<BooleanBuilder>(7)
                .unwrap()
                .append_value(false);
            self.key_authorization.append_null();
        }
    }

    fn append_aa_authorization_list(&mut self, aa_list: Option<&[AAAuthorizationRow]>) {
        if let Some(aa_list) = aa_list {
            for aa in aa_list {
                let sb = self.aa_authorization_list.values();
                sb.field_builder::<UInt64Builder>(0)
                    .unwrap()
                    .append_value(aa.chain_id);
                sb.field_builder::<FixedSizeBinaryBuilder>(1)
                    .unwrap()
                    .append_value(aa.address)
                    .unwrap();
                sb.field_builder::<UInt64Builder>(2)
                    .unwrap()
                    .append_value(aa.nonce);
                sb.field_builder::<FixedSizeBinaryBuilder>(3)
                    .unwrap()
                    .append_value(aa.r)
                    .unwrap();
                sb.field_builder::<FixedSizeBinaryBuilder>(4)
                    .unwrap()
                    .append_value(aa.s)
                    .unwrap();
                sb.field_builder::<BooleanBuilder>(5)
                    .unwrap()
                    .append_value(aa.y_parity);
                sb.append(true);
            }
            self.aa_authorization_list.append(true);
        } else {
            self.aa_authorization_list.append(false);
        }
    }

    fn append_access_list(&mut self, access_list: Option<&[AccessListTuple]>) {
        if let Some(access_list) = access_list {
            for (address, storage_keys) in access_list {
                let struct_builder = self.access_list.values();

                struct_builder
                    .field_builder::<FixedSizeBinaryBuilder>(0)
                    .unwrap()
                    .append_value(address)
                    .unwrap();

                let storage_keys_builder = struct_builder
                    .field_builder::<ListBuilder<FixedSizeBinaryBuilder>>(1)
                    .unwrap();
                for key in storage_keys {
                    storage_keys_builder.values().append_value(key).unwrap();
                }
                storage_keys_builder.append(true);

                struct_builder.append(true);
            }
            self.access_list.append(true);
        } else {
            self.access_list.append(false);
        }
    }

    pub fn build(self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let Self {
            mut special_block_num,
            block_hash,
            mut block_num,
            mut timestamp,
            mut tx_index,
            tx_hash,
            to,
            from,
            mut nonce,
            mut chain_id,
            mut gas_limit,
            mut gas_used,
            mut r#type,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            gas_price,
            mut status,
            mut value,
            mut input,
            r,
            s,
            mut v_parity,
            fee_token,
            nonce_key,
            mut calls,
            mut fee_payer_signature,
            mut key_authorization,
            mut aa_authorization_list,
            mut valid_before,
            mut valid_after,
            mut access_list,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(block_hash.finish()),
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(tx_hash.finish()),
            Arc::new(to.finish()),
            Arc::new(from.finish()),
            Arc::new(nonce.finish()),
            Arc::new(chain_id.finish()),
            Arc::new(gas_limit.finish()),
            Arc::new(gas_used.finish()),
            Arc::new(r#type.finish()),
            Arc::new(max_fee_per_gas.finish()),
            Arc::new(max_priority_fee_per_gas.finish()),
            Arc::new(gas_price.finish()),
            Arc::new(status.finish()),
            Arc::new(value.finish()),
            Arc::new(input.finish()),
            Arc::new(r.finish()),
            Arc::new(s.finish()),
            Arc::new(v_parity.finish()),
            Arc::new(fee_token.finish()),
            Arc::new(nonce_key.finish()),
            Arc::new(calls.finish()),
            Arc::new(fee_payer_signature.finish()),
            Arc::new(key_authorization.finish()),
            Arc::new(aa_authorization_list.finish()),
            Arc::new(valid_before.finish()),
            Arc::new(valid_after.finish()),
            Arc::new(access_list.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}

#[test]
fn default_to_arrow() {
    let tx = Transaction::default();
    let rows = {
        let mut builder =
            TransactionRowsBuilder::with_capacity(1, tx.input.as_ref().map_or(0, |i| i.len()));
        builder.append(&tx);
        builder
            .build(BlockRange {
                numbers: tx.block_num..=tx.block_num,
                network: "test_network".parse().expect("valid network id"),
                hash: tx.block_hash.into(),
                prev_hash: Default::default(),
                timestamp: None,
            })
            .unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 31);
    assert_eq!(rows.rows.num_rows(), 1);
}
