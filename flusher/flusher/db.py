import base64 as b64
from datetime import datetime, date
import sqlalchemy as sa
import enum


class AccountType(enum.Enum):
    """Account types enum based on Cosmos SDK accounts"""

    BaseAccount = 0
    InterchainAccount = 1
    ModuleAccount = 2
    ContinuousVestingAccount = 3
    DelayedVestingAccount = 4
    ClawbackVestingAccount = 5
    ContractAccount = 6
    PeriodicVestingAccount = 7
    PermanentLockedAccount = 8
    BaseVestingAccount = 9


class ProposalStatus(enum.Enum):
    """Proposal statuses enum based on Cosmos SDK governance proposal"""

    Nil = 0
    DepositPeriod = 1
    VotingPeriod = 2
    Passed = 3
    Rejected = 4
    Failed = 5
    Inactive = 6


class CustomAccountType(sa.types.TypeDecorator):
    impl = sa.Enum(AccountType)

    def process_bind_param(self, value, dialect):
        return AccountType(value)


class CustomProposalStatus(sa.types.TypeDecorator):
    impl = sa.Enum(ProposalStatus)

    def process_bind_param(self, value, dialect):
        return ProposalStatus(value)


class CustomDateTime(sa.types.TypeDecorator):
    """Custom DateTime type that accepts Python nanosecond epoch int."""

    impl = sa.DateTime

    def process_bind_param(self, value, dialect):
        return datetime.fromtimestamp(value / 1e9)


class CustomBase64(sa.types.TypeDecorator):
    """Custom LargeBinary type that accepts base64-encoded string."""

    impl = sa.LargeBinary

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        return b64.decodebytes(value.encode())


class CustomDate(sa.types.TypeDecorator):
    """Custom Date type that accepts Python nanosecond epoch int."""

    impl = sa.Date

    def process_bind_param(self, value, dialect):
        dt = datetime.fromtimestamp(value / 1e9)
        return date(dt.year, dt.month, dt.day)


def Column(*args, **kwargs):
    """Forward into SQLAlchemy's Column construct, but with 'nullable' default to False."""
    if "nullable" not in kwargs:
        kwargs["nullable"] = False
    return sa.Column(*args, **kwargs)


metadata = sa.MetaData()

tracking = sa.Table(
    "tracking",
    metadata,
    Column("chain_id", sa.String, primary_key=True),
    Column("topic", sa.String),
    Column("kafka_offset", sa.Integer),
    Column("replay_topic", sa.String),
    Column("replay_offset", sa.Integer),
)

blocks = sa.Table(
    "blocks",
    metadata,
    Column("height", sa.Integer, primary_key=True),
    Column("timestamp", CustomDateTime, index=True),
    Column("proposer", sa.String, sa.ForeignKey("validators.operator_address"), nullable=True),
    Column("hash", CustomBase64),
)

transactions = sa.Table(
    "transactions",
    metadata,
    Column("id", sa.Integer, sa.Sequence("seq_transaction_id"), unique=True),
    Column("hash", CustomBase64, primary_key=True),
    Column("block_height", sa.Integer, sa.ForeignKey("blocks.height"), index=True, primary_key=True,),
    Column("gas_used", sa.Integer),
    Column("gas_limit", sa.Integer),
    Column("gas_fee", sa.String),
    Column("err_msg", sa.String, nullable=True),
    Column("success", sa.Boolean),
    Column("sender", sa.Integer, sa.ForeignKey("accounts.id")),
    Column("memo", sa.String),
    Column("messages", sa.JSON),
    Column("is_ibc", sa.Boolean),
    Column("is_store_code", sa.Boolean),
    Column("is_instantiate", sa.Boolean),
    Column("is_execute", sa.Boolean),
    Column("is_send", sa.Boolean),
    Column("is_update_admin", sa.Boolean),
    Column("is_clear_admin", sa.Boolean),
    Column("is_migrate", sa.Boolean),
)

accounts = sa.Table(
    "accounts",
    metadata,
    Column("id", sa.Integer, sa.Sequence("seq_account_id"), unique=True),
    Column("address", sa.String, primary_key=True),
    Column("type", CustomAccountType, nullable=True),
    Column("name", sa.String, nullable=True),
)

account_transactions = sa.Table(
    "account_transactions",
    metadata,
    Column("transaction_id", sa.Integer, sa.ForeignKey("transactions.id"), primary_key=True),
    Column("account_id", sa.Integer, sa.ForeignKey("accounts.id"), primary_key=True),
    Column("is_signer", sa.Boolean),
    Column("block_height", sa.Integer, sa.ForeignKey("blocks.height")),
)

codes = sa.Table(
    "codes",
    metadata,
    Column("id", sa.Integer, primary_key=True, unique=True),
    Column("uploader", sa.Integer, sa.ForeignKey("accounts.id")),
    Column("contract_instantiated", sa.Integer),
    Column("transaction_id", sa.Integer, sa.ForeignKey("transactions.id"), nullable=True),
    Column("access_config_permission", sa.String),
    Column("access_config_addresses", sa.JSON),
    Column("cw2_contract", sa.String, nullable=True),
    Column("cw2_version", sa.String, nullable=True),
)

contracts = sa.Table(
    "contracts",
    metadata,
    Column("id", sa.Integer, sa.Sequence("seq_contract_id"), unique=True),
    Column("code_id", sa.Integer, sa.ForeignKey("codes.id")),
    Column("address", sa.String, primary_key=True),
    Column("label", sa.String),
    Column("admin", sa.Integer, sa.ForeignKey("accounts.id"), nullable=True),
    Column("init_msg", sa.String, nullable=True),
    Column("init_tx_id", sa.Integer, sa.ForeignKey("transactions.id"), nullable=True),
    Column("init_by", sa.Integer, sa.ForeignKey("accounts.id"), nullable=True),
    Column("contract_executed", sa.Integer),
)

contract_transactions = sa.Table(
    "contract_transactions",
    metadata,
    Column("tx_id", sa.Integer, sa.ForeignKey("transactions.id")),
    Column("contract_id", sa.Integer, sa.ForeignKey("contracts.id")),
)

proposals = sa.Table(
    "proposals",
    metadata,
    Column("id", sa.Integer, primary_key=True),
    Column("proposer_id", sa.Integer, sa.ForeignKey("accounts.id"), nullable=True),
    Column("type", sa.String),
    Column("title", sa.String),
    Column("description", sa.String),
    Column("proposal_route", sa.String),
    Column("status", CustomProposalStatus),
    Column("submit_time", CustomDateTime),
    Column("deposit_end_time", CustomDateTime),
    Column("voting_time", CustomDateTime),
    Column("voting_end_time", CustomDateTime),
    Column("content", sa.JSON, nullable=True),
    Column("total_deposit", sa.JSON),
    Column("yes", sa.BigInteger),
    Column("no", sa.BigInteger),
    Column("abstain", sa.BigInteger),
    Column("no_with_veto", sa.BigInteger),
    Column("is_expedited", sa.Boolean),
    Column("version", sa.String),
    Column("resolved_height", sa.Integer, sa.ForeignKey("blocks.height"), index=True, nullable=True,),
)

proposal_deposits = sa.Table(
    "proposal_deposits",
    metadata,
    Column("proposal_id", sa.Integer, sa.ForeignKey("proposals.id")),
    Column("transaction_id", sa.Integer, sa.ForeignKey("transactions.id"), nullable=True),
    Column("depositor", sa.Integer, sa.ForeignKey("accounts.id")),
    Column("amount", sa.JSON),
)

proposal_votes = sa.Table(
    "proposal_votes",
    metadata,
    Column("proposal_id", sa.Integer, sa.ForeignKey("proposals.id")),
    Column("transaction_id", sa.Integer, sa.ForeignKey("transactions.id"), nullable=True),
    Column("voter", sa.Integer, sa.ForeignKey("accounts.id")),
    Column("is_vote_weighted", sa.Boolean),
    Column("is_validator", sa.Boolean),
    Column("validator_address", sa.String, sa.ForeignKey("validators.operator_address"), nullable=True),
    Column("yes", sa.Numeric),
    Column("no", sa.Numeric),
    Column("abstain", sa.Numeric),
    Column("no_with_veto", sa.Numeric),
)

code_proposals = sa.Table(
    "code_proposals",
    metadata,
    Column("code_id", sa.Integer, sa.ForeignKey("codes.id")),
    Column("proposal_id", sa.Integer, sa.ForeignKey("proposals.id")),
    Column("resolved_height", sa.Integer, sa.ForeignKey("blocks.height"), index=True, nullable=True,),
)

contract_proposals = sa.Table(
    "contract_proposals",
    metadata,
    Column("contract_id", sa.Integer, sa.ForeignKey("contracts.id")),
    Column("proposal_id", sa.Integer, sa.ForeignKey("proposals.id")),
    Column("resolved_height", sa.Integer, sa.ForeignKey("blocks.height"), index=True, nullable=True,),
)

contract_histories = sa.Table(
    "contract_histories",
    metadata,
    Column("contract_id", sa.Integer, sa.ForeignKey("contracts.id"), index=True),
    Column("sender", sa.Integer, sa.ForeignKey("accounts.id")),
    Column("code_id", sa.Integer, sa.ForeignKey("codes.id")),
    Column("block_height", sa.Integer, sa.ForeignKey("blocks.height"), index=True),
    Column("remark", sa.JSON),
)

contract_transactions_view = sa.Table(
    "contract_transactions_view",
    metadata,
    Column("hash", CustomBase64),
    Column("success", sa.Boolean),
    Column("messages", sa.JSON),
    Column("sender", sa.String),
    Column("height", sa.Integer),
    Column("timestamp", CustomDateTime, index=True),
    Column("is_execute", sa.Boolean),
    Column("is_ibc", sa.Boolean),
    Column("is_instantiate", sa.Boolean),
    Column("is_send", sa.Boolean),
    Column("is_store_code", sa.Boolean),
    Column("is_migrate", sa.Boolean),
    Column("is_update_admin", sa.Boolean),
    Column("is_clear_admin", sa.Boolean),
    Column("contract_address", sa.String),
)

lcd_tx_results = sa.Table(
    "lcd_tx_results",
    metadata,
    Column("block_height", sa.Integer, sa.ForeignKey("blocks.height"), index=True),
    Column("transaction_id", sa.Integer, sa.ForeignKey("transactions.id"), index=True),
    Column("result", sa.JSON),
)

begin_block_events = sa.Table(
    "begin_block_events",
    metadata,
    Column("block_height", sa.Integer, sa.ForeignKey("blocks.height"), index=True),
    Column("events", sa.JSON),
)

end_block_events = sa.Table(
    "end_block_events",
    metadata,
    Column("block_height", sa.Integer, sa.ForeignKey("blocks.height"), index=True),
    Column("events", sa.JSON),
)

validators = sa.Table(
    "validators",
    metadata,
    Column("id", sa.Integer, sa.Sequence("seq_validator_id"), unique=True),
    Column("account_id", sa.Integer, sa.ForeignKey("accounts.id"), unique=True),
    Column("operator_address", sa.String, primary_key=True),
    Column("consensus_address", sa.String),
    Column("moniker", sa.String),
    Column("identity", sa.String),
    Column("website", sa.String),
    Column("details", sa.String),
    Column("commission_rate", sa.String),
    Column("commission_max_rate", sa.String),
    Column("commission_max_change", sa.String),
    Column("min_self_delegation", sa.String),
    Column("jailed", sa.Boolean),
)
