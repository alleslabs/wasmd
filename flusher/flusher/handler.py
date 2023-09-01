from sqlalchemy import select, Numeric
from sqlalchemy.dialects.postgresql import insert
from base64 import b64encode

from flusher.db import (
    accounts,
    blocks,
    transactions,
    account_transactions,
    codes,
    contracts,
    contract_transactions,
    proposals,
    proposal_deposits,
    proposal_votes,
    code_proposals,
    contract_proposals,
    contract_histories,
    contract_transactions_view,
    lcd_tx_results,
    validators,
)


class Handler(object):
    def __init__(self, conn):
        self.conn = conn

    def get_transaction_id(self, tx_hash):
        return self.conn.execute(select([transactions.c.id]).where(transactions.c.hash == tx_hash)).scalar()

    def get_account_address(self, account_id):
        if account_id is None:
            return None
        return self.conn.execute(select([accounts.c.address]).where(accounts.c.id == account_id)).scalar()

    def get_account_id(self, address):
        if address is None:
            return None
        id = self.conn.execute(select([accounts.c.id]).where(accounts.c.address == address)).scalar()
        if id is None:
            self.conn.execute(accounts.insert(), {"address": address})
            return self.conn.execute(select([accounts.c.id]).where(accounts.c.address == address)).scalar()
        return id

    def get_validator_id(self, val):
        return self.conn.execute(select([validators.c.id]).where(validators.c.operator_address == val)).scalar()

    def get_transaction_data(self, tx_hash):
        tx_detail = dict(
            self.conn.execute(
                select(
                    [
                        transactions.c.hash,
                        transactions.c.success,
                        transactions.c.messages,
                        transactions.c.is_execute,
                        transactions.c.is_ibc,
                        transactions.c.is_instantiate,
                        transactions.c.is_send,
                        transactions.c.is_store_code,
                        transactions.c.is_migrate,
                        transactions.c.is_update_admin,
                        transactions.c.is_clear_admin,
                        transactions.c.sender,
                        transactions.c.block_height,
                    ]
                )
                .where(transactions.c.hash == tx_hash)
                .limit(1)
            ).fetchall()[0]
        )
        tx_detail["sender"] = self.get_account_address(tx_detail["sender"])

        tx_detail["timestamp"] = (
            self.conn.execute(select([blocks.c.timestamp]).where(blocks.c.height == tx_detail["block_height"]))
            .scalar()
            .timestamp()
            * 1e9
        )

        tx_detail["height"] = tx_detail["block_height"]
        tx_detail["hash"] = b64encode(bytes.fromhex(tx_detail["hash"].hex())).decode()
        del tx_detail["block_height"]

        return tx_detail

    def get_contract_id(self, address):
        return self.conn.execute(select([contracts.c.id]).where(contracts.c.address == address)).scalar()

    def get_transaction_success_by_id(self, tx_id):
        return self.conn.execute(select([transactions.c.success]).where(transactions.c.id == tx_id)).scalar()

    def handle_new_block(self, msg):
        self.conn.execute(blocks.insert(), msg)

    def handle_new_transaction(self, msg):
        msg["memo"] = msg["memo"].replace("\x00", "\uFFFD")
        msg["sender"] = self.get_account_id(msg["sender"])
        self.conn.execute(
            insert(transactions).values(**msg).on_conflict_do_update(constraint="transactions_pkey", set_=msg)
        )

    def handle_set_related_transaction(self, msg):
        tx_id = self.get_transaction_id(msg["hash"])
        related_tx_accounts = msg["related_accounts"]
        for account in related_tx_accounts:
            self.conn.execute(
                insert(account_transactions)
                .values(
                    {
                        "transaction_id": tx_id,
                        "account_id": self.get_account_id(account),
                        "block_height": msg["block_height"],
                        "is_signer": account in msg["signer"],
                    }
                )
                .on_conflict_do_nothing(constraint="account_transactions_pkey")
            )

    def handle_new_code(self, msg):
        if "tx_hash" in msg and msg["tx_hash"] is not None:
            msg["transaction_id"] = self.get_transaction_id(msg["tx_hash"])
            del msg["tx_hash"]
        else:
            msg["transaction_id"] = None

        msg["uploader"] = self.get_account_id(msg["uploader"])
        self.conn.execute(codes.insert(), msg)

    def handle_set_account(self, msg):
        id = self.conn.execute(select([accounts.c.id]).where(accounts.c.address == msg["address"])).scalar()
        if id is None:
            self.conn.execute(accounts.insert(), msg)
        else:
            msg["id"] = id
            self.conn.execute(accounts.update(accounts.c.id == msg["id"]).values(**msg))

    def handle_update_code(self, msg):
        self.conn.execute(
            codes.update(codes.c.id == msg["id"]).values(contract_instantiated=codes.c.contract_instantiated + 1)
        )

    def handle_update_contract(self, id):
        self.conn.execute(
            contracts.update(contracts.c.id == id).values(contract_executed=contracts.c.contract_executed + 1)
        )

    def handle_new_contract(self, msg):
        if "tx_hash" in msg and msg["tx_hash"] is not None:
            msg["init_tx_id"] = self.get_transaction_id(msg["tx_hash"])
            del msg["tx_hash"]
        else:
            msg["init_tx_id"] = None

        msg["init_by"] = self.get_account_id(msg["init_by"])
        if msg["admin"] != "":
            msg["admin"] = self.get_account_id(msg["admin"])
        else:
            del msg["admin"]
        self.conn.execute(contracts.insert(), msg)

    def handle_new_contract_transaction(self, msg):
        if msg["tx_hash"] is not None:
            msg["tx_id"] = self.get_transaction_id(msg["tx_hash"])

        transaction_view = self.get_transaction_data(msg["tx_hash"])
        del msg["tx_hash"]

        transaction_view["contract_address"] = msg["contract_address"]
        msg["contract_id"] = self.get_contract_id(msg["contract_address"])
        del msg["contract_address"]
        if not msg["contract_id"] and not self.get_transaction_success_by_id(msg["tx_id"]):
            return

        if not msg["is_instantiate"]:
            self.handle_update_contract(msg["contract_id"])
        del msg["is_instantiate"]
        self.conn.execute(contract_transactions.insert(), msg)
        self.conn.execute(contract_transactions_view.insert(), transaction_view)

    def handle_update_contract_admin(self, msg):
        msg["address"] = msg["contract"]
        del msg["contract"]
        if msg["admin"] != "":
            msg["admin"] = self.get_account_id(msg["admin"])
        else:
            msg["admin"] = None
        self.conn.execute(contracts.update().where(contracts.c.address == msg["address"]).values(**msg))

    def handle_update_contract_code_id(self, msg):
        msg["address"] = msg["contract"]
        del msg["contract"]
        self.conn.execute(contracts.update().where(contracts.c.address == msg["address"]).values(**msg))

    def handle_new_proposal(self, msg):
        msg["proposer_id"] = self.get_account_id(msg["proposer"])
        del msg["proposer"]
        self.conn.execute(proposals.insert(), msg)

    def handle_update_proposal(self, msg):
        condition = True
        for col in proposals.primary_key.columns.values():
            condition = (col == msg[col.name]) & condition
        self.conn.execute(proposals.update().where(condition).values(**msg))

    def handle_new_proposal_deposit(self, msg):
        msg["transaction_id"] = self.get_transaction_id(msg["tx_hash"])
        del msg["tx_hash"]
        msg["depositor"] = self.get_account_id(msg["depositor"])
        self.conn.execute(proposal_deposits.insert(), msg)

    def handle_new_proposal_vote(self, msg):
        msg["transaction_id"] = self.get_transaction_id(msg["tx_hash"])
        del msg["tx_hash"]
        msg["voter"] = self.get_account_id(msg["voter"])
        self.conn.execute(proposal_votes.insert(), msg)

    def handle_new_code_proposal(self, msg):
        self.conn.execute(code_proposals.insert(), msg)

    def handle_new_contract_proposal(self, msg):
        msg["contract_id"] = self.get_contract_id(msg["contract_address"])
        del msg["contract_address"]
        self.conn.execute(contract_proposals.insert(), msg)

    def handle_update_contract_proposal(self, msg):
        msg["contract_id"] = self.get_contract_id(msg["contract_address"])
        del msg["contract_address"]
        self.conn.execute(
            contract_proposals.update()
            .where(
                (contract_proposals.c.contract_id == msg["contract_id"])
                & (contract_proposals.c.proposal_id == msg["proposal_id"])
            )
            .values(**msg)
        )

    def handle_new_contract_history(self, msg):
        msg["contract_id"] = self.get_contract_id(msg["contract_address"])
        del msg["contract_address"]
        msg["sender"] = self.get_account_id(msg["sender"])
        self.conn.execute(contract_histories.insert(), msg)

    def handle_update_cw2_info(self, msg):
        msg["id"] = msg["code_id"]
        del msg["code_id"]
        self.conn.execute(codes.update().where(codes.c.id == msg["id"]).values(**msg))

    def handle_insert_lcd_tx_results(self, msg):
        if "tx_hash" in msg and msg["tx_hash"] is not None:
            msg["transaction_id"] = self.get_transaction_id(msg["tx_hash"])
            del msg["tx_hash"]
        else:
            msg["transaction_id"] = None
        self.conn.execute(lcd_tx_results.insert(), msg)

    def handle_set_validator(self, msg):
        msg["account_id"] = self.get_account_id(msg["delegator_address"])
        del msg["delegator_address"]
        if self.get_validator_id(msg["operator_address"]) is None:
            self.conn.execute(validators.insert(), msg)
        else:
            condition = True
            for col in validators.primary_key.columns.values():
                condition = (col == msg[col.name]) & condition
            self.conn.execute(validators.update().where(condition).values(**msg))

    def handle_update_validator(self, msg):
        self.conn.execute(
            validators.update().where(validators.c.operator_address == msg["operator_address"]).values(**msg)
        )

