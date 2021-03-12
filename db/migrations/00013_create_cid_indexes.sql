-- +goose Up
-- header indexes
CREATE INDEX block_number_index ON eth.header_cids USING brin (block_number);

CREATE INDEX block_hash_index ON eth.header_cids USING btree (block_hash);

CREATE INDEX header_cid_index ON eth.header_cids USING btree (cid);

CREATE INDEX header_mh_index ON eth.header_cids USING btree (mh_key);

CREATE INDEX state_root_index ON eth.header_cids USING btree (state_root);

CREATE INDEX timestamp_index ON eth.header_cids USING brin (timestamp);

-- transaction indexes
CREATE INDEX tx_header_id_index ON eth.transaction_cids USING btree (header_id);

CREATE INDEX tx_hash_index ON eth.transaction_cids USING btree (tx_hash);

CREATE INDEX tx_cid_index ON eth.transaction_cids USING btree (cid);

CREATE INDEX tx_mh_index ON eth.transaction_cids USING btree (mh_key);

CREATE INDEX tx_dst_index ON eth.transaction_cids USING btree (dst);

CREATE INDEX tx_src_index ON eth.transaction_cids USING btree (src);

-- receipt indexes
CREATE INDEX rct_tx_id_index ON eth.receipt_cids USING btree (tx_id);

CREATE INDEX rct_cid_index ON eth.receipt_cids USING btree (cid);

CREATE INDEX rct_mh_index ON eth.receipt_cids USING btree (mh_key);

CREATE INDEX rct_contract_index ON eth.receipt_cids USING btree (contract);

CREATE INDEX rct_contract_hash_index ON eth.receipt_cids USING btree (contract_hash);

CREATE INDEX rct_topic0_index ON eth.receipt_cids USING gin (topic0s);

CREATE INDEX rct_topic1_index ON eth.receipt_cids USING gin (topic1s);

CREATE INDEX rct_topic2_index ON eth.receipt_cids USING gin (topic2s);

CREATE INDEX rct_topic3_index ON eth.receipt_cids USING gin (topic3s);

CREATE INDEX rct_log_contract_index ON eth.receipt_cids USING gin (log_contracts);

-- state node indexes
CREATE INDEX state_header_id_index ON eth.state_cids USING btree (header_id);

CREATE INDEX state_leaf_key_index ON eth.state_cids USING btree (state_leaf_key);

CREATE INDEX state_cid_index ON eth.state_cids USING btree (cid);

CREATE INDEX state_mh_index ON eth.state_cids USING btree (mh_key);

CREATE INDEX state_path_index ON eth.state_cids USING btree (state_path);

-- storage node indexes
CREATE INDEX storage_state_id_index ON eth.storage_cids USING btree (state_id);

CREATE INDEX storage_leaf_key_index ON eth.storage_cids USING btree (storage_leaf_key);

CREATE INDEX storage_cid_index ON eth.storage_cids USING btree (cid);

CREATE INDEX storage_mh_index ON eth.storage_cids USING btree (mh_key);

CREATE INDEX storage_path_index ON eth.storage_cids USING btree (storage_path);

-- state accounts indexes
CREATE INDEX account_state_id_index ON eth.state_accounts USING btree (state_id);

CREATE INDEX storage_root_index ON eth.state_accounts USING btree (storage_root);

-- +goose Down
-- state account indexes
DROP INDEX eth.storage_root_index;
DROP INDEX eth.account_state_id_index;

-- storage node indexes
DROP INDEX eth.storage_path_index;
DROP INDEX eth.storage_mh_index;
DROP INDEX eth.storage_cid_index;
DROP INDEX eth.storage_leaf_key_index;
DROP INDEX eth.storage_state_id_index;

-- state node indexes
DROP INDEX eth.state_path_index;
DROP INDEX eth.state_mh_index;
DROP INDEX eth.state_cid_index;
DROP INDEX eth.state_leaf_key_index;
DROP INDEX eth.state_header_id_index;

-- receipt indexes
DROP INDEX eth.rct_log_contract_index;
DROP INDEX eth.rct_topic3_index;
DROP INDEX eth.rct_topic2_index;
DROP INDEX eth.rct_topic1_index;
DROP INDEX eth.rct_topic0_index;
DROP INDEX eth.rct_contract_hash_index;
DROP INDEX eth.rct_contract_index;
DROP INDEX eth.rct_mh_index;
DROP INDEX eth.rct_cid_index;
DROP INDEX eth.rct_tx_id_index;

-- transaction indexes
DROP INDEX eth.tx_src_index;
DROP INDEX eth.tx_dst_index;
DROP INDEX eth.tx_mh_index;
DROP INDEX eth.tx_cid_index;
DROP INDEX eth.tx_hash_index;
DROP INDEX eth.tx_header_id_index;

-- header indexes
DROP INDEX eth.timestamp_index;
DROP INDEX eth.state_root_index;
DROP INDEX eth.header_mh_index;
DROP INDEX eth.header_cid_index;
DROP INDEX eth.block_hash_index;
DROP INDEX eth.block_number_index;