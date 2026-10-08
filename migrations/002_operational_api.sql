ALTER TABLE sync_state ADD COLUMN last_reorg jsonb;
ALTER TABLE sync_state ADD COLUMN checkpoint_updated_at timestamptz NOT NULL DEFAULT now();

-- The API's keyset traversal starts at block height and log index. These
-- indexes also keep address/topic0 filtered pages bounded on large histories.
CREATE INDEX logs_page_order ON logs(chain_id, block_number, log_index, block_hash);
CREATE INDEX logs_address_page ON logs(chain_id, address, block_number, log_index, block_hash);
CREATE INDEX logs_topic0_page ON logs(chain_id, (topics[1]), block_number, log_index, block_hash);
