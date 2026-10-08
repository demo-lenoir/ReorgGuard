CREATE TABLE blocks (
    chain_id bigint NOT NULL CHECK (chain_id >= 0),
    number bigint NOT NULL CHECK (number >= 0),
    hash bytea NOT NULL CHECK (octet_length(hash) = 32),
    parent_hash bytea NOT NULL CHECK (octet_length(parent_hash) = 32),
    block_time timestamptz NOT NULL,
    canonical boolean NOT NULL,
    logs_complete boolean NOT NULL,
    log_count bigint CHECK (log_count >= 0),
    log_set_hash bytea CHECK (log_set_hash IS NULL OR octet_length(log_set_hash) = 32),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, hash),
    UNIQUE (chain_id, hash, number),
    CHECK ((logs_complete AND log_count IS NOT NULL AND log_set_hash IS NOT NULL)
        OR (NOT logs_complete AND log_count IS NULL AND log_set_hash IS NULL))
);
CREATE UNIQUE INDEX blocks_one_canonical_height ON blocks(chain_id, number) WHERE canonical;

CREATE TABLE logs (
    chain_id bigint NOT NULL CHECK (chain_id >= 0),
    block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
    block_number bigint NOT NULL CHECK (block_number >= 0),
    tx_hash bytea NOT NULL CHECK (octet_length(tx_hash) = 32),
    tx_index integer NOT NULL CHECK (tx_index >= 0),
    log_index integer NOT NULL CHECK (log_index >= 0),
    address bytea NOT NULL CHECK (octet_length(address) = 20),
    topics bytea[] NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY (chain_id, block_hash, tx_hash, log_index),
    UNIQUE (chain_id, block_hash, log_index),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES blocks(chain_id, hash, number)
);

CREATE TABLE sync_state (
    chain_id bigint PRIMARY KEY CHECK (chain_id >= 0),
    genesis_hash bytea NOT NULL CHECK (octet_length(genesis_hash) = 32),
    filter_hash bytea NOT NULL CHECK (octet_length(filter_hash) = 32),
    start_block bigint NOT NULL CHECK (start_block >= 0),
    checkpoint_number bigint NOT NULL CHECK (checkpoint_number >= 0),
    checkpoint_hash bytea NOT NULL CHECK (octet_length(checkpoint_hash) = 32),
    canonical_revision bigint NOT NULL DEFAULT 0 CHECK (canonical_revision >= 0),
    fail_reason text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (checkpoint_number >= GREATEST(start_block - 1, 0)),
    FOREIGN KEY (chain_id, checkpoint_hash, checkpoint_number)
        REFERENCES blocks(chain_id, hash, number)
);

CREATE FUNCTION guard_block_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.chain_id IS DISTINCT FROM OLD.chain_id
       OR NEW.number IS DISTINCT FROM OLD.number
       OR NEW.hash IS DISTINCT FROM OLD.hash
       OR NEW.parent_hash IS DISTINCT FROM OLD.parent_hash
       OR NEW.block_time IS DISTINCT FROM OLD.block_time
       OR NEW.logs_complete IS DISTINCT FROM OLD.logs_complete
       OR NEW.log_count IS DISTINCT FROM OLD.log_count
       OR NEW.log_set_hash IS DISTINCT FROM OLD.log_set_hash
       OR NEW.first_seen_at IS DISTINCT FROM OLD.first_seen_at THEN
        RAISE EXCEPTION 'immutable block identity changed';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER blocks_immutable BEFORE UPDATE ON blocks
    FOR EACH ROW EXECUTE FUNCTION guard_block_immutable();

CREATE FUNCTION guard_log_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'immutable log changed';
END $$;
CREATE TRIGGER logs_immutable BEFORE UPDATE OR DELETE ON logs
    FOR EACH ROW EXECUTE FUNCTION guard_log_immutable();

CREATE FUNCTION guard_canonical_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM blocks WHERE chain_id=NEW.chain_id
                   AND hash=NEW.checkpoint_hash AND number=NEW.checkpoint_number
                   AND canonical) THEN
        RAISE EXCEPTION 'checkpoint must reference canonical block';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER sync_state_canonical_checkpoint BEFORE INSERT OR UPDATE OF checkpoint_hash, checkpoint_number ON sync_state
    FOR EACH ROW EXECUTE FUNCTION guard_canonical_checkpoint();
