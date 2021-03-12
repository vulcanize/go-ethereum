-- +goose Up
CREATE TABLE eth.storage_cids (
  id                    BIGSERIAL PRIMARY KEY,
  state_id              BIGINT NOT NULL REFERENCES eth.state_cids (id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  storage_leaf_key      VARCHAR(66),
  cid                   TEXT NOT NULL,
  mh_key                TEXT NOT NULL REFERENCES public.blocks (key) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  storage_path          BYTEA,
  node_type             INTEGER NOT NULL,
  diff                  BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE (state_id, storage_path)
);

-- +goose Down
DROP TABLE eth.storage_cids;