-- +goose Up
CREATE TABLE eth.state_cids (
  id                    BIGSERIAL PRIMARY KEY,
  header_id             INTEGER NOT NULL REFERENCES eth.header_cids (id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  state_leaf_key        VARCHAR(66),
  cid                   TEXT NOT NULL,
  mh_key                TEXT NOT NULL REFERENCES public.blocks (key) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  state_path            BYTEA,
  node_type             INTEGER NOT NULL,
  diff                  BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE (header_id, state_path)
);

-- +goose Down
DROP TABLE eth.state_cids;