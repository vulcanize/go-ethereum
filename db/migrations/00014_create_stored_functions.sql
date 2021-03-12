-- +goose Up
-- +goose StatementBegin
-- returns if a storage node at the provided path was removed in the range > the provided height and <= the provided block hash
CREATE OR REPLACE FUNCTION was_storage_removed(path BYTEA, height BIGINT, hash VARCHAR(66)) RETURNS BOOLEAN
AS $$
SELECT exists(SELECT 1
              FROM eth.storage_cids
                       INNER JOIN eth.state_cids ON (storage_cids.state_id = state_cids.id)
                       INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
              WHERE storage_path = path
                AND block_number > height
                AND block_number <= (SELECT block_number
                                     FROM eth.header_cids
                                     WHERE block_hash = hash)
                AND storage_cids.node_type = 3
              LIMIT 1);
$$ LANGUAGE SQL;
-- +goose StatementEnd

-- +goose StatementBegin
-- returns if a state node at the provided path was removed in the range > the provided height and <= the provided block hash
CREATE OR REPLACE FUNCTION was_state_removed(path BYTEA, height BIGINT, hash VARCHAR(66)) RETURNS BOOLEAN
AS $$
SELECT exists(SELECT 1
              FROM eth.state_cids
                       INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
              WHERE state_path = path
                AND block_number > height
                AND block_number <= (SELECT block_number
                                     FROM eth.header_cids
                                     WHERE block_hash = hash)
                AND state_cids.node_type = 3
              LIMIT 1);
$$ LANGUAGE SQL;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TYPE child_result AS (
    has_child BOOLEAN,
    children eth.header_cids[]
);

CREATE OR REPLACE FUNCTION has_child(hash VARCHAR(66), height BIGINT) RETURNS child_result AS
$BODY$
DECLARE
child_height INT;
  temp_child eth.header_cids;
  new_child_result child_result;
BEGIN
  child_height = height + 1;
  -- short circuit if there are no children
SELECT exists(SELECT 1
              FROM eth.header_cids
              WHERE parent_hash = hash
                AND block_number = child_height
              LIMIT 1)
INTO new_child_result.has_child;
-- collect all the children for this header
IF new_child_result.has_child THEN
    FOR temp_child IN
SELECT * FROM eth.header_cids WHERE parent_hash = hash AND block_number = child_height
    LOOP
      new_child_result.children = array_append(new_child_result.children, temp_child);
END LOOP;
END IF;
RETURN new_child_result;
END
$BODY$
LANGUAGE 'plpgsql';
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION canonical_header_from_array(headers eth.header_cids[]) RETURNS eth.header_cids AS
$BODY$
DECLARE
canonical_header eth.header_cids;
  canonical_child eth.header_cids;
  header eth.header_cids;
  current_child_result child_result;
  child_headers eth.header_cids[];
  current_header_with_child eth.header_cids;
  has_children_count INT DEFAULT 0;
BEGIN
  -- for each header in the provided set
  FOREACH header IN ARRAY headers
  LOOP
    -- check if it has any children
    current_child_result = has_child(header.block_hash, header.block_number);
    IF current_child_result.has_child THEN
      -- if it does, take note
      has_children_count = has_children_count + 1;
      current_header_with_child = header;
      -- and add the children to the growing set of child headers
      child_headers = array_cat(child_headers, current_child_result.children);
END IF;
END LOOP;
  -- if none of the headers had children, none is more canonical than the other
  IF has_children_count = 0 THEN
    -- return the first one selected
SELECT * INTO canonical_header FROM unnest(headers) LIMIT 1;
-- if only one header had children, it can be considered the heaviest/canonical header of the set
ELSIF has_children_count = 1 THEN
    -- return the only header with a child
    canonical_header = current_header_with_child;
  -- if there are multiple headers with children
ELSE
    -- find the canonical header from the child set
    canonical_child = canonical_header_from_array(child_headers);
    -- the header that is parent to this header, is the canonical header at this level
SELECT * INTO canonical_header FROM unnest(headers)
WHERE block_hash = canonical_child.parent_hash;
END IF;
RETURN canonical_header;
END
$BODY$
LANGUAGE 'plpgsql';
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION canonical_header_id(height BIGINT) RETURNS INTEGER AS
$BODY$
DECLARE
canonical_header eth.header_cids;
  headers eth.header_cids[];
  header_count INT;
  temp_header eth.header_cids;
BEGIN
  -- collect all headers at this height
FOR temp_header IN
SELECT * FROM eth.header_cids WHERE block_number = height
    LOOP
    headers = array_append(headers, temp_header);
END LOOP;
  -- count the number of headers collected
  header_count = array_length(headers, 1);
  -- if we have less than 1 header, return NULL
  IF header_count IS NULL OR header_count < 1 THEN
    RETURN NULL;
  -- if we have one header, return its id
  ELSIF header_count = 1 THEN
    RETURN headers[1].id;
  -- if we have multiple headers we need to determine which one is canonical
ELSE
    canonical_header = canonical_header_from_array(headers);
RETURN canonical_header.id;
END IF;
END;
$BODY$
LANGUAGE 'plpgsql';
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION was_storage_removed;
DROP FUNCTION was_state_removed;
DROP FUNCTION canonical_header_id;
DROP FUNCTION canonical_header_from_array;
DROP FUNCTION has_child;
DROP TYPE child_result;