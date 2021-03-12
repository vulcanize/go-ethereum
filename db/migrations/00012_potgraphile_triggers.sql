-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION eth.graphql_subscription() returns TRIGGER as $$
declare
    table_name text = TG_ARGV[0];
    attribute text = TG_ARGV[1];
    id text;
begin
    execute 'select $1.' || quote_ident(attribute)
        using new
        into id;
    perform pg_notify('postgraphile:' || table_name,
                      json_build_object(
                              '__node__', json_build_array(
                              table_name,
                              id
                          )
                          )::text
        );
    return new;
end;
$$ language plpgsql;
-- +goose StatementEnd

CREATE TRIGGER header_cids_ai
    after INSERT ON eth.header_cids
    for each row
    execute procedure eth.graphql_subscription('header_cids', 'id');

CREATE TRIGGER receipt_cids_ai
    after INSERT ON eth.receipt_cids
    for each row
    execute procedure eth.graphql_subscription('receipt_cids', 'id');

CREATE TRIGGER state_accounts_ai
    after INSERT ON eth.state_accounts
    for each row
    execute procedure eth.graphql_subscription('state_accounts', 'id');

CREATE TRIGGER state_cids_ai
    after INSERT ON eth.state_cids
    for each row
    execute procedure eth.graphql_subscription('state_cids', 'id');

CREATE TRIGGER storage_cids_ai
    after INSERT ON eth.storage_cids
    for each row
    execute procedure eth.graphql_subscription('storage_cids', 'id');

CREATE TRIGGER transaction_cids_ai
    after INSERT ON eth.transaction_cids
    for each row
    execute procedure eth.graphql_subscription('transaction_cids', 'id');

CREATE TRIGGER uncle_cids_ai
    after INSERT ON eth.uncle_cids
    for each row
    execute procedure eth.graphql_subscription('uncle_cids', 'id');

-- +goose Down
DROP TRIGGER uncle_cids_ai ON eth.uncle_cids;
DROP TRIGGER transaction_cids_ai ON eth.transaction_cids;
DROP TRIGGER storage_cids_ai ON eth.storage_cids;
DROP TRIGGER state_cids_ai ON eth.state_cids;
DROP TRIGGER state_accounts_ai ON eth.state_accounts;
DROP TRIGGER receipt_cids_ai ON eth.receipt_cids;
DROP TRIGGER header_cids_ai ON eth.header_cids;

DROP FUNCTION eth.graphql_subscription();
