-- +goose Up
ALTER TABLE eth.kvstore SET UNLOGGED;

-- +goose Down
ALTER TABLE eth.kvstore SET LOGGED;