-- pg_jsonschema compatibility suite.
--
-- Upstream (supabase/pg_jsonschema) ships no SQL test suite: its tests are
-- pgrx #[pg_test] functions executed inside a private embedded server (see
-- src/lib.rs at the pinned tag). This file is a faithful SQL translation of
-- that corpus — every upstream test case appears below under its upstream
-- name, with the same schema/instance inputs and the same expected outcome —
-- so the identical assertions run through multigateway instead.
CREATE EXTENSION pg_jsonschema;
-- test_json_matches_schema_spi
select json_matches_schema('{"type": "object"}', '{}');
-- test_json_not_matches_schema_spi
select json_matches_schema('{"type": "object"}', '1');
-- test_jsonb_matches_schema_spi
select jsonb_matches_schema('{"type": "object"}', '{}');
-- test_jsonb_not_matches_schema_spi
select jsonb_matches_schema('{"type": "object"}', '1');
-- test_json_matches_schema_rs
select json_matches_schema('{"maxLength": 5}', '"foo"');
-- test_json_not_matches_schema_rs
select json_matches_schema('{"maxLength": 5}', '"foobar"');
-- test_jsonb_matches_schema_rs
select jsonb_matches_schema('{"maxLength": 5}', '"foo"');
-- test_jsonb_not_matches_schema_rs
select jsonb_matches_schema('{"maxLength": 5}', '"foobar"');
-- test_json_matches_schema_arbitrary_precision
select json_matches_schema('{"type": "number", "multipleOf": 0.1}', '17.2');
select json_matches_schema('{"type": "number", "multipleOf": 0.2}', '17.2');
select json_matches_schema('{"type": "number", "multipleOf": 0.3}', '17.2');
-- test_jsonschema_is_valid
select jsonschema_is_valid('{"type": "object"}');
-- test_jsonschema_is_not_valid
select jsonschema_is_valid('{"type": "obj"}');
-- test_jsonschema_unknown_specification
select jsonschema_is_valid('{"$schema": "invalid-uri", "type": "string"}');
-- test_jsonschema_validation_errors_none
select jsonschema_validation_errors('{"maxLength": 4}', '"foo"');
-- test_jsonschema_validation_erros_one
select jsonschema_validation_errors('{"maxLength": 4}', '"123456789"');
-- test_jsonschema_validation_errors_multiple
select jsonschema_validation_errors(
  '{
      "type": "object",
      "properties": {
          "foo": {"type": "string"},
          "bar": {"type": "number"},
          "baz": {"type": "boolean"},
          "additionalProperties": false
      }
  }',
  '{"foo": 1, "bar": [], "baz": "1"}');
