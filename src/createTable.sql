CREATE TABLE IF NOT EXISTS events (
  id SERIAL,
  stream_context VARCHAR(256) NOT NULL,
  stream_name VARCHAR(256) NOT NULL,
  stream_id VARCHAR(256) NOT NULL,
  type VARCHAR(256) NOT NULL,
  sequence_number INT NOT NULL CHECK (sequence_number > 0),
  stored_on TIMESTAMP NOT NULL DEFAULT NOW(),
  data TEXT NOT NULL,
  correlation_id VARCHAR(256),
  transaction_id VARCHAR(36) NOT NULL,

  PRIMARY KEY (id),
  UNIQUE (stream_context, stream_name, stream_id, sequence_number)
);
