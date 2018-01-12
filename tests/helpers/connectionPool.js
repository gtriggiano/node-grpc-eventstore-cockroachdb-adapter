import pg from 'pg'

export default new pg.Pool({
  host: 'cockroachdb',
  port: 26257,
  database: 'eventstore',
  user: 'root',
})
