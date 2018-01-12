import pool from './connectionPool'

export default function truncateEventsTable () {
  return pool.connect()
    .then((client) =>
      client
      .query(`TRUNCATE events`)
      .then(() => client.release())
      .catch((e) => {
        client.release()
        throw e
      })
    )
}
