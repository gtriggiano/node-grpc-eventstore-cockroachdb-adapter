import pool from './connectionPool'

export default function getEventsCount () {
  return pool.connect()
    .then((client) =>
      client
      .query(`SELECT COUNT(*) AS total_events FROM events`)
      .then(({rows}) => {
        client.release()
        return parseInt(rows[0].total_events, 10)
      })
      .catch((e) => {
        client.release()
        throw e
      })
    )
}
