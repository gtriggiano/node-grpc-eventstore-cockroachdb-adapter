import pool from './connectionPool'

export default function getStreams () {
  return pool.connect()
    .then((client) =>
      client
      .query(`SELECT DISTINCT stream_context, stream_name, stream_id FROM events`)
      .then(({rows}) => {
        client.release()
        return rows.map(({stream_context, stream_name, stream_id}) => ({
          id: stream_id,
          type: {
            context: stream_context,
            name: stream_name,
          },
        }))
      })
      .catch((e) => {
        client.release()
        throw e
      })
    )
}
