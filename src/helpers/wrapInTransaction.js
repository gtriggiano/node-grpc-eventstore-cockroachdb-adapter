export default function wrapInTransaction (client, execution) {
  return beginTransaction(client)
  .then(() => {
    let executionToRetry = true

    function execute () {
      return execution(client)
        .then((storedEvents) => {
          return releaseTransactionSavepoint(client)
            .then(() => {
              executionToRetry = false
              return storedEvents
            })
        })
        .catch((error) => handleExecutionError(client, error))
        .then((storedEvents) => {
          if (executionToRetry) return execute()
          return commitTransaction(client)
            .then(() => storedEvents)
        })
    }

    return execute()
  })
  .catch((e) =>
    rollbackTransaction(client)
    .catch(() => {})
    .then(() => { throw e })
  )
}

function beginTransaction (client) {
  return client.query(`BEGIN; SAVEPOINT cockroach_restart`)
}
function commitTransaction (client) {
  return client.query('COMMIT')
}
function rollbackTransaction (client) {
  return client.query('ROLLBACK')
}
function releaseTransactionSavepoint (client) {
  return client.query(`RELEASE SAVEPOINT cockroach_restart`)
}
function rollbackTransactionToSavepoint (client) {
  return client.query(`ROLLBACK TO SAVEPOINT cockroach_restart`)
}
function handleExecutionError (client, error) {
  if (error.code === '40001') {
    return rollbackTransactionToSavepoint(client)
  } else {
    return Promise.reject(error)
  }
}
