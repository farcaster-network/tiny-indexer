import { App } from './app'
import { pino } from 'pino'
import { BACKFILL_FIDS, CONCURRENCY, MAX_FID } from './env'
import { newWorker } from './worker'

export default async function backfill(app: App, log: pino.Logger) {
  log.info('Starting backfill...')

  const maxFidResult = await app.hubSubscriber.hubClient!.getFids({
    pageSize: 1,
    reverse: true,
  })

  if (maxFidResult.isErr()) {
    log.error('Failed to get max fid', maxFidResult.error)
    throw maxFidResult.error
  }

  let maxFid = maxFidResult.value.fids[0]
  if (!maxFid) {
    log.error('Max fid was undefined')
    throw new Error('Max fid was undefined')
  }

  // TEST
  maxFid = 10

  log.info(`Queuing fids up to: ${maxFid}`)
  // create an array of arrays in batches of 100 up to maxFid
  const batchSize = 10
  const fids = Array.from({ length: Math.ceil(maxFid / batchSize) }, (_, i) => i * batchSize).map((fid) => fid + 1)

  for (const start of fids) {
    const subset = Array.from({ length: batchSize }, (_, i) => start + i)
    await app.backfillQueue.add('backfillFID', { fids: subset })
  }

  newWorker(app, app.redis.client, log, CONCURRENCY)
}
