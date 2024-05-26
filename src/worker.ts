import { Redis } from 'ioredis'
import { Job, Queue, Worker } from 'bullmq'
import { App } from './app'
import { pino } from 'pino'

export function newWorker(app: App, redis: Redis, log: pino.Logger, concurrency = 1) {
  const worker = new Worker(
    'backfill',
    async (job: Job) => {
      log.info(`Starting worker...`)

      if (job.name === 'backfillFID') {
        const start = Date.now()
        const fids = job.data.fids as number[]
        await app.reconcileFids(fids)
        const elapsed = (Date.now() - start) / 1000
        const lastFid = fids[fids.length - 1]
        log.info(`Backfilled ${fids.length} FIDs up to ${lastFid} in ${elapsed}s at ${new Date().toISOString()}`)
      }
    },
    {
      useWorkerThreads: concurrency > 1,
      concurrency,
      connection: redis,
      removeOnComplete: { count: 100 }, // Keep at most this many completed jobs
      removeOnFail: { count: 100 }, // Keep at most this many failed jobs
    }
  )

  return worker
}

export function newQueue(redis: Redis, name: string) {
  return new Queue(name, {
    connection: redis,
    defaultJobOptions: {
      attempts: 3,
      backoff: { delay: 1000, type: 'exponential' },
    },
  })
}
