import { Redis } from 'ioredis'
import { Job, Queue, Worker } from 'bullmq'
import { App } from './app'
import { pino } from 'pino'

export function newWorker(app: App, redis: Redis, log: pino.Logger, concurrency = 1) {
  const worker = new Worker(
    app.QUEUE_NAME,
    async (job: Job) => {
      if (job.name === 'backfillFID') {
        const start = Date.now()
        const fids = job.data.fids as number[]
        await app.reconcileFids(fids)
        const elapsed = (Date.now() - start) / 1000
        const lastFid = fids[fids.length - 1]
        log.info(`Backfilled ${fids.length} FIDs up to ${lastFid} in ${elapsed}s at ${new Date().toISOString()}`)
      } else if (job.name === 'backfillCompleted') {
        // TODO: Update key in redis so event streaming can start
        const startedAt = new Date(job.data.startedAt as number)
        const duration = (Date.now() - startedAt.getTime()) / 1000 / 60
        log.info(
          `Backfill started at ${startedAt.toISOString()} complete at ${new Date().toISOString()} ${duration} minutes`
        )
      }
    },
    {
      autorun: false, // Don't start yet
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
