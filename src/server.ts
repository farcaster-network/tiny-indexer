import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'
import { ExpressAdapter } from '@bull-board/express'
import express from 'express'
import { log } from './log'
import { App } from './app'
import { POSTGRES_URL, REDIS_URL, HUB_HOST, HUB_SSL } from './env'

export async function startMonitoringServer() {
  const _server = express()
  const serverAdapter = new ExpressAdapter()
  const app = await App.getInstance(POSTGRES_URL, REDIS_URL, HUB_HOST, HUB_SSL)

  _server.listen(3001, () => {
    log.info('Server started on http://localhost:3001')
  })

  serverAdapter.setBasePath('/')
  _server.use('/', serverAdapter.getRouter())

  createBullBoard({
    queues: [new BullMQAdapter(app.backfillQueue)],
    serverAdapter,
  })
}
