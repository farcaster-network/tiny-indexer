import { pino } from 'pino'
import { LOG_LEVEL } from './env'

export const log = pino({
  level: LOG_LEVEL,
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      singleLine: true,
    },
  },
})

export type Logger = pino.Logger
