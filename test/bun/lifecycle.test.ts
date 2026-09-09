// Conformance suite for fromBunSql, run by `bun test` (see npm run test:bun) because Bun's SQL
// client only exists inside the Bun runtime. vitest ignores this directory: its include pattern
// is test/**/*Test.ts, and these files end in .test.ts.
//
// The vitest half (test/bunAdapterTest.ts) pins the adapter's rewriting against a fake client.
// This half proves the rewriting is the right one by driving the whole of pg-boss - install,
// workers, cron and maintenance included - through the real driver.
import { SQL } from 'bun'
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { PgBoss, fromBunSql } from '../../src/index.ts'

const config = await Bun.file(new URL('../config.json', import.meta.url)).json()
const host = process.env.POSTGRES_HOST || config.host
const connectionString = `postgres://${config.user}:${config.password}@${host}:${config.port}/${config.database}`

const schema = 'bun_conformance_' + Math.random().toString(36).slice(2, 10)
const client = new SQL(connectionString)
const db = fromBunSql(client)

let boss: PgBoss

beforeAll(async () => {
  boss = new PgBoss({ db, schema, superviseIntervalSeconds: 1 })
  boss.on('error', () => {})
  await boss.start()
})

afterAll(async () => {
  await boss?.stop({ graceful: false }).catch(() => {})
  await client.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).catch(() => {})
  await client.end()
})

describe('pg-boss on Bun.SQL', () => {
  test('installs its schema through the reserved-connection path', async () => {
    // start() runs the BEGIN/COMMIT install script, which a pooled Bun connection refuses
    const { rows } = await db.executeSql(`SELECT version FROM ${schema}.version`)
    expect(rows[0].version).toBeGreaterThan(0)
  })

  test('runs a job through send, fetch and complete', async () => {
    const queue = 'lifecycle'
    await boss.createQueue(queue)

    const id = await boss.send(queue, { hello: 'world' })
    const [fetched] = await boss.fetch(queue)
    expect(fetched.id).toBe(id!)
    expect(fetched.data).toEqual({ hello: 'world' })

    // complete() binds the id list as `= ANY($N::uuid[])`, which is the array parameter Bun
    // cannot encode
    const result = await boss.complete(queue, id!, { done: true })
    expect(result.affected).toBe(1)

    const job = await boss.getJobById(queue, id!)
    expect(job!.state).toBe('completed')
    expect(job!.output).toEqual({ done: true })
  })

  test('completes a batch of ids in one call', async () => {
    const queue = 'batch'
    await boss.createQueue(queue)

    const ids = [await boss.send(queue, {}), await boss.send(queue, {})] as string[]
    await boss.fetch(queue, { batchSize: 2 })

    const result = await boss.complete(queue, ids, {}, { batch: true })
    expect(result.affected).toBe(2)
  })

  test('fails, cancels and deletes by id', async () => {
    const queue = 'terminal'
    await boss.createQueue(queue)

    const failed = await boss.send(queue, {}) as string
    await boss.fetch(queue)
    expect((await boss.fail(queue, failed, new Error('nope'))).affected).toBe(1)

    const cancelled = await boss.send(queue, {}) as string
    expect((await boss.cancel(queue, cancelled)).affected).toBe(1)

    const deleted = await boss.send(queue, {}) as string
    expect((await boss.deleteJob(queue, deleted)).affected).toBe(1)
  })

  test('inserts a batch and reads the queue back', async () => {
    const queue = 'inserted'
    await boss.createQueue(queue)

    await boss.insert(queue, [{ data: { n: 1 } }, { data: { n: 2 } }])

    const [stats] = await boss.getQueueStats(queue)
    expect(stats.queuedCount).toBe(2)

    const queues = await boss.getQueues([queue])
    expect(queues).toHaveLength(1)
  })

  test('updates a queue, which binds an object to a direct jsonb cast', async () => {
    const queue = 'updatable'
    await boss.createQueue(queue)

    await boss.updateQueue(queue, { retryLimit: 7 })

    expect((await boss.getQueue(queue))!.retryLimit).toBe(7)
  })

  test('delivers a job to a worker', async () => {
    const queue = 'worked'
    await boss.createQueue(queue)

    const { promise, resolve } = Promise.withResolvers<any>()
    await boss.work(queue, async ([job]) => resolve(job.data))
    await boss.send(queue, { via: 'worker' })

    expect(await promise).toEqual({ via: 'worker' })
  })

  test('schedules and unschedules', async () => {
    const queue = 'scheduled'
    await boss.createQueue(queue)

    await boss.schedule(queue, '* * * * *', { s: 1 })
    expect((await boss.getSchedules()).some(s => s.name === queue)).toBe(true)

    await boss.unschedule(queue)
    expect((await boss.getSchedules()).some(s => s.name === queue)).toBe(false)
  })

  test('runs maintenance, whose locked scripts also need a reserved connection', async () => {
    await boss.supervise()

    const { rows } = await db.executeSql(`SELECT cron_on FROM ${schema}.version`)
    expect(rows[0].cron_on).toBeTruthy()
  })

  test('surfaces a SQLSTATE where pg-boss reads it', async () => {
    const queue = 'duplicated'
    await boss.createQueue(queue)

    // the in-SQL insert guard raises 22012 when a flow's jobs collide, which pg-boss translates
    // only if the state reached `code` rather than staying in Bun's `errno`
    const id = crypto.randomUUID()

    await expect(boss.flow([
      { ref: 'parent', name: queue, options: { id } },
      { ref: 'child', name: queue, options: { id }, dependsOn: ['parent'] }
    ])).rejects.toThrow('one or more jobs could not be created')
  })
})
