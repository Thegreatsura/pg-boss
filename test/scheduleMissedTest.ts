import { expect } from 'vitest'
import { delay } from '../src/tools.ts'
import * as helper from './testHelper.ts'
import * as plans from '../src/plans.ts'
import Timekeeper from '../src/timekeeper.ts'
import { PgBoss } from '../src/index.ts'
import type { Job } from '../src/types.ts'
import { ctx } from './hooks.ts'

const MINUTE = 60_000
const DAY = 24 * 60 * MINUTE

// A daily schedule, so a gap holds a countable number of occurrences and the due window holds none:
// every job a test sees is one the catch-up sent.
const DAILY = '0 3 * * *'

/**
 * A Timekeeper over a database that answers the clock query and records every statement, which is
 * all a pass needs: occurrences are pure arithmetic on (expression, clock, zone), and the writes a
 * pass makes are the relabel and the warning insert.
 */
function makeTk () {
  const executed: Array<{ sql: string, params: unknown[] }> = []

  const db = {
    executeSql: async (sql: string, params: unknown[] = []) => {
      executed.push({ sql, params })

      return { rows: [{ time: String(Date.now()) }] }
    }
  }

  const tk = new Timekeeper(db as any, {} as any, { schema: 'test' } as any)

  return Object.assign(tk, { executed })
}

/** The 60-second throttle slot a forwarded job lands in, as the insert files it: UTC, zoneless. */
function slotOf (epochMs: number) {
  return new Date(Math.floor(epochMs / MINUTE) * MINUTE).toISOString().replace('T', ' ').slice(0, 19)
}

/** The iCalendar spelling of an instant, for a DTSTART built around the clock. */
function ical (epochMs: number) {
  return new Date(epochMs).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '')
}

/**
 * A pass over `schedules` with the database clock at `databaseTime` and the last pass at
 * `priorCronOn`, answering with the jobs it forwarded.
 */
async function pass (tk: ReturnType<typeof makeTk>, databaseTime: number, priorCronOn: Date | null, schedules: unknown[]) {
  const inserted: any[] = []

  ;(tk as any).stopped = false
  ;(tk as any).manager = { insert: async (_q: string, jobs: any[]) => { inserted.push(...jobs) } }
  ;(tk as any).getSchedules = async () => schedules

  tk.clockSkew = databaseTime - Date.now()

  await tk.cron(priorCronOn)

  return inserted
}

/** One schedule row, as getSchedules() hands it to the pass. */
function row (cron: string, missed?: string, extra: Record<string, unknown> = {}) {
  return {
    name: 'q',
    key: '',
    data: null,
    options: missed === undefined ? {} : { missed },
    kind: plans.SCHEDULE_KINDS.cron,
    cron,
    timezone: 'UTC',
    ...extra
  }
}

/** The slots a pass filed catch-up jobs in, which is every job it filed bar the due cron one. */
function slots (inserted: any[]) {
  return inserted.filter(job => job.__singletonSlot !== undefined).map(job => job.__singletonSlot)
}

async function waitForJobs (boss: PgBoss, count: number): Promise<Job[]> {
  const jobs: Job[] = []
  const deadline = Date.now() + 8_000

  while (Date.now() < deadline) {
    jobs.push(...await boss.fetch<object>(ctx.schema, { batchSize: 100 }))

    if (jobs.length >= count) {
      break
    }

    await delay(100)
  }

  return jobs
}

function firingConfig () {
  return {
    ...ctx.bossConfig,
    cronMonitorIntervalSeconds: 1,
    cronWorkerIntervalSeconds: 1,
    schedule: true
  }
}

/**
 * Backdates the version row's cron pass timestamp and the schedule's own creation, which is a
 * deployment that has been down since `gapStart` with a schedule older than the outage.
 */
async function openGap (gapStart: Date) {
  const db = await helper.getDb()

  try {
    await db.executeSql(`UPDATE ${ctx.schema}.schedule SET created_on = $1`, [new Date(gapStart.getTime() - DAY).toISOString()])
    await db.executeSql(`UPDATE ${ctx.schema}.version SET cron_on = $1`, [gapStart.toISOString()])
  } finally {
    await db.close()
  }
}

describe('schedule missed', function () {
  it('sends nothing for a gap by default', async function () {
    const tk = makeTk()
    const now = Date.now()

    // Ten minutes of a per-minute schedule went by with no pass, and the default policy is that a
    // pass sends the due window and nothing else, which is what every release before it did.
    const inserted = await pass(tk, now, new Date(now - 10 * MINUTE), [row('* * * * *')])

    expect(slots(inserted)).toEqual([])
    expect(inserted).toHaveLength(1)
    expect(inserted[0].singletonSeconds).toBe(60)
  })

  it('sends one job for the whole gap under once', async function () {
    const tk = makeTk()

    // A pass mid-minute, so the occurrences the gap holds are the minute boundaries behind it.
    const minute = Math.floor(Date.now() / MINUTE) * MINUTE
    const now = minute + 30_000

    const inserted = await pass(tk, now, new Date(now - 10 * MINUTE), [row('* * * * *', 'once')])

    // The most recent one, which for a schedule whose job reads the current state of the world is
    // the only one worth running.
    expect(slots(inserted)).toEqual([slotOf(minute - MINUTE)])
  })

  it('sends a job for every missed occurrence under all', async function () {
    const tk = makeTk()

    const minute = Math.floor(Date.now() / MINUTE) * MINUTE
    const now = minute + 30_000

    const inserted = await pass(tk, now, new Date(now - 5 * MINUTE), [row('* * * * *', 'all')])

    // The four boundaries between the last pass and the due window, oldest first, each in the slot
    // it fell in rather than the one the pass is running in.
    expect(slots(inserted)).toEqual([
      slotOf(minute - 4 * MINUTE),
      slotOf(minute - 3 * MINUTE),
      slotOf(minute - 2 * MINUTE),
      slotOf(minute - MINUTE)
    ])

    // and the occurrence in the due window is still filed the way it always was
    expect(inserted[inserted.length - 1].singletonSeconds).toBe(60)
  })

  it('reads a recurrence rule backwards over the gap', async function () {
    const tk = makeTk()

    const minute = Math.floor(Date.now() / MINUTE) * MINUTE
    const now = minute + 30_000

    const inserted = await pass(tk, now, new Date(now - 3 * MINUTE), [
      row('FREQ=MINUTELY', 'all', { kind: plans.SCHEDULE_KINDS.rrule })
    ])

    // Both formats catch up on the same range: the two boundaries in the gap, then the due one,
    // which a rule files in its own slot rather than the insert's.
    expect(slots(inserted)).toEqual([
      slotOf(minute - 2 * MINUTE),
      slotOf(minute - MINUTE),
      slotOf(minute)
    ])
  })

  it('catches up on every occurrence of a rule carrying an RDATE', async function () {
    const tk = makeTk()

    const minute = Math.floor(Date.now() / MINUTE) * MINUTE
    const now = minute + 30_000

    // A calendar export pairs a rule with one-off dates, and an RDATE is an absolute instant rather
    // than a phase of the rule: read backwards through rrule-temporal's previous() the walk jumped
    // to the RDATE and resumed from there, so every regular occurrence between the cursor and the
    // RDATE was dropped. Read in chunks of between(), the catch-up sees what the due window sees.
    const cron = [
      `DTSTART:${ical(minute - 10 * MINUTE)}`,
      'RRULE:FREQ=MINUTELY',
      `RDATE:${ical(minute - 3 * MINUTE + 12_000)}`
    ].join('\n')

    const inserted = await pass(tk, now, new Date(now - 5 * MINUTE), [
      row(cron, 'all', { kind: plans.SCHEDULE_KINDS.rrule })
    ])

    // Every minute the gap held, plus the due one. The RDATE shares the slot of the occurrence it
    // sits beside, so it collapses into that job rather than adding one.
    expect(slots(inserted)).toEqual([
      slotOf(minute - 4 * MINUTE),
      slotOf(minute - 3 * MINUTE),
      slotOf(minute - 2 * MINUTE),
      slotOf(minute - MINUTE),
      slotOf(minute)
    ])
  })

  it('sends nothing when the last pass is inside the due window', async function () {
    const tk = makeTk()
    const now = Date.now()

    // The steady state: a pass claims at most cronMonitorIntervalSeconds after the one before it,
    // 45 at the ceiling, and the window is 60 wide, so there is never a gap between them to catch
    // up on and the policy costs nothing.
    for (const seconds of [1, 30, 45, 60]) {
      const inserted = await pass(tk, now, new Date(now - seconds * 1000), [row('* * * * * *', 'all')])

      expect(slots(inserted)).toEqual([])
    }
  })

  it('does not reach back past the moment the schedule was created', async function () {
    const tk = makeTk()

    const minute = Math.floor(Date.now() / MINUTE) * MINUTE
    const now = minute + 30_000

    // A schedule written during the gap, by a process that was up while nothing ran a pass. The
    // occurrences before it are of an expression that was not in the table yet.
    const inserted = await pass(tk, now, new Date(now - 10 * MINUTE), [
      row('* * * * *', 'all', { createdOn: new Date(minute - 2 * MINUTE - 30_000) })
    ])

    expect(slots(inserted)).toEqual([slotOf(minute - 2 * MINUTE), slotOf(minute - MINUTE)])
  })

  it('reads a policy it does not recognize as skip', async function () {
    const tk = makeTk()
    const now = Date.now()

    // A row written straight into the table with SQL, or by a release naming a policy this one does
    // not: the pass sends what it has always sent rather than picking a policy on the row's behalf.
    const inserted = await pass(tk, now, new Date(now - 10 * MINUTE), [row('* * * * *', 'hourly-ish')])

    expect(slots(inserted)).toEqual([])
  })

  it('files a missed occurrence and a due one that share a slot as one job', async function () {
    const tk = makeTk()

    // A pass on the half minute, so the window opens mid-slot: the occurrence on the bound is
    // missed, the one a second later is due, and both belong to the same minute.
    const minute = Math.floor(Date.now() / MINUTE) * MINUTE
    const now = minute + 30_000
    const bound = now - MINUTE

    const cron = `DTSTART:${ical(bound)}\nRRULE:FREQ=SECONDLY;COUNT=2`

    const inserted = await pass(tk, now, new Date(now - 10 * MINUTE), [
      row(cron, 'all', { kind: plans.SCHEDULE_KINDS.rrule })
    ])

    expect(slots(inserted)).toEqual([slotOf(bound)])
  })

  it('caps a backlog under all and warns that it did', async function () {
    const tk = makeTk()

    const warnings: any[] = []
    tk.on('warning', warning => warnings.push(warning))

    const minute = Math.floor(Date.now() / MINUTE) * MINUTE
    const now = minute + 30_000

    // Two thousand minutes down, which is more than one pass sends for one schedule.
    const inserted = await pass(tk, now, new Date(now - 2000 * MINUTE), [row('* * * * *', 'all')])

    const filed = slots(inserted)

    expect(filed).toHaveLength(1000)

    // The recent end of the backlog is the part kept: a schedule catching up on a day and a half
    // has a day and a half of work behind it either way.
    expect(filed[filed.length - 1]).toBe(slotOf(minute - MINUTE))
    expect(filed[0]).toBe(slotOf(minute - 1000 * MINUTE))

    expect(warnings).toHaveLength(1)
    expect(warnings[0].message).toMatch(/came due more than 1000 times/)
    expect(warnings[0].data).toMatchObject({ queue: 'q', key: '', limit: 1000 })
  })

  it('does not warn when once collapses a backlog, which is what it is for', async function () {
    const tk = makeTk()

    const warnings: any[] = []
    tk.on('warning', warning => warnings.push(warning))

    const now = Date.now()

    const inserted = await pass(tk, now, new Date(now - 2000 * MINUTE), [row('* * * * *', 'once')])

    expect(slots(inserted)).toHaveLength(1)
    expect(warnings).toEqual([])
  })

  it('rejects a policy no pass would honor', async function () {
    const tk = makeTk()

    await expect(tk.schedule('q', DAILY, null, { missed: 'sometimes' } as any))
      .rejects.toThrow(/missed must be one of: skip, once, all/)
  })

  it('reads a nullish policy as none given, the way a falsy time zone is read', async function () {
    const tk = makeTk()

    // Same shape as `tz`: a policy threaded out of a config object arrives as null rather than
    // absent, and a policy name is never falsy, so nothing a caller could have meant is read past.
    // The pass reads the row as `skip` either way.
    await expect(tk.schedule('q', DAILY, null, { missed: null } as any)).resolves.toBeUndefined()

    const now = Date.now()
    const inserted = await pass(tk, now, new Date(now - 10 * MINUTE), [row(DAILY, null as any)])

    expect(slots(inserted)).toEqual([])
  })

  it('stores the policy on the schedule and reads it back', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, schedule: false })

    await ctx.boss.schedule(ctx.schema, DAILY, null, { missed: 'all' })

    const schedule = await ctx.boss.getSchedule(ctx.schema)

    expect(schedule!.options).toMatchObject({ missed: 'all' })
  })

  it('answers the cron claim with the timestamp it replaced', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, schedule: false })

    const db = await helper.getDb()

    try {
      // No pass has run against this schema, so there is no gap to report and nothing to catch up
      // on.
      const first = await db.executeSql(plans.trySetCronTime(ctx.schema, 30))

      expect(first.rows).toHaveLength(1)
      expect(first.rows[0].priorCronOn).toBeNull()

      // A second claim inside the interval takes nothing, which is what keeps two instances from
      // running the same pass.
      const contended = await db.executeSql(plans.trySetCronTime(ctx.schema, 30))

      expect(contended.rows).toHaveLength(0)

      const gapStart = new Date(Date.now() - 5 * MINUTE)

      await db.executeSql(`UPDATE ${ctx.schema}.version SET cron_on = $1`, [gapStart.toISOString()])

      const claimed = await db.executeSql(plans.trySetCronTime(ctx.schema, 30))

      expect(claimed.rows).toHaveLength(1)
      expect(new Date(claimed.rows[0].priorCronOn).getTime()).toBe(gapStart.getTime())
    } finally {
      await db.close()
    }
  })

  it('sends the occurrences an outage held', async function () {
    ctx.boss = await helper.start(firingConfig())

    await ctx.boss.schedule(ctx.schema, DAILY, null, { missed: 'all' })

    const gapStart = new Date(Date.now() - 3 * DAY)

    await openGap(gapStart)

    // What the gap held, computed the way a caller would: the occurrences after it that are already
    // older than the 60-second due window.
    const expected = ctx.boss.previewSchedule(DAILY, { from: gapStart, count: 10 })
      .filter(occurrence => occurrence.getTime() <= Date.now() - MINUTE)

    expect(expected.length).toBeGreaterThan(1)

    const jobs = await waitForJobs(ctx.boss, expected.length)

    expect(jobs).toHaveLength(expected.length)

    // A pass a second later has no gap left to read, so the backlog is sent once rather than again
    // on every pass.
    await delay(2_000)

    expect(await ctx.boss.fetch(ctx.schema, { batchSize: 100 })).toEqual([])
  })

  it('sends one job for an outage under once', async function () {
    ctx.boss = await helper.start(firingConfig())

    await ctx.boss.schedule(ctx.schema, DAILY, null, { missed: 'once' })

    await openGap(new Date(Date.now() - 3 * DAY))

    const jobs = await waitForJobs(ctx.boss, 1)

    expect(jobs).toHaveLength(1)

    await delay(2_000)

    expect(await ctx.boss.fetch(ctx.schema, { batchSize: 100 })).toEqual([])
  })

  it('sends nothing for an outage by default', async function () {
    ctx.boss = await helper.start(firingConfig())

    await ctx.boss.schedule(ctx.schema, DAILY)

    await openGap(new Date(Date.now() - 3 * DAY))

    // Three days of a daily schedule, and a due window holding none of it: the pass sends nothing,
    // which is the behavior every schedule keeps unless it asks for another.
    await delay(3_000)

    expect(await ctx.boss.fetch(ctx.schema, { batchSize: 100 })).toEqual([])
  })

  it('records the newest occurrence of a catch-up batch as the last job', async function () {
    const tk = makeTk()

    const sent: string[] = []

    ;(tk as any).manager = {
      send: async () => {
        const id = `job-${sent.length}`

        sent.push(id)

        return id
      }
    }

    const minute = Math.floor(Date.now() / MINUTE) * MINUTE

    // One schedule, three occurrences, in an order no fetch promises to avoid: a catch-up creates
    // every job in a single insert, so they share a created_on and come back however they come back.
    const batch = [minute - MINUTE, minute - 3 * MINUTE, minute - 2 * MINUTE]
      .map(instant => ({ data: { name: 'q', key: '', slot: slotOf(instant) } }))

    await (tk as any).onSendIt(batch)

    const [{ params }] = tk.executed.filter(({ sql }) => sql.includes('last_job_id'))

    // The job of the newest occurrence, rather than whichever settled last.
    expect(JSON.parse(params[0] as string)).toEqual([{ name: 'q', key: '', jobId: 'job-0' }])
  })
})
