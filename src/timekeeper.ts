import { CronExpressionParser } from 'cron-parser'
import assert from 'node:assert'
import EventEmitter from 'node:events'

import * as Attorney from './attorney.ts'
import type Manager from './manager.ts'
import * as plans from './plans.ts'
import { delay } from './tools.ts'
import * as types from './types.ts'
import { emitAndPersistWarning, type WarningContext } from './warning.ts'

export const QUEUES = {
  SEND_IT: '__pgboss__send-it'
}

const EVENTS = {
  error: 'error',
  schedule: 'schedule',
  warning: 'warning'
}

const WARNINGS = {
  CLOCK_SKEW: {
    message: 'Warning: Clock skew between this instance and the database server. This will not break scheduling, but is emitted any time the skew exceeds 60 seconds.'
  }
}

const WARNING_TYPES = {
  CLOCK_SKEW: 'clock_skew',
  INVALID_SCHEDULE: 'invalid_schedule'
} as const

// previewSchedule() defaults and ceilings. The count ceiling is not a database limit, since the walk
// is pure cron-parser arithmetic, but an unbounded count on a per-second expression is a foot-gun.
// A caller that genuinely wants more can page by passing the last occurrence back as `from`.
const PREVIEW_DEFAULT_COUNT = 5
const PREVIEW_MAX_COUNT = 1000

// Count is the wrong budget on its own, because occurrences are not equally priced: 1000 of a
// per-second expression cost about 8ms, 1000 of '0 0 1 1 *' about 160ms, and 1000 of '0 0 29 2 *'
// about 6 seconds, since each next() on a sparse expression searches years of candidate dates. The
// walk is synchronous, so those 6 seconds are 6 seconds of blocked event loop for every worker
// poll, cron tick and heartbeat in the process. Bounding wall clock as well caps that at a second
// while leaving the full documented count reachable for any expression that is not pathological.
const PREVIEW_TIME_BUDGET_MS = 1000

// What the cron pass puts on the send-it queue. `key` identifies the schedule row the occurrence
// came from, so the handler can record the job it produced. It is absent on rows written by an
// instance older than 12.31.0, which is why the handler treats it as optional rather than required.
type ScheduledRequest = types.Request & { key?: string }

// One schedule occurrence that produced a job, as handed to plans.setScheduleLastJobIds. camelCase
// to match the recordset column list the plan quotes, which is how every other JSON payload crossing
// into SQL is shaped.
type FiredSchedule = { name: string, key: string, jobId: string }

/**
 * Parses a recurrence the way the cron pass will evaluate it, mapping cron-parser's failures onto
 * messages that name the input actually at fault. Deliberately reuses cron-parser rather than an
 * independent Intl check, so what schedule() accepts is exactly what the cron pass can evaluate.
 *
 * The expression is checked first, against UTC, so a bad expression reports as one rather than as a
 * time zone problem. That first parse tolerates any zone only because cron-parser validates `tz`
 * lazily: with no reference date it never constructs a CronDate, so a typo like 'America/New_Yrok'
 * survives to the real parse below, which does have a reference date and fails there with an opaque
 * "CronDate: unhandled timestamp". Rethrown naming the zone, so schedule() rejects it rather than
 * persisting it to the schedule table.
 *
 * A non-string zone earns no such failure: cron-parser reads it as "unset" and quietly evaluates in
 * the host's local zone. The schedule.timezone column is nullable, so a row written before
 * schedule() validated zones reads back as null, and previewing it would be right only on a host
 * that happens to run in the zone that was meant. Rejected explicitly.
 *
 * The returned interval is the one the caller wants anyway, so the walk costs a single parse.
 */
function parseRecurrence (cron: string, tz: string, currentDate: Date) {
  CronExpressionParser.parse(cron, { tz: 'UTC', strict: false })

  // Quoted so an empty string renders as `""` rather than a dangling colon
  const unusableZone = `Unknown or unsupported time zone: ${typeof tz === 'string' ? `"${tz}"` : String(tz)}`

  if (typeof tz !== 'string') {
    throw new Error(unusableZone)
  }

  try {
    return CronExpressionParser.parse(cron, { tz, strict: false, currentDate })
  } catch {
    throw new Error(unusableZone)
  }
}

/**
 * Validates a recurrence for a caller with no reference date of its own and no use for the
 * interval, so previewSchedule() and schedule() reject exactly the same inputs.
 */
function assertRecurrence (cron: string, tz: string): void {
  parseRecurrence(cron, tz, new Date())
}

class Timekeeper extends EventEmitter implements types.EventsMixin {
  db: types.IDatabase
  config: types.ResolvedConstructorOptions
  manager: Manager

  private stopped = true
  private cronMonitorInterval: NodeJS.Timeout | null | undefined
  private skewMonitorInterval: NodeJS.Timeout | null | undefined
  private timekeeping: boolean | undefined
  private _checkingSkew = false

  // Rows already warned about, keyed on (name, key, cron, timezone). Unlike every other warning
  // type, an unusable schedule never heals on its own: clock skew converges, a backlog drains, a
  // slow query is a one-off, but a bad row sits there until a human edits it. Warning every pass
  // would persist a row every cronMonitorIntervalSeconds forever, and warningRetentionDays has no
  // default, so a single typo could grow the warning table without bound. Rebuilt each pass from
  // the rows still broken, so a fixed or deleted schedule drops out and would warn again if it
  // came back.
  private warnedSchedules = new Set<string>()

  clockSkew = 0
  events = EVENTS

  constructor (db: types.IDatabase, manager: Manager, config: types.ResolvedConstructorOptions) {
    super()

    this.db = db
    this.config = config
    this.manager = manager
  }

  get checkingSkew (): boolean {
    return this._checkingSkew
  }

  // The instance's reading of the database clock. previewSchedule() promises the reference point the
  // cron pass evaluates against, so both read it here rather than each repeating the arithmetic.
  // Zero skew until cacheClockSkew() has run, which start() only reaches when the instance was
  // configured with scheduling enabled.
  private get databaseTime (): number {
    return Date.now() + this.clockSkew
  }

  private get warningContext (): WarningContext {
    return {
      emitter: this,
      db: this.db,
      schema: this.config.schema,
      persistWarnings: this.config.persistWarnings,
      warningEvent: this.events.warning,
      errorEvent: this.events.error
    }
  }

  async start () {
    this.stopped = false
    // A restart should re-surface a row nobody has fixed yet
    this.warnedSchedules.clear()

    await this.cacheClockSkew()
    await this.manager.createQueue(QUEUES.SEND_IT)

    const options = {
      pollingIntervalSeconds: this.config.cronWorkerIntervalSeconds,
      batchSize: 50
    }

    await this.manager.work<ScheduledRequest>(QUEUES.SEND_IT, options, (jobs) => this.onSendIt(jobs))

    setImmediate(() => this.onCron())

    this.cronMonitorInterval = setInterval(async () => await this.onCron(), this.config.cronMonitorIntervalSeconds! * 1000)
    this.skewMonitorInterval = setInterval(async () => await this.cacheClockSkew(), this.config.clockMonitorIntervalSeconds! * 1000)
  }

  async stop () {
    if (this.stopped) {
      return
    }

    this.stopped = true

    await this.manager.offWork(QUEUES.SEND_IT, { wait: true })

    if (this.skewMonitorInterval) {
      clearInterval(this.skewMonitorInterval)
      this.skewMonitorInterval = null
    }

    if (this.cronMonitorInterval) {
      clearInterval(this.cronMonitorInterval)
      this.cronMonitorInterval = null
    }

    while (this.timekeeping || this._checkingSkew) {
      await delay(10)
    }
  }

  async cacheClockSkew () {
    let skew = 0

    this._checkingSkew = true

    try {
      if (this.config.__test__force_clock_monitoring_error) {
        throw new Error(this.config.__test__force_clock_monitoring_error)
      }

      if (this.config.__test__delay_clock_skew_ms) {
        await delay(this.config.__test__delay_clock_skew_ms)
      }

      const { rows } = await this.db.executeSql(plans.getTime())

      const local = Date.now()

      const dbTime = parseFloat(rows[0].time)

      skew = dbTime - local

      const skewSeconds = Math.abs(skew) / 1000

      if (skewSeconds >= 60 || this.config.__test__force_clock_skew_warning) {
        await emitAndPersistWarning(
          this.warningContext,
          WARNING_TYPES.CLOCK_SKEW,
          WARNINGS.CLOCK_SKEW.message,
          { seconds: skewSeconds, direction: skew > 0 ? 'slower' : 'faster' }
        )
      }

      this.clockSkew = skew
    } catch (err) {
      this.emit(this.events.error, err)
    } finally {
      this._checkingSkew = false
    }
  }

  async onCron () {
    try {
      if (this.stopped || this.timekeeping) return

      if (this.config.__test__force_cron_monitoring_error) {
        throw new Error(this.config.__test__force_cron_monitoring_error)
      }

      this.timekeeping = true

      const sql = plans.trySetCronTime(this.config.schema, this.config.cronMonitorIntervalSeconds)

      if (!this.stopped) {
        const { rows } = await this.db.executeSql(sql)

        if (!this.stopped && rows.length === 1) {
          await this.cron()
        }
      }
    } catch (err) {
      this.emit(this.events.error, err)
    } finally {
      this.timekeeping = false
    }
  }

  async cron () {
    const schedules = await this.getSchedules()

    const scheduled: types.JobInsert[] = []
    const stillBroken = new Set<string>()

    for (const { name, key, data, options, cron, timezone } of schedules) {
      let due: boolean

      try {
        due = this.shouldSendIt(cron, timezone)
      } catch (err) {
        // Evaluating one row must not decide the fate of the others. schedule() now rejects an
        // unusable time zone, but a row written by an earlier release — or straight into the table —
        // still throws here. This was a single filter() over every schedule, so one such row
        // propagated out of cron() and silently stopped scheduling for every queue in the
        // deployment, on every pass, until someone found the row. Skip it and warn instead, naming
        // the schedule so it is actually fixable.
        const warned = JSON.stringify([name, key, cron, timezone])

        stillBroken.add(warned)

        if (!this.warnedSchedules.has(warned)) {
          await emitAndPersistWarning(
            this.warningContext,
            WARNING_TYPES.INVALID_SCHEDULE,
            `Warning: schedule for queue "${name}" (key "${key}") could not be evaluated and was skipped: ${(err as Error).message}`,
            { queue: name, key, cron, timezone }
          )
        }

        continue
      }

      if (due) {
        // JSON rather than `${name}__${key}`: underscores are legal in both a queue name and a
        // schedule key, so the concatenation collapsed ('report_', 'daily') and ('report', '_daily')
        // onto one key and the 60s singleton then dropped whichever occurrence lost the race. An
        // instance still on the old format writes the old key, so a mixed-version deployment can
        // fire a schedule twice in the minute the rollout straddles.
        scheduled.push({ data: { name, key, data, options }, singletonKey: JSON.stringify([name, key]), singletonSeconds: 60 })
      }
    }

    this.warnedSchedules = stillBroken

    if (scheduled.length > 0 && !this.stopped) {
      await this.manager.insert(QUEUES.SEND_IT, scheduled)
    }
  }

  shouldSendIt (cron: string, tz: string) {
    const databaseTime = this.databaseTime

    const interval = CronExpressionParser.parse(cron, { tz, strict: false, currentDate: new Date(databaseTime) })

    const prevTime = interval.prev()

    const prevDiff = (databaseTime - prevTime.getTime()) / 1000

    return prevDiff < 60
  }

  // Reports a problem the send-it handler must survive. Node treats an `error` event with no
  // listener as a throw, and index.ts re-promotes this one onto the PgBoss instance, so a plain
  // emit() here could escape the handler, fail the send-it job and replay the whole batch, sending
  // every occurrence in it a second time.
  private reportSendItError (err: unknown): void {
    try {
      this.emit(this.events.error, err)
    } catch {
      // nothing left to report it to
    }
  }

  private async onSendIt (jobs: types.Job<ScheduledRequest>[]): Promise<void> {
    // async so a malformed payload rejects its own settlement rather than throwing synchronously
    // out of map() and taking the whole batch with it
    const results = await Promise.allSettled(jobs.map(async ({ data }) => {
      const { key, ...request } = data
      return await this.manager.send(request)
    }))

    // Keyed on (name, key) so a batch that spans two minute buckets for the same schedule resolves
    // to its latest occurrence. Feeding both to the UPDATE would let postgres pick either source
    // row, and last_job_id could end up naming the older job.
    const fired = new Map<string, FiredSchedule>()

    // Surface any failed forward so a lost cron tick isn't silent
    for (const [index, result] of results.entries()) {
      if (result.status === 'rejected') {
        this.reportSendItError(result.reason)
        continue
      }

      const { name, key } = jobs[index].data

      // send() resolves null when a throttle or queue policy dropped the job, so there is nothing
      // to point last_job_id at. `key` is absent on a payload written by an older instance.
      if (result.value && key !== undefined) {
        fired.set(JSON.stringify([name, key]), { name, key, jobId: result.value })
      }
    }

    if (fired.size > 0) {
      await this.setLastJobIds([...fired.values()])
    }
  }

  // Best effort: the schedule fired and the job exists, so failing to annotate the schedule row
  // must not fail the send-it job and replay the occurrence. Reported through `error` instead.
  private async setLastJobIds (fired: FiredSchedule[]): Promise<void> {
    try {
      const sql = plans.setScheduleLastJobIds(this.config.schema)
      await this.db.executeSql(sql, [JSON.stringify(fired)])
    } catch (err) {
      // Named, because a bare driver error here is indistinguishable from the forwarding failures
      // emitted above, and the two call for different responses: this one leaves the jobs created
      // and only the bookkeeping behind.
      const schedules = fired.map(({ name, key }) => `"${name}" (key "${key}")`).join(', ')
      const message = `Warning: schedules fired but their last job id could not be recorded for ${schedules}: ${(err as Error).message}`

      this.reportSendItError(new Error(message, { cause: err }))
    }
  }

  async getSchedules (name?: string, key?: string): Promise<types.Schedule[]> {
    let sql = plans.getSchedules(this.config.schema)
    let params: unknown[] = []

    if (name && key !== undefined) {
      sql = plans.getSchedulesByQueueAndKey(this.config.schema)
      params = [name, key]
    } else if (name) {
      sql = plans.getSchedulesByQueue(this.config.schema)
      params = [name]
    }

    const { rows } = await this.db.executeSql(sql, params)

    return rows
  }

  async getSchedule (name: string, key = ''): Promise<types.Schedule | null> {
    // Only that a name is present, and only because getSchedules() reads a falsy one as "every
    // schedule" and would hand back an arbitrary row as though it belonged to this key. Neither the
    // name nor the key is checked against the rules schedule() enforces on the way in: a value
    // those rules reject cannot have a row either, so `null` is the honest answer, and this stays a
    // drop-in for the `const [schedule] = await getSchedules(name, key)` it replaces rather than
    // throwing where that returns nothing.
    assert(name, 'Name is required')
    assert(typeof name === 'string', 'Name must be a string')

    const [schedule] = await this.getSchedules(name, key)

    return schedule ?? null
  }

  /**
   * The occurrences a cron expression produces, computed in process without touching the database
   * or the schedule table.
   *
   * `from` defaults to database time: this instance's clock plus the skew cached against the
   * database, the same reading the cron pass evaluates against, so a preview taken from an instance
   * that runs schedules lines up with what that instance will send. Skew is cached by the
   * timekeeper, which start() only runs when the instance was configured with scheduling enabled,
   * so anywhere else (a never-started instance, or the proxy, which defaults `schedule` to false)
   * it is zero and the default is this process's plain local clock. Pass `from` to be certain.
   *
   * Occurrences are strictly after `from`, so paging is a matter of passing the last one back in.
   *
   * The result describes the expression, not the delivery. The cron pass runs every
   * `cronMonitorIntervalSeconds` and matches an occurrence within the preceding 60 seconds, so a
   * job lands at or shortly after each listed time.
   */
  previewSchedule (cron: string, options: types.PreviewScheduleOptions = {}): Date[] {
    const { tz = 'UTC', count = PREVIEW_DEFAULT_COUNT } = options

    const from = options.from ?? new Date(this.databaseTime)

    assert(from instanceof Date && !Number.isNaN(from.getTime()), 'from must be a valid Date')

    // The expression before the count, so an out-of-range count cannot mask an expression that
    // could never be stored. `from` has to precede both: the parse reads it.
    const interval = parseRecurrence(cron, tz, from)

    assert(Number.isInteger(count) && count >= 1 && count <= PREVIEW_MAX_COUNT,
      `count must be an integer between 1 and ${PREVIEW_MAX_COUNT}`)

    const deadline = Date.now() + PREVIEW_TIME_BUDGET_MS
    const occurrences: Date[] = []

    while (occurrences.length < count) {
      occurrences.push(interval.next().toDate())

      if (occurrences.length < count && Date.now() > deadline) {
        throw new Error(`Gave up after ${PREVIEW_TIME_BUDGET_MS}ms with ${occurrences.length} of ${count} occurrences of "${cron}". Ask for fewer and page with \`from\`.`)
      }
    }

    return occurrences
  }

  async schedule (name: string, cron: string, data?: unknown, options: types.ScheduleOptions = {}): Promise<void> {
    const { tz = 'UTC', key = '', ...rest } = options

    assertRecurrence(cron, tz)

    Attorney.checkSendArgs([name, data, { ...rest }])
    Attorney.assertKey(key)

    try {
      const sql = plans.schedule(this.config.schema)
      await this.db.executeSql(sql, [name, key, cron, tz, data, options])
    } catch (err: any) {
      if (err.message.includes('foreign key')) {
        err.message = `Queue ${name} not found`
      }

      throw err
    }
  }

  async unschedule (name: string, key = ''): Promise<void> {
    const sql = plans.unschedule(this.config.schema)
    await this.db.executeSql(sql, [name, key])
  }
}

export default Timekeeper
