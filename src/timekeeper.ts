import { CronExpressionParser } from 'cron-parser'
import assert from 'node:assert'
import EventEmitter from 'node:events'

import * as Attorney from './attorney.ts'
import type Manager from './manager.ts'
import * as plans from './plans.ts'
import { isRrule, nextOccurrence, occurrencesBefore, occurrencesInWindow, assertRrule, assertRruleSends } from './rrule.ts'
import { assertTimezone } from './timezone.ts'
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
  INVALID_SCHEDULE: 'invalid_schedule',
  MISSED_OCCURRENCES_CAPPED: 'missed_occurrences_capped'
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
// came from, so the handler can record the job it produced. `slot` is the throttle slot that
// occurrence was filed in, which is how the handler tells the runs in a catch-up batch apart and
// records the latest of them. Both are absent on rows written by an instance older than 12.31.0,
// and `slot` is absent on a cron occurrence in the due window, which the insert files from its own
// clock, so the handler treats them as optional rather than required.
type ScheduledRequest = types.Request & { key?: string, slot?: string }

// One schedule occurrence that produced a job, as handed to plans.setScheduleLastJobIds. camelCase
// to match the recordset column list the plan quotes, which is how every other JSON payload crossing
// into SQL is shaped.
type FiredSchedule = { name: string, key: string, jobId: string }

// How long an occurrence stays due, and the width of the throttle slot a forwarded job is filed in.
// One value because the two have to agree: a window wider than the slot lets two slots claim the
// same occurrence and send it twice, and a slot wider than the window collapses two occurrences a
// window apart into one job.
const OCCURRENCE_WINDOW_SECONDS = 60

// The most occurrences one schedule catches up on in a single pass under `missed: 'all'`.
//
// A gap has no natural size: a database resumed after a month owes a per-minute schedule some forty
// thousand jobs, and creating them is the least of it, since every one of them is then a job a
// worker has to run. The cap is also what bounds the walk, which is synchronous like the rest of
// the pass: 1000 occurrences of a dense expression cost around a tenth of a second on the event
// loop, and a gap ten times wider costs the same.
//
// The newest are the ones kept. A schedule catching up on a month has a month of work behind it
// either way, and the recent end of that is the part still worth doing.
const MAX_MISSED_OCCURRENCES = 1000

/** The policy names, for the check schedule() performs and the one the pass performs. */
const MISSED_POLICIES = Object.values(plans.SCHEDULE_MISSED_POLICIES)

// __singletonSlot is an internal field of the insert path rather than a documented send option, so
// the forwarded job widens JobInsert here rather than the type widening for everyone. The prefix is
// what keeps it internal: insert() stringifies caller objects straight into the recordset, so an
// unprefixed name would be a live, undeclared option on the public path.
type ForwardedJob = types.JobInsert & { __singletonSlot?: string }

/** What a schedule has come due for, and the format its expression was read in to find out. */
type DueOccurrences = { kind: types.ScheduleKind, occurrences: Date[] }

/**
 * The throttle slot an instant falls in, as the timestamp the insert files a job under.
 *
 * `singleton_on` is a timestamp without a zone holding UTC wall time, which is what the insert's own
 * slot expression computes from `now()` for a cron occurrence, so a slot measured here is rendered
 * in the same terms.
 */
function throttleSlot (instant: Date): string {
  const width = OCCURRENCE_WINDOW_SECONDS * 1000

  return new Date(Math.floor(instant.getTime() / width) * width).toISOString().replace('T', ' ').slice(0, 19)
}

/**
 * A timestamp column as the driver in front of this instance hands it back: node-postgres parses
 * one into a Date, and an adapter over a backend that speaks JSON hands back the string it was
 * sent. Null for anything that is neither, which is what an absent column reads as.
 */
function toTime (value: unknown): number | null {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.getTime()
  }

  if (typeof value === 'string' || typeof value === 'number') {
    const time = new Date(value).getTime()

    return Number.isNaN(time) ? null : time
  }

  return null
}

/**
 * The catch-up policy a schedule row asks for, which is `skip` for every schedule written before
 * the option existed: a pass sends what the due window holds and nothing else.
 *
 * A value schedule() would have refused can still be on a row, written into the table with SQL or
 * by a release that names a policy this one does not, and reads as `skip` as well. The pass sends
 * what it has always sent rather than picking one of the other two on the row's behalf.
 */
function missedPolicy (options: types.ScheduleOptions | undefined): types.ScheduleMissedPolicy {
  const missed = options?.missed

  return missed !== undefined && MISSED_POLICIES.includes(missed)
    ? missed
    : plans.SCHEDULE_MISSED_POLICIES.skip
}

/**
 * Rejects a catch-up policy no pass would honor, at the one point a caller can be told about it: a
 * value the pass does not recognize reads as `skip`, so a typo left unchecked here is a schedule
 * that silently never catches up on anything.
 */
function assertMissedPolicy (missed: unknown): void {
  assert(missed === undefined || MISSED_POLICIES.includes(missed as types.ScheduleMissedPolicy),
    `missed must be one of: ${MISSED_POLICIES.join(', ')}`)
}

/**
 * Parses a cron expression the way the cron pass will evaluate it, mapping cron-parser's failures
 * onto messages that name the input actually at fault. Deliberately reuses cron-parser rather than
 * an independent Intl check, so what schedule() accepts is exactly what the cron pass can evaluate.
 *
 * The expression is checked first, against UTC, so a bad expression reports as one rather than as a
 * time zone problem. That first parse tolerates any zone only because cron-parser validates `tz`
 * lazily: with no reference date it never constructs a CronDate, so a typo like 'America/New_Yrok'
 * survives it. assertTimezone forces that construction and names the zone, which is also what the
 * rrule path judges its zone with, so the two report an unusable one in the same words.
 *
 * The returned interval is the one the caller wants anyway, so the walk costs a single parse.
 */
function parseRecurrence (cron: string, tz: string, currentDate: Date) {
  CronExpressionParser.parse(cron, { tz: 'UTC', strict: false })

  assertTimezone(tz)

  return CronExpressionParser.parse(cron, { tz, strict: false, currentDate })
}

/**
 * Validates a recurrence in whichever of the two formats it is written, so previewSchedule() and
 * schedule() reject exactly the same expressions.
 */
function assertRecurrence (expression: string, tz: string): void {
  if (isRrule(expression)) {
    assertRrule(expression, tz)
  } else {
    parseRecurrence(expression, tz, new Date())
  }
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
          // The claim answers with the timestamp it replaced, which is when an instance last ran a
          // pass. Anything older than the due window between then and now is a gap no pass covered,
          // and a schedule's `missed` policy decides what it owes for it.
          await this.cron(rows[0].priorCronOn)
        }
      }
    } catch (err) {
      this.emit(this.events.error, err)
    } finally {
      this.timekeeping = false
    }
  }

  /**
   * `priorCronOn` is when an instance last ran a pass, as the claim in onCron() read it off the row
   * it advanced. Null on a database no pass has run against, and left null by a caller that does
   * not know: either way no schedule has a gap to catch up on and the pass sends the due window,
   * which is what every release before catch-up sent.
   */
  async cron (priorCronOn: unknown = null) {
    const schedules = await this.getSchedules()

    const scheduled: ForwardedJob[] = []
    const stillBroken = new Set<string>()

    // Rows whose stored kind disagrees with the expression on them, as found out by reading the
    // expression the other way. Relabelled once the pass has sent what it owes.
    const relabelled: Array<Pick<types.Schedule, 'name' | 'key' | 'kind'>> = []

    // One instant for the whole pass, so every schedule is judged against the same clock and the
    // throttle slot of a forwarded job is measured from the same place its occurrence was.
    const databaseTime = this.databaseTime

    // Where the due window opens, and with it where a gap ends: an occurrence inside the window is
    // due now, and one older than it came due while nothing was looking.
    const windowStart = databaseTime - OCCURRENCE_WINDOW_SECONDS * 1000

    const lastPass = toTime(priorCronOn)

    for (const schedule of schedules) {
      const { name, key, data, options, kind, cron, timezone } = schedule

      let due: DueOccurrences
      let missed: Date[]

      try {
        due = this.dueOccurrences(cron, kind, timezone, databaseTime)

        // In the same try as the due window: both read the same expression, so whatever makes one
        // unreadable makes the other unreadable, and the row is reported once rather than twice. A
        // gap closes on the next pass whatever happens here, since the claim has already moved.
        missed = await this.missedOccurrences(schedule, due.kind, lastPass, windowStart)
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

      if (due.kind !== kind) {
        relabelled.push({ name, key, kind: due.kind })
      }

      // The payload carries the schedule's key beside its queue name, so the send-it handler knows
      // which row an occurrence came from and can record the job it produced.
      //
      // A JSON singleton key rather than `${name}__${key}`: underscores are legal in both a queue
      // name and a schedule key, so the concatenation collapsed ('report_', 'daily') and
      // ('report', '_daily') onto one key and the 60s singleton then dropped whichever occurrence
      // lost the race. An instance still on the old format writes the old key, so a mixed-version
      // deployment can fire a schedule twice in the minute the rollout straddles.
      const forwarded = { data: { name, key, data, options }, singletonKey: JSON.stringify([name, key]) }

      // A recurrence rule can put an occurrence anywhere in the minute, and a slot measured from
      // insert time would then straddle it: two passes on either side of a slot boundary both find
      // the occurrence inside the window and file it in a slot of their own, sending it twice. So a
      // rule occurrence names the slot it falls in outright. An offset from the insert's own now()
      // would not pin it: everything between reading the clock here and the insert committing
      // counts towards the shifted instant, which lands in the next slot whenever that adds up to a
      // boundary crossing.
      //
      // A missed occurrence names its slot for that reason and one more: it is older than the
      // window, so a slot off insert time would file the whole backlog in the slot the pass runs in
      // and collapse it into a single job. Every slot a missed occurrence names is older than the
      // slot insert time computes, so none of them can collide with the cron job filed below.
      //
      // One job per slot rather than one per occurrence, which is the resolution the docs promise:
      // a rule finer than a slot sends a job a slot, and two occurrences inside one window that
      // fall in slots of their own each send.
      const slots = new Set(missed.map(throttleSlot))

      if (due.kind === plans.SCHEDULE_KINDS.rrule) {
        // Through the set the missed occurrences went through, since the window's lower bound falls
        // inside a slot rather than on one: an occurrence on the bound is missed, one a millisecond
        // later is due, and both belong to the same slot and so to the same job.
        for (const occurrence of due.occurrences) {
          slots.add(throttleSlot(occurrence))
        }
      }

      for (const slot of slots) {
        scheduled.push({ ...forwarded, data: { ...forwarded.data, slot }, __singletonSlot: slot })
      }

      // Anything not read as a rule is read as cron, which is what a row carrying no kind at all
      // means: the column defaults to cron, and a reader that cannot see it reads the row the way
      // every release before the column did.
      if (due.kind !== plans.SCHEDULE_KINDS.rrule && due.occurrences.length > 0) {
        // A cron occurrence keeps the slot every release has always filed it in, since an instance
        // still running an older one during a rolling upgrade computes that slot and nothing else,
        // and a slot the two disagree on collapses nothing.
        scheduled.push({ ...forwarded, singletonSeconds: OCCURRENCE_WINDOW_SECONDS })
      }
    }

    this.warnedSchedules = stillBroken

    if (scheduled.length > 0 && !this.stopped) {
      await this.manager.insert(QUEUES.SEND_IT, scheduled)
    }

    // After the sends, so a failed relabel cannot cost an occurrence. Nothing depends on the write:
    // the fallback in dueOccurrences fires the row either way. What it buys is getSchedules() no
    // longer reporting a format the expression is not in, and the row leaving that fallback path.
    if (relabelled.length > 0 && !this.stopped) {
      await this.db.executeSql(plans.setScheduleKinds(this.config.schema), [JSON.stringify(relabelled)])
    }
  }

  shouldSendIt (expression: string, tz: string, kind: types.ScheduleKind = plans.SCHEDULE_KINDS.cron) {
    return this.dueOccurrences(expression, kind, tz).occurrences.length > 0
  }

  /**
   * The occurrences a schedule has come due for, and the format they were read in.
   *
   * `kind` says how to read the expression, and comes off the schedule row: the format was settled
   * when the schedule was written, so a pass reads the expression the one way its author meant it
   * rather than guessing again every 30 seconds.
   *
   * The column is a hint rather than a verdict, though, because two ordinary upgrade paths leave it
   * disagreeing with the expression beside it. A 12.30.x instance's `schedule()` does not name the
   * column, so an upsert from one during a rolling upgrade replaces the expression and leaves
   * whatever kind a newer instance last wrote; a v41 rollback drops the column, and the re-upgrade
   * labels every row from its default. Either way the row reads fine and never fires again. So when
   * an expression cannot be read the way the column says, and is written the other way, it is read
   * the way it is written: one regex, on a path that was already about to give up.
   */
  private dueOccurrences (expression: string, kind: types.ScheduleKind, tz: string, databaseTime = this.databaseTime): DueOccurrences {
    try {
      return { kind, occurrences: this.readOccurrences(expression, kind, tz, databaseTime) }
    } catch (err) {
      const detected: types.ScheduleKind = isRrule(expression) ? plans.SCHEDULE_KINDS.rrule : plans.SCHEDULE_KINDS.cron

      // The column and the expression agree, so the expression itself is what is wrong with the row,
      // and the caller names it in a warning.
      if (detected === kind) {
        throw err
      }

      return { kind: detected, occurrences: this.readOccurrences(expression, detected, tz, databaseTime) }
    }
  }

  /**
   * What a schedule owes for the gap since the last pass, oldest first, as its `missed` policy asks
   * for it. Empty for every schedule under the default policy, and for every schedule at all in a
   * deployment whose passes keep running.
   *
   * The gap is (lastPass, windowStart]: older than the due window, so no pass has sent it, and
   * newer than the moment an instance last ran a pass, so no pass has skipped it either. A pass
   * claims at most `cronMonitorIntervalSeconds` after the one before it, 45 seconds at the
   * configurable ceiling, against a 60-second window, so the range is empty while passes keep
   * running and fills up when they stop: a deployment that is down, between deploys, or running
   * with scheduling switched off.
   *
   * Bounded below by the row's own `created_on` as well, so a schedule written during the gap does
   * not start life owing the occurrences of an expression that was not in the table yet. Not by
   * `updated_on`, which a deployment calling schedule() on every boot rewrites on the way up: that
   * would leave the policy with nothing to catch up on in precisely the case it exists for.
   */
  private async missedOccurrences (schedule: types.Schedule, kind: types.ScheduleKind, lastPass: number | null, windowStart: number): Promise<Date[]> {
    const { name, key, cron, timezone, options, createdOn } = schedule

    const policy = missedPolicy(options)

    if (policy === plans.SCHEDULE_MISSED_POLICIES.skip || lastPass === null) {
      return []
    }

    const from = Math.max(lastPass, toTime(createdOn) ?? lastPass)

    if (from >= windowStart) {
      return []
    }

    // `once` is one job however deep the backlog, which is what a schedule whose job reads the
    // current state of the world wants: a report that missed three nights is one report.
    const limit = policy === plans.SCHEDULE_MISSED_POLICIES.once ? 1 : MAX_MISSED_OCCURRENCES

    // One past the limit, so hitting it is told apart from landing on it exactly, and the walk pays
    // for a single extra occurrence to know which it was.
    const occurrences = this.readOccurrencesBefore(cron, kind, timezone, new Date(from), new Date(windowStart), limit + 1)

    if (occurrences.length > limit) {
      occurrences.length = limit

      // Only under `all`, which is the policy that asked for every occurrence and is not getting
      // them. A deeper backlog under `once` is the policy working as documented.
      if (policy === plans.SCHEDULE_MISSED_POLICIES.all) {
        await emitAndPersistWarning(
          this.warningContext,
          WARNING_TYPES.MISSED_OCCURRENCES_CAPPED,
          `Warning: schedule for queue "${name}" (key "${key}") came due more than ${limit} times while no cron pass ran; the ${limit} most recent were sent and the rest dropped.`,
          { queue: name, key, cron, timezone, limit, since: new Date(from).toISOString() }
        )
      }
    }

    // Chronological, so a backlog reaches the send-it queue in the order it came due. Nothing
    // downstream promises to run it in that order: the jobs are created by one insert and share a
    // created_on, so a worker picking two of them up orders them however the fetch does.
    return occurrences.reverse()
  }

  /**
   * The occurrences an expression produces in (after, until], newest first, at most `limit` of
   * them, read as `kind` says to read it.
   *
   * Backwards and bounded rather than a walk forwards over the range: the range is as wide as the
   * gap it covers, the walk is synchronous like the rest of the pass, and what it costs has to
   * follow the number of jobs it will send rather than the length of an outage.
   */
  private readOccurrencesBefore (expression: string, kind: types.ScheduleKind, tz: string, after: Date, until: Date, limit: number): Date[] {
    if (kind === plans.SCHEDULE_KINDS.rrule) {
      return occurrencesBefore(expression, after, until, tz, limit)
    }

    // cron-parser's prev() answers strictly before its reference date, so the reference is a
    // millisecond past `until` to leave the upper bound included. That is where the due window's
    // own lower bound leaves off, and an occurrence exactly on it belongs to one range or the
    // other, never both.
    const interval = CronExpressionParser.parse(expression, { tz, strict: false, currentDate: new Date(until.getTime() + 1) })

    const occurrences: Date[] = []

    while (occurrences.length < limit) {
      const occurrence = interval.prev().toDate()

      if (occurrence.getTime() <= after.getTime()) {
        break
      }

      occurrences.push(occurrence)
    }

    return occurrences
  }

  /**
   * Every occurrence of an expression inside the due window, read as `kind` says to read it.
   *
   * Due means "an occurrence in the last minute", whatever the pass interval: a pass runs every
   * `cronMonitorIntervalSeconds` (30 by default), so the window has to be wide enough that an
   * occurrence is still due when the next pass reaches it, and the throttle slot of the forwarded
   * job is what keeps the passes that follow from sending it a second time.
   *
   * The window rather than its most recent point, since a rule can put two occurrences inside it
   * and a read that answers with one of them drops the other. A cron expression cannot: its finest
   * resolution is a second, and consecutive occurrences a second apart share a throttle slot, so
   * only the most recent one can produce a job.
   */
  private readOccurrences (expression: string, kind: types.ScheduleKind, tz: string, databaseTime: number): Date[] {
    const window = new Date(databaseTime - OCCURRENCE_WINDOW_SECONDS * 1000)

    if (kind === plans.SCHEDULE_KINDS.rrule) {
      return occurrencesInWindow(expression, window, new Date(databaseTime), tz)
    }

    const interval = CronExpressionParser.parse(expression, { tz, strict: false, currentDate: new Date(databaseTime) })

    const previous = interval.prev().toDate()

    return previous.getTime() > window.getTime() ? [previous] : []
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
      // key and slot are the pass's own bookkeeping, read below rather than sent: send() takes the
      // request the schedule row described and nothing else.
      const { key, slot, ...request } = data
      return await this.manager.send(request)
    }))

    // Keyed on (name, key) so a batch holding more than one occurrence of the same schedule
    // resolves to its latest. Feeding several to the UPDATE would let postgres pick any of the
    // source rows, and last_job_id could end up naming an older job. A batch holds more than one
    // whenever it spans two minute buckets, and a schedule catching up on a gap can put a job per
    // missed occurrence in it, which the fetch hands over in no particular order: the slot each
    // occurrence was filed in is what orders them here.
    const fired = new Map<string, { record: FiredSchedule, slot: string }>()

    // Surface any failed forward so a lost cron tick isn't silent
    for (const [index, result] of results.entries()) {
      if (result.status === 'rejected') {
        this.reportSendItError(result.reason)
        continue
      }

      const { name, key, slot } = jobs[index].data

      // send() resolves null when a throttle or queue policy dropped the job, so there is nothing
      // to point last_job_id at. `key` is absent on a payload written by an older instance.
      if (result.value === null || key === undefined) {
        continue
      }

      const id = JSON.stringify([name, key])

      // A cron occurrence in the due window names no slot, since the insert files it from its own
      // clock. That slot is the one the pass is running in, which is later than every slot a
      // catch-up occurrence can name, so the current one stands in for it.
      const filed = slot ?? throttleSlot(new Date(this.databaseTime))

      const latest = fired.get(id)

      if (latest === undefined || latest.slot <= filed) {
        fired.set(id, { record: { name, key, jobId: result.value }, slot: filed })
      }
    }

    if (fired.size > 0) {
      await this.setLastJobIds([...fired.values()].map(({ record }) => record))
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
   * A walk of an expression's occurrences after `from`, one call at a time, answering null once a
   * finite rule has run out.
   *
   * Validating is part of it, since neither format can be walked without being parsed: cron-parser
   * carries its own cursor from the reference date it was built with, and a rule is asked for the
   * occurrence after the last one handed back.
   */
  private occurrenceWalker (expression: string, tz: string, from: Date): () => Date | null {
    if (isRrule(expression)) {
      assertRrule(expression, tz)

      let after = from

      return () => {
        const occurrence = nextOccurrence(expression, after, tz)

        if (occurrence !== null) {
          after = occurrence
        }

        return occurrence
      }
    }

    const interval = parseRecurrence(expression, tz, from)

    return () => interval.next().toDate()
  }

  /**
   * The occurrences an expression produces, in either format, computed in process without touching
   * the database or the schedule table.
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
    // could never be stored. `from` has to precede both: the walk reads it.
    const next = this.occurrenceWalker(cron, tz, from)

    assert(Number.isInteger(count) && count >= 1 && count <= PREVIEW_MAX_COUNT,
      `count must be an integer between 1 and ${PREVIEW_MAX_COUNT}`)

    const deadline = Date.now() + PREVIEW_TIME_BUDGET_MS
    const occurrences: Date[] = []

    while (occurrences.length < count) {
      const occurrence = next()

      // A finite rule runs out, and a list shorter than `count` is the honest answer for one that
      // has. schedule() is where a rule with nothing left to send is refused instead.
      if (occurrence === null) {
        break
      }

      occurrences.push(occurrence)

      if (occurrences.length < count && Date.now() > deadline) {
        throw new Error(`Gave up after ${PREVIEW_TIME_BUDGET_MS}ms with ${occurrences.length} of ${count} occurrences of "${cron}". Ask for fewer and page with \`from\`.`)
      }
    }

    return occurrences
  }

  async schedule (name: string, cron: string, data?: unknown, options: types.ScheduleOptions = {}): Promise<void> {
    // `missed` comes out with tz and key: it tells the pass what to do about a gap and is no more a
    // send option than they are, so the send-option check below is not handed it.
    const { tz = 'UTC', key = '', missed, ...rest } = options

    // The one place the format of an expression is decided. Every reader takes it from the stored
    // kind instead, so a schedule cannot be validated as one format and later evaluated as the
    // other, and a row can say what it is without anyone parsing it.
    const kind: types.ScheduleKind = isRrule(cron) ? plans.SCHEDULE_KINDS.rrule : plans.SCHEDULE_KINDS.cron

    assertRecurrence(cron, tz)

    // A rule, unlike a cron expression, can have nothing left to send, which is the one failure a
    // caller cannot see: the row sits in the table, every pass evaluates it, and no job is ever
    // sent. Judged from the database's clock, since that is the one the pass reads, so a rule
    // expiring inside the skew window is judged the way it will be evaluated. previewSchedule()
    // makes no such demand, since an empty list is the honest answer for a rule that has finished.
    if (kind === plans.SCHEDULE_KINDS.rrule) {
      assertRruleSends(cron, tz, new Date(this.databaseTime))
    }

    Attorney.checkSendArgs([name, data, { ...rest }])
    Attorney.assertKey(key)
    assertMissedPolicy(missed)

    try {
      const sql = plans.schedule(this.config.schema)
      await this.db.executeSql(sql, [name, key, kind, cron, tz, data, options])
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
