import assert from 'node:assert'
import { RRuleTemporal } from 'rrule-temporal'

import { assertTimezone } from './timezone.ts'

/**
 * The DTSTART a rule that carries none of its own recurs from.
 *
 * A recurrence rule is not a complete recurrence on its own: RFC 5545 takes the phase of every
 * INTERVAL, and the time of day of any component no BYxxx part pins, from DTSTART. Most callers
 * write `FREQ=DAILY;BYHOUR=9` and mean "every day at nine", so one has to be supplied, and it has
 * to be the same one in every process and every release: a schedule is evaluated by whichever
 * instance runs the pass, and an anchor of "now" would move the phase of `INTERVAL=2` on every
 * restart.
 *
 * Left floating (no TZID) so the schedule's own `tz` resolves it: the anchor is midnight in the
 * caller's zone rather than midnight in UTC.
 */
const EPOCH_DTSTART = '19700101T000000'

/**
 * The properties an expression may contain. Anything else is rejected rather than ignored: a
 * mistyped `DTSRAT` would otherwise be dropped silently and leave the rule anchored on the epoch,
 * which is a schedule that runs at the wrong time rather than one that reports a problem.
 */
const PROPERTIES = new Set(['DTSTART', 'RRULE', 'RDATE', 'EXDATE'])

/**
 * The recur rule parts of RFC 5545 3.3.10, plus RSCALE and SKIP from RFC 7529, each against the
 * option rrule-temporal parses it into.
 *
 * A part no parser reads is rejected rather than ignored: `BYHOURS=9` is not an hour of the day to
 * any of them, and evaluating it as `FREQ=DAILY` alone would put the job at midnight without a
 * word. The option a part lands in is what makes the same mistake in its value visible, since a
 * value out of range is dropped just as quietly (see assertValues).
 */
const PART_OPTIONS: Record<string, string> = {
  FREQ: 'freq',
  UNTIL: 'until',
  COUNT: 'count',
  INTERVAL: 'interval',
  BYSECOND: 'bySecond',
  BYMINUTE: 'byMinute',
  BYHOUR: 'byHour',
  BYDAY: 'byDay',
  BYMONTHDAY: 'byMonthDay',
  BYYEARDAY: 'byYearDay',
  BYWEEKNO: 'byWeekNo',
  BYMONTH: 'byMonth',
  BYSETPOS: 'bySetPos',
  WKST: 'wkst',
  RSCALE: 'rscale',
  SKIP: 'skip'
}

/** The property a line names, or nothing at all for a bare recurrence rule. */
const PROPERTY = /^([a-z][a-z0-9-]*)[;:]/i

/**
 * What tells a recurrence rule from a cron expression: an iCalendar property at the head of a line,
 * or the FREQ part every RRULE is required to carry. A cron expression has neither, and cannot: no
 * cron field contains `=`, `:` or `;`.
 *
 * Any line, not only the first. A calendar entry pasted in opens with a BEGIN line and one written
 * by hand may open with anything, and reading those as cron leaves the cron parser reporting
 * characters it has no name for, when what it was handed is a rule with something specific wrong
 * with it.
 */
const RRULE_SHAPE = /^[ \t]*[a-z][a-z0-9-]*[;:]|(?:^|[\s;])FREQ=/im

/** True if `expression` is a recurrence rule rather than a cron expression. */
export function isRrule (expression: string): boolean {
  return RRULE_SHAPE.test(expression)
}

/**
 * Rewrites an expression as the iCalendar text rrule-temporal parses, and rejects what it would
 * otherwise read past. Answers with the RRULE value as well, which is what assertValues judges.
 *
 * Two shapes reach this: the RRULE property's value on its own (`FREQ=DAILY;BYHOUR=9`), which is
 * what a caller reaching for a cron replacement writes, and the DTSTART, RRULE, RDATE and EXDATE
 * lines of a calendar entry. Both end up as the latter.
 */
function toIcs (expression: string): { ics: string, rule: string } {
  const lines: string[] = []

  // The RDATE and EXDATE values, judged once the loop is done: each has to have the same value type
  // as DTSTART, which may be named on a line below them.
  const dates: Array<[string, string]> = []

  let dtstart: string | null = null
  let rule: string | null = null

  // A line break followed by a space or tab is a fold, not a new property (RFC 5545 3.1), and an
  // export with a long EXDATE list arrives folded. Unfolded here so a continuation is not read as
  // a property of its own and rejected as one.
  for (const line of expression.replace(/\r?\n[ \t]/g, '').split(/\r?\n/)) {
    const text = line.trim()

    if (text.length === 0) {
      continue
    }

    const property = PROPERTY.exec(text)?.[1].toUpperCase()

    // No property name means the line is the rule itself, which is exactly the value half of an
    // RRULE line, so naming it is all that is needed to make the expression parseable.
    const name = property || 'RRULE'
    const value = property ? text.slice(text.indexOf(':') + 1) : text

    // A calendar entry is more than its recurrence: an export carries a UID, a SUMMARY, a DTEND and
    // often a VTIMEZONE block, none of which say anything about when a job should run. Rejected
    // rather than skipped over, since skipping whatever sits between the BEGIN and END lines is
    // what would let a mistyped DTSTART through.
    assert(name !== 'BEGIN' && name !== 'END',
      'rrule expression should be the DTSTART, RRULE, RDATE and EXDATE lines of a calendar entry, without the BEGIN and END lines around them')

    assert(PROPERTIES.has(name),
      `Unsupported property "${name}" in rrule expression. Supported properties: ${[...PROPERTIES].join(', ')}`)

    if (name === 'DTSTART') {
      // A second one does not merge with the first, it replaces it, so an expression carrying two
      // has one of them evaluated and the other quietly discarded.
      assert(dtstart === null, 'rrule expression has more than one DTSTART')
      dtstart = value
    }

    if (name === 'RRULE') {
      assert(rule === null, 'rrule expression has more than one RRULE')
      rule = value
      assertParts(value)
    }

    if (name === 'RDATE' || name === 'EXDATE') {
      dates.push([name, value])
    }

    lines.push(property ? text : `RRULE:${text}`)
  }

  // rrule-temporal recurs on an RRULE and nothing else, so an entry carrying only RDATE lines has
  // no recurrence for it to read. Named here rather than left to the parser, which answers a
  // missing rule with the FREQ part missing from it.
  assert(rule !== null, 'rrule expression has no RRULE to recur on')

  if (dtstart === null) {
    // COUNT counts from DTSTART, so on the epoch anchor every count worth having is long spent: the
    // rule parses, has no occurrence left, and the schedule it lands on never sends anything. Named
    // here rather than left to the check in assertRrule, which reports the same rule as spent
    // without saying what would fix it.
    assert(!/(^|;)COUNT=/i.test(rule),
      'rrule expression uses COUNT, which counts occurrences from DTSTART, so it needs a DTSTART of its own')

    lines.unshift(`DTSTART:${EPOCH_DTSTART}`)
  }

  assertDates(dates, dtstart ?? EPOCH_DTSTART)

  return { ics: lines.join('\n'), rule }
}

/** Rejects a recur rule part no parser reads, which would otherwise change the schedule silently. */
function assertParts (rule: string): void {
  const named = new Set<string>()

  for (const part of rule.split(';')) {
    if (part.length === 0) {
      continue
    }

    const [name, value = ''] = part.toUpperCase().split('=')

    // A part is NAME=VALUE, and the name on its own passes the allowlist below, so without this the
    // parser is handed a part with no value and dies inside on it.
    assert(value.length > 0, `rrule part "${part}" has no value`)

    // X- names are the extension mechanism RFC 5545 3.3.10 leaves open, so they are passed through
    // to the parser rather than judged here.
    assert(name in PART_OPTIONS || name.startsWith('X-'), `Unsupported part "${part}" in rrule expression`)

    // RFC 5545 3.3.10 gives a part one appearance. A second does not widen the first, it replaces
    // it, so `BYHOUR=9;BYHOUR=17` is a schedule that skips the morning without a word.
    assert(!named.has(name), `rrule expression has more than one ${name} part`)

    named.add(name)
  }
}

/**
 * Rejects an RDATE or EXDATE whose value type differs from DTSTART's.
 *
 * RFC 5545 3.8.5 requires the two to match, and a caller feels the mismatch: a date on its own has
 * no time of day, so rrule-temporal reads `EXDATE;VALUE=DATE:20261224` as midnight and sends the
 * 09:00 occurrence it was meant to exclude anyway. The parser reports the same mismatch for UNTIL
 * and not for these, and a holiday nobody excluded is a job at a time somebody ruled out, so it is
 * reported here.
 */
function assertDates (dates: Array<[string, string]>, dtstart: string): void {
  // A date time carries the T of RFC 5545 3.3.5; a date on its own is eight digits.
  const dtstartIsDate = !dtstart.includes('T')

  for (const [name, value] of dates) {
    for (const date of value.split(',')) {
      // The value it should have been, so the message says what to write rather than only what is
      // wrong with what was written.
      const expected = dtstartIsDate ? `a date such as ${date.slice(0, 8)}` : `a date time such as ${date}T090000`

      assert(date.includes('T') !== dtstartIsDate,
        `rrule ${name} "${date}" must have the same value type as DTSTART: ${expected}`)
    }
  }
}

/**
 * Rejects a part value the parser read past, which is the same silent wrong time an unknown part
 * name would be.
 *
 * rrule-temporal reports a value no reading of RFC 5545 allows (BYDAY=XX, INTERVAL=0) and drops one
 * that is merely out of range: `BYHOUR=25` leaves the rule with no BYHOUR at all, so the job runs at
 * the anchor's midnight rather than the hour it names, and `BYHOUR=9,25` keeps nine and loses the
 * second send of the day. Both show up in the options the rule was built with, which hold either
 * nothing for the part or fewer values than it was given.
 */
function assertValues (rule: string, built: RRuleTemporal): void {
  const options: Record<string, unknown> = { ...built.options() }

  for (const part of rule.split(';')) {
    const [name, value = ''] = part.split('=')
    const option = PART_OPTIONS[name.toUpperCase()]

    // An X- part has nothing to be compared against: the extension namespace is passed through to
    // the parser, which carries it in none of its options. Any other name assertParts has already
    // rejected.
    if (option === undefined) {
      continue
    }

    const parsed = options[option]
    const message = `Unsupported value in rrule part "${part}"`

    assert(parsed !== undefined, message)

    // A list keeps the values it could read and drops the rest, so the count is what shows one went
    // missing. Against the distinct values given, since the parser collapses a repeat.
    if (Array.isArray(parsed)) {
      assert(parsed.length === new Set(value.toUpperCase().split(',')).size, message)
    }
  }
}

function buildRule (ics: string, tz: string): RRuleTemporal {
  // strict enforces the combinations RFC 5545 3.3.10 forbids outright (BYMONTHDAY with a weekly
  // frequency, an ordinal BYDAY on anything but a monthly or yearly one). Without it a rule that no
  // two engines would agree on is evaluated anyway, and a schedule fires at a time its author has
  // no way to predict.
  return new RRuleTemporal({ rruleString: ics, tzid: tz, strict: true })
}

/**
 * The rule `expression` evaluates to in `tz`.
 *
 * Built per read rather than kept between them. An RRuleTemporal is immutable, so an instance could
 * be held for as long as its row is in the schedule table, but building one costs about a tenth of
 * a millisecond and a pass reads a schedule twice, so keeping the built rules saves a rule schedule
 * roughly 0.1ms a pass. The one caller that did read the same rule over and over, the preview walk,
 * builds it once and closes over it instead (see rruleWalker).
 */
function parseRule (expression: string, tz: string): RRuleTemporal {
  return buildRule(toIcs(expression).ics, tz)
}

/**
 * The first occurrence strictly after `after`, or null when the rule has none: an exhausted COUNT
 * or a passed UNTIL leaves a schedule with nothing left to send.
 *
 * The time zone comes from DTSTART when it names one, and from `tz` otherwise, which is both what
 * RFC 5545 says and what lets a rule written without a DTSTART behave like a cron expression on the
 * same schedule.
 */
export function nextOccurrence (expression: string, after: Date, tz: string): Date | null {
  const occurrence = parseRule(expression, tz).next(after)

  return occurrence === null ? null : toDate(occurrence)
}

/**
 * A walk of the occurrences after `from`, one call at a time, answering null once a finite rule has
 * run out and on every call after that.
 *
 * The rule is built once and the walk closes over it, which is the difference between a preview and
 * a pass: a pass reads a schedule twice and a preview asks for up to a thousand occurrences of one
 * expression, so building per occurrence would spend most of the caller's time budget on the parse.
 * A sparse rule near the budget then loses occurrences it would otherwise have returned, rather
 * than only milliseconds.
 */
export function rruleWalker (expression: string, tz: string, from: Date): () => Date | null {
  const rule = parseRule(expression, tz)

  let after = from

  return () => {
    const occurrence = rule.next(after)

    if (occurrence === null) {
      return null
    }

    after = toDate(occurrence)

    return after
  }
}

/**
 * Every occurrence in `after` to `until`, the lower bound excluded and the upper included, in order.
 *
 * The whole window rather than the first occurrence in it, because a rule can put two occurrences
 * closer together than the interval between passes: a calendar export pairing an hourly rule with
 * an RDATE seconds before one of its occurrences is the ordinary shape of that. A read answering
 * with a single point leaves the second of the two visible only while no pass lands between them,
 * so it is dropped rather than throttled, and reading backwards from now only moves the loss to
 * the other one.
 */
export function occurrencesInWindow (expression: string, after: Date, until: Date, tz: string): Date[] {
  // between() excludes both ends. Excluding the lower one is right: an occurrence exactly on it is
  // one the window before ended on. The upper one is the pass's own reading of the clock, and an
  // occurrence landing on it is due now rather than next time, so the bound is nudged past it.
  const occurrences = parseRule(expression, tz).between(after, new Date(until.getTime() + 1))

  return occurrences.map(toDate)
}

/**
 * An occurrence as the Date the cron pass compares against.
 *
 * The nanoseconds a Temporal instant carries have nowhere to go, and nothing is lost: an iCalendar
 * DTSTART is second-precision, and every occurrence is derived from it.
 */
function toDate (occurrence: { epochMilliseconds: number }): Date {
  return new Date(occurrence.epochMilliseconds)
}

/**
 * How wide the first backwards step of a catch-up read is, and the factor each one after it grows
 * by. An hour is the floor because no rule recurs more often than once a second, so a step that
 * narrow can never hold more than 3600 occurrences; four steps of growth reach a month, so a sparse
 * expression over a long outage is found in a handful of reads.
 */
const STEP_SECONDS = 60 * 60
const STEP_GROWTH = 4

/**
 * The most recent occurrence in `after` to `until`, the lower bound excluded and the upper
 * included, or null when the range holds none.
 *
 * Read in steps that widen backwards from `until` rather than in one between() over the whole
 * range, because the range a catch-up reads is as wide as the gap it covers: a single between()
 * over three weeks of a per-second rule materializes every occurrence in those three weeks to hand
 * back the one the caller will use. A dense expression answers out of the first step, and a sparse
 * one reaches `after` in a few more, since each step is four times the last.
 *
 * Widening is a guess about density, and a rule that is empty near `until` and dense further back
 * defeats it: the steps over the empty stretch grow until one spans more occurrences than
 * rrule-temporal will generate in a single call, and it throws rather than truncating. A per-second
 * rule whose UNTIL passed a day or more before the gap opened is that shape, and an ordinary one.
 * So a step that overruns is halved and read again, and the width it overran at becomes a ceiling no
 * later step goes back over. A step at the floor that still overruns is the expression rather than
 * the width, and the caller hears about it instead.
 *
 * Narrowing is what the widening costs, and the pass is blocked on it: with the ceiling at the floor
 * the read walks an hour of range per step, so it costs what it searches rather than what it
 * returns. Paying that takes an overrun and a long empty stretch behind it, which is a dense rule
 * that is dense only rarely. `FREQ=SECONDLY;BYMONTHDAY=1` is that shape, and it answers in around
 * 1.3 seconds, bounded by the distance back to the nearest occurrence rather than by the width of
 * the gap, so 30 days and 90 cost the same. A rule already spent when the range opens never
 * overruns, keeps its widening, and answers null in 10ms.
 *
 * Deliberately not rrule-temporal's previous(). That walks backwards from a phase-aligned DTSTART,
 * and an RDATE is an absolute instant rather than a phase, so a rule carrying one answers with the
 * RDATE in place of the rule occurrence that follows it and loses every occurrence in between.
 * between() has no such problem, and it is what the due window has always read.
 */
export function latestOccurrenceBefore (expression: string, after: Date, until: Date, tz: string): Date | null {
  const rule = parseRule(expression, tz)
  const floor = STEP_SECONDS * 1000
  const start = after.getTime()

  let upper = until.getTime()
  let width = floor
  let ceiling = Number.POSITIVE_INFINITY

  while (upper > start) {
    const lower = Math.max(start, upper - width)

    let step: ReturnType<typeof rule.between>

    try {
      // between() excludes both ends. The upper bound is nudged past so an occurrence on it is
      // included, and the lower one stays excluded because it is the next step's upper bound, which
      // is also what leaves the range's own lower bound, `after`, excluded.
      step = rule.between(new Date(lower), new Date(upper + 1))
    } catch (err) {
      if (upper - lower <= floor) {
        throw err
      }

      ceiling = Math.max(floor, Math.floor((upper - lower) / 2))
      width = ceiling

      continue
    }

    if (step.length > 0) {
      return toDate(step[step.length - 1]!)
    }

    upper = lower
    width = Math.min(width * STEP_GROWTH, ceiling)
  }

  return null
}

/**
 * Rejects an expression or time zone no pass could evaluate, so a schedule that cannot be read is
 * reported to the caller rather than to a warning on every later pass.
 */
export function assertRrule (expression: string, tz: string): void {
  const { ics, rule } = toIcs(expression)

  // The expression against a zone known to be usable, so a caller who got both wrong hears about
  // the rule, which is the order the cron path judges its two in as well.
  assertValues(rule, buildRule(ics, 'UTC'))

  // Judged on its own rather than inferred from a second build in the caller's zone: rrule-temporal
  // ignores the zone it is handed whenever DTSTART names one, so that build reports nothing for the
  // expressions most likely to name a zone at all.
  assertTimezone(tz)
}

/**
 * Rejects a rule with nothing left to send, which is the one failure a caller of `schedule()`
 * cannot see: the row sits in the table, every pass evaluates it, and no job is ever sent. A spent
 * COUNT and a passed UNTIL both land here.
 *
 * In the schedule's own zone, since that is the one the pass evaluates, and from `from`, which is
 * the clock the pass reads, so a rule expiring inside the skew window is judged the way it will be
 * evaluated rather than the way one process happens to see the time.
 *
 * Separate from assertRrule because a preview makes no such demand: a rule that has finished has an
 * empty list of occurrences left, which is an answer, while a schedule that quietly does nothing
 * forever is not.
 */
export function assertRruleSends (expression: string, tz: string, from: Date): void {
  assert(nextOccurrence(expression, from, tz) !== null,
    'rrule expression has no occurrence left, so the schedule would never send a job')
}
