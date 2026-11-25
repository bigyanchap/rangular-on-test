/**
 * @rangular-framework/runtime, small pure-TypeScript helpers for generated apps.
 * EventEmitter (for @Output), the signals core (signal, computed,
 * effect, untracked and the SignalRenderTracker React bridge), a DI Container,
 * the ReactiveService base class generated services extend (Stage 3),
 * HttpClient over fetch, returning real RxJS Observables (Stage 3;
 * the RxJS adoption replaced the old Observable-lite shape, so every
 * stream this package hands out takes the full operator library),
 * and the forms core (Stage 8): Validators, FormControl and FormGroup, mirroring
 * @angular/forms semantics (Stage 8), and the routing core (Stage 9):
 * matchRoute, RgNavigator, Router and ActivatedRoute, mirroring
 * @angular/router, and the snackbar service (Stage 10): SnackBar and
 * SnackBarRef, mirroring @angular/material's MatSnackBar. No react or
 * react-native imports live in this file: generated components bridge
 * services to React themselves (useSyncExternalStore over
 * __subscribe / __getVersion), so this package stays environment-free
 * and unit-testable.
 */

import { BehaviorSubject, Observable, Subject } from 'rxjs';
import { map, merge } from 'rxjs';

export type { Subscription } from 'rxjs';

export type Listener<T> = (value: T) => void;

/**
 * An output emitter, as in Angular: an EventEmitter IS a Subject, so
 * it can be subscribed, piped and composed like any other stream
 * (the RxJS adoption made this literally true; before it, the class
 * was a small listener set shaped like one). `emit` is the Angular
 * spelling of `next`. `on` and `off` stay as the framework's older
 * convenience pair: `on` subscribes and returns an unsubscribe
 * function, `off` removes a listener registered through `on`.
 */
export class EventEmitter<T = void> extends Subject<T> {
  private readonly tracked = new Map<Listener<T>, { unsubscribe(): void }>();

  /** Subscribe; returns an unsubscribe function. */
  on(listener: Listener<T>): () => void {
    const sub = this.subscribe(listener);
    this.tracked.set(listener, sub);
    return () => this.off(listener);
  }

  off(listener: Listener<T>): void {
    this.tracked.get(listener)?.unsubscribe();
    this.tracked.delete(listener);
  }

  emit(value: T): void {
    this.next(value);
  }

  get listenerCount(): number {
    return this.observers.length;
  }
}

export interface Signal<T> {
  (): T;
}

/** A signal whose value can be written, as in Angular. */
export interface WritableSignal<T> extends Signal<T> {
  set(value: T): void;
  update(fn: (value: T) => T): void;
  asReadonly(): Signal<T>;
}

export interface EffectRef {
  destroy(): void;
}

/**
 * The reactive core (signals adoption): callable signals, computed
 * values and effects with automatic dependency tracking, mirroring
 * @angular/core semantics. A signal is a producer; computed values and
 * effects are consumers that record exactly which producers they read,
 * with the version each producer had at the read. A write bumps the
 * producer's version and notifies its consumers: computed consumers
 * mark themselves stale and recompute lazily on the next read (pull),
 * so a chain never observes a half-updated mix, and a computed whose
 * inputs settle to the same value keeps its version, so its own
 * consumers do not recompute either. Effects re-run through a queue
 * flushed on a microtask (flushEffects runs it synchronously), never
 * inside the write itself. Until this adoption the runtime carried a
 * manual get/set/subscribe helper under the signal name; it had no
 * call sites and is superseded by this core.
 */
interface ProducerNode {
  version: number;
  consumers: Set<ConsumerNode>;
}

interface ConsumerNode {
  producers: Map<ProducerNode, number>;
  onProducerChanged(): void;
}

let activeConsumer: ConsumerNode | null = null;
let computedDepth = 0;

function trackRead(producer: ProducerNode): void {
  if (!activeConsumer) return;
  activeConsumer.producers.set(producer, producer.version);
  producer.consumers.add(activeConsumer);
}

function detachConsumer(consumer: ConsumerNode): void {
  for (const producer of consumer.producers.keys()) producer.consumers.delete(consumer);
  consumer.producers.clear();
}

function notifyConsumers(producer: ProducerNode): void {
  for (const consumer of [...producer.consumers]) consumer.onProducerChanged();
}

class SignalNode<T> implements ProducerNode {
  version = 0;
  consumers = new Set<ConsumerNode>();
  value: T;
  readonly equal: (a: T, b: T) => boolean;
  constructor(initial: T, equal: (a: T, b: T) => boolean) {
    this.value = initial;
    this.equal = equal;
  }
}

/** Create a writable signal holding `initial`, as in Angular. */
export function signal<T>(initial: T, options?: { equal?: (a: T, b: T) => boolean }): WritableSignal<T> {
  const node = new SignalNode<T>(initial, options?.equal ?? Object.is);
  let readonlyView: Signal<T> | undefined;
  const read = (() => {
    trackRead(node);
    return node.value;
  }) as WritableSignal<T>;
  read.set = (value: T) => {
    if (computedDepth > 0) {
      throw new Error('Writing to a signal is not allowed inside a computed value; move the write into a method or an effect.');
    }
    if (node.equal(node.value, value)) return;
    node.value = value;
    node.version++;
    notifyConsumers(node);
  };
  read.update = (fn: (value: T) => T) => {
    read.set(fn(node.value));
  };
  read.asReadonly = () => {
    if (!readonlyView) {
      readonlyView = (() => {
        trackRead(node);
        return node.value;
      }) as Signal<T>;
    }
    return readonlyView;
  };
  return read;
}

class ComputedNode<T> implements ProducerNode, ConsumerNode {
  version = 0;
  consumers = new Set<ConsumerNode>();
  producers = new Map<ProducerNode, number>();
  private value: T | undefined;
  private hasValue = false;
  private stale = true;
  private readonly fn: () => T;
  private readonly equal: (a: T, b: T) => boolean;
  constructor(fn: () => T, equal: (a: T, b: T) => boolean) {
    this.fn = fn;
    this.equal = equal;
  }

  onProducerChanged(): void {
    if (this.stale) return;
    this.stale = true;
    notifyConsumers(this);
  }

  refreshIfStale(): void {
    if (this.stale) this.refresh();
  }

  read(): T {
    if (this.stale || !this.hasValue) this.refresh();
    trackRead(this);
    return this.value as T;
  }

  private refresh(): void {
    if (this.hasValue) {
      let changed = false;
      for (const [producer, seen] of this.producers) {
        if (producer instanceof ComputedNode) producer.refreshIfStale();
        if (producer.version !== seen) { changed = true; break; }
      }
      if (!changed) {
        this.stale = false;
        return;
      }
    }
    detachConsumer(this);
    const prev = activeConsumer;
    activeConsumer = this;
    computedDepth++;
    let next: T;
    try {
      next = this.fn();
    } finally {
      computedDepth--;
      activeConsumer = prev;
    }
    const valueChanged = !this.hasValue || !this.equal(this.value as T, next);
    this.value = next;
    this.hasValue = true;
    this.stale = false;
    if (valueChanged) this.version++;
  }
}

/**
 * Create a computed value, as in Angular: lazy (nothing runs until the
 * first read), cached (a read with unchanged inputs returns the cached
 * value), and tracked (the inputs are whatever the function read last).
 */
export function computed<T>(fn: () => T, options?: { equal?: (a: T, b: T) => boolean }): Signal<T> {
  const node = new ComputedNode<T>(fn, options?.equal ?? Object.is);
  return (() => node.read()) as Signal<T>;
}

const pendingEffects = new Set<EffectNode>();
let effectFlushQueued = false;

function queueEffectFlush(): void {
  if (effectFlushQueued) return;
  effectFlushQueued = true;
  queueMicrotask(() => {
    effectFlushQueued = false;
    flushEffects();
  });
}

/** Run every queued effect now, including any those runs queue. */
export function flushEffects(): void {
  for (let guard = 0; pendingEffects.size > 0 && guard < 100; guard++) {
    const batch = [...pendingEffects];
    pendingEffects.clear();
    for (const node of batch) node.run();
  }
  pendingEffects.clear();
}

const effectScopeStack: Array<Set<EffectNode>> = [];

/**
 * Generated components open an effect scope around their mount effect:
 * every effect created while a scope is open joins it, and the cleanup
 * the matching end call returns destroys them all (the component
 * unmount path).
 */
export function beginEffectScope(): void {
  effectScopeStack.push(new Set());
}

export function endEffectScope(): () => void {
  const scope = effectScopeStack.pop();
  return () => {
    if (scope) for (const node of [...scope]) node.destroy();
  };
}

class EffectNode implements ConsumerNode {
  producers = new Map<ProducerNode, number>();
  private cleanups: Array<() => void> = [];
  private destroyed = false;
  private queued = false;
  private scope: Set<EffectNode> | undefined;
  private readonly fn: (onCleanup: (cleanup: () => void) => void) => void;
  constructor(fn: (onCleanup: (cleanup: () => void) => void) => void) {
    this.fn = fn;
  }

  onProducerChanged(): void {
    if (this.destroyed || this.queued) return;
    this.queued = true;
    pendingEffects.add(this);
    queueEffectFlush();
  }

  joinScope(scope: Set<EffectNode>): void {
    this.scope = scope;
  }

  run(): void {
    this.queued = false;
    if (this.destroyed) return;
    this.runCleanups();
    detachConsumer(this);
    const prev = activeConsumer;
    activeConsumer = this;
    try {
      this.fn((cleanup) => { this.cleanups.push(cleanup); });
    } finally {
      activeConsumer = prev;
    }
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    pendingEffects.delete(this);
    if (this.scope) this.scope.delete(this);
    this.runCleanups();
    detachConsumer(this);
  }

  private runCleanups(): void {
    const cleanups = this.cleanups;
    this.cleanups = [];
    for (const cleanup of cleanups) cleanup();
  }
}

/**
 * Create an effect, as in Angular: the function runs once at creation
 * and re-runs whenever a signal or computed it read changes. The
 * function receives onCleanup to register teardown that runs before
 * each re-run and at destroy. Effects created inside a component's
 * mount scope are destroyed with the component.
 */
export function effect(fn: (onCleanup: (cleanup: () => void) => void) => void): EffectRef {
  const node = new EffectNode(fn);
  const scope = effectScopeStack[effectScopeStack.length - 1];
  if (scope) {
    scope.add(node);
    node.joinScope(scope);
  }
  node.run();
  return { destroy: () => { node.destroy(); } };
}

/** Run `fn` with dependency tracking suspended, as in Angular. */
export function untracked<T>(fn: () => T): T {
  const prev = activeConsumer;
  activeConsumer = null;
  try {
    return fn();
  } finally {
    activeConsumer = prev;
  }
}

/**
 * The React bridge for generated components: a tracker is the consumer
 * for one component's render. Codegen opens it (begin) just before the
 * returned view is evaluated and closes it (end) right after, so every
 * signal read by the view, including reads inside methods the view
 * calls, subscribes exactly this component. A later write to any of
 * those signals calls onChange once, and the next render re-tracks
 * from scratch, so a component follows the signals its current view
 * actually reads.
 */
export class SignalRenderTracker implements ConsumerNode {
  producers = new Map<ProducerNode, number>();
  private rendering = false;
  private disposed = false;
  private prevConsumer: ConsumerNode | null = null;
  private readonly onChange: () => void;
  constructor(onChange: () => void) {
    this.onChange = onChange;
  }

  begin(): void {
    detachConsumer(this);
    this.rendering = true;
    this.prevConsumer = activeConsumer;
    activeConsumer = this;
  }

  end(): void {
    activeConsumer = this.prevConsumer;
    this.prevConsumer = null;
    this.rendering = false;
  }

  onProducerChanged(): void {
    if (!this.rendering && !this.disposed) this.onChange();
  }

  dispose(): void {
    this.disposed = true;
    detachConsumer(this);
  }
}

/**
 * Pipe helper referenced by generated code for `expr | pipeName` template
 * expressions: the full Stage 12 built-in set (uppercase, lowercase,
 * titlecase, json, date, number, percent, currency, slice, keyvalue,
 * async). The semantics are the compiler's canonical ones
 * (packages/compiler/src/template/pipes.ts); a parity test runs one
 * case table through both. Unknown pipes return the value unchanged:
 * generated code only reaches this helper for pipes the compiler has
 * already validated, so an unknown name here means a hand-edited file.
 *
 * The `async` case is only a fallback: codegen replaces every
 * template `| async` with a live subscription hook, so this helper
 * sees an observable only from hand-written calls. Its behaviour is
 * the pure snapshot: subscribe, keep what arrives synchronously,
 * unsubscribe, return the latest value (null before any emission).
 */
export function pipe<T>(name: string, value: T, ...args: unknown[]): unknown {
  switch (name) {
    case 'uppercase': return typeof value === 'string' ? value.toUpperCase() : value;
    case 'lowercase': return typeof value === 'string' ? value.toLowerCase() : value;
    case 'titlecase':
      return typeof value === 'string'
        ? value.replace(/\w\S*/g, (w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
        : value;
    case 'json': return JSON.stringify(value);
    case 'date': return pipeDate(value, args[0]);
    case 'number':
      return typeof value === 'number'
        ? formatPipeNumber(value, parsePipeDigits(args[0], { minInt: 1, minFrac: 0, maxFrac: 3 }))
        : value;
    case 'percent':
      return typeof value === 'number'
        ? formatPipeNumber(value * 100, parsePipeDigits(args[0], { minInt: 1, minFrac: 0, maxFrac: 3 })) + '%'
        : value;
    case 'currency': return pipeCurrency(value, args);
    case 'slice': {
      if (typeof value !== 'string' && !Array.isArray(value)) return value;
      const start = typeof args[0] === 'number' ? args[0] : 0;
      const end = typeof args[1] === 'number' ? args[1] : undefined;
      return (value as unknown as string).slice(start, end);
    }
    case 'keyvalue': {
      if (value === null || typeof value !== 'object' || Array.isArray(value)) return value;
      const rec = value as Record<string, unknown>;
      return Object.keys(rec)
        .map((key) => ({ key, value: rec[key] }))
        .sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    }
    case 'async': return pipeAsyncSnapshot(value);
    default: throw new Error(`Unknown pipe '${name}'`);
  }
}

interface PipeDigits { minInt: number; minFrac: number; maxFrac: number }

function parsePipeDigits(info: unknown, fallback: PipeDigits): PipeDigits {
  if (typeof info !== 'string') return fallback;
  const m = /^(\d+)\.(\d+)-(\d+)$/.exec(info.trim());
  if (!m) return fallback;
  const parsed = { minInt: Number(m[1]), minFrac: Number(m[2]), maxFrac: Number(m[3]) };
  return parsed.maxFrac < parsed.minFrac ? fallback : parsed;
}

function formatPipeNumber(value: number, digits: PipeDigits): string {
  if (!Number.isFinite(value)) return String(value);
  const negative = value < 0;
  const fixed = Math.abs(value).toFixed(digits.maxFrac);
  const dot = fixed.indexOf('.');
  let intPart = dot >= 0 ? fixed.slice(0, dot) : fixed;
  let fracPart = dot >= 0 ? fixed.slice(dot + 1) : '';
  while (fracPart.length > digits.minFrac && fracPart.endsWith('0')) fracPart = fracPart.slice(0, -1);
  while (fracPart.length < digits.minFrac) fracPart += '0';
  while (intPart.length < digits.minInt) intPart = '0' + intPart;
  const grouped = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return (negative ? '-' : '') + grouped + (fracPart ? '.' + fracPart : '');
}

const PIPE_MONTH_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const PIPE_DATE_FORMATS: Record<string, string> = {
  shortDate: 'M/d/yy',
  mediumDate: 'MMM d, yyyy',
  shortTime: 'h:mm a',
  medium: 'MMM d, yyyy, h:mm:ss a',
  short: 'M/d/yy, h:mm a',
};

function pipeDate(value: unknown, formatArg: unknown): unknown {
  let d: Date | undefined;
  if (value instanceof Date) d = Number.isNaN(value.getTime()) ? undefined : value;
  else if (typeof value === 'number' && Number.isFinite(value)) d = new Date(value);
  else if (typeof value === 'string' && value.trim() !== '') {
    const parsed = new Date(value);
    d = Number.isNaN(parsed.getTime()) ? undefined : parsed;
  }
  if (!d) return value;
  const named = typeof formatArg === 'string' ? formatArg : 'mediumDate';
  const format = PIPE_DATE_FORMATS[named] ?? named;
  const pad = (n: number): string => String(n).padStart(2, '0');
  const hours = d.getHours();
  const hour12 = hours % 12 === 0 ? 12 : hours % 12;
  const tokens: [string, string][] = [
    ['yyyy', String(d.getFullYear())],
    ['MMM', PIPE_MONTH_SHORT[d.getMonth()]],
    ['yy', pad(d.getFullYear() % 100)],
    ['MM', pad(d.getMonth() + 1)],
    ['dd', pad(d.getDate())],
    ['HH', pad(hours)],
    ['hh', pad(hour12)],
    ['mm', pad(d.getMinutes())],
    ['ss', pad(d.getSeconds())],
    ['M', String(d.getMonth() + 1)],
    ['d', String(d.getDate())],
    ['H', String(hours)],
    ['h', String(hour12)],
    ['a', hours < 12 ? 'AM' : 'PM'],
  ];
  let out = '';
  let i = 0;
  while (i < format.length) {
    const hit = tokens.find(([tok]) => format.startsWith(tok, i));
    if (hit) { out += hit[1]; i += hit[0].length; } else { out += format[i]; i++; }
  }
  return out;
}

const PIPE_CURRENCY_SYMBOLS: Record<string, string> = {
  USD: '$', EUR: '€', GBP: '£', INR: '₹', NPR: 'Rs',
};

function pipeCurrency(value: unknown, args: unknown[]): unknown {
  if (typeof value !== 'number') return value;
  const code = typeof args[0] === 'string' && args[0] !== '' ? args[0] : 'USD';
  const display = typeof args[1] === 'string' ? args[1] : 'symbol';
  const digits = parsePipeDigits(args[2], { minInt: 1, minFrac: 2, maxFrac: 2 });
  const symbol = display === 'code' ? undefined : PIPE_CURRENCY_SYMBOLS[code];
  const formatted = formatPipeNumber(Math.abs(value), digits);
  const sign = value < 0 ? '-' : '';
  return symbol !== undefined ? `${sign}${symbol}${formatted}` : `${sign}${code} ${formatted}`;
}

function pipeAsyncSnapshot(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  const sub = value as { subscribe?: unknown };
  if (typeof sub.subscribe !== 'function') return value;
  let latest: unknown = null;
  try {
    const handle = (sub.subscribe as (next: (v: unknown) => void) => { unsubscribe(): void } | void)(
      (v) => { latest = v; },
    );
    if (handle && typeof handle.unsubscribe === 'function') handle.unsubscribe();
  } catch {
    return latest;
  }
  return latest;
}

/**
 * Base class for generated services (Stage 3, M6).
 *
 * A service's state is plain class fields. Whenever a translated service
 * method assigns a field, the codegen appends `this.__notify()`; subscribed
 * components re-render. Generated components subscribe with React's
 * useSyncExternalStore over __subscribe/__getVersion, the version counter
 * is the snapshot, so every notification produces a new snapshot value.
 */
export class ReactiveService {
  private __listeners = new Set<() => void>();
  private __version = 0;

  /** Bump the version and notify every subscribed component. */
  protected __notify(): void {
    this.__version++;
    for (const listener of [...this.__listeners]) listener();
  }

  /** Subscribe to changes; returns an unsubscribe function. */
  __subscribe(listener: () => void): () => void {
    this.__listeners.add(listener);
    return () => { this.__listeners.delete(listener); };
  }

  /** Monotonic snapshot for useSyncExternalStore. */
  __getVersion(): number {
    return this.__version;
  }
}

/**
 * The stream type this package hands out: a real RxJS Observable.
 * Before the RxJS adoption this name was a small home-grown interface
 * with subscribe only; it stays exported as an alias so older code
 * that names the type keeps compiling, but the value behind it is
 * the genuine Observable, with pipe and the whole operator library.
 */
export type ObservableLite<T> = Observable<T>;

/** The slice of fetch HttpClient needs, injectable so tests never touch the network. */
export type FetchLike = (
  url: string,
  init?: { method?: string; headers?: Record<string, string>; body?: string },
) => Promise<{ json(): Promise<unknown> }>;

/**
 * Angular-shaped HttpClient over fetch (Stage 3, M6). Registered in the
 * generated app's container under its own class token, so components inject
 * it exactly like a service: `constructor(private http: HttpClient)`.
 * Responses are parsed as JSON (Angular's default responseType). The four
 * verbs share one request path: get and delete send no body, post and put
 * send theirs as JSON (the verbs joined in 0.6.5, for lesson 63's examples).
 */
export class HttpClient {
  constructor(
    private fetchImpl: FetchLike = (
      url: string,
      init?: { method?: string; headers?: Record<string, string>; body?: string },
    ) => fetch(url, init),
  ) {}

  get<T = unknown>(url: string): Observable<T> {
    return this.request<T>('GET', url);
  }

  post<T = unknown>(url: string, body: unknown): Observable<T> {
    return this.request<T>('POST', url, body);
  }

  put<T = unknown>(url: string, body: unknown): Observable<T> {
    return this.request<T>('PUT', url, body);
  }

  delete<T = unknown>(url: string): Observable<T> {
    return this.request<T>('DELETE', url);
  }

  private request<T>(method: string, url: string, body?: unknown): Observable<T> {
    const fetchImpl = this.fetchImpl;
    return new Observable<T>((subscriber) => {
      let cancelled = false;
      const init = body === undefined
        ? { method }
        : { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) };
      fetchImpl(url, init)
        .then((res) => res.json())
        .then((data) => {
          if (cancelled) return;
          subscriber.next(data as T);
          subscriber.complete();
        })
        .catch((err) => { if (!cancelled) subscriber.error(err); });
      return () => { cancelled = true; };
    });
  }
}

export type Token = string | symbol | Function;

/**
 * A tiny dependency-injection registry. Generated apps register providers
 * under a token and components resolve them with get(). A later stage bridges
 * this to React Context; the registry semantics stay the same.
 */
export class Container {
  private providers = new Map<Token, unknown>();

  register<T>(token: Token, instance: T): this {
    this.providers.set(token, instance);
    return this;
  }

  has(token: Token): boolean {
    return this.providers.has(token);
  }

  get<T>(token: Token): T {
    if (!this.providers.has(token)) {
      const name = typeof token === 'function' ? token.name : String(token);
      throw new Error(`Rangular DI: no provider registered for token '${name}'`);
    }
    return this.providers.get(token) as T;
  }
}

// --- Forms (Stage 8) ---------------------------------------------------------
//
// The @angular/forms core, at the size Rangular templates can drive:
// Validators, FormControl, FormGroup, FormArray and FormBuilder, plus
// validateValue, the merge
// helper generated code uses for template-driven (ngModel) inputs.
// Semantics and error shapes follow Angular:
//
//   required              -> { required: true }
//   minLength(n)          -> { minlength: { requiredLength: n, actualLength: len } }
//   maxLength(n)          -> { maxlength: { requiredLength: n, actualLength: len } }
//   email                 -> { email: true }
//   pattern(p)            -> { pattern: { requiredPattern, actualValue } }
//
// As in Angular, every validator except `required` treats an empty value
// (null, undefined, '') as valid: emptiness is `required`'s job alone.
// A control is valid exactly when its merged errors object is null.

export type ValidationErrors = { [key: string]: unknown };
export type ValidatorFn = (value: unknown) => ValidationErrors | null;

function isEmptyValue(value: unknown): boolean {
  return value === null || value === undefined || value === ''
    || (Array.isArray(value) && value.length === 0);
}

function valueLength(value: unknown): number {
  if (typeof value === 'string' || Array.isArray(value)) return value.length;
  return String(value).length;
}

// Angular's own email check is a very long RFC regex; this is the same
// shape (local part, @, dotted domain, a TLD of at least two letters),
// kept readable on purpose.
const EMAIL_RE = /^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$/;

/** Anchor a string pattern the way Angular does: ^ at the start, $ at the end. */
function anchorPattern(pattern: string): string {
  let out = pattern;
  if (!out.startsWith('^')) out = `^${out}`;
  if (!out.endsWith('$')) out = `${out}$`;
  return out;
}

export const Validators = {
  required(value: unknown): ValidationErrors | null {
    return isEmptyValue(value) ? { required: true } : null;
  },

  minLength(requiredLength: number): ValidatorFn {
    return (value: unknown) => {
      if (isEmptyValue(value)) return null;
      const actualLength = valueLength(value);
      return actualLength < requiredLength
        ? { minlength: { requiredLength, actualLength } }
        : null;
    };
  },

  maxLength(requiredLength: number): ValidatorFn {
    return (value: unknown) => {
      if (isEmptyValue(value)) return null;
      const actualLength = valueLength(value);
      return actualLength > requiredLength
        ? { maxlength: { requiredLength, actualLength } }
        : null;
    };
  },

  email(value: unknown): ValidationErrors | null {
    if (isEmptyValue(value)) return null;
    return EMAIL_RE.test(String(value)) ? null : { email: true };
  },

  pattern(p: string | RegExp): ValidatorFn {
    const requiredPattern = typeof p === 'string' ? anchorPattern(p) : p.source;
    const regex = typeof p === 'string' ? new RegExp(requiredPattern) : p;
    return (value: unknown) => {
      if (isEmptyValue(value)) return null;
      return regex.test(String(value))
        ? null
        : { pattern: { requiredPattern, actualValue: value } };
    };
  },
};

/** Run validators in order and merge their error objects; null when all pass. */
export function validateValue(value: unknown, validators: ValidatorFn[]): ValidationErrors | null {
  let merged: ValidationErrors | null = null;
  for (const validator of validators) {
    const errors = validator(value);
    if (errors) merged = { ...(merged ?? {}), ...errors };
  }
  return merged;
}

/**
 * Any node of a reactive form tree: a single control, a named group or
 * an ordered array. The three classes share this one shape, so a group
 * or an array can hold any of them and every aggregate simply recurses
 * into its children. setValue and patchValue take any value at this
 * level, exactly as Angular's AbstractControl declares them; each
 * class narrows the shape it expects.
 */
export interface FormNode {
  readonly value: unknown;
  readonly valid: boolean;
  readonly invalid: boolean;
  readonly errors: ValidationErrors | null;
  readonly touched: boolean;
  readonly dirty: boolean;
  readonly valueChanges: Observable<any>;
  setValue(value: any): void;
  patchValue(value: any): void;
  markAllAsTouched(): void;
}

/**
 * One reactive form control (Stage 8). Holds a value, validates it on
 * construction and on every setValue, tracks touched/dirty the way
 * Angular does, and exposes valueChanges as a real RxJS Observable
 * fed by a Subject the control owns.
 */
export class FormControl implements FormNode {
  value: unknown;
  errors: ValidationErrors | null = null;
  touched = false;
  dirty = false;
  readonly valueChanges: Observable<unknown>;
  private readonly validators: ValidatorFn[];
  private readonly changes = new Subject<unknown>();

  constructor(initialValue: unknown = null, validators?: ValidatorFn | ValidatorFn[] | null) {
    this.value = initialValue;
    this.validators = validators == null ? [] : Array.isArray(validators) ? [...validators] : [validators];
    this.valueChanges = this.changes.asObservable();
    this.revalidate();
  }

  get valid(): boolean { return this.errors === null; }
  get invalid(): boolean { return this.errors !== null; }
  get pristine(): boolean { return !this.dirty; }
  get untouched(): boolean { return !this.touched; }

  setValue(value: unknown): void {
    this.value = value;
    this.dirty = true;
    this.revalidate();
    this.changes.next(value);
  }

  patchValue(value: unknown): void { this.setValue(value); }

  markAsTouched(): void { this.touched = true; }
  markAllAsTouched(): void { this.touched = true; }
  markAsDirty(): void { this.dirty = true; }

  private revalidate(): void {
    this.errors = validateValue(this.value, this.validators);
  }
}


/** Write a value into any form node, recursing into containers. */
function setNodeValue(node: FormNode, value: unknown): void {
  if (node instanceof FormControl) node.setValue(value);
  else if (node instanceof FormGroup) node.setValue(value as Record<string, unknown>);
  else node.setValue(value as unknown[]);
}

/** Write a partial value into any form node, recursing into containers. */
function patchNodeValue(node: FormNode, value: unknown): void {
  if (node instanceof FormControl) node.patchValue(value);
  else if (node instanceof FormGroup) node.patchValue(value as Record<string, unknown>);
  else node.patchValue(value as unknown[]);
}

/**
 * A named set of form nodes (Stage 8; tree nodes since the forms
 * completion pass). The group's value is the plain object of child
 * values (a FormArray child contributes an array, a nested FormGroup
 * an object); the group is valid exactly when every child is valid.
 * There are no group-level validators in this version, so the group's
 * own errors are always null, as in Angular when no group validator
 * is registered. The group's valueChanges emits its value whenever
 * any descendant control changes.
 */
export class FormGroup implements FormNode {
  readonly controls: Record<string, FormNode>;
  readonly valueChanges: Observable<Record<string, unknown>>;

  constructor(controls: Record<string, FormNode>) {
    this.controls = controls;
    this.valueChanges = merge(...Object.values(this.controls).map((child) => child.valueChanges)).pipe(
      map(() => this.value),
    );
  }

  get(name: string): FormNode {
    return this.controls[name];
  }

  get value(): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [name, child] of Object.entries(this.controls)) out[name] = child.value;
    return out;
  }

  get valid(): boolean {
    return Object.values(this.controls).every((c) => c.valid);
  }

  get invalid(): boolean { return !this.valid; }
  get errors(): ValidationErrors | null { return null; }

  get touched(): boolean {
    return Object.values(this.controls).some((c) => c.touched);
  }

  get dirty(): boolean {
    return Object.values(this.controls).some((c) => c.dirty);
  }

  markAllAsTouched(): void {
    for (const child of Object.values(this.controls)) child.markAllAsTouched();
  }

  /** Set every child from a complete value object, as in Angular. */
  setValue(value: Record<string, unknown>): void {
    for (const name of Object.keys(this.controls)) {
      if (!(name in value)) throw new Error(`FormGroup.setValue: no value given for control '${name}'`);
    }
    for (const [name, v] of Object.entries(value)) {
      const child = this.controls[name];
      if (!child) throw new Error(`FormGroup.setValue: no control named '${name}'`);
      setNodeValue(child, v);
    }
  }

  /** Set the children named in a partial value object, as in Angular. */
  patchValue(value: Record<string, unknown>): void {
    for (const [name, v] of Object.entries(value)) {
      const child = this.controls[name];
      if (child) patchNodeValue(child, v);
    }
  }
}

/**
 * An ordered list of form nodes (forms completion pass): the dynamic
 * side of reactive forms, for rows the user can add and remove. The
 * array's value is the array of child values; it is valid exactly
 * when every child is valid. Its valueChanges emits whenever a child
 * changes and whenever the structure changes (push, insert, removeAt,
 * clear), so a component subscribed once stays live across edits.
 * There are no array-level validators in this version, so the array's
 * own errors are always null.
 */
export class FormArray implements FormNode {
  readonly controls: FormNode[];
  readonly valueChanges: Observable<unknown[]>;
  private readonly changes = new Subject<unknown[]>();
  private readonly childSubs = new Map<FormNode, { unsubscribe(): void }>();

  constructor(controls: FormNode[] = []) {
    this.controls = [...controls];
    this.valueChanges = this.changes.asObservable();
    for (const child of this.controls) this.watch(child);
  }

  private watch(child: FormNode): void {
    this.childSubs.set(child, child.valueChanges.subscribe(() => this.emit()));
  }

  private unwatch(child: FormNode): void {
    this.childSubs.get(child)?.unsubscribe();
    this.childSubs.delete(child);
  }

  private emit(): void {
    this.changes.next(this.value);
  }

  get length(): number { return this.controls.length; }

  at(index: number): FormNode {
    return this.controls[index];
  }

  push(control: FormNode): void {
    this.controls.push(control);
    this.watch(control);
    this.emit();
  }

  insert(index: number, control: FormNode): void {
    this.controls.splice(index, 0, control);
    this.watch(control);
    this.emit();
  }

  removeAt(index: number): void {
    const removed = this.controls.splice(index, 1)[0];
    if (removed) this.unwatch(removed);
    this.emit();
  }

  clear(): void {
    for (const child of this.controls) this.unwatch(child);
    this.controls.length = 0;
    this.emit();
  }

  get value(): unknown[] {
    return this.controls.map((c) => c.value);
  }

  get valid(): boolean {
    return this.controls.every((c) => c.valid);
  }

  get invalid(): boolean { return !this.valid; }
  get errors(): ValidationErrors | null { return null; }

  get touched(): boolean {
    return this.controls.some((c) => c.touched);
  }

  get dirty(): boolean {
    return this.controls.some((c) => c.dirty);
  }

  markAllAsTouched(): void {
    for (const child of this.controls) child.markAllAsTouched();
  }

  /** Set every child from a complete array of values, as in Angular. */
  setValue(values: unknown[]): void {
    if (values.length !== this.controls.length) {
      throw new Error(`FormArray.setValue: expected ${this.controls.length} values, got ${values.length}`);
    }
    this.controls.forEach((child, i) => setNodeValue(child, values[i]));
  }

  /** Set the first children from a partial array of values. */
  patchValue(values: unknown[]): void {
    values.forEach((v, i) => {
      if (i < this.controls.length) patchNodeValue(this.controls[i], v);
    });
  }
}

/**
 * The config-object factory for form trees (forms completion pass),
 * Angular's convenience over the classes above. An injected built-in,
 * like HttpClient: constructor(private fb: FormBuilder). Each config
 * entry becomes a node: a FormControl, FormGroup or FormArray instance
 * is used as-is; a tuple [initialValue, validators] becomes a
 * FormControl with those validators; any other value becomes a
 * FormControl holding it. A tuple's third element (Angular's async
 * validators) is ignored: there are no async validators in this
 * version.
 */
export class FormBuilder {
  group(config: Record<string, unknown>): FormGroup {
    const controls: Record<string, FormNode> = {};
    for (const [name, entry] of Object.entries(config)) controls[name] = this.coerce(entry);
    return new FormGroup(controls);
  }

  control(initialValue: unknown = null, validators?: ValidatorFn | ValidatorFn[] | null): FormControl {
    return new FormControl(initialValue, validators);
  }

  array(entries: unknown[] = []): FormArray {
    return new FormArray(entries.map((entry) => this.coerce(entry)));
  }

  private coerce(entry: unknown): FormNode {
    if (entry instanceof FormControl || entry instanceof FormGroup || entry instanceof FormArray) return entry;
    if (Array.isArray(entry)) {
      const validators = entry.length > 1 ? entry[1] as ValidatorFn | ValidatorFn[] : undefined;
      return new FormControl(entry.length ? entry[0] : null, validators);
    }
    return new FormControl(entry ?? null);
  }
}

// --- Routing (Stage 9) -------------------------------------------------------
//
// The @angular/router model, at the size a phone app needs: a Routes
// array of path patterns, a native navigation stack under it, and the
// two injectables Angular authors know, Router (navigate from code)
// and ActivatedRoute (read the current route's parameters).
//
// Matching rules (v1, stated plainly):
//  - paths are segment lists; a leading or trailing slash means nothing
//    ('/mail/3', 'mail/3' and 'mail/3/' are the same path);
//  - entries are tried in declaration order, the first match wins;
//  - a ':name' segment captures one segment into params (always a
//    string, exactly as in Angular);
//  - '**' matches any path, including the empty one;
//  - an entry with redirectTo matches on its full path only (the
//    pathMatch: 'full' behaviour) and matching restarts at the target;
//    redirect loops and chains longer than 10 hops match nothing;
//  - every other entry must match the whole path: there are no child
//    routes or prefix matches in v1, so segment counts must be equal.
//
// The preview page runtime mirrors matchRoute in plain JavaScript; the
// two implementations are parity-tested against the same table of
// paths in tests/stage9.test.ts.

/** One route pattern, the shape the generated navigator is built from. */
export interface RgRouteDef {
  path: string;
  redirectTo?: string;
}

/** A resolved navigation: the concrete path, its params, and the pattern that matched. */
export interface RouteMatch {
  /** The concrete path navigated to, normalised (no leading or trailing slash). */
  path: string;
  /** Captured ':param' segments, values are always strings. */
  params: Record<string, string>;
  /** The route pattern that matched, e.g. 'mail/:id'. */
  routePath: string;
}

/** Normalise a path: split on '/', drop empty segments, rejoin. */
export function normalizeRoutePath(raw: string): string {
  return raw.split('/').filter((s) => s.length > 0).join('/');
}

function segmentsOf(path: string): string[] {
  const normalized = normalizeRoutePath(path);
  return normalized === '' ? [] : normalized.split('/');
}

/** Match one pattern against one concrete path; params or null. */
function matchSegments(pattern: string, path: string): Record<string, string> | null {
  if (pattern === '**') return {};
  const want = segmentsOf(pattern);
  const got = segmentsOf(path);
  if (want.length !== got.length) return null;
  const params: Record<string, string> = {};
  for (let i = 0; i < want.length; i++) {
    if (want[i].startsWith(':')) params[want[i].slice(1)] = got[i];
    else if (want[i] !== got[i]) return null;
  }
  return params;
}

/**
 * Match a path against a route table, following redirects. Returns the
 * final match, or null when nothing matches (no entry and no wildcard).
 */
export function matchRoute(routes: RgRouteDef[], rawPath: string): RouteMatch | null {
  let current = normalizeRoutePath(rawPath);
  const seen = new Set<string>();
  for (let hop = 0; hop <= 10; hop++) {
    let hit: { def: RgRouteDef; params: Record<string, string> } | null = null;
    for (const def of routes) {
      const params = matchSegments(def.path, current);
      if (params) { hit = { def, params }; break; }
    }
    if (!hit) return null;
    if (hit.def.redirectTo === undefined) {
      return { path: current, params: hit.params, routePath: hit.def.path };
    }
    if (seen.has(current)) return null;
    seen.add(current);
    current = normalizeRoutePath(hit.def.redirectTo);
  }
  return null;
}

/**
 * The navigation stack of a generated app (Stage 9). One instance lives
 * at module level in the generated file; the root component subscribes
 * and re-renders the outlet whenever the top entry changes. The stack
 * always holds at least the initial entry (the match for the empty
 * path, redirects followed), so `current` is always defined and
 * `back()` at depth 1 is a no-op that returns false.
 */
export class RgNavigator {
  private entries: RouteMatch[];
  private listeners = new Set<() => void>();

  constructor(private readonly routes: RgRouteDef[]) {
    const initial = matchRoute(routes, '');
    this.entries = [initial ?? { path: '', params: {}, routePath: '' }];
  }

  /** The entry on top of the stack: what the outlet renders. */
  get current(): RouteMatch {
    return this.entries[this.entries.length - 1];
  }

  /** Stack depth; the app can go back exactly when this is above 1. */
  get depth(): number {
    return this.entries.length;
  }

  /** Push a new entry; false (and no change) when no route matches. */
  navigate(rawPath: string): boolean {
    const match = matchRoute(this.routes, rawPath);
    if (!match) return false;
    this.entries.push(match);
    this.emit();
    return true;
  }

  /** Pop back to the previous entry; false at the root of the stack. */
  back(): boolean {
    if (this.entries.length <= 1) return false;
    this.entries.pop();
    this.emit();
    return true;
  }

  /** Subscribe to navigation changes; returns an unsubscribe function. */
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  private emit(): void {
    for (const listener of [...this.listeners]) listener();
  }
}

/**
 * The injectable Angular authors navigate with (Stage 9). Generated
 * components receive the one instance bound to the app's navigator.
 *
 * `navigate(commands)` joins the command segments with '/': numbers
 * are stringified, and slashes at segment edges mean nothing, so
 * `navigate(['/mail', 3])` and `navigate(['mail', '3'])` both land on
 * 'mail/3'. In v1 commands always resolve from the app root; Angular's
 * relative-to-current-route navigation is not in this version.
 */
export class Router {
  constructor(private readonly navigator: RgNavigator) {}

  navigate(commands: unknown[]): boolean {
    return this.navigator.navigate(commands.map((c) => String(c)).join('/'));
  }

  navigateByUrl(url: string): boolean {
    return this.navigator.navigate(url);
  }
}

/**
 * The injectable that carries the current route's parameters into a
 * route component (Stage 9). The outlet creates one instance per mount
 * from the current navigation entry and passes it to the component.
 *
 * `snapshot.params` is the plain params object. `paramMap` is a real
 * RxJS Observable fed by a BehaviorSubject: it emits the current
 * params on subscribe and, while subscribed, again whenever the
 * navigator's current entry changes.
 * In v1 the generated outlet remounts a route component when the path
 * changes (it keys the component by the concrete path), so a component
 * normally sees exactly one emission per mount; the snapshot is the
 * primary surface, exactly the pattern Angular's own docs teach first.
 */
export class ActivatedRoute {
  readonly paramMap: Observable<Record<string, string>>;
  private readonly paramsChanges: BehaviorSubject<Record<string, string>>;

  constructor(
    private readonly initialParams: Record<string, string>,
    private readonly navigatorRef?: RgNavigator,
  ) {
    this.paramsChanges = new BehaviorSubject<Record<string, string>>(initialParams);
    if (this.navigatorRef) {
      const nav = this.navigatorRef;
      nav.subscribe(() => this.paramsChanges.next(nav.current.params));
    }
    this.paramMap = this.paramsChanges.asObservable();
  }

  /**
   * The current parameters, read live: with a navigator attached the
   * snapshot reflects the latest navigation (Angular refreshes the
   * snapshot on each activation the same way); without one it is the
   * fixed set the instance was constructed with.
   */
  get snapshot(): { params: Record<string, string> } {
    return { params: this.navigatorRef ? this.navigatorRef.current.params : this.initialParams };
  }
}

// --- Snackbar (Stage 10) -----------------------------------------------------
//
// The @angular/material snack bar, at the size a phone app needs.
// Angular authors inject MatSnackBar and call
// `open(message, action, { duration })`; Rangular authors inject the
// runtime class `SnackBar` and call `open(message, actionLabel,
// durationMs)` with the same meaning. Opening a bar replaces any bar
// currently showing, as in Material. The bar dismisses itself after
// its duration (4000 ms by default), when its action is pressed, or
// when dismiss() is called.
//
// The class holds only state and subscribers; rendering belongs to
// the generated root component, which mirrors `current` in React
// state and renders the RgSnackbar primitive. The preview page
// runtime mirrors this class in plain JavaScript.

/** What the generated host renders for the currently open bar. */
export interface SnackBarState {
  message: string;
  actionLabel?: string;
  durationMs: number;
}

/**
 * The handle `SnackBar.open` returns, mirroring MatSnackBarRef.
 * `onAction(callback)` registers what runs when the bar's action is
 * pressed (the Undo pattern); `dismiss()` closes the bar early.
 */
export class SnackBarRef {
  private actionCallbacks: Array<() => void> = [];

  constructor(private readonly bar: SnackBar) {}

  onAction(callback: () => void): void {
    this.actionCallbacks.push(callback);
  }

  dismiss(): void {
    this.bar.dismiss();
  }

  /** Fired by the owning SnackBar when the action is pressed. Hosts never call this. */
  __fireAction(): void {
    for (const callback of [...this.actionCallbacks]) callback();
  }
}

export class SnackBar {
  private state: SnackBarState | null = null;
  private currentRef: SnackBarRef | null = null;
  private listeners = new Set<() => void>();
  private timer: ReturnType<typeof setTimeout> | undefined;

  /** The bar currently showing, or null. The generated host renders this. */
  get current(): SnackBarState | null {
    return this.state;
  }

  /** Subscribe to open/dismiss changes; returns an unsubscribe function. */
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /**
   * Open a bar, replacing any bar currently showing (its action
   * callbacks do not fire). Returns the bar's ref.
   */
  open(message: string, actionLabel?: string, durationMs = 4000): SnackBarRef {
    this.clearTimer();
    const ref = new SnackBarRef(this);
    this.currentRef = ref;
    this.state = { message, actionLabel, durationMs };
    if (typeof setTimeout === 'function' && durationMs > 0) {
      this.timer = setTimeout(() => this.dismiss(), durationMs);
      // In Node (tests, tooling) a pending auto-dismiss must not hold
      // the process open; in React Native the timer has no unref.
      (this.timer as unknown as { unref?: () => void }).unref?.();
    }
    this.emit();
    return ref;
  }

  /** Close the current bar without firing its action callbacks. */
  dismiss(): void {
    if (this.state === null) return;
    this.clearTimer();
    this.state = null;
    this.currentRef = null;
    this.emit();
  }

  /**
   * The action was pressed: the bar dismisses and the current ref's
   * onAction callbacks fire, in that order. Called by the generated
   * snackbar host; authors use SnackBarRef.onAction instead.
   */
  pressAction(): void {
    const ref = this.currentRef;
    this.dismiss();
    ref?.__fireAction();
  }

  private clearTimer(): void {
    if (this.timer !== undefined && typeof clearTimeout === 'function') {
      clearTimeout(this.timer);
    }
    this.timer = undefined;
  }

  private emit(): void {
    for (const listener of [...this.listeners]) listener();
  }
}
