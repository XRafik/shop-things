import {randomUUID} from 'node:crypto';
import {constants} from 'node:fs';
import {copyFile} from 'node:fs/promises';
import {resolve} from 'node:path';

import type {
  ImportReview,
  ActionHandlers,
  ActionResults,
  ContractError,
  CustomerRecord,
  DatabaseState,
  ErrorCode,
  UpdateCustomerInput,
} from '@shop-things/contract';
import {DatabaseError} from '@shop-things/db';
import type {CustomerChanges, CustomerData, DatabaseHandle} from '@shop-things/db';
import * as nativeDatabase from '@shop-things/db/effect';
import type {DatabaseFailure} from '@shop-things/db/effect';
import {Cause, Deferred, Effect, Exit} from 'effect';

import type {ActionServiceOptions, DatabaseOperations} from './actionService.js';
import {readCustomerImportEffect} from './customerImport.js';
import {numberCustomerImportRows} from './customerImportNumbers.js';
import type {DraftLease} from './draftCoordinator.js';
import {NativeFailure, native, immediateError, originalCause} from './effectBoundary.js';
import {
  copyBackupEffect,
  removeDatabaseFileEffect,
  temporaryPath,
  writeCustomerCsvEffect,
} from './files.js';
import {matchCustomerImportRows, summarizeCustomerImport} from './importMatches.js';

type EffectHandlers = {
  [K in Exclude<keyof ActionHandlers, 'telemetry.status' | 'telemetry.save'>]: (
    ...args: Parameters<ActionHandlers[K]>
  ) => Effect.Effect<Awaited<ReturnType<ActionHandlers[K]>>>;
};
type BackendFailure = ActionError | NativeFailure | DatabaseFailure;

type DatabasePort = {
  [K in keyof typeof nativeDatabase & keyof DatabaseOperations]: (
    ...args: Parameters<DatabaseOperations[K]>
  ) => Effect.Effect<Awaited<ReturnType<DatabaseOperations[K]>>, BackendFailure>;
};

type Admission = {idle: Deferred.Deferred<void>} | null;
export const effectDatabaseOperations = {...nativeDatabase};

type Lifecycle = 'open' | 'closing' | 'reloading' | 'unprotected';
interface OwnedState {
  active: DatabaseHandle | null;
  database: DatabaseState;
  imports: Map<string, ImportReview>;
  admission: Admission;
  lifecycle: Lifecycle;
}

function databasePort(injected?: DatabaseOperations): DatabasePort {
  if (!injected) {
    return effectDatabaseOperations;
  }

  return Object.fromEntries(
    Object.entries(injected).map(([key, work]) => [
      key,
      (...args: unknown[]) => native(() => Reflect.apply(work, injected, args)),
    ])
  ) as DatabasePort;
}

type Outcome<T> =
  | {status: 'success'; value: T}
  | Exclude<ActionResults['database.status'], {status: 'success'}>;

class ActionError extends Error {
  readonly _tag = 'ActionError';
  constructor(
    readonly code: ErrorCode,
    message: string
  ) {
    super(message);
  }
}

function definedChanges(changes: UpdateCustomerInput): CustomerChanges {
  return {
    ...(changes.firstName !== undefined ? {firstName: changes.firstName} : {}),
    ...(changes.lastName !== undefined ? {lastName: changes.lastName} : {}),
    ...(changes.address !== undefined ? {address: changes.address} : {}),
    ...(changes.city !== undefined ? {city: changes.city} : {}),
    ...(changes.province !== undefined ? {province: changes.province} : {}),
    ...(changes.postalCode !== undefined ? {postalCode: changes.postalCode} : {}),
    ...(changes.phone !== undefined ? {phone: changes.phone} : {}),
    ...(changes.email !== undefined ? {email: changes.email} : {}),
    ...(changes.stock !== undefined ? {stock: changes.stock} : {}),
    ...(changes.balance !== undefined ? {balance: changes.balance} : {}),
    ...(changes.previousBalance !== undefined
      ? {previousBalance: changes.previousBalance}
      : {}),
    ...(changes.donate !== undefined ? {donate: changes.donate} : {}),
    ...(changes.comments !== undefined ? {comments: changes.comments} : {}),
    ...(changes.customerNumber !== undefined
      ? {customerNumber: changes.customerNumber}
      : {}),
  };
}

export class Backend {
  readonly handlers: EffectHandlers;
  private readonly database: DatabasePort;
  private readonly owned: OwnedState = {
    active: null,
    database: {available: false, selectedPath: null, session: null, version: 0},
    imports: new Map(),
    admission: null,
    lifecycle: 'open',
  };
  private readonly listeners = new Set<(state: DatabaseState) => void>();
  private get active() {
    return this.owned.active;
  }
  private set active(value: DatabaseHandle | null) {
    this.owned.active = value;
  }
  private get state() {
    return this.owned.database;
  }
  private set state(value: DatabaseState) {
    this.owned.database = value;
  }
  private get imports() {
    return this.owned.imports;
  }

  constructor(private readonly options: ActionServiceOptions) {
    this.database = options.effectDatabase ?? databasePort(options.database);
    this.handlers = {
      'imports.commit': args =>
        this.ordinary(() =>
          Effect.uninterruptible(
            Effect.gen({self: this}, function* () {
              const handle = this.requireSession(args.session);
              const review = this.imports.get(args.importId);
              if (!review || review.session !== args.session) {
                return yield* Effect.fail(
                  new ActionError(
                    'STALE_SESSION',
                    'This import expired. Choose the file again.'
                  )
                );
              }

              if (
                review.status !== 'ready' ||
                !review.choicesResolved ||
                review.includedCount === 0
              ) {
                return yield* Effect.fail(
                  new ActionError(
                    'VALIDATION',
                    'Resolve every possible match before adding customers.'
                  )
                );
              }

              let revised = this.recheckImport(
                review,
                yield* this.database.listCustomers(handle.db)
              );
              if (JSON.stringify(revised) !== JSON.stringify(review)) {
                this.imports.set(review.importId, revised);
                return {kind: 'changed' as const, review: structuredClone(revised)};
              }

              if ((yield* this.backup(args.session)) === null) {
                return null;
              }

              this.requireSession(args.session);
              const added = yield* this.database.importCustomerBatch(handle.db, saved => {
                revised = this.recheckImport(review, saved);
                if (JSON.stringify(revised) !== JSON.stringify(review)) {
                  return null;
                }

                return revised.rows
                  .filter(row => row.choice === 'include' || row.choice === 'add')
                  .map(row => {
                    if (row.assignedCustomerNumber === null) {
                      throw new ActionError(
                        'VALIDATION',
                        'Every included customer needs a number.'
                      );
                    }

                    return {...row.values, customerNumber: row.assignedCustomerNumber};
                  });
              });
              if (added === null) {
                this.imports.set(review.importId, revised);
                return {kind: 'changed' as const, review: structuredClone(revised)};
              }

              this.imports.delete(review.importId);
              return {
                kind: 'committed' as const,
                session: args.session,
                addedCount: added.length,
                skippedCount: review.skippedCount,
              };
            })
          )
        ),
      'imports.prepare': args =>
        this.ordinary(() =>
          Effect.gen({self: this}, function* () {
            this.requireSession(args.session);
            const path = yield* native(() => this.options.dialogs.importCsv());
            if (path === null) {
              return null;
            }

            this.requireSession(args.session);
            const parsed = yield* this.effectPhase('import.read', () =>
              readCustomerImportEffect(path, args.session)
            );
            const saved =
              parsed.status === 'ready'
                ? yield* this.database.listCustomers(this.requireSession(args.session).db)
                : [];
            const review = this.finalizeImport(
              {
                ...parsed,
                ...this.phase('import.match', () =>
                  matchCustomerImportRows(parsed.rows, saved)
                ),
              },
              saved
            );
            this.imports.set(review.importId, review);
            return structuredClone(review);
          })
        ),
      'imports.review': args =>
        this.ordinary(() =>
          Effect.gen({self: this}, function* () {
            this.requireSession(args.session);
            const review = this.imports.get(args.importId);
            if (!review || review.session !== args.session) {
              return yield* Effect.fail(
                new ActionError(
                  'STALE_SESSION',
                  'This import expired. Choose the file again.'
                )
              );
            }

            return structuredClone(review);
          })
        ),
      'imports.resolve': args =>
        this.ordinary(() =>
          Effect.gen({self: this}, function* () {
            this.requireSession(args.session);
            const review = this.imports.get(args.importId);
            if (!review || review.session !== args.session) {
              return yield* Effect.fail(
                new ActionError(
                  'STALE_SESSION',
                  'This import expired. Choose the file again.'
                )
              );
            }

            const row = review.rows.find(item => item.recordNumber === args.recordNumber);
            if (
              review.status !== 'ready' ||
              !row ||
              (!row.matches.length &&
                !row.collisionFields?.length &&
                row.choice === 'include')
            ) {
              return yield* Effect.fail(
                new ActionError(
                  'VALIDATION',
                  'Edit contact details or choose Add anyway or Skip only for a flagged record.'
                )
              );
            }

            const saved = yield* this.database.listCustomers(
              this.requireSession(args.session).db
            );
            const updated =
              'choice' in args
                ? this.finalizeImport(
                    {
                      ...review,
                      rows: review.rows.map(item =>
                        item === row ? {...item, choice: args.choice} : item
                      ),
                    },
                    saved
                  )
                : this.recheckImport(
                    {
                      ...review,
                      rows: review.rows.map(item =>
                        item === row
                          ? {
                              ...item,
                              values: {...item.values, [args.field]: args.value},
                              editedFields: [
                                ...new Set([...(item.editedFields ?? []), args.field]),
                              ],
                            }
                          : item
                      ),
                    },
                    saved,
                    row.recordNumber
                  );
            this.imports.set(review.importId, updated);
            return structuredClone(updated);
          })
        ),
      'database.status': () =>
        Effect.sync(() => ({status: 'success' as const, value: this.status()})),
      'database.retry': () =>
        this.ordinary(() => this.transition(lease => this.retry(lease))),
      'database.create': () =>
        this.ordinary(() => this.transition(lease => this.select('create', lease))),
      'database.open': () =>
        this.ordinary(() => this.transition(lease => this.select('open', lease))),
      'customers.list': args =>
        this.ordinary(() =>
          Effect.gen({self: this}, function* () {
            const handle = this.requireSession(args.session);
            return (yield* this.database.listCustomers(handle.db, args.query)).map(row =>
              this.record(row)
            );
          })
        ),
      'customers.get': args =>
        this.ordinary(() =>
          Effect.gen({self: this}, function* () {
            const row = yield* this.database.getCustomer(
              this.requireSession(args.session).db,
              args.id
            );
            if (!row) {
              return yield* Effect.fail(
                new ActionError(
                  'CUSTOMER_DELETED',
                  'This customer no longer exists. Reload the list.'
                )
              );
            }

            return this.record(row);
          })
        ),
      'customers.create': args =>
        this.ordinary(() =>
          Effect.gen({self: this}, function* () {
            return this.record(
              yield* this.database.createCustomer(
                this.requireSession(args.session).db,
                args.values
              )
            );
          })
        ),
      'customers.update': args =>
        this.ordinary(() =>
          Effect.gen({self: this}, function* () {
            return this.record(
              yield* this.database.updateCustomer(
                this.requireSession(args.reference.session).db,
                args.reference,
                definedChanges(args.changes)
              )
            );
          })
        ),
      'customers.delete': args =>
        this.ordinary(() =>
          Effect.gen({self: this}, function* () {
            yield* this.database.deleteCustomer(
              this.requireSession(args.reference.session).db,
              args.reference
            );
            return {deleted: true as const};
          })
        ),
      'database.backup': args => this.ordinary(() => this.backup(args.session)),
      'database.restore': () =>
        this.ordinary(() => this.transition(lease => this.restore(lease))),
      'exports.csv': args => this.ordinary(() => this.exportCsv(args.session)),
      'drafts.confirmDiscard': () =>
        this.outcome(
          native(() => this.options.dialogs.confirmDiscard()).pipe(
            Effect.map(approved => (approved ? {approved: true as const} : null))
          )
        ),
    };
  }
  status(): DatabaseState {
    return structuredClone(this.state);
  }

  onStateChanged(callback: (state: DatabaseState) => void): () => void {
    this.listeners.add(callback);
    return () => {
      this.listeners.delete(callback);
    };
  }

  private recheckImport(
    review: ImportReview,
    saved: CustomerData[],
    editedRecordNumber?: number
  ): ImportReview {
    const matched = matchCustomerImportRows(review.rows, saved);
    function signatures(groups: ImportReview['matchGroups']) {
      return new Map(
        groups.map(group => [
          group.id,
          JSON.stringify([
            group.reason,
            group.targets
              .map(target =>
                target.kind === 'csv'
                  ? `csv:${target.recordNumber}`
                  : `customer:${target.id}`
              )
              .sort(),
          ]),
        ])
      );
    }

    const previousMatches = signatures(review.matchGroups);
    const currentMatches = signatures(matched.matchGroups);
    return this.finalizeImport(
      {
        ...review,
        ...matched,
        rows: matched.rows.map((row, index) => {
          const previous = review.rows[index]!;
          if (row.recordNumber === editedRecordNumber) {
            return {...row, choice: 'unresolved'};
          }

          if (editedRecordNumber !== undefined && !row.matches.length) {
            if (previous.choice === 'unresolved' || previous.choice === 'skip') {
              return {...row, choice: previous.choice};
            }
          }

          const unchanged =
            row.matches.length === previous.matches.length &&
            row.matches.every(id => previousMatches.get(id) === currentMatches.get(id));
          return unchanged ? {...row, choice: previous.choice} : row;
        }),
      },
      saved
    );
  }

  private finalizeImport(review: ImportReview, saved: CustomerData[]): ImportReview {
    const updated = summarizeCustomerImport(review);
    const savedNumbers = saved.map(customer => customer.customerNumber);
    const planned = numberCustomerImportRows(
      updated.rows,
      savedNumbers,
      new Set(
        updated.rows
          .filter(row => row.choice === 'include' || row.choice === 'add')
          .map(row => row.recordNumber)
      )
    );
    const proposed = updated.rows.some(
      row => row.choice === 'unresolved' && !row.matches.length
    )
      ? numberCustomerImportRows(
          updated.rows,
          savedNumbers,
          new Set(
            updated.rows.filter(row => row.choice !== 'skip').map(row => row.recordNumber)
          )
        )
      : null;
    return {
      ...updated,
      numberChangeCount: planned.numberChangeCount,
      rows: planned.rows.map((row, index) => ({
        ...row,
        proposedCustomerNumber:
          row.choice === 'unresolved' && !row.matches.length
            ? (proposed?.rows[index]?.assignedCustomerNumber ?? undefined)
            : undefined,
      })),
    };
  }

  private record(row: CustomerData): CustomerRecord {
    const {revision, ...customer} = row;
    if (!this.state.session) {
      throw new ActionError('DATABASE_UNAVAILABLE', 'Choose a database first.');
    }

    return {customer, reference: {session: this.state.session, id: row.id, revision}};
  }

  private requireSession(session: string): DatabaseHandle {
    if (!this.active) {
      throw new ActionError(
        'DATABASE_UNAVAILABLE',
        'Create or open a customer database first.'
      );
    }

    if (this.state.session !== session) {
      throw new ActionError(
        'STALE_SESSION',
        'The selected database changed. Reload the customer list.'
      );
    }

    return this.active;
  }

  private publish(next: Omit<DatabaseState, 'version'>): void {
    this.state = {...next, version: this.state.version + 1};
    for (const listener of this.listeners) {
      try {
        listener(this.status());
      } catch (error) {
        this.logError(error);
      }
    }
  }

  private safeError(error: unknown): ContractError {
    error = immediateError(error);
    if (error instanceof ActionError) {
      return {code: error.code, message: error.message};
    }

    if (error instanceof DatabaseError) {
      switch (error.code) {
        case 'NOT_FOUND':
          return {
            code: 'CUSTOMER_DELETED',
            message: 'This customer no longer exists. Reload the list.',
          };
        case 'STALE_CUSTOMER':
          return {
            code: 'STALE_REVISION',
            message: 'This customer changed. Reload before editing.',
          };
        case 'VALIDATION':
        case 'CUSTOMER_NUMBER_CONFLICT':
        case 'MONEY_PRECISION':
          return {code: 'VALIDATION', message: error.message};
        case 'READ_ONLY':
          return {
            code: 'DATABASE_UNAVAILABLE',
            message: 'The database is read-only. Choose a writable copy.',
          };
        case 'UNSUPPORTED_DATABASE':
          return {
            code: 'DATABASE_UNAVAILABLE',
            message:
              'Choose a supported Shop Things database. Legacy or unrelated files cannot be opened.',
          };
      }
    }

    if (error instanceof Error && 'code' in error) {
      if (error.code === 'EEXIST') {
        return {
          code: 'DATABASE_UNAVAILABLE',
          message:
            'A file already exists at this destination. Choose another name or location.',
        };
      }

      if (error.code === 'EACCES' || error.code === 'EROFS' || error.code === 'ENOENT') {
        return {
          code: 'DATABASE_UNAVAILABLE',
          message:
            'The file could not be saved. Choose a writable folder and a new filename, then try again.',
        };
      }
    }

    this.logError(error);
    return {
      code: 'INTERNAL',
      message:
        'The operation failed. Check the file and folder permissions, then try again.',
    };
  }

  private phase<T>(name: string, work: () => T): T {
    return this.options.phase ? this.options.phase(name, work) : work();
  }

  private effectPhase<A, E, R>(
    name: string,
    work: () => Effect.Effect<A, E, R>
  ): Effect.Effect<A, E, R> {
    return Effect.suspend(() =>
      this.options.effectPhase ? this.options.effectPhase(name, work) : work()
    );
  }

  private logError(error: unknown): void {
    try {
      this.options.logError?.(immediateError(error));
    } catch {}
  }

  start(): Effect.Effect<void> {
    return this.ordinary(() => this.retry()).pipe(Effect.asVoid);
  }

  closeUnprotected(): void {
    if (this.owned.admission) {
      throw new Error('Wait for the current database operation');
    }

    this.active?.close();
    this.active = null;
    this.publish({
      available: false,
      selectedPath: this.state.selectedPath,
      session: null,
    });
  }

  closeUnprotectedWhenIdle(): Effect.Effect<void, BackendFailure> {
    return Effect.suspend((): Effect.Effect<void, BackendFailure> => {
      if (this.owned.lifecycle !== 'open') {
        return Effect.fail(
          new ActionError(
            'BUSY',
            'A protected lifecycle operation is already running. Try again when it finishes.'
          )
        );
      }

      this.owned.lifecycle = 'unprotected';
      return Effect.ensuring(
        this.drain().pipe(Effect.andThen(Effect.sync(() => this.closeUnprotected()))),
        Effect.sync(() => {
          if (this.owned.lifecycle === 'unprotected') {
            this.owned.lifecycle = 'open';
          }
        })
      );
    });
  }

  get lifecyclePending(): boolean {
    return this.owned.lifecycle !== 'open';
  }

  requestClose(): Effect.Effect<Outcome<{closed: true}>> {
    return this.lifecycle('closing', () =>
      this.transition(
        lease =>
          Effect.sync(() => {
            lease?.assertCurrent();
            this.closeUnprotected();
            return {closed: true as const};
          }),
        true
      )
    );
  }

  requestReload(
    scheduleReload: () => void | Promise<void>
  ): Effect.Effect<Outcome<{reloaded: true}>> {
    return this.lifecycle('reloading', () =>
      this.transition(
        lease =>
          Effect.gen(function* () {
            lease?.assertCurrent();
            const scheduled = scheduleReload();
            lease?.finish('committed');
            yield* native(() => Promise.resolve(scheduled));
            return {reloaded: true as const};
          }),
        true
      )
    );
  }

  private drain(): Effect.Effect<void> {
    return Effect.suspend(() => {
      const admission = this.owned.admission;
      return Effect.callback<void>(resume => {
        queueMicrotask(() =>
          resume(admission ? Deferred.await(admission.idle) : Effect.void)
        );
      });
    });
  }

  private lifecycle<T>(
    kind: Lifecycle,
    work: () => Effect.Effect<T | null, BackendFailure>
  ): Effect.Effect<Outcome<T>> {
    return Effect.uninterruptibleMask(restore =>
      Effect.suspend((): Effect.Effect<Outcome<T>> => {
        if (this.owned.lifecycle !== 'open') {
          return Effect.succeed({
            status: 'error' as const,
            error: {
              code: 'BUSY' as const,
              message:
                'A protected lifecycle operation is already running. Try again when it finishes.',
            },
          });
        }

        this.owned.lifecycle = kind;
        return Effect.ensuring(
          restore(this.outcome(this.drain().pipe(Effect.andThen(Effect.suspend(work))))),
          Effect.sync(() => {
            this.owned.lifecycle = 'open';
          })
        );
      })
    );
  }

  private ordinary<T>(
    work: () => Effect.Effect<T | null, BackendFailure>
  ): Effect.Effect<Outcome<T>> {
    return Effect.uninterruptibleMask(restore =>
      Effect.suspend((): Effect.Effect<Outcome<T>> => {
        if (this.owned.admission || this.owned.lifecycle !== 'open') {
          return Effect.succeed({
            status: 'error' as const,
            error: {
              code: 'BUSY' as const,
              message: 'Another operation is running. Please try again when it finishes.',
            },
          });
        }

        const admission = {idle: Deferred.makeUnsafe<void>()};
        this.owned.admission = admission;
        return Effect.ensuring(
          restore(this.outcome(Effect.suspend(work))),
          Effect.callback<void>(resume => {
            queueMicrotask(() => {
              if (this.owned.admission === admission) {
                this.owned.admission = null;
              }

              Deferred.doneUnsafe(admission.idle, Effect.void);
              resume(Effect.void);
            });
          })
        );
      })
    );
  }

  private outcome<T>(
    work: Effect.Effect<T | null, BackendFailure>
  ): Effect.Effect<Outcome<T>> {
    return work.pipe(
      Effect.map(value =>
        value === null
          ? {status: 'cancelled' as const}
          : {status: 'success' as const, value}
      ),
      Effect.catchCause(cause =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.interrupt
          : Effect.sync(() => ({
              status: 'error' as const,
              error: this.safeError(originalCause(cause)),
            }))
      )
    );
  }

  private transition<T>(
    work: (lease?: DraftLease) => Effect.Effect<T | null, BackendFailure>,
    closing = false
  ): Effect.Effect<T | null, BackendFailure> {
    return Effect.gen({self: this}, function* () {
      if (!this.active && !closing && !this.options.drafts) {
        return yield* work();
      }

      if (!this.options.drafts) {
        return yield* Effect.fail(
          new ActionError(
            'DATABASE_UNAVAILABLE',
            'Protected draft coordination is unavailable. Keep the application open.'
          )
        );
      }

      return yield* Effect.acquireUseRelease(
        this.options.drafts
          .prepareEffect()
          .pipe(Effect.mapError(error => new NativeFailure(error))),
        lease =>
          Effect.gen({self: this}, function* () {
            if (
              lease.hasUnsavedDraft &&
              !(yield* native(() => this.options.dialogs.confirmDiscard()))
            ) {
              return null;
            }

            lease.assertCurrent();
            return yield* work(lease);
          }),
        (lease, exit) =>
          Effect.sync(() =>
            lease.finish(
              Exit.isSuccess(exit) && exit.value !== null ? 'committed' : 'aborted'
            )
          )
      );
    });
  }

  private exportCsv(
    session: string
  ): Effect.Effect<{path: string} | null, BackendFailure> {
    return Effect.gen({self: this}, function* () {
      const handle = this.requireSession(session);
      const selected = yield* native(() => this.options.dialogs.exportCsv());
      if (selected === null) {
        return null;
      }

      const path = resolve(selected);
      const customers = yield* this.database.listCustomers(handle.db);
      yield* this.effectPhase('export.write', () =>
        writeCustomerCsvEffect(path, customers)
      );
      return {path};
    });
  }

  private backup(session: string): Effect.Effect<{path: string} | null, BackendFailure> {
    return Effect.gen({self: this}, function* () {
      const handle = this.requireSession(session);
      const selected = yield* native(() =>
        this.options.dialogs.backupDatabase(this.state.selectedPath!)
      );
      if (selected === null) {
        return null;
      }

      const path = resolve(selected);
      const temporary = temporaryPath(path);
      const result = yield* Effect.exit(
        Effect.gen({self: this}, function* () {
          yield* this.database.backupDatabase(handle.db, temporary);
          yield* native(() => copyFile(temporary, path, constants.COPYFILE_EXCL));
        })
      );
      yield* removeDatabaseFileEffect(temporary);
      if (Exit.isFailure(result)) {
        return yield* Effect.failCause(result.cause);
      }

      return {path};
    }).pipe(Effect.uninterruptible);
  }

  private restore(
    lease?: DraftLease
  ): Effect.Effect<DatabaseState | null, BackendFailure> {
    return Effect.gen({self: this}, function* () {
      const source = yield* native(() => this.options.dialogs.restoreSource());
      if (source === null) {
        return null;
      }

      const destination = yield* native(() => this.options.dialogs.restoreDestination());
      if (destination === null) {
        return null;
      }

      const path = resolve(destination);
      yield* this.effectPhase('restore.copy', () =>
        copyBackupEffect(resolve(source), path)
      );
      const prepared = yield* Effect.exit(this.prepareCandidate(path, false, lease));
      if (Exit.isFailure(prepared)) {
        yield* removeDatabaseFileEffect(path);
        return yield* Effect.failCause(prepared.cause);
      }

      return this.status();
    }).pipe(Effect.uninterruptible);
  }

  private retry(lease?: DraftLease): Effect.Effect<DatabaseState, BackendFailure> {
    return Effect.gen({self: this}, function* () {
      const attempted = yield* Effect.exit(
        Effect.gen({self: this}, function* () {
          const path = yield* (
            this.options.settings.readEffect?.() ??
              native(() => this.options.settings.read())
          );
          if (!path) {
            return this.status();
          }

          if (!this.active) {
            this.state = {...this.state, selectedPath: path};
          }

          yield* this.prepareCandidate(path, false, lease);
          return this.status();
        })
      );
      if (Exit.isSuccess(attempted)) {
        return attempted.value;
      }

      const safe = this.safeError(originalCause(attempted.cause));
      const recoveryError =
        safe.code === 'INTERNAL'
          ? {
              code: 'DATABASE_UNAVAILABLE' as const,
              message:
                'The remembered database could not be opened. Retry or choose Create/Open.',
            }
          : safe;
      if (!this.active) {
        this.publish({
          available: false,
          selectedPath: this.state.selectedPath,
          session: null,
          recoveryError,
        });
      }

      return yield* Effect.fail(
        new ActionError(recoveryError.code, recoveryError.message)
      );
    });
  }

  private select(
    kind: 'create' | 'open',
    lease?: DraftLease
  ): Effect.Effect<DatabaseState | null, BackendFailure> {
    return Effect.gen({self: this}, function* () {
      const path = yield* native(() =>
        kind === 'create'
          ? this.options.dialogs.createDatabase()
          : this.options.dialogs.openDatabase()
      );
      if (path === null) {
        return null;
      }

      const prepared = yield* Effect.exit(
        this.prepareCandidate(resolve(path), kind === 'create', lease)
      );
      if (Exit.isFailure(prepared)) {
        if (!this.active) {
          this.publish({
            available: false,
            selectedPath: this.state.selectedPath,
            session: null,
            recoveryError: this.safeError(originalCause(prepared.cause)),
          });
        }

        return yield* Effect.failCause(prepared.cause);
      }

      return this.status();
    });
  }

  private prepareCandidate(
    path: string,
    creation: boolean,
    lease?: DraftLease
  ): Effect.Effect<void, BackendFailure> {
    return Effect.gen({self: this}, function* () {
      let candidate: DatabaseHandle | null = null;
      const prepared = yield* Effect.exit(
        Effect.gen({self: this}, function* () {
          const open = creation
            ? this.database.createDatabase
            : this.database.openExistingDatabase;
          candidate = yield* open(path, {
            migrationsFolder: this.options.migrationsFolder,
          });
          lease?.assertCurrent();
          yield* this.writeSettings(path);
          const checked = yield* Effect.exit(Effect.sync(() => lease?.assertCurrent()));
          if (Exit.isFailure(checked)) {
            if (this.state.selectedPath) {
              yield* this.writeSettings(this.state.selectedPath);
            }

            return yield* Effect.failCause(checked.cause);
          }
        })
      );
      if (Exit.isFailure(prepared)) {
        const acquired = candidate as DatabaseHandle | null;
        acquired?.close();
        const error = originalCause(prepared.cause);
        if (error instanceof DatabaseError) {
          return yield* Effect.fail(error);
        }

        if (
          creation &&
          typeof error === 'object' &&
          error !== null &&
          'code' in error &&
          error.code === 'EEXIST'
        ) {
          return yield* Effect.fail(
            new ActionError(
              'DATABASE_UNAVAILABLE',
              'A file already exists at this destination. Choose another name or location.'
            )
          );
        }

        this.logError(error);
        return yield* Effect.fail(
          new ActionError(
            'DATABASE_UNAVAILABLE',
            'The database could not be selected. Check the file and folder permissions, then retry.'
          )
        );
      }

      const superseded = this.active;
      this.active = candidate;
      this.imports.clear();
      this.publish({available: true, selectedPath: path, session: randomUUID()});
      try {
        superseded?.close();
      } catch (error) {
        this.logError(error);
      }
    }).pipe(Effect.uninterruptible);
  }

  private writeSettings(path: string): Effect.Effect<void, BackendFailure> {
    return (
      this.options.settings.writeEffect?.(path) ??
      native(() => this.options.settings.write(path))
    );
  }
}
