import type {DraftRequest, DraftResolution} from '@shop-things/contract';
import {expect, test} from 'vitest';

import {
  ActionService,
  databaseOperations,
  type ActionServiceOptions,
} from '../src/actionService.js';
import {DraftCoordinator} from '../src/draftCoordinator.js';
import {fixture, success, values} from './backendFixture.js';

function simulatedEditor(coordinator: DraftCoordinator) {
  const editor = {
    frozen: false,
    draft: '',
    selection: 1 as number | null,
    requests: [] as DraftRequest[],
    resolutions: [] as DraftResolution[],
    respond: true,
    dirty: false,
  };
  const participant = {
    documentId: 'document-1',
    prepare(request: DraftRequest) {
      editor.frozen = true;
      editor.requests.push(request);
      if (editor.respond) {
        coordinator.reply(participant, {...request, hasUnsavedDraft: editor.dirty});
      }
    },
    resolve(resolution: DraftResolution) {
      editor.resolutions.push(resolution);
      editor.frozen = false;
      if (resolution.outcome === 'committed') {
        editor.draft = '';
        editor.selection = null;
      }
    },
  };
  const unregister = coordinator.register(participant);
  return {
    editor,
    participant,
    unregister,
    edit() {
      editor.dirty = true;
      editor.draft = 'unsaved';
      editor.selection = 1;
      editor.requests = [];
      editor.resolutions = [];
    },
  };
}

// Poll until a protected operation has parked at the discard-consent step: the
// editor is frozen with the unsaved draft, so the operation holds the backend's
// lifecycle (closing/reloading) without draining or closing anything.
async function untilParked(editor: {frozen: boolean}, timeoutMs = 4000): Promise<void> {
  const start = Date.now();
  while (!editor.frozen) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('Protected operation did not park at the discard consent');
    }

    await new Promise(resolve => setTimeout(resolve, 1));
  }
}

function withHold(options: ActionServiceOptions): {
  service: ActionService;
  release: (consent: boolean) => void;
  closed: () => number;
} {
  let consent: Promise<boolean> | null = null;
  let resolveConsent: ((value: boolean) => void) | null = null;
  let closed = 0;
  const held: ActionServiceOptions = {
    ...options,
    dialogs: {
      ...options.dialogs,
      confirmDiscard: async () => {
        // Hold the consent so a protected operation with an unsaved draft parks
        // here (holding the lifecycle) instead of proceeding.
        if (!consent) {
          consent = new Promise<boolean>(resolve => {
            resolveConsent = resolve;
          });
        }

        return consent;
      },
    },
    database: {
      ...databaseOperations,
      createDatabase: async (
        ...args: Parameters<typeof databaseOperations.createDatabase>
      ) => {
        const handle = await databaseOperations.createDatabase(...args);
        return {
          ...handle,
          close() {
            closed++;
            handle.close();
          },
        };
      },
    },
  };
  return {
    service: new ActionService(held),
    release: value => {
      resolveConsent?.(value);
    },
    closed: () => closed,
  };
}

test('unprotected close refuses with BUSY while a protected close owns the backend', async () => {
  const drafts = new DraftCoordinator();
  const participant = simulatedEditor(drafts);
  const f = await fixture({drafts});
  const hold = withHold(f.options);
  try {
    const state = success(await hold.service.handlers['database.create']());
    success(
      await hold.service.handlers['customers.create']({session: state.session!, values})
    );
    participant.edit();

    // Start a protected close; it parks at the discard consent for the unsaved draft.
    const close = hold.service.requestClose();
    await untilParked(participant.editor);

    // An unprotected close arriving while the protected close owns the backend must
    // be refused with BUSY and must not close the active database handle.
    await expect(hold.service.closeUnprotectedWhenIdle()).rejects.toMatchObject({
      code: 'BUSY',
    });
    expect(hold.closed()).toBe(0);
    expect(hold.service.status()).toEqual(state);

    // Ordinary requests remain blocked while the protected owner is parked.
    expect(
      await hold.service.handlers['customers.create']({session: state.session!, values})
    ).toMatchObject({status: 'error', error: {code: 'BUSY'}});

    // The user chooses Stay: the protected close aborts and the draft + database
    // must remain available afterward.
    hold.release(false);
    const closeResult = await close;
    expect(closeResult).toMatchObject({status: 'cancelled'});
    expect(hold.service.status()).toEqual(state);
    expect(participant.editor).toMatchObject({
      draft: 'unsaved',
      selection: 1,
      frozen: false,
    });
    expect(hold.closed()).toBe(0);
    expect(
      success(
        await hold.service.handlers['customers.get']({session: state.session!, id: 1})
      )
    ).toMatchObject({customer: {firstName: 'Anne'}});

    // Now that the protected owner has finished and the DB is idle, a normal
    // unprotected close drains and closes exactly once.
    await hold.service.closeUnprotectedWhenIdle();
    expect(hold.closed()).toBe(1);
    expect(hold.service.status()).toMatchObject({available: false, session: null});
  } finally {
    participant.unregister();
    hold.service.closeUnprotected();
    await f.cleanup();
  }
});

test('unprotected close refuses with BUSY while a protected reload owns the backend', async () => {
  const drafts = new DraftCoordinator();
  const participant = simulatedEditor(drafts);
  const f = await fixture({drafts});
  const hold = withHold(f.options);
  try {
    const state = success(await hold.service.handlers['database.create']());
    success(
      await hold.service.handlers['customers.create']({session: state.session!, values})
    );
    participant.edit();

    // Start a protected reload; it parks at the discard consent.
    const reload = hold.service.requestReload(() => {});
    await untilParked(participant.editor);

    await expect(hold.service.closeUnprotectedWhenIdle()).rejects.toMatchObject({
      code: 'BUSY',
    });
    expect(hold.closed()).toBe(0);
    expect(hold.service.status()).toEqual(state);

    // The reload stays parked; the draft and database survive.
    hold.release(false);
    const reloadResult = await reload;
    expect(reloadResult).toMatchObject({status: 'cancelled'});
    expect(hold.service.status()).toEqual(state);
    expect(participant.editor).toMatchObject({draft: 'unsaved', frozen: false});
    expect(
      success(
        await hold.service.handlers['customers.get']({session: state.session!, id: 1})
      )
    ).toMatchObject({customer: {firstName: 'Anne'}});

    await hold.service.closeUnprotectedWhenIdle();
    expect(hold.closed()).toBe(1);
  } finally {
    participant.unregister();
    hold.service.closeUnprotected();
    await f.cleanup();
  }
});

test('repeated unprotected close facade calls share one promise and stay fresh after a refusal', async () => {
  const drafts = new DraftCoordinator();
  const participant = simulatedEditor(drafts);
  const f = await fixture({drafts});
  const hold = withHold(f.options);
  try {
    success(await hold.service.handlers['database.create']());
    participant.edit();
    const close = hold.service.requestClose();
    await untilParked(participant.editor);

    // Both unprotected close calls return the same pending promise; both settle as
    // BUSY refusals while the protected close owns the backend.
    const first = hold.service.closeUnprotectedWhenIdle();
    const second = hold.service.closeUnprotectedWhenIdle();
    expect(second).toBe(first);
    await Promise.allSettled([first, second]);
    // The pending slot is cleared, so a later unprotected close is a fresh call
    // (not poisoned by the earlier refusal) and is still refused while parked.
    const after = hold.service.closeUnprotectedWhenIdle();
    expect(after).not.toBe(first);
    await expect(after).rejects.toMatchObject({code: 'BUSY'});
    expect(hold.service.status().available).toBe(true);
    expect(hold.closed()).toBe(0);

    hold.release(false);
    const [settledClose] = await Promise.allSettled([close]);
    expect(settledClose.status).toBe('fulfilled');
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((settledClose as any).value).toMatchObject({status: 'cancelled'});
    await hold.service.closeUnprotectedWhenIdle();
    expect(hold.closed()).toBe(1);
  } finally {
    participant.unregister();
    hold.service.closeUnprotected();
    await f.cleanup();
  }
});
