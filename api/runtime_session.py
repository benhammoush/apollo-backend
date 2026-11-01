"""Local task-mediated worker activation, fenced against Stop and reused Windows logins."""

import asyncio
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

from api.mt5_binding import target_revision
from api.mt5_ownership import pair_state
from shared.mt5_ownership import operation_fence, OwnershipBusy
from shared import windows_session as windows


def policy(root):
    """Load the private, administrator-owned task/account binding; never accept caller commands."""
    path = Path(root) / 'runtime-session.local.json'
    if any(p.is_symlink() or p.is_junction() for p in (path, *path.parents)):
        raise ValueError('Linked runtime policy forbidden')
    data = read_json(path)
    if (set(data) != {'sid', 'task_name', 'dispatcher'} or
            data['task_name'] != 'Apollo-Runtime-Workers' or
            data['dispatcher'] != r'C:\ApolloRuntime\tools\Invoke-ApolloRuntimeTask.ps1' or
            data['sid'] != windows.account_sid(os.environ['COMPUTERNAME'] + '\\apollo-runtime')):
        raise ValueError('Invalid runtime session policy')
    windows.assert_account(data['sid'])
    return data


def read_json(path):
    """Read bounded local control state; corruption is unavailable, not absent intent."""
    with Path(path).open('rb') as stream:
        raw = stream.read(16385)
    data = json.loads(raw)
    if len(raw) > 16384 or type(data) is not dict:
        raise ValueError('Invalid runtime control state')
    return data


def state_path(root):
    """One small private activation record per selected runtime, not a process registry."""
    return Path(root) / 'cache/mt5/ownership/runtime-session.json'


def read_state(root):
    """An absent activation is inert; malformed existing state fails closed."""
    path = state_path(root)
    return read_json(path) if path.exists() else None


def write_state(root, state):
    """Atomically replace control state while the caller owns the operation fence."""
    path = state_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(state), encoding='utf-8')
    temporary.replace(path)


def invalidate(root):
    """Disable pending/recovery authority before Stop kills any worker; retain no queued command."""
    write_state(root, {'disabled': True})


def valid_activation(root, terminal_id, binding=None):
    """Require exact active execution metadata and the same still-existing Windows login."""
    config = policy(root)
    state = read_state(root)
    if not state or state.get('disabled') is True:
        raise ValueError('Use workers start to activate this runtime login')
    revision, _ = target_revision(Path(root) / 'cache/mt5/config.json', terminal_id, require_active=True)
    if (set(state) != {'activation', 'binding', 'terminal_id', 'revision', 'source', 'pending', 'result', 'established'} or
            state['terminal_id'] != terminal_id or state['revision'] != revision or
            not re.fullmatch(r'[0-9a-f]{32}', state['activation']) or
            type(state['established']) is not bool or
            type(state['source']) is not str or not state['source'] or
            state['binding'] != (binding or windows.selected_session(config['sid']))):
        raise ValueError('Runtime activation changed or belongs to an ended login')
    return state


def assert_local_launch(root, terminal_id, activation):
    """Used by the task and child: no native MT5 initialization outside the approved login."""
    state = valid_activation(root, terminal_id)
    if state['activation'] != activation:
        raise ValueError('Worker activation was cancelled or replaced')
    windows.assert_current(state['binding'])
    return state


def inspect_bound(host, terminal_id, binding):
    """Inventory all matching workers; never reuse a pair from another account/session."""
    import psutil
    entries = host.inspect(host.root, terminal_id)
    for entry in entries:
        process = psutil.Process(entry.pid)
        if (process.create_time() != entry.created or
                windows.account_sid(process.username()) != binding['sid'] or
                windows.process_session(entry.pid) != binding['session_id']):
            raise ValueError('Worker identity/session mismatch; use explicit Stop')
    return entries


def dispatch(root, session_id):
    """Ask one fixed Windows task to run; COM acceptance is not worker-start confirmation."""
    config = policy(root)
    powershell = Path(os.environ['SystemRoot']) / 'System32/WindowsPowerShell/v1.0/powershell.exe'
    result = subprocess.run([str(powershell), '-NoLogo', '-NoProfile', '-NonInteractive',
        '-File', config['dispatcher'], '-SessionId', str(session_id)],
        cwd=root, capture_output=True, timeout=15, creationflags=subprocess.CREATE_NO_WINDOW)
    if result.returncode:
        raise ValueError('Runtime task dispatch failed; inspect private task configuration')


def _request(host, terminal_id, source, *, explicit):
    """Prepare one fenced request; dispatch/wait happens only after releasing this lock."""
    from local_workers import host_fence, save_intent
    config = policy(host.root)
    binding = windows.selected_session(config['sid'])
    with operation_fence(host.root):
        try:
            with host_fence(host.root):
                pass
        except OwnershipBusy:
            pass
        else:
            raise ValueError('Start the local API host before starting workers')
        if explicit:
            previous = read_state(host.root)
            if (previous and previous.get('pending') is not None and
                    previous['pending']['expires'] >= time.time()):
                raise OwnershipBusy('Worker launch already pending; inspect before retrying')
            entries = inspect_bound(host, terminal_id, binding)
            current = pair_state(host.root, terminal_id, entries)
            if current not in {'stopped', 'running'}:
                raise ValueError('Partial or unknown workers require explicit Stop')
            if current == 'running':
                state = valid_activation(host.root, terminal_id, binding)
                if state['pending'] is not None:
                    raise OwnershipBusy('Worker launch already pending')
                return None
            save_intent(host, terminal_id, True)
            revision, _ = target_revision(host.config_path, terminal_id, require_active=True)
            try:
                host.approved_source_pin(terminal_id, revision, source)
            except Exception:
                save_intent(host, terminal_id, False)
                raise
            state = {'activation': uuid.uuid4().hex, 'binding': binding, 'terminal_id': terminal_id,
                'revision': revision, 'source': source, 'pending': None, 'result': None, 'established': False}
        else:
            state = valid_activation(host.root, terminal_id, binding)
            if state['pending'] is not None:
                if state['pending']['expires'] >= time.time():
                    return None
                state['pending'] = None
                state['result'] = None
                write_state(host.root, state)
            if not state['established']:
                invalidate(host.root)
                save_intent(host, terminal_id, False)
                raise ValueError('Unconfirmed Start expired; use explicit Start again')
            entries = inspect_bound(host, terminal_id, binding)
            current = pair_state(host.root, terminal_id, entries)
            if current in {'running', 'starting'}:
                return None
            if current not in {'stopped', 'partial'}:
                raise ValueError('Ambiguous workers block recovery')
        request_id = uuid.uuid4().hex
        state['pending'] = {'id': request_id, 'expires': time.time() + 25}
        state['result'] = None
        write_state(host.root, state)
        return state['activation'], request_id, binding


async def request_launch(host, terminal_id, source=None, *, explicit=False):
    """Run a bounded task request and invalidate timed-out authority without automatic repair."""
    # A cancelled to_thread await does not stop its thread: retain preparation ownership.
    preparation = asyncio.create_task(asyncio.to_thread(_request, host, terminal_id, source, explicit=explicit))
    interrupted = False
    while True:
        try:
            request = await asyncio.shield(preparation)
            break
        except asyncio.CancelledError:
            if preparation.cancelled():
                raise
            interrupted = True
    if request is None:
        if interrupted:
            raise asyncio.CancelledError
        return False
    activation, request_id, binding = request
    try:
        if interrupted:
            raise asyncio.CancelledError
        await asyncio.to_thread(dispatch, host.root, binding['session_id'])
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                with operation_fence(host.root):
                    state = valid_activation(host.root, terminal_id, binding)
                    if state['activation'] != activation:
                        raise ValueError('Worker Start was superseded')
                    result = state['result']
                    if result is not None and result['id'] == request_id:
                        if result['ok'] is not True:
                            raise ValueError('Runtime task failed; inspect worker logs and use Stop before retrying')
                        return True
            except OwnershipBusy:
                pass
            await asyncio.sleep(0.1)
        raise ValueError('Worker task confirmation timed out; inspect before retrying')
    except BaseException:
        # A late/queued task must not resurrect this Start. Never disable a newer activation.
        deadline = time.monotonic() + 5
        while True:
            try:
                with operation_fence(host.root):
                    state = read_state(host.root)
                    owns_request = (state and state.get('activation') == activation and
                        ((state.get('pending') or {}).get('id') == request_id or
                         (state.get('result') or {}).get('id') == request_id))
                    if owns_request and explicit:
                        from local_workers import save_intent
                        invalidate(host.root)
                        save_intent(host, terminal_id, False)
                    elif owns_request and (state.get('pending') or {}).get('id') == request_id:
                        # API shutdown cancels this recovery request, not the established activation.
                        state['pending'] = None
                        state['result'] = None
                        write_state(host.root, state)
                break
            except OwnershipBusy:
                if time.monotonic() >= deadline:
                    raise ValueError('Runtime cancellation could not acquire ownership; inspect before retrying')
                await asyncio.sleep(0.05)
        raise


def run_pending(root, policy_config):
    """Task action: recheck bounded current intent and fill missing modes in this desktop only."""
    from api.mt5_host import MT5Host
    from local_workers import load_targets
    config = policy(root)
    binding = windows.selected_session(config['sid'])
    windows.assert_current(binding)
    with operation_fence(root):
        state = read_state(root)
        if not state or state.get('disabled') is True:
            return
        terminal_id = state['terminal_id']
        state = valid_activation(root, terminal_id, binding)
        pending = state['pending']
        if pending is None:
            return
        ok = False
        try:
            if pending['expires'] < time.time():
                raise ValueError('Expired worker launch request')
            targets = load_targets(root, policy_config)
            if targets.get(terminal_id) != state['source']:
                raise ValueError('Approved market source changed')
            host = MT5Host(root, symbols=policy_config.symbols, runtime_activation=state['activation'])
            inspect_bound(host, terminal_id, binding)
            host.start_pair(terminal_id, expected_source=state['source'])
            ok = True
            state['established'] = True
        finally:
            state['pending'] = None
            state['result'] = {'id': pending['id'], 'ok': ok}
            write_state(root, state)


def status(host, terminal_id):
    """Report process placement separately from persisted intent and unverified feed health."""
    config = policy(host.root)
    try:
        binding = windows.selected_session(config['sid'])
    except windows.SessionError:
        return {'session_id': None, 'session': 'unavailable', 'activation': 'inactive', 'connection': 'unverified'}
    inspect_bound(host, terminal_id, binding)
    try:
        state = valid_activation(host.root, terminal_id, binding)
        activation = 'starting' if state['pending'] else 'enabled'
    except ValueError:
        activation = 'inactive'
    return {'session_id': binding['session_id'], 'session': 'available',
            'activation': activation, 'connection': 'unverified'}
