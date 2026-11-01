"""Windows-local worker controls; never exposed through the visitor HTTP API."""

import argparse
import asyncio
import json
from pathlib import Path
import re

from api import worker_registry
from api.mt5_binding import MAX_CONFIG_BYTES, target_revision
from api.mt5_host import MT5Host
from api.mt5_ownership import pair_state
from shared.mt5_ownership import FileFence, OwnershipBusy, operation_fence

ROOT = Path(__file__).resolve().parent


def host_fence(root):
    """One local API host owns this source root for its full lifetime."""
    return FileFence(Path(root) / 'cache/mt5/ownership/host.lock')


def load_targets(root, policy):
    """Bind explicit local terminals to selected source aliases, without activation."""
    path = Path(root) / 'config.local.json'
    with path.open('rb') as stream:
        raw = stream.read(MAX_CONFIG_BYTES + 1)
    if len(raw) > MAX_CONFIG_BYTES:
        raise ValueError('Oversized local profile')
    profile = json.loads(raw)
    if type(profile) is not dict or set(profile) != {'targets'} or type(profile['targets']) is not list:
        raise ValueError('Invalid local profile')
    targets = {}
    for item in profile['targets']:
        if type(item) is not dict or set(item) != {'terminal_id', 'source_alias'}:
            raise ValueError('Invalid local target')
        tid, alias = item['terminal_id'], item['source_alias']
        if (type(tid) is not str or re.fullmatch(r'term_[0-9a-f]{8}', tid) is None
                or type(alias) is not str or alias not in policy.sources or tid in targets):
            raise ValueError('Invalid local target binding')
        target_revision(Path(root) / 'cache/mt5/config.json', tid)
        targets[tid] = policy.sources[alias]
    if not targets or len(set(targets.values())) != len(targets):
        raise ValueError('Explicit unique local targets required')
    return targets


def save_intent(host, terminal_id, active):
    """Replace config atomically while the caller holds the operation fence."""
    target_revision(host.config_path, terminal_id)
    data = json.loads(host.config_path.read_text(encoding='utf-8'))
    for terminal in data['terminals']:
        if terminal['id'] == terminal_id:
            terminal['active'] = active
    temporary = host.config_path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, indent=2), encoding='utf-8')
    temporary.replace(host.config_path)


async def control(root, action, terminal_id, targets, *, host=None):
    """Serialize local intent and exact-process operations, retaining cancellation ownership."""
    if terminal_id not in targets or action not in {'start', 'stop', 'status'}:
        raise ValueError('Unknown local command or target')
    host = host if host is not None else MT5Host(root)
    async def operation():
        if action == 'start' and host.runtime_managed:
            from api.runtime_session import request_launch
            await request_launch(host, terminal_id, targets[terminal_id], explicit=True)
        with operation_fence(root):
            if action == 'start' and not host.runtime_managed:
                try:
                    with host_fence(root):
                        pass
                except OwnershipBusy:
                    pass
                else:
                    raise ValueError('Start the local API host before starting workers')
                current = pair_state(root, terminal_id, host.inspect(root, terminal_id))
                if current not in {'stopped', 'running'}:
                    raise ValueError('Partial or unknown worker state requires an explicit stop')
                save_intent(host, terminal_id, True)
                host.start_pair(terminal_id, expected_source=targets[terminal_id])
            elif action == 'stop':
                # Persist stop before killing so watchdog recovery cannot revive the pair.
                if host.runtime_managed:
                    from api.runtime_session import invalidate, policy
                    policy(root)
                    invalidate(root)
                save_intent(host, terminal_id, False)
                await host.stop_pair(terminal_id, worker_registry)
            _, active = target_revision(host.config_path, terminal_id)
            state = pair_state(root, terminal_id, host.inspect(root, terminal_id))
            result = {'terminal_id': terminal_id, 'active': active, 'state': state}
            if host.runtime_managed:
                from api.runtime_session import status
                result.update(status(host, terminal_id))
            return result
    task = asyncio.create_task(operation())
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return result


def main():
    """Control only targets selected by the private local profile and visitor policy."""
    from run_read_only import load_policy
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('start', 'stop', 'status'))
    parser.add_argument('--terminal-id', required=True)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    try:
        policy = load_policy(args.config.resolve())
        targets = load_targets(ROOT, policy)
        print(json.dumps(asyncio.run(control(ROOT, args.action, args.terminal_id, targets,
                                            host=MT5Host(ROOT, symbols=policy.symbols)))))
    except (OSError, ValueError, TypeError, KeyError, OwnershipBusy):
        parser.exit(2, 'Local worker command failed; check profile, ownership and private worker logs.\n')


if __name__ == '__main__':
    main()
