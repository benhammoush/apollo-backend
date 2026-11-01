"""Single visitor/callback host with local-only worker controls and explicit recovery scope."""

import argparse
import asyncio
from pathlib import Path
import sys

from api.read_only import validate_market_start_binding
from api.mt5_host import MT5Host
from local_workers import ROOT, host_fence, load_targets
from run_read_only import load_policy, serve_live


async def serve_local(policy, targets):
    """Recover only selected current-login activations; startup never creates fresh worker intent."""
    with host_fence(ROOT):
        await serve_live(policy, 8001, callback_port=8000, recovery_terminal_ids=tuple(targets),
                         mt5_host=MT5Host(ROOT, symbols=policy.symbols))


def main():
    """Validate the live private profile before binding either loopback listener."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    try:
        policy = load_policy(args.config.resolve())
        validate_market_start_binding(policy, None)
        if policy.fixture:
            raise ValueError('Live policy required')
        targets = load_targets(ROOT, policy)
    except (OSError, ValueError, TypeError, KeyError):
        parser.exit(2, 'Invalid local live profile.\n')
    # Socket-only Windows selector I/O avoids the previously observed Proactor listener loss.
    factory = asyncio.SelectorEventLoop if sys.platform == 'win32' else None
    asyncio.run(serve_local(policy, targets), loop_factory=factory)


if __name__ == '__main__':
    main()
