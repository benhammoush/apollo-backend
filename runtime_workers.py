"""Fixed non-elevated Windows task action; no externally supplied commands or worker targets."""

import logging
from pathlib import Path


def main():
    """Load only the installed private policy, recording errors without credentials or raw config."""
    from api.runtime_session import run_pending
    from run_read_only import load_policy
    root = Path(__file__).resolve().parent
    log = root / 'cache/logs/runtime-task.log'
    log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=log, level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    try:
        run_pending(root, load_policy(root / 'visitor-policy.local.json'))
    except Exception:
        logging.exception('Runtime task failed')
        raise


if __name__ == '__main__':
    main()
