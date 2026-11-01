"""Market-only worker defaults; no private strategy or writer policy."""

from dataclasses import dataclass, field

from shared.market_defaults import default_symbols


@dataclass
class MT5WorkerConfig:
    """Independent ordered symbols, with explicit host/private override support."""
    symbols: list[str] = field(default_factory=default_symbols)


DEFAULT_WORKER_CONFIG = MT5WorkerConfig()
