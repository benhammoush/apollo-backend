"""Public market-symbol defaults only, independent of private strategy settings."""

DEFAULT_SYMBOLS = (
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD", "AUDJPY", "AUDCHF", "AUDCAD",
    "AUDNZD", "NZDJPY", "NZDCHF", "NZDCAD", "CADJPY", "CADCHF", "CHFJPY",
)


def default_symbols() -> list[str]:
    """Return a fresh ordered list so private configuration instances stay independent."""
    return list(DEFAULT_SYMBOLS)
