from dataclasses import dataclass
from typing import List, Dict


@dataclass
class Coin:
    symbol: str       # Internal (e.g. "BTC")
    name: str         # Display (e.g. "Bitcoin")
    yf_ticker: str    # Yahoo Finance (e.g. "BTC-USD")
    cg_id: str        # CoinGecko (e.g. "bitcoin")
    cd_instrument: str  # CoinDesk (e.g. "BTC-USD")


class CoinRegistry:
    _COINS = [
        Coin("BTC", "Bitcoin", "BTC-USD", "bitcoin", "BTC-USD"),
        Coin("ETH", "Ethereum", "ETH-USD", "ethereum", "ETH-USD"),
        Coin("SOL", "Solana", "SOL-USD", "solana", "SOL-USD"),
        Coin("XRP", "Ripple", "XRP-USD", "ripple", "XRP-USD"),
    ]

    @classmethod
    def get_yf_map(cls) -> Dict[str, str]:
        """Returns {yf_ticker: symbol} for Yahoo Finance scripts."""
        return {c.yf_ticker: c.symbol for c in cls._COINS}

    @classmethod
    def get_cg_ids(cls) -> List[str]:
        """Returns list of CoinGecko IDs."""
        return [c.cg_id for c in cls._COINS]

    @classmethod
    def get_cd_instruments(cls) -> str:
        """Returns comma-separated CoinDesk instruments string."""
        return ",".join(c.cd_instrument for c in cls._COINS)

    @classmethod
    def get_all(cls) -> List[Coin]:
        """Returns all registered coins."""
        return cls._COINS.copy()

    @classmethod
    def get_by_symbol(cls, symbol: str) -> Coin:
        """Get a coin by its internal symbol."""
        for c in cls._COINS:
            if c.symbol == symbol:
                return c
        raise ValueError(f"Coin not found: {symbol}")
