"""
Configuration centralisée du consumer Binance.

Ce module regroupe les paramètres de connexion au stream de klines
Binance dans une dataclass immuable.

Une "kline" (contraction de "candlestick line") est une bougie de
chandelier japonais. Elle représente l'activité de marché sur un
intervalle de temps donné (ici 1 minute) et contient les valeurs OHLCV :

    - O (Open)   : prix d'ouverture de l'intervalle
    - H (High)   : prix le plus haut atteint
    - L (Low)    : prix le plus bas atteint
    - C (Close)  : prix de clôture de l'intervalle
    - V (Volume) : volume échangé pendant l'intervalle

Tant que l'intervalle n'est pas écoulé, la kline est "ouverte" et ses
valeurs évoluent en temps réel (champ x=False). À la fin de l'intervalle
elle est "fermée" et devient définitive (champ x=True) : c'est à ce
moment qu'elle doit être persistée pour éviter les doublons.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class BinanceConfig:
    ws_base: str = "wss://data-stream.binance.vision/ws"
    symbol: str = "btcusdt"
    interval: str = "1m"
    max_reconnect_delay: int = 60

    def build_url(self) -> str:
        return f"{self.ws_base}/{self.symbol.lower()}@kline_{self.interval}"