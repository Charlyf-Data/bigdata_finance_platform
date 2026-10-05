def transrorm_kline(data: dict) -> dict:
    """Transforme les données brutes du flux Binance en un format plus simple.

    Args:
        data: Données brutes reçues du flux Binance.

    Returns:
        dict: Données transformées avec les champs pertinents.
    """
    kline = data["k"]

    transformed_kline = {
        "symbol": data["s"],
        "interval": kline["i"],
        "open": float(kline["o"]),
        "close": float(kline["c"]),
        "high": float(kline["h"]),
        "low": float(kline["l"]),
        "volume": float(kline["v"]),
        "nb_trades": kline["n"],
        "is_closed": kline["x"],
        "open_time": kline["t"],
        "close_time": kline["T"],
        "event_time": data["E"],
    }

    return transformed_kline