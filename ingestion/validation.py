"""Validation des messages reçus du stream Binance.

Fonctions pures sans état. Principe de sécurité : ne jamais faire
confiance aux données externes avant de les avoir validées.
"""


def is_valid_kline(data: dict) -> bool:
    """Vérifie qu'un message reçu a la structure attendue d'une kline.

    Args:
        data: Dictionnaire issu du parsing JSON du message Binance.

    Returns:
        True si le message contient une kline avec tous les champs
        OHLCV requis, False sinon.
    """
    if not isinstance(data, dict) or "k" not in data:
        return False

    required_fields = ["o", "h", "l", "c", "v", "x"]
    kline = data["k"]

    return isinstance(kline, dict) and all(field in kline for field in required_fields)


def is_closed_candle(data: dict) -> bool:
    """Indique si la bougie est fermée et définitive.

    Seules les bougies fermées (x=True) doivent être envoyées vers
    Kafka pour éviter les doublons de bougies en cours de formation.

    Args:
        data: Dictionnaire d'une kline déjà validée.

    Returns:
        True si la bougie est fermée, False si elle est en cours.
    """
    return data["k"]["x"] is True








def is_closed_candle(data: dict) -> bool:
    """Indique si la bougie (kline) est fermée et définitive.

    Une kline est fermée quand son intervalle de temps est écoulé.
    Le champ 'x' du message Binance vaut alors True. Tant que x=False,
    la bougie est encore en formation et ses valeurs OHLCV changent.

    Seules les bougies fermées doivent être envoyées vers Kafka pour
    éviter d'enregistrer plusieurs versions de la même bougie.

    Args:
        data: Dictionnaire d'une kline déjà validée.

    Returns:
        True si la bougie est fermée (x=True), False si en cours.
    """
    return data["k"]["x"] is True