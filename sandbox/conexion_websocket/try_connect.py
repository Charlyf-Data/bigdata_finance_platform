import websocket
import time

tentative = 0
running = True

def on_open(wsapp):
    global tentative, running
    tentative = 0
    running = True
    print("Connexion établie")

def on_message(wsapp, message):
    print(f"Message reçu: {message}")

def on_error(wsapp, error):
    print(f"Erreur: {error}")

def on_close(wsapp, close_status_code, close_msg):
    global running
    running = False
    print(f"Connexion fermée — code: {close_status_code}")

def on_ping(wsapp, message):
    print("Ping reçu — pong automatique")

def on_pong(wsapp, message):
    print("Pong reçu")

wsapp = websocket.WebSocketApp(
    "wss://data-stream.binance.vision/ws/btcusdt@kline_1m",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_ping=on_ping,
    on_pong=on_pong
)

while running:
    try:
        wsapp.run_forever()
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if running:
            delai = min(2 ** tentative, 60)
            print(f"Reconnexion dans {delai}s... (tentative {tentative})")
            time.sleep(delai)
            tentative += 1