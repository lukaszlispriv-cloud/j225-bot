# Konfiguracja gunicorna - wczytywana automatycznie z katalogu roboczego.
# Nadzorca bota MUSI startować w procesie workera (po fork-u), nie w procesie nadrzędnym.
workers = 1
threads = 4
timeout = 60

def post_fork(server, worker):
    import app
    app.start_background()
