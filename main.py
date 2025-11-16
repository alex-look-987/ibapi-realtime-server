import sys, signal, subprocess, time, os

def run_script(script_name, server_port=None, clientid=None, contract=None):
    if server_port:
        return subprocess.Popen([sys.executable, script_name, server_port, clientid, contract])
    else:
        return subprocess.Popen([sys.executable, script_name])

def handler(sig, frame):
    print("\nSolicitando apagado del servidor...")
    open("shutdown.flag", "w").close()  # Señal simple

    # Dar unos segundos para permitir al server cerrar limpiamente
    time.sleep(2)

    print("Terminando procesos hijos...")
    for p in processes:
        if p.poll() is None:
            print(f"Matando PID {p.pid}")
            p.terminate()

    if os.path.exists("shutdown.flag"):
        os.remove("shutdown.flag")

    sys.exit(0)

accounts_no = 1
clientid = "12346"
contract = "EURUSD"
server_port = "8771"

if __name__ == "__main__":
    scripts = [
        "server.py",
        "streaming_data.py",
    ]

    processes: list[subprocess.Popen] = []

    for script in scripts:
        p = run_script(script, server_port=server_port, clientid=clientid, contract=contract)
        processes.append(p)

        print(f"Lanzado {script} PID={p.pid}")

    signal.signal(signal.SIGINT, handler)

    print("Presiona Ctrl+C para terminar procesos.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handler(None, None)
