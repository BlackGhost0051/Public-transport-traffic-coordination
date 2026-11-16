#!/bin/bash
# Uruchamia cały system inteligentnego transportu miejskiego
set -e
cd "$(dirname "$0")"

mkdir -p logs

echo "=== Inteligentny Transport Miejski ==="

# Sprawdź Mosquitto
if ! mosquitto_pub -h localhost -p 1883 -t "test/ping" -m "ping" 2>/dev/null; then
    echo "Uruchamiam Mosquitto..."
    sudo systemctl start mosquitto
    sleep 2
fi
echo "[OK] Mosquitto działa"

# Seed bazy danych
if [ ! -f data/transit.db ]; then
    echo "Generuję historię w bazie danych..."
    python simulator/seed_history.py
    echo "[OK] Baza danych gotowa"
fi

PID_FILE="pids.txt"
> "$PID_FILE"

start_agent() {
    local name="$1"
    shift
    python "$@" > "logs/${name}.log" 2>&1 &
    echo $! >> "$PID_FILE"
    echo "  [start] $name (PID $!)"
}

echo "Uruchamiam agentów..."

# Agenci pojazdów
start_agent "vehicle_A01" agents/vehicle_agent.py --id A01 --line 17
start_agent "vehicle_A02" agents/vehicle_agent.py --id A02 --line 17
start_agent "vehicle_A03" agents/vehicle_agent.py --id A03 --line 17
start_agent "vehicle_T01" agents/vehicle_agent.py --id T01 --line 4
start_agent "vehicle_T02" agents/vehicle_agent.py --id T02 --line 4
start_agent "vehicle_T03" agents/vehicle_agent.py --id T03 --line 4

# Agenci przystanków
for stop in S1 S2 S3 S4 S5 S6 S7 S8; do
    start_agent "stop_${stop}" agents/stop_agent.py --id "$stop"
done

# Koordynatorzy
start_agent "coordinator_17" agents/coordinator_agent.py --line 17
start_agent "coordinator_4"  agents/coordinator_agent.py --line 4

# Pozostałe agenty
start_agent "depot"      agents/depot_agent.py
start_agent "forecast"   agents/forecast_agent.py
start_agent "weather"    agents/weather_agent.py --mode scenario
start_agent "supervisor" agents/supervisor_agent.py
start_agent "events"     agents/events_agent.py
start_agent "offers"     agents/offers_agent.py

# Sygnalizacja
start_agent "signal_X01" agents/signal_agent.py --id X01
start_agent "signal_X02" agents/signal_agent.py --id X02
start_agent "signal_X03" agents/signal_agent.py --id X03

# Partnerzy i bridge
start_agent "partners"   partners.py
start_agent "bridge"     bridge/mqtt_ws_bridge.py

echo ""
echo "Wszystkie agenty uruchomione. Czekam 3s na połączenia MQTT..."
sleep 3

echo ""
echo "=== System gotowy. Uruchamiam symulator ==="
echo "    (Ctrl+C aby zatrzymać wszystko)"
echo ""

# Trap SIGINT – zatrzymaj wszystkie procesy
cleanup() {
    echo ""
    echo "Zatrzymuję system..."
    kill $(cat "$PID_FILE" 2>/dev/null) 2>/dev/null
    rm -f "$PID_FILE"
    echo "System zatrzymany."
}
trap cleanup SIGINT SIGTERM

python simulator/simulator.py --seed 42
