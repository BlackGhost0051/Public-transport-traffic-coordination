#!/bin/bash
cd "$(dirname "$0")"
echo "Uruchamianie wariantu bazowego (bez agentów)..."
python simulator/simulator.py --baseline --seed 42
