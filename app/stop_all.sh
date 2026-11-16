#!/bin/bash
if [ -f pids.txt ]; then
    kill $(cat pids.txt 2>/dev/null) 2>/dev/null
    rm -f pids.txt
    echo "Wszystkie procesy zatrzymane."
else
    echo "Brak pliku pids.txt."
fi
