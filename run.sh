#!/bin/bash

cd /home/admin/StrategyExecutionEngine/
echo "Navigated to StrategyExecutionEngine."

source venv/bin/activate
echo "Python virtual environment activated."

cd trading_system/
echo "Navigated to trading_system."

echo "Running main.py..."
PYTHONPATH=. python3 app/main.py
echo "main.py finished running."

deactivate
echo "Python virtual environment deactivated."