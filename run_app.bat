@echo off
echo ====================================================================
echo      RailReserve DCS - Distributed Railway Reservation Engine
echo ====================================================================
echo.
cd /d "%~dp0dcs_project"

echo Server listening on:
echo   - Local PC:    http://localhost:8000
echo   - Mobile / LAN: http://192.168.1.6:8000
echo.
echo Press Ctrl+C in this terminal window to stop the server anytime.
echo.

python -m uvicorn server:app --host 0.0.0.0 --port 8000 --reload
pause
