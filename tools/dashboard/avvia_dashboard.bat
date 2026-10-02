@echo off
REM Avvia la dashboard VegMon su http://localhost:8765 (Windows).
REM Se l'ambiente conda e' in un'altra cartella, modificare PYTHON_EXE.
set PYTHON_EXE=C:\ProgramData\anaconda3\envs\msimne\python.exe
cd /d "%~dp0"
start "" http://localhost:8765
"%PYTHON_EXE%" vegmon_monitor.py --port 8765
pause
