@echo off

REM Save the current directory
set CURRENT_DIR=%~dp0

REM Activate the Conda environment
call activate pyQuant_3_11

REM Run the Python script
python "%CURRENT_DIR%\get_fundamentalist_data4.py"

REM Pause the script to keep the window open
pause