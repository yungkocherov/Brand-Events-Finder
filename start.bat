@echo off
if not exist .venv (
    echo Creating virtual environment...
    python -m venv .venv
    call .venv\Scripts\activate
    pip install -r requirements.txt
) else (
    call .venv\Scripts\activate
)
echo Starting server at http://localhost:8080
uvicorn app.main:app --host 0.0.0.0 --port 8080
pause
