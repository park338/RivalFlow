param(
  [int]$Port = 8000
)

Set-Location "$PSScriptRoot\backend"
python -m uvicorn app.main:app --host 0.0.0.0 --port $Port --reload
