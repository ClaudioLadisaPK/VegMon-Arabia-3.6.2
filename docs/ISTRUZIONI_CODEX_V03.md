# Istruzioni per Codex: aggiornare VegMon alla V03 sulla VM Windows

Sei Codex e lavori sulla VM Windows del cliente (PowerShell). Obiettivo: sostituire **solo il codice**
del processore VegMon in `C:\VegMon-Arabia-3.6.2` con la versione V03 contenuta in `C:\VegMon_V03.zip`,
**senza toccare gli output esistenti** e senza usare git (non disponibile sulla VM).

## Regole

- **Non modificare, spostare o cancellare nulla dentro `C:\VegMon-Arabia-3.6.2\outputs\`.** Contiene prodotti validi.
- Non cancellare il backup che crei al passo 3.
- Se un controllo dei passi 1-2 non torna, **fermati e chiedi all'utente** prima di proseguire.
- Esegui i passi in ordine e alla fine riporta all'utente l'esito di ogni passo (comandi, output rilevante, errori).
- Percorso Python atteso: `C:\ProgramData\anaconda3\envs\msimne\python.exe`. Se non esiste, trova quello giusto
  con `conda env list` e usalo al posto di `$Py` in tutti i comandi (e correggi `PYTHON_EXE` al passo 8).

```powershell
$Root = "C:\VegMon-Arabia-3.6.2"
$Zip  = "C:\VegMon_V03.zip"
$Py   = "C:\ProgramData\anaconda3\envs\msimne\python.exe"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
```

## 1. Nessuna run VegMon in corso

```powershell
Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Select-Object ProcessId, CreationDate, CommandLine | Format-List
Get-Content "$Root\resume_20261002_stdout*" -Tail 20 -ErrorAction SilentlyContinue
Get-Content "$Root\resume_20261002_stderr*" -Tail 20 -ErrorAction SilentlyContinue
```

Se c'e' un processo con `3.6.2.py` o `msimne` nella riga di comando: **fermati** e chiedi all'utente se aspettare o interromperlo.
Riporta comunque il contenuto dei file `resume_20261002_*`.

## 2. Task Scheduler non attivo

```powershell
Get-ScheduledTask | Where-Object { ($_.Actions | ForEach-Object { "$($_.Execute) $($_.Arguments)" }) -match "VegMon|vegmon|3\.6\.2|msimne" } |
  Select-Object TaskName, TaskPath, State | Format-Table -AutoSize
```

Se esiste un task VegMon in stato `Ready` o `Running`, disattivalo (l'utente lo riattivera' dopo i test) e riportalo:
`Disable-ScheduledTask -TaskName "<nome>" -TaskPath "<path>"`.

## 3. Fotografia degli output e backup del codice V02

```powershell
# fotografia degli output (serve per verificare alla fine che nulla sia cambiato)
Get-ChildItem "$Root\outputs" -Recurse -File | Select-Object FullName, Length, LastWriteTime |
  Export-Csv "C:\VegMon_outputs_prima_$Stamp.csv" -NoTypeInformation

# backup del solo codice (NON outputs)
$Backup = "C:\VegMon_backup_V02_$Stamp"
New-Item -ItemType Directory $Backup | Out-Null
Get-ChildItem $Root -Force | Where-Object { $_.Name -ne "outputs" -and $_.Name -notlike "outputs_*" } |
  ForEach-Object { Copy-Item $_.FullName -Destination $Backup -Recurse -Force }
Get-ChildItem $Backup | Select-Object Name
```

## 4. Estrazione e controllo dello zip

```powershell
$Tmp = "C:\VegMon_V03_tmp"
if (Test-Path $Tmp) { Remove-Item $Tmp -Recurse -Force }
Expand-Archive -Path $Zip -DestinationPath $Tmp
$New = Join-Path $Tmp "VegMon-Arabia-3.6.2"
Get-ChildItem $New | Select-Object Name
Get-Content (Join-Path $New "VERSIONE.txt")
```

Devono esserci `3.6.2.py`, `src`, `inputs`, `tests`, `tools`, `test_runs`, `README.md`, `VERSIONE.txt` e **nessuna** cartella `outputs`.

## 5. Sostituzione del codice (outputs escluso)

```powershell
# src e tests vanno sostituiti per intero (eliminano moduli vecchi e cache)
foreach ($d in @("src", "tests")) {
  if (Test-Path "$Root\$d") { Remove-Item "$Root\$d" -Recurse -Force }
  Copy-Item "$New\$d" -Destination "$Root\$d" -Recurse
}
# cartelle nuove o aggiornate
foreach ($d in @("tools", "test_runs", "docs", "inputs")) {
  Copy-Item "$New\$d" -Destination $Root -Recurse -Force
}
# file di progetto
Get-ChildItem $New -File -Force | ForEach-Object { Copy-Item $_.FullName -Destination $Root -Force }
Get-ChildItem $Root -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```

## 6. Ambiente Python

```powershell
& $Py -c "import sys, shapely, pystac_client, rasterio, odc.stac, dask, geopandas, psutil; print(sys.version); print('shapely', shapely.__version__, '| pystac_client', pystac_client.__version__, '| rasterio', rasterio.__version__, '| odc.stac', odc.stac.__version__, '| dask', dask.__version__, '| psutil', psutil.__version__)"
& $Py -c "import msimne, os; print('msimne importato da', os.path.dirname(msimne.__file__))"
```

Requisiti: **shapely >= 2.0**, **pystac-client >= 0.7**, **psutil** presente. Se manca qualcosa:
`conda install -n msimne -c conda-forge "shapely>=2.0" "pystac-client>=0.7" psutil` (riporta all'utente cosa hai installato).

Nota: `3.6.2.py` usa sempre il codice in `$Root\src` (lo mette in testa al percorso di import), anche se nell'ambiente
c'e' una vecchia installazione di `msimne`. Per i test si forza lo stesso con `PYTHONPATH`.

## 7. Test automatici e controllo parametri

```powershell
Set-Location $Root
$env:PYTHONPATH = "$Root\src"
& $Py -m pytest -q tests
& $Py 3.6.2.py --help
Remove-Item Env:PYTHONPATH
```

Atteso: **27 passed**; nell'help devono comparire `--profile`, i gruppi "Fase tile" / "Fase mosaico", `--aoi-simplify-m`.

## 8. Dashboard

Apri `$Root\tools\dashboard\avvia_dashboard.bat` con un editor e verifica che `PYTHON_EXE` sia il Python dell'ambiente
`msimne` (correggilo se diverso). Poi avviala in una finestra separata:

```powershell
Start-Process -FilePath "$Root\tools\dashboard\avvia_dashboard.bat"
Start-Sleep 10
Invoke-WebRequest http://localhost:8765/api/status -UseBasicParsing | Select-Object StatusCode
```

Atteso: `StatusCode 200`. L'utente la apre nel browser della VM su http://localhost:8765.

## 9. Test breve su R12 (cartella separata, non tocca outputs\)

```powershell
Set-Location $Root
powershell -ExecutionPolicy Bypass -File tools\windows\run_vegmon.ps1 -Region R12 -Month 2026-07 -OutputsDir outputs_test\R12_vm
$log = Get-ChildItem "$Root\outputs_test\R12_vm\logs\*.log" | Sort-Object LastWriteTime | Select-Object -Last 1
Select-String -Path $log.FullName -Pattern "Parametri", "Spazio stimato", "Tempi fasi", "Copertura NDVI", "Tentativo", "Rete non", "ERROR" | ForEach-Object { $_.Line }
Get-Content "$Root\outputs_test\R12_vm\S2\STATS\*.csv"
```

Su un PC di test R12 richiede ~7 minuti. Riporta all'utente le righe trovate (soprattutto `Tempi fasi R12` e
`Copertura NDVI`) e la durata. Se la copertura e' >= 98% e il CSV esiste, il test e' superato.

## 10. Verifica che gli output non siano cambiati

```powershell
$prima = Import-Csv "C:\VegMon_outputs_prima_$Stamp.csv"
$dopo  = Get-ChildItem "$Root\outputs" -Recurse -File | Select-Object FullName, @{n = "Length"; e = { "$($_.Length)" } }
Compare-Object $prima $dopo -Property FullName, Length | Format-Table -AutoSize
```

Atteso: **nessuna differenza** (eventuali righe nuove possono essere solo log in `outputs\logs` se qualcuno ha lanciato
una run su `outputs\`; segnalale).

## 11. Pulizia e resoconto

```powershell
Remove-Item "C:\VegMon_V03_tmp" -Recurse -Force
```

Non cancellare `C:\VegMon_V03.zip` ne' il backup. Riporta all'utente: esito passi 1-10, versioni librerie,
risultato test R12, eventuali problemi.

## Rollback (solo se qualcosa non funziona)

```powershell
foreach ($d in @("src", "tests", "tools", "test_runs", "docs")) { if (Test-Path "$Root\$d") { Remove-Item "$Root\$d" -Recurse -Force } }
Get-ChildItem $Backup -Force | ForEach-Object { Copy-Item $_.FullName -Destination $Root -Recurse -Force }
```

## Dopo l'aggiornamento (per l'utente)

- Comando run singola regione: `powershell -ExecutionPolicy Bypass -File tools\windows\run_vegmon.ps1 -Region R05 -Month 2026-07 -OutputsDir outputs_test\R05_vm`
- Sequenza operativa su `outputs\` (quando si riattiva lo scheduler): `tools\windows\run_vegmon.ps1 -AllPending`
- Alla ripresa i mesi gia' presenti vengono ricontrollati: un mese con copertura < 98% risulta `failed_quality`
  (i file non vengono cancellati).
