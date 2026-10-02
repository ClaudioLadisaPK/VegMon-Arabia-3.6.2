# Veg Mon

Processore Sentinel-2 per:

- download e caricamento dati da STAC
- calcolo di compositi mensili
- generazione stack multispettrali
- calcolo NDVI
- controllo copertura tile
- mosaico finale
- statistiche per AOI

Il progetto e stato rifattorizzato a partire da `3.6.2.py` in una struttura piu adatta a manutenzione e pubblicazione su GitHub.

## Versione V03 (ottobre 2026): cosa cambia

Formato e nomi degli output invariati (`outputs/S2/{STACK,NDVI,STATS}`, EPSG:3857, 10 m, COG Int16).

- **Niente buchi**: le scene Sentinel-2 vengono scelte anche in base a quanta parte della tile coprono
  (prima solo per nuvolosita': scene parziali sul bordo dei riquadri MGRS lasciavano tile al 5%).
  Il fallback stagionale riempie solo i pixel mancanti, non sostituisce piu' la tile del mese.
  R13 2026-07: copertura da 93,93% a 100%, nessun fallback.
- **Profili macchina** `--profile wsl` (PC ~15 GB) e `--profile vm` (VM 72 vCPU / 240 GB);
  ogni parametro esplicito ha la precedenza sul profilo. `python 3.6.2.py --help` mostra i parametri
  divisi in "Fase tile" e "Fase mosaico".
- **AOI grandi**: `--aoi-simplify-m 5` (mezzo pixel) e controllo copertura NDVI veloce (R05: da ~73 min a secondi).
- **Rete instabile**: tentativi e timeout HTTP configurabili (`--http-timeout`, `--http-max-retry`),
  attesa che Planetary Computer torni raggiungibile (`--network-wait-max-seconds`); una tile fallita
  non ferma la regione.
- **Ripresa sicura**: file temporanei `.partial` rinominati solo a fine scrittura; alla ripresa si
  saltano tile e mosaici gia' completi e la copertura dei mosaici esistenti viene ricontrollata davvero
  (un mese gia' presente sotto il 98% risulta `failed_quality`).
- **Mai interpolazione**: gap-fill rimosso.
- **Log**: riga `Parametri:` per fase e riga finale `Tempi fasi <regione>: ...`.
- **Dashboard** in `tools/dashboard` (Linux/WSL e Windows): `tools\dashboard\avvia_dashboard.bat`,
  poi http://localhost:8765.
- **Registro test** in `test_runs/registro_test.csv`.

Comando consigliato sulla VM Windows (oppure `tools\windows\run_vegmon.ps1`):

```powershell
python 3.6.2.py --region R12 --month 2026-07 --profile vm --aoi-simplify-m 5 `
  --http-timeout 120 --http-max-retry 10 --network-wait-max-seconds 3600 --outputs-dir outputs_test\R12_vm
```

Le sezioni seguenti descrivono setup e uso generale; dove riportano parametri numerici di performance
fanno fede i profili sopra.

## Stato operativo

Il target operativo e una VM Windows con input/output locali. Per evitare problemi di DLL con
`GDAL` e `rasterio`, l'ambiente consigliato e `conda-forge` tramite Miniforge o Mambaforge.

## Struttura del progetto

- `3.6.2.py`: entrypoint principale
- `src/msimne/config.py`: configurazione e percorsi
- `src/msimne/cli.py`: CLI e modalita interattiva
- `src/msimne/stac.py`: accesso STAC e caricamento Sentinel-2
- `src/msimne/composite.py`: generazione tile, NDVI, controllo copertura
- `src/msimne/mosaic.py`: mosaici finali con GDAL
- `src/msimne/stats.py`: statistiche finali NDVI
- `src/msimne/quality.py`: controllo copertura NDVI e gap filling finale
- `src/msimne/state.py`: stato persistente SQLite e report run
- `src/msimne/pipeline.py`: orchestrazione end-to-end
- `inputs/regions`: AOI regionali `R01.geojson` ... `R14.geojson`
- `inputs/grids`: griglia di lavoro
- `outputs/`: risultati generati dal refactor
- `tests/`: test automatici minimi

## Prerequisiti Windows VM

Serve:

- Windows Server/Windows VM
- Miniforge o Mambaforge
- Git
- input in `inputs/regions` e `inputs/grids`
- output locali sulla VM

Verifica risorse:

```powershell
Get-CimInstance Win32_ComputerSystem | Select-Object NumberOfLogicalProcessors,TotalPhysicalMemory
```

Verifica eventuale subscription key Planetary Computer:

```powershell
echo $env:PC_SDK_SUBSCRIPTION_KEY
```

La pipeline funziona anche senza key, ma usa retry e logging conservativi.

## Setup ambiente Windows consigliato

Da PowerShell/Miniforge Prompt:

```powershell
conda env create -f environment.yml
conda activate msimne
pip install -e .
```

Verifica:

```powershell
python -c "import rasterio; import odc.stac; import dask; print('ok')"
gdalinfo --version
```

## Run sequenziale operativo

Il comando schedulabile elabora tutte le regioni operative `R01`-`R13` partendo da marzo 2026.
Ad ogni esecuzione sceglie il primo mese non ancora completato nello stato SQLite:

```powershell
python 3.6.2.py --next-pending-month --from-month 2026-03 --all-regions
```

Quindi il primo run elabora `2026-03`. Se tutte le regioni `R01`-`R13` risultano completate,
il run successivo passa a `2026-04`. Se una o piu regioni falliscono, il run successivo resta
sullo stesso mese finche la copertura del mese non e completa.

Esempio per forzare manualmente un mese specifico:

```powershell
python 3.6.2.py --month 2026-06 --all-regions
```

Se una regione fallisce, il processo continua con le successive. Il run produce:

- log leggibile in `outputs/logs`
- stato SQLite in `outputs/state/pipeline.sqlite`
- report CSV in `outputs/reports`

Gli output finali restano COG e mantengono il naming originale:

- `outputs/S2/STACK/S2_stack_RXX_YYYY-MM.tif`
- `outputs/S2/NDVI/S2_ndvi_RXX_YYYY-MM.tif`
- `outputs/S2/STATS/S2_stats_RXX_YYYY-MM.csv`

Il file NDVI finale viene validato e, se necessario, interpolato direttamente nello stesso file.
Lo stack multispettrale non viene interpolato.

### Parametri performance VM

La VM di riferimento ha 64 logical processors e circa 240 GB RAM. I default sono conservativi:

```powershell
python 3.6.2.py --next-pending-month --from-month 2026-03 --all-regions --workers 16 --threads-per-worker 2 --memory-limit 12GB --gdal-threads 16 --gdal-warp-memory-mb 16384
```

Non conviene usare sempre tutti i 64 thread, perche Dask, GDAL, compressione COG e I/O disco
possono saturarsi a vicenda.

## Scheduling Windows Task Scheduler

Creare una task con ripetizione ogni 31 giorni, con:

```text
Program/script: C:\Path\To\Miniforge3\envs\msimne\python.exe
Arguments: 3.6.2.py --next-pending-month --from-month 2026-03 --all-regions
Start in: C:\Path\To\VegMon-Arabia-3.6.2
```

La cadenza operativa e: un mese di dati per run, massimo 31 giorni per completare tutte le regioni,
poi avanzamento al mese successivo alla prossima esecuzione schedulata.

Script PowerShell consigliato per Task Scheduler:

```powershell
Set-Location "C:\VegMon-Arabia-3.6.2_new"

& "C:\ProgramData\anaconda3\envs\msimne\python.exe" "C:\VegMon-Arabia-3.6.2_new\3.6.2.py" `
  --next-pending-month `
  --from-month 2026-03 `
  --all-regions `
  --workers 16 `
  --threads-per-worker 2 `
  --memory-limit 12GB `
  --gdal-threads 16 `
  --gdal-warp-memory-mb 16384
```

## Setup ambiente alternativo in WSL

## Setup ambiente finale funzionante in WSL

Questa e la procedura finale che ha funzionato davvero.

### 1. Apri WSL

Apri Ubuntu / WSL e vai nella cartella progetto:

```bash
cd /mnt/c/Users/*****/Desktop/msimne
```

### 2. Installa GDAL di sistema

```bash
sudo apt update
sudo apt install -y gdal-bin libgdal-dev python3-dev build-essential
```

Verifica:

```bash
gdalinfo --version
```

### 3. Crea l'ambiente Python Linux

```bash
python3 -m venv .venv_linux
```

### 4. Attiva l'ambiente

```bash
source .venv_linux/bin/activate
```

Quando e attivo, vedrai qualcosa come:

```bash
(.venv_linux)
```

### 5. Aggiorna pip

```bash
pip install --upgrade pip
```

### 6. Installa il progetto con le dipendenze

Le dipendenze runtime sono dichiarate in `pyproject.toml`, quindi usa:

```bash
pip install -e .
```

Se vuoi anche i tool di test:

```bash
pip install -e ".[dev]"
```

### 7. Test rapido dell'ambiente

```bash
python -c "import rasterio; import odc.stac; import dask; print('ok')"
```

Se stampa `ok`, l'ambiente e pronto.

## Attivazione quotidiana

Ogni volta che riapri WSL:

```bash
cd /mnt/c/Users/*****/Desktop/msimne
source .venv_linux/bin/activate
```

## Come avviare il processore

### Modalita interattiva

Questa e la modalita piu comoda:

```bash
python 3.6.2.py --interactive
```

Il programma chiede:

- regione
- data inizio
- data fine

Esempio:

```text
Seleziona regione (R01..R14): R05
Inizio (YYYY-MM[-DD]): 2025-03
Fine   (YYYY-MM[-DD]): 2025-04
```

### Modalita non interattiva

```bash
python 3.6.2.py --region R05 --start 2025-03 --end 2025-04
```

### Con override opzionali

```bash
python 3.6.2.py --region R05 --start 2025-03 --end 2025-04 --inputs-dir ./inputs --outputs-dir ./outputs
```

La cartella temporanea puo essere separata dagli output finali:

```bash
python 3.6.2.py --region R05 --start 2025-03 --end 2025-04 \
  --outputs-dir ./outputs \
  --work-dir ./scratch/working_s2
```

Gli stessi percorsi possono essere impostati anche con variabili ambiente:

```bash
export MSIMNE_INPUTS_DIR=/path/to/inputs
export MSIMNE_OUTPUTS_DIR=/path/to/outputs
export MSIMNE_WORK_DIR=/path/to/scratch/working_s2
export MSIMNE_GRID_FILE=/path/to/inputs/grids/ARAB_GRIGLIA.geojson
export MSIMNE_INTERMEDIATE_COMPRESSION=none
export MSIMNE_INITIAL_MAX_ITEMS=6
export MSIMNE_TILE_PARALLELISM=1
```

## Barra di avanzamento

Se `tqdm` e installato, il pipeline mostra una barra di avanzamento per:

- finestre temporali
- tile elaborati

Questa e utile per monitorare processi lunghi.

## Input attesi

Il refactor usa:

- `inputs/regions/R01.geojson` ... `inputs/regions/R14.geojson`
- `inputs/grids/ARAB_GRIGLIA.geojson`

La griglia viene cercata in questo percorso, salvo override esplicito con `--grid-file`.

## Output

Gli output del refactor vengono scritti in:

- `outputs/S2/STACK`
- `outputs/S2/NDVI`
- `outputs/S2/STATS`
- `outputs/working_s2` come area temporanea di lavoro

## Uso con PyCharm

La strada consigliata e:

1. usare PyCharm come editor
2. aprire il terminale WSL
3. attivare `.venv_linux`
4. lanciare il processore da terminale

Esempio:

```bash
cd /mnt/c/Users/*****/Desktop/msimne
source .venv_linux/bin/activate
python 3.6.2.py --interactive
```

## Note importanti

- Windows nativo non e il target consigliato per questo progetto
- il motivo principale sono i problemi di compatibilita tra `rasterio`, `GDAL` e DLL Windows
- per uso stabile e riproducibile conviene WSL/Linux

## Uso su Lightning

Su Lightning conviene tenere separati:

- codice del progetto
- output persistenti
- file temporanei di lavoro

Esempio di run prudente per una macchina cloud media:

```bash
python 3.6.2.py \
  --next-pending-month \
  --from-month 2026-03 \
  --all-regions \
  --outputs-dir /teamspace/studios/this_studio/outputs \
  --work-dir /teamspace/studios/this_studio/scratch/working_s2 \
  --workers 4 \
  --threads-per-worker 2 \
  --memory-limit 6GB \
  --gdal-threads 4 \
  --gdal-warp-memory-mb 4096 \
  --initial-max-items 6 \
  --max-items 10 \
  --intermediate-compression none \
  --tile-parallelism 1
```

Se la macchina Lightning ha piu RAM e CPU, aumentare gradualmente `--workers`, `--memory-limit`,
`--gdal-threads`, `--gdal-warp-memory-mb` e poi `--tile-parallelism`.

Per accelerare una regione grande senza togliere lo stack multispettrale:

- `--initial-max-items 6 --max-items 10` prova prima le 6 scene migliori e sale a 10 solo se serve.
- `--intermediate-compression none` evita compressione CPU-intensive sui GeoTIFF temporanei in `working_s2`.
- `--tile-parallelism 2` puo elaborare due tile insieme, ma va usato solo se RAM, disco e rete reggono.

Gli output finali in `outputs/S2/STACK`, `outputs/S2/NDVI` e `outputs/S2/STATS` restano prodotti.

Per usare variabili ambiente invece degli argomenti:

```bash
export MSIMNE_OUTPUTS_DIR=/teamspace/studios/this_studio/outputs
export MSIMNE_WORK_DIR=/teamspace/studios/this_studio/scratch/working_s2
export MSIMNE_INTERMEDIATE_COMPRESSION=none
export MSIMNE_INITIAL_MAX_ITEMS=6
export MSIMNE_TILE_PARALLELISM=1
python 3.6.2.py --next-pending-month --from-month 2026-03 --all-regions
```

Se disponibile, configurare la subscription key Planetary Computer come variabile ambiente della
sessione Lightning, senza inserirla nel codice:

```bash
export PC_SDK_SUBSCRIPTION_KEY=...
```

## Uso con Docker

Docker non e obbligatorio, ma e utile per avere un ambiente piu riproducibile.

### Build immagine

Dalla root del progetto:

```bash
docker build -t msimne .
```

### Avvio interattivo

```bash
docker run --rm -it -v "$(pwd)/outputs:/app/outputs" msimne --interactive
```

### Avvio non interattivo

```bash
docker run --rm -it -v "$(pwd)/outputs:/app/outputs" msimne --region R05 --start 2025-03 --end 2025-04
```

Nota:

- gli input sono gia dentro l'immagine al momento della build
- gli output vengono salvati fuori dal container tramite volume mount su `outputs/`

## Come aggiornare GitHub dopo nuove modifiche

Se hai gia fatto un primo push, i push successivi si fanno normalmente.

Esempio:

```bash
git status
git add .
git commit -m "Add Docker support and remove legacy module"
git push
```

Non devi creare un nuovo repository: fai solo nuovi commit e nuovi push sullo stesso repo.
