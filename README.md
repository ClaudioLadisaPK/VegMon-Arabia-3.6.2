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

## Run mensile operativo

Il comando schedulabile elabora tutte le regioni operative `R01`-`R13` per il mese precedente:

```powershell
python 3.6.2.py --previous-month --all-regions
```

Esempio per forzare un mese specifico:

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
python 3.6.2.py --previous-month --all-regions --workers 16 --threads-per-worker 2 --memory-limit 12GB --gdal-threads 16 --gdal-warp-memory-mb 16384
```

Non conviene usare sempre tutti i 64 thread, perche Dask, GDAL, compressione COG e I/O disco
possono saturarsi a vicenda.

## Scheduling Windows Task Scheduler

Creare una task mensile il giorno 15, ad esempio alle 02:00, con:

```text
Program/script: C:\Path\To\Miniforge3\envs\msimne\python.exe
Arguments: 3.6.2.py --previous-month --all-regions
Start in: C:\Path\To\VegMon-Arabia-3.6.2
```

Il processo del 15 elabora solo il mese precedente e non recupera automaticamente mesi arretrati.

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
