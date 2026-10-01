# VegMon Arabia 3.6.2

Pipeline Sentinel-2 (STAC → composite mensili per tile → mosaico NDVI/STACK → statistiche) per le regioni dell'Arabia Saudita. Dettagli di setup e uso in `README.md`.

## Ambiente
- WSL Ubuntu, conda env `msimne` (`conda activate msimne`); GDAL/PROJ vengono da quell'env.
- Entry point: `python 3.6.2.py ...` → `src/msimne/cli.py`.
- Codice: `src/msimne/` (`pipeline.py` orchestrazione, `composite.py`, `stac.py`, `mosaic.py` GDAL, `quality.py` soglia copertura, `stats.py`, `config.py` default `Settings`).
- Output in `outputs/`: `S2/{STACK,NDVI,STATS}`, `logs/run_<mese>_<regione>_<timestamp>.log`, `reports/`, `state/`, `working_s2/` (tile intermedie, svuotata a fine run riuscita).

## Comandi di riferimento
```bash
# PC locale WSL (profilo equivalente ai parametri low-RAM che hanno completato R04)
python 3.6.2.py --month 2026-07 --region R04 --profile wsl --aoi-simplify-m 5
# VM cliente
python 3.6.2.py --month 2026-07 --region R05 --profile vm --aoi-simplify-m 5
```
- Precedenza parametri: esplicito > variabile d'ambiente `MSIMNE_*` > `--profile` > default storico (senza profilo restano i default VM grande: 16 worker, 16 thread GDAL, warp 16 GB).
- Per i test usare sempre `--outputs-dir outputs_test/<nome>` (ignorato da git): con mosaici gia presenti la run salta l'elaborazione.
- La run è riprendibile: dopo Ctrl+C rilanciando lo stesso comando le tile complete vengono saltate (tile e COG sono scritti come `.partial` e rinominati a fine scrittura) e anche i mosaici finali gia completi.
- Opzioni aggiunte: `--no-scl` (con `--max-items 6-8`), `--aoi-simplify-m` (0 = poligono originale, output identici), `--final-gdal-threads`, `--final-parallel-mosaics`, `--tile-retries`, `--network-wait-max-seconds`, `--http-timeout`, `--http-max-retry`, `--skip-disk-check`.
- A fine run il log riporta `Tempi fasi <regione>: ...` (tile, copertura_tile, mosaico_stack, mosaico_ndvi, controllo_copertura, statistiche).
- Registro dei test (versione codice, regione, mese, parametri, tempi per fase, copertura, statistiche, pesi, RAM): `test_runs/registro_test.csv` (versionato, separatore `;`, decimali con virgola) + CSV statistiche in `test_runs/stats/`. Log completi archiviati in `outputs_test/archivio/` (non versionato: i log DEBUG possono contenere URL firmati). I raster dei test vengono cancellati dopo la registrazione per risparmiare spazio su C:.
- Confronto pixel per pixel tra due output: `outputs_test/confronta.py <rif> <test> <suffisso>`; worktree con il codice base: `/home/ladisa/vegmon_baseline` (commit af5cec8).

## Benchmark: R04 Al Qaseem, 2026-07 (completata 2026-10-01)
- AOI 87.800 km², 200 tile; mosaico finale 41066×46820 px, EPSG:3857, 10 m.
- STACK COG 5,6 GB (4 bande Int16), NDVI COG 1,4 GB.
- Copertura NDVI 99,49% (soglia `final_ndvi_valid_ratio` 98%, gap-fill disattivato) → superata.
- Statistiche: NDVI medio 0,069, std 0,046, area vegetata 93.508 ha.
- Tile sotto soglia critica anche dopo fallback stagionale 2025/2024: N3105550_22-E4765514_86 (0%), N3006075_92-E4765514_86 (74%), N2907278_46-E4698723_17 (80%), N3006075_92-E4720987_07 (81%).
- Tempi: catena da zero 1 h 19 min effettivi (con due riprese). Fase tile ~2,7–3,5 tile/min; mosaico STACK ~7 min, NDVI ~2 min, statistiche ~13 s. Stima run pulita per ~200 tile: 1 h 15 – 1 h 25 min.
- Risorse: RAM di picco ~5,5 GB su 15 (gdal_translate ~4,3 GB, worker Dask ~0,6 GB ciascuno), swap inutilizzato, ~10 GB scritti su disco. Il mosaico è sicuro lato memoria: Dask viene chiuso prima e GDAL lavora a blocchi.
- Tentativi precedenti dello stesso giorno: le run del mattino (da PyCharm) morivano durante il mosaico GDAL senza traceback, probabilmente per OOM con i default tarati per la VM grande (`gdal-warp-memory-mb 16384`, `gdal-threads 16`, `workers 16`, `memory-limit 12GB`) su WSL da 15 GB. Con il profilo low-RAM sopra il mosaico regge. Alcune run sono fallite anche per errori DNS (`NameResolutionError`) verso il catalogo STAC.
- Storico e parametri per la VM cliente: memorie Codex `~/.codex/memories/VEGMON.md` e `NCVC_VEG_MON.md`.

## Benchmark: R06 Aseer, 2026-07 (2026-10-01, profilo wsl, test T06)
- 206 tile, 71,3 min: tile 64,6 min (90%), mosaico STACK 4,9 (warp 1,5 + COG 3,4), NDVI 1,2, controllo copertura 6 s. Copertura 99,82%.
- Mosaico con 6 thread GDAL: picco RAM GDAL 6,5 GB (sistema 8,5/15 GB) senza guadagno rispetto a 2 thread → profilo wsl a 4 thread. Il collo di bottiglia del mosaico e' la conversione COG (overview + statistiche raster calcolate due volte: `-stats` e `STATISTICS=YES`).
- Fase tile: CPU ~25%, RAM ~3 GB, rete ~18 MB/s → margine per alzare workers/threads/tile-parallelism (da misurare su R10).

## Benchmark: R12 Al Bahah, 2026-07 (2026-10-01, profilo wsl)
- Codice base 12 min 57 s; codice branch `ottimizzazione-aoi-grandi` 11 min 50 s con output identici pixel per pixel (tile 10:58, mosaici 45 s, nessun ricalcolo critico inutile).
- Il 30/09 con `--workers 4 --threads-per-worker 2 --tile-parallelism 4` R12 era durata 6 min 38 s: per regioni piccole il profilo wsl e' prudente.

## Note sul codice
- Requisiti decisi (2026-10-01): stesso codice per VM Windows e WSL, adattato dai parametri; priorita velocita e robustezza su AOI grandi; output con formato identico; **mai interpolare** (gap-fill rimosso).
- Controllo copertura NDVI (`quality.ndvi_valid_ratio`): rasterizza l'AOI solo sui blocchi di bordo; risultato identico al vecchio metodo, su R05 la parte geometrica passa da ~73 min a secondi.
- Se la copertura NDVI è < 98%: `ValueError` → stato `failed_quality` (`cli.py`), i mosaici restano, il CSV non viene scritto, `working_s2/` non viene pulita. Alla ripresa la copertura viene ricalcolata davvero.
- Tile critiche: se il fallback stagionale e' gia stato fatto senza SCL (marker `.fallback_noscl`), la fase di copertura non la ricalcola (darebbe lo stesso risultato).
- Una tile che fallisce dopo i tentativi non ferma la regione: viene ritentata da `ensure_full_grid_coverage`. Su errori di rete si attende che Planetary Computer torni raggiungibile.
- In WSL il controllo spazio guarda anche Windows C: (il disco Linux e' un .vhdx su C:).
- Da valutare: fallback stagionale che sostituisce l'intera tile invece dei soli buchi; scrittura diretta COG da gdalwarp; letture SCL ripetute per tile.
