# VegMon Arabia 3.6.2

Pipeline Sentinel-2 (STAC → composite mensili per tile → mosaico NDVI/STACK → statistiche) per le regioni dell'Arabia Saudita. Dettagli di setup e uso in `README.md`.

## Ambiente
- WSL Ubuntu, conda env `msimne` (`conda activate msimne`); GDAL/PROJ vengono da quell'env.
- Entry point: `python 3.6.2.py ...` → `src/msimne/cli.py`.
- Codice: `src/msimne/` (`pipeline.py` orchestrazione, `composite.py`, `stac.py`, `mosaic.py` GDAL, `quality.py` soglia copertura, `stats.py`, `config.py` default `Settings`).
- Output in `outputs/`: `S2/{STACK,NDVI,STATS}`, `logs/run_<mese>_<regione>_<timestamp>.log`, `reports/`, `state/`, `working_s2/` (tile intermedie, svuotata a fine run riuscita).

## Comando di riferimento (testato su R04)
```bash
python 3.6.2.py --month 2026-07 --region R04 --workers 3 --threads-per-worker 1 --memory-limit 5GB --gdal-threads 2 --gdal-warp-memory-mb 512 --initial-max-items 6 --max-items 10 --intermediate-compression none --tile-parallelism 3
```
La run è riprendibile: dopo Ctrl+C, rilanciando lo stesso comando le tile già presenti in `working_s2/` vengono saltate.

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

## Note sul codice
- Contatore di avanzamento in `pipeline.py`: il totale è `len(tiles)` (prima `len(tile_jobs)`, che dopo una ripresa mostrava ad es. `149/130`).
- **Problema aperto**: `validate_existing_window` (`pipeline.py`) alla ripresa di una finestra con mosaici già presenti salta il controllo copertura e registra `valid_ratio` = 1.0 fisso, quindi una regione fallita per `failed_quality` risulta al 100% nel report.
- Se la copertura NDVI è < 98%: `ValueError` → stato `failed_quality` (`cli.py`), i mosaici restano, il CSV non viene scritto, `working_s2/` non viene pulita.
- Il gap-fill finale (`enable_final_gap_fill`) legge l'intero raster NDVI in RAM (~8–10 GB per una regione come R04) e con `gap_fill_max_search_distance = 0` non riempie nulla.
