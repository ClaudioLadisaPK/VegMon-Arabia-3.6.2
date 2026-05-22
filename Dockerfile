FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md environment.yml 3.6.2.py ./
COPY src ./src
COPY inputs ./inputs

RUN python -m pip install --upgrade pip && \
    pip install \
    rasterio \
    geopandas \
    rioxarray \
    xarray \
    numpy \
    pandas \
    shapely \
    pyproj \
    pyogrio \
    dask \
    distributed \
    pystac-client \
    odc-stac \
    planetary-computer \
    python-dateutil \
    requests \
    tqdm

ENTRYPOINT ["python", "3.6.2.py"]
CMD ["--interactive"]
