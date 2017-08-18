FROM python:3.5

RUN mkdir -p /provisioning
WORKDIR /provisioning

# Install build dependencies
RUN apt-get update && \
    apt-get -yq install \
        build-essential \
        gcc \
        libgeos-dev \
        zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# also need libgeos for Shapely, Pandana
RUN echo "Installing Spatial Index library..." && \
    mkdir -p /provisioning/spatialindex && \
    cd /provisioning/spatialindex && \
    curl -# -O http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz && \
    tar -xzf spatialindex-src-1.8.5.tar.gz && \
    cd spatialindex-src-1.8.5 && \
    ./configure --prefix=/usr/local && \
    make -j$(python -c 'import multiprocessing; print(multiprocessing.cpu_count())') && \
    make install && \
    ldconfig && \
    rm -rf /provisioning/spatialindex*

RUN echo "Installing GEOS library..." && \
    mkdir -p /provisioning/geos && \
    cd /provisioning/geos && \
    curl -# -O http://download.osgeo.org/geos/geos-3.5.1.tar.bz2 && \
    tar -xjf geos-3.5.1.tar.bz2 && \
    cd geos-3.5.1 && \
    ./configure && \
    make -j$(python -c 'import multiprocessing; print(multiprocessing.cpu_count())') && \
    make install && \
    ldconfig -v && \
    rm -rf /provisioning/geos*

RUN echo "Installing Proj4 library..." && \
    mkdir -p /provisioning/proj4 && \
    cd /provisioning/proj4 && \
    curl -# -O http://download.osgeo.org/proj/proj-4.9.3.tar.gz && \
    tar -xzf proj-4.9.3.tar.gz && \
    cd proj-4.9.3 && \
    ./configure && \
    make -j$(python -c 'import multiprocessing; print(multiprocessing.cpu_count())') && \
    make install && \
    ldconfig -v && \
    rm -rf /provisioning/proj4

# basemap (incorrectly) requires numpy to be installed *before* installing it
RUN pip install --upgrade numpy

RUN echo "Installing Basemap plotting library..." && \
    mkdir -p /provisioning/matplotlib-basemap && \
    cd /provisioning/matplotlib-basemap && \
    curl -# -o basemap-1.0.7rel.tar.gz https://codeload.github.com/matplotlib/basemap/tar.gz/v1.0.7rel && \
    tar -xzf basemap-1.0.7rel.tar.gz && \
    cd basemap-1.0.7rel && \
    python setup.py install && \
    rm -rf /provisioning/matplotlib-basemap

RUN mkdir -p /code/
WORKDIR /code
COPY . /code

# run tests which has added bonus of installing
# test dependencies so they won't have to be installed
# on every single test run
RUN ./test.sh

# install this repo's python package
RUN pip install .
