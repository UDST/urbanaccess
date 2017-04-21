FROM python:2.7

ENV HOME /root

# first steps intended to enable HDF5 installation, operation
RUN apt-get update
RUN apt-get -yq install gcc build-essential zlib1g-dev wget

# build HDF5
RUN cd ; wget https://support.hdfgroup.org/ftp/HDF5/prev-releases/hdf5-1.8.17/src/hdf5-1.8.17.tar.gz
RUN cd ; tar zxf hdf5-1.8.17.tar.gz
RUN cd ; mv hdf5-1.8.17 hdf5-setup
RUN cd ; cd hdf5-setup ; ./configure --prefix=/usr/local/
RUN cd ; cd hdf5-setup ; make && make install

# cleanup
RUN cd ; rm -rf hdf5*
RUN apt-get -yq autoremove
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip install --upgrade numpy
RUN pip install pandas
RUN pip install bottleneck
RUN pip install tables

# now free to prepare environment for installation of UrbanAccess
RUN mkdir -p /provisioning
WORKDIR /provisioning

# also need libgeos for Python Shapely package
RUN apt-get update
RUN apt-get -yq install libgeos-dev
RUN echo "Installing GEOS libraries..." && \
    mkdir -p /provisioning/spatialindex && \
    cd /provisioning/spatialindex && \
    curl -# -O http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz && \
    tar -xf spatialindex-src-1.8.5.tar.gz && \
    cd spatialindex-src-1.8.5 && \
    ./configure --prefix=/usr/local && \
    make && \
    make install && \
    ldconfig && \
    rm -rf /provisioning/spatialindex

# return to main directory after installing libgeos
RUN cd /provisioning
COPY ./urbanaccess.egg-info/requires.txt /provisioning/requires.txt

RUN pip install -r ./requires.txt

COPY . /provisioning

# can now install Urban Access via repo
RUN python ./setup.py develop
