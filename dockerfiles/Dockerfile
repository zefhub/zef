FROM quay.io/pypa/manylinux_2_24_x86_64

# RUN pacman -Syu --noconfirm
# RUN pacman -S --noconfirm \
#     go \
#     wget \
#     cmake \
#     unzip \
#     git \
#     perl \
#     patchelf \
#     jq

# RUN pacman -S --noconfirm \
#     python-pip \
#     base-devel

# COPY dist dist

COPY . .

# RUN apt-get update
# RUN apt-get install -y --no-install-recommends \
#     golang \
#     cmake \
#     curl \
#     zstd \
#     libzstd-dev \
#     openssl \
#     libssl-dev \
#     libsecret-1-0 \
#     libcurl4-openssl-dev

ENV PATH=/opt/python/cp310-cp310/bin:$PATH
RUN echo $PATH
RUN pip install cmake
RUN bash make_cleanslate.sh
RUN bash make_everything.sh
# RUN mkdir cmake_build && \
#     cd cmake_build && \
#     cmake ..