# Có thể chọn base tương thích GPU đã cài bằng --build-arg BASE_IMAGE=...
ARG BASE_IMAGE=pytorch/pytorch:2.9.1-cuda12.8-cudnn9-runtime
FROM ${BASE_IMAGE}
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 OPENCV_LOG_LEVEL=OFF \
    XDG_CACHE_HOME=/tmp/.cache YOLO_CONFIG_DIR=/tmp/ultralytics \
    TORCHINDUCTOR_CACHE_DIR=/tmp/torchinductor
WORKDIR /app
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates libglib2.0-0 libgl1 && rm -rf /var/lib/apt/lists/*
RUN if ! getent passwd 1000 >/dev/null; then useradd --uid 1000 --create-home --shell /usr/sbin/nologin fight; fi
COPY requirements.txt ./
RUN (python -m pip --version || python -m ensurepip --upgrade) \
    && python -m pip install --no-cache-dir -r requirements.txt
COPY run.py bbox.schema.json event.schema.json evaluate.py download_eval_dataset.py ./
COPY vendor ./vendor
COPY test_events.py test_geometry.py test_storage.py ./
RUN mkdir -p /app/output /app/weights && chown 1000:1000 /app /app/output /app/weights
USER 1000:1000
STOPSIGNAL SIGTERM
ENTRYPOINT ["python", "-u", "run.py"]
