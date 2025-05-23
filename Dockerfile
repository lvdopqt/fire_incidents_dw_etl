FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/app

COPY requirements.txt ./
RUN pip install --no-cache-dir --no-warn-script-location -r requirements.txt

COPY . .

CMD ["bash"]
ENTRYPOINT []
