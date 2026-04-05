FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir -e .

ENV DAGSTER_HOME=/dagster_home

# dagster.yaml is copied to /app/dagster.yaml (inside the image, not the volume).
# The entrypoint writes it into DAGSTER_HOME at container start so it is always
# present even when /dagster_home is a named volume that starts empty.
COPY dagster.yaml /app/dagster.yaml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 3000

ENTRYPOINT ["/entrypoint.sh"]
