# QTP CLI image — packages the `qtp` command and its SDK/compliance dependencies.
# Used by the optional `qtp-scheduler` compose service (qtp schedule run) and lets the
# whole CLI run as a container: `docker run --rm --env-file .env qtp demo run`.
FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg2-binary/boto3 wheels are bundled, so no apt build chain needed.
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

# Install the package itself (provides the `qtp` entry point).
COPY qtp ./qtp
COPY config ./config
RUN pip install --no-cache-dir .

ENTRYPOINT ["qtp"]
CMD ["--help"]
