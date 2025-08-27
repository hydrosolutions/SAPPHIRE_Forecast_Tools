FROM python:3.11-slim

# Install luigi
RUN pip install --no-cache-dir luigi==3.5.0

# Create non-root user and writable state/log dirs
RUN useradd -ms /bin/bash luigi && \
    mkdir -p /var/lib/luigi /var/log/luigi && \
    chown -R luigi:luigi /var/lib/luigi /var/log/luigi

USER luigi
EXPOSE 8082

# Start luigid with explicit state/log paths
CMD ["luigid", "--port", "8082", "--state-path", "/var/lib/luigi", "--logdir", "/var/log/luigi"]