# test_container/Dockerfile
FROM python:3.9-slim

# Accept an environment variable that determines if the container should fail
ENV SHOULD_FAIL=false
ENV FAIL_COUNT=0

COPY script.py /script.py
CMD ["python", "/script.py"]