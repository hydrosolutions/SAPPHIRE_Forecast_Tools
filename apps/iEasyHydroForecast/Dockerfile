# Container image that runs your code
FROM alpine:3.10

# Copies your code file from your action repository to the filesystem path `/` of the container
COPY say_hello.sh /app/say_hello.sh

# Set the script permissions
RUN chmod +x /app/say_hello.sh

# Code file to execute when the docker container starts up (`entrypoint.sh`)
ENTRYPOINT ["/app/say_hello.sh"]

# Add a command that will ensure the container runs for a long time to test watchtower
CMD ["tail", "-f", "/dev/null"]