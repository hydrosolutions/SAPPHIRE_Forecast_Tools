# Image Verification Keys

This directory contains the public keys used to verify Docker image signatures.

## cosign.pub

This is the public key corresponding to the private key used in GitHub Actions to sign Docker images.
This key should be used to verify the integrity and authenticity of all Docker images pulled from our registry.

## Usage

The public key is automatically used by the `pull_docker_images.sh` script:

```bash
./bin/utils/pull_docker_images.sh <image_tag>
```

The public key is available in the `keys` directory of this git repository. We recommend you clone the repository and use the public key to verify the integrity of the Docker images (see deployment instructions in the `docs` directory of this git repository).

If you need to use a different key, you can specify it with:

```bash
COSIGN_PUBLIC_KEY=/path/to/your/key.pub ./bin/utils/pull_docker_images.sh latest
```

## Key Security
- This public key can be safely stored in the repository
- The private key is securely stored in GitHub Actions secrets
- Do not add private keys to this directory

## Key Rotation
When keys are rotated:

- The new public key will be committed to this directory
- Old keys will be kept with a date suffix for a transition period
- The deployment process will be updated to use the new key