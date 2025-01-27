import luigi
import docker
import time
from typing import Optional

class TestPreprocessingGateway(luigi.Task):
    max_retries = 3
    retry_delay = 5
    fail_count = luigi.IntParameter(default=2)

    def output(self):
        return luigi.LocalTarget(f'test_output_{self.fail_count}.txt')

    def _run_container(self, attempt_number) -> tuple[Optional[str], int, str]:
        client = docker.from_env()

        try:
            container = client.containers.run(
                "test-prepgateway:latest",
                detach=True,
                environment={
                    'FAIL_COUNT': str(self.fail_count),
                    'ATTEMPT_NUMBER': str(attempt_number)
                },
                name=f"test_prepgateway_{time.time()}"
            )

            print(f"Container {container.id} is running.")
            result = container.wait()
            exit_status = result['StatusCode']
            logs = container.logs().decode('utf-8')

            print(f"Container {container.id} exited with status code {exit_status}")
            print(f"Logs from container {container.id}:\n{logs}")

            # Clean up container
            container.remove()

            return container.id, exit_status, logs

        except Exception as e:
            print(f"Error running container: {str(e)}")
            return None, 1, str(e)

    def run(self):
        print("------------------------------------")
        print(" Running Test PreprocessingGateway task.")
        print("------------------------------------")

        attempts = 0
        while attempts < self.max_retries:
            attempts += 1
            print(f"Attempt {attempts} of {self.max_retries}")

            container_id, exit_status, logs = self._run_container(attempts)

            if exit_status == 0:
                # Success - write output and exit
                with self.output().open('w') as f:
                    f.write('Task completed successfully\n')
                    f.write(f'Container ID: {container_id}\n')
                    f.write(f'Logs:\n{logs}')
                return

            if attempts < self.max_retries:
                print(f"Container failed with status {exit_status}. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
            else:
                print(f"Container failed after {self.max_retries} attempts.")
                raise RuntimeError(f"Task failed after {self.max_retries} attempts. Last exit status: {exit_status}\nLogs:\n{logs}")