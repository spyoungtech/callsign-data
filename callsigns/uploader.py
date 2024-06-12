import logging
import os
import queue
import threading
from typing import Type
from typing import TYPE_CHECKING


class STOP:
    ...


class Uploader:
    def __init__(
        self, rootdir: str, bucket_name: str, num_workers: int = 3, _dry_run: bool = False, quiet: int = 0
    ) -> None:
        self.rootdir: str = rootdir
        self.queue: queue.Queue[str | Type[STOP]] = queue.Queue()
        self._dry_run = _dry_run
        self.bucket_name = bucket_name
        self.num_workers = num_workers
        self.workers = []
        self.upload_errors: list[tuple[str, str]] = []
        self.quiet = quiet
        for _ in range(num_workers):
            worker = threading.Thread(target=self.worker)
            worker.start()
            self.workers.append(worker)

    def queue_upload(self, key: str) -> None:
        self.queue.put(key)

    def worker(self) -> None:
        import boto3
        from botocore.config import Config

        c = Config(retries={'max_attempts': 10, 'mode': 'standard'})
        session = boto3.Session()
        client = session.client('s3', config=c)
        while True:
            key = self.queue.get()
            if key is STOP:
                self.queue.task_done()
                return
            if TYPE_CHECKING:
                assert isinstance(key, str)
            local_path = os.path.join(self.rootdir, key)
            logging.info(f'Uploading {local_path} to s3://{self.bucket_name}/{key}')
            if not self._dry_run:
                try:
                    client.upload_file(local_path, self.bucket_name, key)
                except Exception as e:
                    logging.error(f'Problem uploading file {local_path}', exc_info=True)
                    self.upload_errors.append((local_path, str(e)))
            self.queue.task_done()

    def join(self) -> None:
        for _ in range(self.num_workers):
            self.queue.put_nowait(STOP)
        for worker in self.workers:
            worker.join()
