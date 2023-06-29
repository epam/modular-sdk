import os
from typing import Optional
from pathlib import Path
import sys

SDK_FOLDER = 'modular_sdk'


class ImportFromSourceContext:
    """
    Context object to import lambdas and packages. It's necessary because
    root path is not the path to the project
    Also, environment context
    """

    def __init__(self, source_folder: Optional[str] = SDK_FOLDER,
                 envs: Optional[dict] = None):
        self.envs = envs or {}
        self._old_envs = {}
        self.source_folder = source_folder
        self.assert_source_path_exists()

    @property
    def project_path(self) -> Path:
        return Path(__file__).parent.parent.parent

    @property
    def source_path(self) -> Path:
        return Path(self.project_path, self.source_folder)

    def assert_source_path_exists(self):
        source_path = self.source_path
        if not source_path.exists():
            print(f'Source path "{source_path}" does not exist.',
                  file=sys.stderr)
            sys.exit(1)

    def _add_source_to_path(self):
        source_path = str(self.source_path)
        if source_path not in sys.path:
            sys.path.append(source_path)

    def _remove_source_from_path(self):
        source_path = str(self.source_path)
        if source_path in sys.path:
            sys.path.remove(source_path)

    def _add_envs(self):
        for k, v in self.envs.items():
            if k in os.environ:
                self._old_envs[k] = os.environ[k]
            os.environ[k] = v

    def _remove_envs(self):
        for k in self.envs:
            os.environ.pop(k, None)
        os.environ.update(self._old_envs)

    def __enter__(self):
        self._add_source_to_path()
        self._add_envs()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._remove_source_from_path()
        self._remove_envs()
