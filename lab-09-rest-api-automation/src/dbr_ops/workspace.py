import io
import logging
from datetime import datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

log = logging.getLogger(__name__)


def upload_notebook(w: WorkspaceClient, local_file: Path, owner: str) -> str:
    folder = f"/Workspace/Users/{owner}/dbrops_tmp"
    path = f"{folder}/{local_file.stem}_{datetime.now():%Y%m%d_%H%M%S}"
    log.info("Uploading %s to %s", local_file, path)
    w.workspace.mkdirs(folder)
    w.workspace.upload(
        path,
        io.BytesIO(local_file.read_bytes()),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    return path


def delete_notebook(w: WorkspaceClient, path: str) -> None:
    log.info("Deleting temporary notebook %s", path)
    w.workspace.delete(path)
