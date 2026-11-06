from databricks.sdk import WorkspaceClient
from dbr_ops.models import Identity


def get_client(profile=None) -> WorkspaceClient:
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def get_identity(w: WorkspaceClient) -> Identity:
    me = w.current_user.me()
    return Identity(
        user_name=me.user_name,
        display_name=me.display_name,
        host=w.config.host
    )

