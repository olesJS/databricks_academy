from databricks.sdk.errors import DatabricksError

from dbr_ops import cli


def test_parser_knows_all_commands():
    parser = cli.build_parser()
    for argv in (
        ["whoami"],
        ["cluster", "create", "--name", "x"],
        ["job", "run", "my_job"],
        ["notebook", "run", "/path"],
        ["status", "123"],
        ["pipeline", "run", "my_pipeline"],
        ["full_run"],
    ):
        assert parser.parse_args(argv).command == argv[0]


def test_main_returns_1_on_databricks_error(monkeypatch):
    def boom(profile):
        raise DatabricksError("boom")

    monkeypatch.setattr(cli, "get_client", boom)
    assert cli.main(["whoami"]) == 1


def test_full_run_fails_fast_when_notebook_file_missing():
    assert cli.main(["full_run", "--notebook_file", "no_such_file.py"]) == 1
