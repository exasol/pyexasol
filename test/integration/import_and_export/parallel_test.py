"""
Integration tests for the parallel HTTP transport, see :ref:`http_transport_parallel`.

Each node of the transport runs in a separate process, while the main process runs the
``EXPORT`` or ``IMPORT`` statement through
:meth:`pyexasol.ExaConnection.export_parallel` or
:meth:`pyexasol.ExaConnection.import_parallel`.
"""

import multiprocessing

import pytest

import pyexasol
from pyexasol.callback import (
    export_to_file,
    import_from_file,
)
from test.integration.import_and_export.helper import select_result

NUMBER_OF_PROCESSES = 2


class TransportProcess(multiprocessing.Process):
    """Child process that serves one HTTP transport endpoint and reports its address."""

    def __init__(self, node, callback, path, compression, encryption):
        self.node = node
        self.callback = callback
        self.path = path
        self.compression = compression
        self.encryption = encryption
        self.read_pipe, self.write_pipe = multiprocessing.Pipe(False)
        super().__init__()

    def start(self):
        super().start()
        self.write_pipe.close()

    @property
    def exa_address(self):
        return self.read_pipe.recv()

    def run(self):
        self.read_pipe.close()
        http = pyexasol.http_transport(
            self.node["ipaddr"],
            self.node["port"],
            compression=self.compression,
            encryption=self.encryption,
        )
        self.write_pipe.send(http.exa_address)
        self.write_pipe.close()

        if self.callback is export_to_file:
            http.export_to_callback(export_to_file, self.path)
        else:
            http.import_from_callback(import_from_file, self.path)


def run_parallel(connection, callback, paths, parallel_call, compression=False):
    """
    Start one transport process per path, then run ``parallel_call(address_list)``
    in the main process and wait for the children.
    """
    nodes = connection.get_nodes(len(paths))
    processes = [
        TransportProcess(
            node,
            callback,
            path,
            compression=compression,
            encryption=connection.options["encryption"],
        )
        for node, path in zip(nodes, paths)
    ]
    address_list = []
    try:
        for process in processes:
            process.start()
            address_list.append(process.exa_address)

        parallel_call(address_list)
    except BaseException:
        for process in processes:
            process.terminate()
        raise
    finally:
        for process in processes:
            process.join(timeout=60)

    assert all(process.exitcode == 0 for process in processes)


def exported_lines(paths) -> list[str]:
    lines = []
    for path in paths:
        lines.extend(path.read_text().splitlines())
    return lines


@pytest.mark.etl
class TestExportParallel:
    @staticmethod
    @pytest.mark.parametrize("compression", [False, True], ids=["plain", "compressed"])
    def test_table_is_exported_by_all_processes(
        connection_factory, fill_table, all_data, tmp_path, compression
    ):
        paths = [tmp_path / f"export_{i}.csv" for i in range(NUMBER_OF_PROCESSES)]

        with connection_factory(compression=compression) as connection:
            run_parallel(
                connection,
                export_to_file,
                paths,
                lambda addresses: connection.export_parallel(addresses, fill_table),
                compression=compression,
            )

        expected = all_data.csv_str().splitlines()
        assert sorted(exported_lines(paths)) == sorted(expected)

    @staticmethod
    def test_query_with_params_is_exported(connection, fill_table, all_data, tmp_path):
        paths = [tmp_path / f"export_{i}.csv" for i in range(NUMBER_OF_PROCESSES)]
        first_name = all_data.list_dict[0]["FIRST_NAME"]

        run_parallel(
            connection,
            export_to_file,
            paths,
            lambda addresses: connection.export_parallel(
                addresses,
                f"SELECT FIRST_NAME FROM {fill_table} WHERE FIRST_NAME = {{name}}",
                query_params={"name": first_name},
            ),
        )

        expected = [
            row["FIRST_NAME"]
            for row in all_data.list_dict
            if row["FIRST_NAME"] == first_name
        ]
        assert exported_lines(paths) == expected


@pytest.mark.etl
class TestImportParallel:
    @staticmethod
    @pytest.mark.parametrize("compression", [False, True], ids=["plain", "compressed"])
    def test_files_are_imported_by_all_processes(
        connection_factory, empty_table, all_data, tmp_path, compression
    ):
        half = len(all_data.list_dict) // 2
        paths = []
        for index, rows in enumerate(
            (all_data.list_dict[:half], all_data.list_dict[half:])
        ):
            sample = type(all_data)(columns=all_data.columns, list_dict=rows)
            directory = tmp_path / f"part_{index}"
            directory.mkdir()
            paths.append(sample.write_csv(directory=directory))

        with connection_factory(compression=compression) as connection:
            run_parallel(
                connection,
                import_from_file,
                paths,
                lambda addresses: connection.import_parallel(addresses, empty_table),
                compression=compression,
            )
            result = select_result(connection)

        assert result == all_data.list_tuple()
