"""The version of a collection in the model that a client gets."""

import pytest

from chromadb.api import ClientAPI
from chromadb.test.conftest import is_client_in_process


def test_the_model_of_a_collection_in_process_has_its_version(
    client: ClientAPI,
) -> None:
    """The single node grows the version of a collection at each apply of records. The client in process reads it,
    by every way it gets a collection, as a client of a server reads it from the JSON."""
    if not is_client_in_process(client):
        pytest.skip("a client of a server reads the version from the JSON of the server")
    client.reset()
    collection = client.create_collection("version_in_process")
    collection.add(ids=["a", "b"], embeddings=[[1.0, 2.0], [3.0, 4.0]])
    collection.add(ids=["c"], embeddings=[[5.0, 6.0]])

    by_name = client.get_collection("version_in_process").get_model()
    assert by_name["version"] == 2
    assert client._server.get_collection_by_id(by_name["id"])["version"] == 2  # type: ignore[attr-defined]
    listed = [c for c in client.list_collections() if c.name == "version_in_process"]
    assert listed[0].get_model()["version"] == 2
    again = client.get_or_create_collection("version_in_process")
    assert again.get_model()["version"] == 2
