import pytest

@pytest.fixture
def sample_data():
    return [
        {"id": 1, "name": "Carrier A"},
        {"id": 2, "name": "Carrier B"}
    ]
