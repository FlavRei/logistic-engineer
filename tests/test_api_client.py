from src.extract.api_client import fetch_data

def test_fetch_data_success(mocker, sample_data):
    mock_get = mocker.patch("src.extract.api_client.requests.get")
    mock_resp = mock_get.return_value
    mock_resp.status_code = 200
    mock_resp.json.return_value = sample_data

    result = fetch_data("carriers")
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["name"] == "Carrier A"
    mock_get.assert_called_once_with("http://localhost:8000/carriers")
