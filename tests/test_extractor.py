import os
import pandas as pd
from src.extract.extractor import save_raw_data, extract_all

def test_save_raw_data_creates_parquet(tmp_path, sample_data, mocker):
    mocker.patch("src.extract.extractor.DATA_DIR", str(tmp_path))
    dataset_name = "test_dataset"
    file_path = save_raw_data(sample_data, dataset_name)

    assert os.path.exists(file_path)
    df = pd.read_parquet(file_path)
    assert len(df) == len(sample_data)
    assert "name" in df.columns

def test_extract_all_calls_fetch_data(tmp_path, mocker, sample_data):
    mocker.patch("src.extract.extractor.DATA_DIR", str(tmp_path))
    mock_fetch = mocker.patch("src.extract.extractor.fetch_data", return_value=sample_data)

    saved_files = extract_all()

    assert len(saved_files) == 4
    for f in saved_files:
        assert os.path.exists(f)
    mock_fetch.assert_called()
