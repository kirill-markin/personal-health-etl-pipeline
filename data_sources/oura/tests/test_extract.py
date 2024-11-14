import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from ..etl.extract import OuraExtractor

@pytest.fixture
def mock_config():
    return {
        'api': {
            'base_url': 'https://api.ouraring.com/v2',
            'endpoints': {
                'sleep': '/sleep',
                'activity': '/activity'
            }
        }
    }

@pytest.fixture
def extractor(mock_config):
    with patch('yaml.safe_load', return_value=mock_config):
        with patch.dict('os.environ', {'OURA_API_TOKEN': 'test_token'}):
            return OuraExtractor('dummy_path')

def test_make_request_success(extractor):
    mock_response = MagicMock()
    mock_response.json.return_value = {'data': []}
    
    with patch('requests.get', return_value=mock_response):
        data = extractor._make_request(
            '/sleep',
            datetime(2024, 1, 1),
            datetime(2024, 1, 2)
        )
        
        assert data == {'data': []}

def test_make_request_failure(extractor):
    with patch('requests.get', side_effect=Exception('API Error')):
        with pytest.raises(Exception):
            extractor._make_request(
                '/sleep',
                datetime(2024, 1, 1),
                datetime(2024, 1, 2)
            )
