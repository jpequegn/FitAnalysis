import pytest
import tempfile
import os
import io
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import pandas as pd

from main import app


class TestFastAPIEndpoints:
    """Test suite for FastAPI endpoints."""

    def setup_method(self):
        """Set up test client."""
        self.client = TestClient(app)

    def test_get_root_endpoint(self):
        """Test the root endpoint returns HTML form."""
        response = self.client.get("/")
        assert response.status_code == 200
        assert "multipart/form-data" in response.text
        assert "file" in response.text
        assert "submit" in response.text

    def test_upload_valid_fit_file(self):
        """Test uploading a valid FIT file."""
        # Create a mock FIT file content
        fit_content = b"fake_fit_file_content"
        
        with patch('main.FitDataLoader') as mock_loader:
            # Mock the loader to return a DataFrame
            mock_instance = MagicMock()
            mock_df = pd.DataFrame({'heart_rate': [120, 130, 140], 'power': [200, 210, 220]})
            mock_instance.load.return_value = mock_df
            mock_loader.return_value = mock_instance
            
            # Upload the file
            response = self.client.post(
                "/upload/",
                files={"file": ("test_activity.fit", fit_content, "application/octet-stream")}
            )
            
            assert response.status_code == 200
            # Check that the response contains JSON data
            json_data = response.json()
            assert "columns" in json_data
            assert "data" in json_data
            
            # Verify the loader was called
            mock_loader.assert_called_once()
            mock_instance.load.assert_called_once()

    def test_upload_invalid_file_extension(self):
        """Test uploading a file with invalid extension."""
        invalid_content = b"not_a_fit_file"
        
        response = self.client.post(
            "/upload/",
            files={"file": ("test_file.txt", invalid_content, "text/plain")}
        )
        
        assert response.status_code == 400
        assert "Invalid file type" in response.json()["detail"]

    def test_upload_corrupted_fit_file(self):
        """Test uploading a corrupted FIT file."""
        corrupted_content = b"corrupted_fit_file"
        
        with patch('main.FitDataLoader') as mock_loader:
            # Mock the loader to raise an exception
            mock_instance = MagicMock()
            mock_instance.load.side_effect = Exception("Corrupted file")
            mock_loader.return_value = mock_instance
            
            response = self.client.post(
                "/upload/",
                files={"file": ("corrupted.fit", corrupted_content, "application/octet-stream")}
            )
            
            assert response.status_code == 500
            assert "Error processing file" in response.json()["detail"]

    def test_upload_empty_file(self):
        """Test uploading an empty file."""
        empty_content = b""
        
        response = self.client.post(
            "/upload/",
            files={"file": ("empty.fit", empty_content, "application/octet-stream")}
        )
        
        assert response.status_code == 500
        assert "Error processing file" in response.json()["detail"]

    def test_upload_no_file_provided(self):
        """Test request without file parameter."""
        response = self.client.post("/upload/")
        
        assert response.status_code == 422  # Unprocessable Entity

    def test_upload_large_file(self):
        """Test uploading a large file."""
        # Create a large file content (1MB)
        large_content = b"x" * (1024 * 1024)
        
        with patch('main.FitDataLoader') as mock_loader:
            # Mock the loader to return a DataFrame
            mock_instance = MagicMock()
            mock_df = pd.DataFrame({'heart_rate': [120], 'power': [200]})
            mock_instance.load.return_value = mock_df
            mock_loader.return_value = mock_instance
            
            response = self.client.post(
                "/upload/",
                files={"file": ("large_file.fit", large_content, "application/octet-stream")}
            )
            
            assert response.status_code == 200

    def test_temporary_file_cleanup(self):
        """Test that temporary files are properly cleaned up."""
        fit_content = b"fake_fit_file_content"
        
        with patch('main.FitDataLoader') as mock_loader:
            # Mock the loader to return a DataFrame
            mock_instance = MagicMock()
            mock_df = pd.DataFrame({'heart_rate': [120], 'power': [200]})
            mock_instance.load.return_value = mock_df
            mock_loader.return_value = mock_instance
            
            # Track temporary file creation
            original_tempfile = tempfile.NamedTemporaryFile
            temp_files_created = []
            
            def track_tempfile(*args, **kwargs):
                temp_file = original_tempfile(*args, **kwargs)
                temp_files_created.append(temp_file.name)
                return temp_file
            
            with patch('main.tempfile.NamedTemporaryFile', side_effect=track_tempfile):
                response = self.client.post(
                    "/upload/",
                    files={"file": ("test_activity.fit", fit_content, "application/octet-stream")}
                )
                
                assert response.status_code == 200
                
                # Check that temporary files were created and cleaned up
                assert len(temp_files_created) > 0
                for temp_file_path in temp_files_created:
                    assert not os.path.exists(temp_file_path), f"Temporary file {temp_file_path} was not cleaned up"

    def test_upload_file_with_special_characters(self):
        """Test uploading a file with special characters in filename."""
        fit_content = b"fake_fit_file_content"
        
        with patch('main.FitDataLoader') as mock_loader:
            # Mock the loader to return a DataFrame
            mock_instance = MagicMock()
            mock_df = pd.DataFrame({'heart_rate': [120], 'power': [200]})
            mock_instance.load.return_value = mock_df
            mock_loader.return_value = mock_instance
            
            response = self.client.post(
                "/upload/",
                files={"file": ("test file with spaces & symbols.fit", fit_content, "application/octet-stream")}
            )
            
            assert response.status_code == 200

    def test_concurrent_uploads(self):
        """Test handling multiple concurrent uploads."""
        fit_content = b"fake_fit_file_content"
        
        with patch('main.FitDataLoader') as mock_loader:
            # Mock the loader to return a DataFrame
            mock_instance = MagicMock()
            mock_df = pd.DataFrame({'heart_rate': [120], 'power': [200]})
            mock_instance.load.return_value = mock_df
            mock_loader.return_value = mock_instance
            
            # Simulate concurrent requests
            responses = []
            for i in range(5):
                response = self.client.post(
                    "/upload/",
                    files={"file": (f"test_activity_{i}.fit", fit_content, "application/octet-stream")}
                )
                responses.append(response)
            
            # All requests should succeed
            for response in responses:
                assert response.status_code == 200

    def test_response_format(self):
        """Test that response format is correct JSON."""
        fit_content = b"fake_fit_file_content"
        
        with patch('main.FitDataLoader') as mock_loader:
            # Mock the loader to return a specific DataFrame
            mock_instance = MagicMock()
            mock_df = pd.DataFrame({
                'heart_rate': [120, 130, 140],
                'power': [200, 210, 220]
            })
            mock_instance.load.return_value = mock_df
            mock_loader.return_value = mock_instance
            
            response = self.client.post(
                "/upload/",
                files={"file": ("test_activity.fit", fit_content, "application/octet-stream")}
            )
            
            assert response.status_code == 200
            
            # The response should be a JSON string (from DataFrame.to_json)
            # Let's parse it to check the structure
            import json
            json_data = json.loads(response.json())
            
            # Check JSON structure (pandas DataFrame to_json with orient='split')
            assert "columns" in json_data
            assert "data" in json_data
            assert "index" in json_data
            
            # Check that columns contain expected data
            assert "heart_rate" in json_data["columns"]
            assert "power" in json_data["columns"]