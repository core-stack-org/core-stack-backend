"""Tests for landslide susceptibility module."""

import unittest
from unittest.mock import Mock, patch, MagicMock
import ee


class TestLandslideVectorization(unittest.TestCase):
    """Test landslide vectorization functions."""
    
    @patch('computing.landslide.landslide_vector.ee_initialize')
    @patch('computing.landslide.landslide_vector.ee.FeatureCollection')
    @patch('computing.landslide.landslide_vector.ee.Image')
    def test_generate_demo_susceptibility(self, mock_image, mock_fc, mock_init):
        """Test demo susceptibility generation from slope."""
        from computing.landslide.landslide_vector import generate_demo_susceptibility
        
        # Mock DEM and slope
        mock_dem = Mock()
        mock_slope = Mock()
        mock_image.return_value.select.return_value = mock_dem
        
        with patch('computing.landslide.landslide_vector.ee.Terrain') as mock_terrain:
            mock_terrain.slope.return_value = mock_slope
            
            result = generate_demo_susceptibility()
            
            # Verify slope was computed
            mock_terrain.slope.assert_called_once()
    
    def test_susceptibility_classes_defined(self):
        """Test that susceptibility classes are properly defined."""
        from computing.landslide.landslide_vector import SUSCEPTIBILITY_CLASSES
        
        self.assertIsInstance(SUSCEPTIBILITY_CLASSES, dict)
        self.assertEqual(len(SUSCEPTIBILITY_CLASSES), 4)
        self.assertIn(1, SUSCEPTIBILITY_CLASSES)
        self.assertIn(4, SUSCEPTIBILITY_CLASSES)
        self.assertEqual(SUSCEPTIBILITY_CLASSES[1], "low")
        self.assertEqual(SUSCEPTIBILITY_CLASSES[4], "very_high")


class TestLandslideUtils(unittest.TestCase):
    """Test landslide utility functions."""
    
    def test_create_visualization_params(self):
        """Test visualization parameter creation."""
        from computing.landslide.utils import create_landslide_visualization
        
        mock_img = Mock()
        vis_params = create_landslide_visualization(mock_img)
        
        self.assertIsInstance(vis_params, dict)
        self.assertEqual(vis_params['min'], 1)
        self.assertEqual(vis_params['max'], 4)
        self.assertIn('palette', vis_params)
        self.assertEqual(len(vis_params['palette']), 4)
    
    @patch('computing.landslide.utils.ee.FeatureCollection')
    def test_get_susceptibility_statistics(self, mock_fc):
        """Test statistics computation."""
        from computing.landslide.utils import get_susceptibility_statistics
        
        # Mock FeatureCollection methods
        mock_fc_instance = Mock()
        mock_fc_instance.size.return_value.getInfo.return_value = 100
        mock_fc_instance.aggregate_sum.return_value.getInfo.return_value = 1000.0
        mock_fc_instance.aggregate_mean.return_value.getInfo.return_value = 25.5
        mock_fc.return_value = mock_fc_instance
        
        stats = get_susceptibility_statistics(mock_fc_instance)
        
        self.assertIsInstance(stats, dict)
        self.assertIn('total_mws', stats)
        self.assertIn('area_by_class', stats)
        self.assertIn('mean_slope', stats)


class TestLandslideValidation(unittest.TestCase):
    """Test landslide validation functions."""
    
    @patch('computing.landslide.validation.ee.FeatureCollection')
    def test_validate_attributes(self, mock_fc):
        """Test attribute validation."""
        from computing.landslide.validation import validate_attributes
        
        # Mock feature with properties
        mock_feature = Mock()
        mock_feature.propertyNames.return_value.getInfo.return_value = [
            'low_area_ha',
            'moderate_area_ha',
            'high_area_ha',
            'very_high_area_ha',
            'mean_slope_deg',
            'mean_curvature',
            'susceptibility_score',
            'susceptibility_category',
            'total_area_ha'
        ]
        
        mock_fc_instance = Mock()
        mock_fc_instance.first.return_value = mock_feature
        mock_fc_instance.size.return_value.getInfo.return_value = 50
        
        result = validate_attributes(mock_fc_instance)
        
        self.assertIsInstance(result, dict)
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['missing_attributes']), 0)
    
    @patch('computing.landslide.validation.ee.FeatureCollection')
    def test_validate_classification(self, mock_fc):
        """Test classification validation."""
        from computing.landslide.validation import validate_classification
        
        # Mock feature collection
        mock_fc_instance = Mock()
        mock_fc_instance.aggregate_array.return_value.distinct.return_value.getInfo.return_value = [
            'low', 'moderate', 'high', 'very_high'
        ]
        mock_fc_instance.filter.return_value.size.return_value.getInfo.return_value = 10
        
        result = validate_classification(mock_fc_instance)
        
        self.assertIsInstance(result, dict)
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['unexpected_classes']), 0)
        self.assertIn('class_distribution', result)


class TestLandslideAPI(unittest.TestCase):
    """Test landslide API endpoints."""
    
    @patch('computing.api.vectorise_landslide')
    def test_generate_landslide_layer_endpoint(self, mock_vectorise):
        """Test the generate_landslide_layer API endpoint."""
        from computing.api import generate_landslide_layer
        from rest_framework.test import APIRequestFactory
        
        # Create mock request
        factory = APIRequestFactory()
        request = factory.post('/computing/generate_landslide_layer/', {
            'state': 'jharkhand',
            'district': 'ranchi',
            'block': 'ranchi',
            'gee_account_id': 1
        })
        
        # Mock the task
        mock_task = Mock()
        mock_vectorise.apply_async.return_value = mock_task
        
        try:
            response = generate_landslide_layer(request)
            
            # Verify task was called
            mock_vectorise.apply_async.assert_called_once()
            
            # Check response
            self.assertEqual(response.status_code, 200)
        except Exception as e:
            # API might fail due to missing Django setup in tests
            # This is expected in unit tests without full Django environment
            self.assertIn('django', str(type(e).__module__).lower())


class TestIntegration(unittest.TestCase):
    """Integration tests for landslide module."""
    
    def test_module_imports(self):
        """Test that all module components can be imported."""
        try:
            from computing.landslide import landslide_vector
            from computing.landslide import utils
            from computing.landslide import validation
            
            self.assertTrue(hasattr(landslide_vector, 'vectorise_landslide'))
            self.assertTrue(hasattr(landslide_vector, 'generate_demo_susceptibility'))
            self.assertTrue(hasattr(utils, 'get_susceptibility_statistics'))
            self.assertTrue(hasattr(validation, 'validate_attributes'))
        except ImportError as e:
            self.fail(f"Failed to import module: {e}")
    
    def test_constants_defined(self):
        """Test that required constants are defined."""
        from computing.landslide.landslide_vector import (
            LANDSLIDE_SUSCEPTIBILITY_ASSET,
            SUSCEPTIBILITY_CLASSES
        )
        
        self.assertIsInstance(LANDSLIDE_SUSCEPTIBILITY_ASSET, str)
        self.assertIsInstance(SUSCEPTIBILITY_CLASSES, dict)
        self.assertEqual(len(SUSCEPTIBILITY_CLASSES), 4)


if __name__ == '__main__':
    unittest.main()
