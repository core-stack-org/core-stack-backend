"""Landslide susceptibility module for CoRE Stack.

This module implements landslide susceptibility mapping following the methodology
from https://www.sciencedirect.com/science/article/pii/S0341816223007440

The workflow:
1. Clip pan-India landslide susceptibility raster to tehsil boundaries
2. Vectorize at MWS (micro-watershed) level
3. Compute attributes: susceptibility class, area, slope, curvature, LULC
4. Export to GEE assets and sync to GeoServer
5. Provide visualization and validation
"""
