<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld
http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>terrain_raster</Name>
    <UserStyle>
      <Title>A raster style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
              <ColorMapEntry quantity="0" label="Background" color="#f7fcf5" opacity="0.0" />
                <ColorMapEntry quantity="1" label="Double-Single" color="#f7fcf5" opacity="0.7" />  
              	<ColorMapEntry quantity="2" label="Tripple_or_annual_or_perennial-Single" color="#ff4500" opacity="0.7" /> 
                <ColorMapEntry quantity="3" label="Tripple_or_annual_or_perennial-Double" color="#ff0000" opacity="0.7" /> 
                 <ColorMapEntry quantity="4" label="Single-Double" color="#00ff00" opacity="0.0" /> 
                 <ColorMapEntry quantity="5" label="Single-Tripple_or_annual_or_perennial" color="#32cd32" opacity="0.0" /> 
                 <ColorMapEntry quantity="6" label="Double-Tripple_or_annual_or_perennial" color="#228b22" opacity="0.0" /> 
                 <ColorMapEntry quantity="7" label="Single-Single" color="#4227F5" opacity="0.0" /> 
              	 <ColorMapEntry quantity="8" label="Double-Double" color="#712103" opacity="0.0" />
                 <ColorMapEntry quantity="9" label="Tripple_or_annual_or_perennial-Tripple_or_annual_or_perennial" color="#AD27F5" opacity="0.0" /> 

            </ColorMap>
            
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>