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
                <ColorMapEntry quantity="1.0" label="Deep valleys and canyons" color="#313695" opacity="0.0" />
				<ColorMapEntry quantity="2.0" label="Incised drainages and low ridges" color="#4575b4" opacity="0.7" />
              <ColorMapEntry quantity="3.0" label="Mountain tops and high ridges" color="#a50026" opacity="0.7" />
              <ColorMapEntry quantity="4.0" label="U-shape valleys" color="#e0f3f8" opacity="0.7" />
              <ColorMapEntry quantity="5.0" label="Broad Flat Areas" color="#fffc00" opacity="0.7" />
              <ColorMapEntry quantity="6.0" label="Broad open slopes" color="#feb24c" opacity="0.7" />
              <ColorMapEntry quantity="7.0" label="Flat tops" color="#f46d43" opacity="0.7" />
              <ColorMapEntry quantity="8.0" label="Upper Slopes" color="#d73027" opacity="0.7" />
              <ColorMapEntry quantity="9.0" label="Deep valleys and canyons" color="#313695" opacity="0.7" />
              <ColorMapEntry quantity="10.0" label="Incised drainages and low ridges" color="#4575b4" opacity="0.7" />
              <ColorMapEntry quantity="11.0" label="Mountain tops and high ridges" color="#a50026" opacity="0.7" />
              <ColorMapEntry quantity="12.0" label="Background" color="#ffffff" opacity="0.0" />
              
            </ColorMap>
            
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>