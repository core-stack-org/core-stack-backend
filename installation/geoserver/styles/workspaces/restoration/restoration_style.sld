<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld
http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>ccd_style</Name>
    <UserStyle>
      <Title>A raster style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>

              <ColorMapEntry color="#ffff00" quantity="0.0" label="Excluded Areas" opacity="0.0" />
              <ColorMapEntry color="#d79b0f" quantity="1.0" label="Mosaic Restoration" opacity="0.7" />
              <ColorMapEntry color="#0f077c" quantity="2.0" label="Wide-scale Restoration" opacity="0.7" />
              <ColorMapEntry color="#4fbc14" quantity="3.0" label="Protection" opacity="0.7" />
            </ColorMap> 
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>