<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld
http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>testClart</Name>
    <UserStyle>
      <Title>A raster style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
            	<ColorMapEntry color="#F5F6FE" quantity="0" label="Empty" opacity="0.0" />
            	<ColorMapEntry color="#4EE323" quantity="1" label="Good recharge" opacity="1.0" />
            	<ColorMapEntry color="#F3FF33" quantity="2" label="Moderate recharge" opacity="1.0" />
            	<ColorMapEntry color="#F21223" quantity="3" label="Surface water harvesting" opacity="1.0" />
              	<ColorMapEntry color="#B40F7D" quantity="4" label="Regeneration" opacity="1.0" />
              	<ColorMapEntry color="#1774DE" quantity="5" label="High runoff zone" opacity="1.0" />
            </ColorMap>
            
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>