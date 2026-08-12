<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld
http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>tree_overall_ch_style</Name>
    <UserStyle>
      <Title>A raster style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
                <ColorMapEntry color="#FF0000" quantity="-2.0" label="Deforestation" opacity="0.7" />
              	<ColorMapEntry color="#FFA500" quantity="-1.0" label="Degradation" opacity="0.7" />
              	<ColorMapEntry color="#FFFFFF" quantity="0.0" label="No Change" opacity="0.7" />
              	<ColorMapEntry color="#8AFF8A" quantity="1.0" label="Improvement" opacity="0.7" />
              	<ColorMapEntry color="#007500" quantity="2.0" label="Afforestation" opacity="0.7" />
              	<ColorMapEntry color="#DEE64C" quantity="3.0" label="Partially Degraded" opacity="0.7" />
              	<ColorMapEntry color="#DEE64C" quantity="4.0" label="Partially Degraded" opacity="0.7" />
              	<ColorMapEntry color="#000000" quantity="5.0" label="Missing Data" opacity="0.7" />
            </ColorMap> 
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>