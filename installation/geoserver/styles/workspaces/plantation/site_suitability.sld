<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld
  http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">

  <NamedLayer>
    <Name>site_suitability</Name>
    <UserStyle>
      <Title>Site Suitability - Band 7</Title>
      <FeatureTypeStyle>

        <Rule>
          <RasterSymbolizer>

            <ChannelSelection>
              <GrayChannel>
                <SourceChannelName>7</SourceChannelName>
              </GrayChannel>
            </ChannelSelection>

            <ColorMap type="values">
              <ColorMapEntry quantity="1" label="Very Good" color="#2E7D32" opacity="0.7"/>
              <ColorMapEntry quantity="2" label="Good" color="#66BB6A" opacity="0.7"/>
              <ColorMapEntry quantity="3" label="Moderate" color="#FDD835" opacity="0.7"/>
              <ColorMapEntry quantity="4" label="Marginally Suitable" color="#FF8F00" opacity="0.7"/>
              <ColorMapEntry quantity="5" label="Unsuitable" color="#D32F2F" opacity="0.7"/>
            </ColorMap>

          </RasterSymbolizer>
        </Rule>

      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>