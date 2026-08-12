<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld
http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>lulc_level_1_style</Name>
    <UserStyle>
      <Title>A raster style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
                <ColorMapEntry color="#000000" quantity="0.0" label="Background" opacity="0.0" />
              	<ColorMapEntry color="#ff0000" quantity="1.0" label="Built Up" opacity="0.0" />
              	<ColorMapEntry color="#74CCF4" quantity="2.0" label="Kharif Water" opacity="0.7" />
              	<ColorMapEntry color="#1ca3ec" quantity="3.0" label="Kharif and Rabi Water" opacity="0.7" />
              	<ColorMapEntry color="#0f5e9c" quantity="4.0" label="Kharif, Rabi and Zaid Water" opacity="0.7" />
              	<ColorMapEntry color="#73bb53" quantity="6.0" label="Trees / Forests" opacity="0.0" />
              	<ColorMapEntry color="#A9A9A9" quantity="7.0" label="Barren Lands" opacity="0.0" />
              	<ColorMapEntry color="#73bb53" quantity="8.0" label="Single Kharif" opacity="0.0" />
              	<ColorMapEntry color="#73bb53" quantity="9.0" label="Single Non-Kharif" opacity="0.0" />
              	<ColorMapEntry color="#73bb53" quantity="10.0" label="Double Cropping" opacity="0.0" />
              	<ColorMapEntry color="#73bb53" quantity="11.0" label="Triple Cropping" opacity="0.0" />
              	<ColorMapEntry color="#eaa4f0" quantity="12.0" label="Shrubs and Scrubs" opacity="0.0" />
            </ColorMap>
            
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>