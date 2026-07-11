<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld
  http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">

  <NamedLayer>
    <Name>slope_percentage</Name>
    <UserStyle>
      <Title>Slope Percentage</Title>
      <Abstract>Raster color ramp based on slope percentage classes</Abstract>

      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
            <ColorMap type="intervals">

              <ColorMapEntry color="#ced4da" quantity="3"   label="0–3 %"   opacity="1.0"/>
              <ColorMapEntry color="#adb5bd" quantity="5"   label="3–5 %"   opacity="1.0"/>
              <ColorMapEntry color="#6c757d" quantity="7"   label="5–7 %"   opacity="1.0"/>
              <ColorMapEntry color="#495057" quantity="10"  label="7–10 %"  opacity="1.0"/>
              <ColorMapEntry color="#343a40" quantity="25"  label="10–25 %" opacity="1.0"/>
              <ColorMapEntry color="#212529" quantity="99999" label="> 25 %" opacity="1.0"/>

            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>

    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>