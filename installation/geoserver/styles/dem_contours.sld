<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>dem_contours</Name>
    <UserStyle>
      <Title>DEM Contour Lines</Title>
      <FeatureTypeStyle>
        <Transformation>
          <ogc:Function name="ras:Contour">

            <ogc:Function name="parameter">
              <ogc:Literal>data</ogc:Literal>
            </ogc:Function>

            <!--
              interval read from WMS ENV parameter: &ENV=interval:50
              Falls back to 100 if ENV is not provided.
            -->
            <ogc:Function name="parameter">
              <ogc:Literal>interval</ogc:Literal>
              <ogc:Function name="env">
                <ogc:Literal>interval</ogc:Literal>
                <ogc:Literal>100</ogc:Literal>
              </ogc:Function>
            </ogc:Function>

            <ogc:Function name="parameter">
              <ogc:Literal>smooth</ogc:Literal>
              <ogc:Literal>true</ogc:Literal>
            </ogc:Function>

          </ogc:Function>
        </Transformation>

        <Rule>
          <LineSymbolizer>
            <Stroke>
              <CssParameter name="stroke">#8B4513</CssParameter>
              <CssParameter name="stroke-width">1</CssParameter>
              <CssParameter name="stroke-opacity">0.9</CssParameter>
            </Stroke>
          </LineSymbolizer>
        </Rule>

      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>