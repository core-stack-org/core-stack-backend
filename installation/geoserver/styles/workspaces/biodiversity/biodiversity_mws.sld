<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
    xmlns="http://www.opengis.net/sld"
    xmlns:ogc="http://www.opengis.net/ogc"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">
  <NamedLayer>
    <Name>biodiversity_mws</Name>
    <UserStyle>
      <Title>Biodiversity - species richness</Title>
      <Abstract>
        Per-MWS species richness choropleth (green ramp). Data-poor watersheds (fewer than 20
        occurrence records) are drawn grey, because GBIF absence does not confirm true absence.
      </Abstract>
      <FeatureTypeStyle>

        <!-- Data-poor MWS: grey, regardless of richness (survey gap, not low biodiversity) -->
        <Rule>
          <Title>Under-surveyed (data poor)</Title>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>data_poor</ogc:PropertyName>
              <ogc:Literal>1</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#d9d9d9</CssParameter></Fill>
            <Stroke><CssParameter name="stroke">#7f7f7f</CssParameter><CssParameter name="stroke-width">0.4</CssParameter></Stroke>
          </PolygonSymbolizer>
        </Rule>

        <!-- Richness ramp (only for MWS with adequate survey effort) -->
        <Rule>
          <Title>Very Low (0-9)</Title>
          <ogc:Filter><ogc:And>
            <ogc:PropertyIsNotEqualTo><ogc:PropertyName>data_poor</ogc:PropertyName><ogc:Literal>1</ogc:Literal></ogc:PropertyIsNotEqualTo>
            <ogc:PropertyIsLessThan><ogc:PropertyName>species_richness</ogc:PropertyName><ogc:Literal>10</ogc:Literal></ogc:PropertyIsLessThan>
          </ogc:And></ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#edf8e9</CssParameter></Fill>
            <Stroke><CssParameter name="stroke">#7f7f7f</CssParameter><CssParameter name="stroke-width">0.4</CssParameter></Stroke>
          </PolygonSymbolizer>
        </Rule>
        <Rule>
          <Title>Low (10-24)</Title>
          <ogc:Filter><ogc:And>
            <ogc:PropertyIsNotEqualTo><ogc:PropertyName>data_poor</ogc:PropertyName><ogc:Literal>1</ogc:Literal></ogc:PropertyIsNotEqualTo>
            <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>species_richness</ogc:PropertyName><ogc:Literal>10</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
            <ogc:PropertyIsLessThan><ogc:PropertyName>species_richness</ogc:PropertyName><ogc:Literal>25</ogc:Literal></ogc:PropertyIsLessThan>
          </ogc:And></ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#bae4b3</CssParameter></Fill>
            <Stroke><CssParameter name="stroke">#7f7f7f</CssParameter><CssParameter name="stroke-width">0.4</CssParameter></Stroke>
          </PolygonSymbolizer>
        </Rule>
        <Rule>
          <Title>Moderate (25-49)</Title>
          <ogc:Filter><ogc:And>
            <ogc:PropertyIsNotEqualTo><ogc:PropertyName>data_poor</ogc:PropertyName><ogc:Literal>1</ogc:Literal></ogc:PropertyIsNotEqualTo>
            <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>species_richness</ogc:PropertyName><ogc:Literal>25</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
            <ogc:PropertyIsLessThan><ogc:PropertyName>species_richness</ogc:PropertyName><ogc:Literal>50</ogc:Literal></ogc:PropertyIsLessThan>
          </ogc:And></ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#74c476</CssParameter></Fill>
            <Stroke><CssParameter name="stroke">#7f7f7f</CssParameter><CssParameter name="stroke-width">0.4</CssParameter></Stroke>
          </PolygonSymbolizer>
        </Rule>
        <Rule>
          <Title>High (50-99)</Title>
          <ogc:Filter><ogc:And>
            <ogc:PropertyIsNotEqualTo><ogc:PropertyName>data_poor</ogc:PropertyName><ogc:Literal>1</ogc:Literal></ogc:PropertyIsNotEqualTo>
            <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>species_richness</ogc:PropertyName><ogc:Literal>50</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
            <ogc:PropertyIsLessThan><ogc:PropertyName>species_richness</ogc:PropertyName><ogc:Literal>100</ogc:Literal></ogc:PropertyIsLessThan>
          </ogc:And></ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#31a354</CssParameter></Fill>
            <Stroke><CssParameter name="stroke">#7f7f7f</CssParameter><CssParameter name="stroke-width">0.4</CssParameter></Stroke>
          </PolygonSymbolizer>
        </Rule>
        <Rule>
          <Title>Very High (100+)</Title>
          <ogc:Filter><ogc:And>
            <ogc:PropertyIsNotEqualTo><ogc:PropertyName>data_poor</ogc:PropertyName><ogc:Literal>1</ogc:Literal></ogc:PropertyIsNotEqualTo>
            <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>species_richness</ogc:PropertyName><ogc:Literal>100</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
          </ogc:And></ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#006d2c</CssParameter></Fill>
            <Stroke><CssParameter name="stroke">#7f7f7f</CssParameter><CssParameter name="stroke-width">0.4</CssParameter></Stroke>
          </PolygonSymbolizer>
        </Rule>

      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
