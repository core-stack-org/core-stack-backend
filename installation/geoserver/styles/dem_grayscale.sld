<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>dem_gradient</Name>
    <UserStyle>
      <Title>DEM Elevation Gradient</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <Opacity>1</Opacity>
            <ColorMap type="ramp">

              <ColorMapEntry color="#000000" quantity="-9999" opacity="0"  label="NoData"/>

              <!-- ── Low elevations: deep blues ─────────────────────────── -->
              <ColorMapEntry color="#0d0030" quantity="0"    opacity="1"  label="0m"/>
              <ColorMapEntry color="#1a0f6e" quantity="50"   opacity="1"  label="50m"/>
              <ColorMapEntry color="#1746a0" quantity="100"  opacity="1"  label="100m"/>
              <ColorMapEntry color="#1a72c0" quantity="150"  opacity="1"  label="150m"/>

              <!-- ── Mid-low: blue fading into teal ─────────────────────── -->
              <ColorMapEntry color="#2191c0" quantity="200"  opacity="1"  label="200m"/>
              <ColorMapEntry color="#1aab9e" quantity="250"  opacity="1"  label="250m"/>
              <ColorMapEntry color="#16a085" quantity="300"  opacity="1"  label="300m"/>

              <!-- ── Mid: teal into green ───────────────────────────────── -->
              <ColorMapEntry color="#1cb870" quantity="340"  opacity="1"  label="340m"/>
              <ColorMapEntry color="#27ae60" quantity="380"  opacity="1"  label="380m"/>
              <ColorMapEntry color="#5ab836" quantity="410"  opacity="1"  label="410m"/>

              <!-- ── Mid-high: green into yellow ────────────────────────── -->
              <ColorMapEntry color="#95c623" quantity="440"  opacity="1"  label="440m"/>
              <ColorMapEntry color="#d4d400" quantity="470"  opacity="1"  label="470m"/>
              <ColorMapEntry color="#f1c40f" quantity="500"  opacity="1"  label="500m"/>

              <!-- ── High: yellow into earthy tones ─────────────────────── -->
              <ColorMapEntry color="#e09a30" quantity="530"  opacity="1"  label="530m"/>
              <ColorMapEntry color="#d4845a" quantity="560"  opacity="1"  label="560m"/>
              <ColorMapEntry color="#b0623a" quantity="590"  opacity="1"  label="590m"/>

              <!-- ── Very high: warm brown to ivory ─────────────────────── -->
              <ColorMapEntry color="#8b5e3c" quantity="620"  opacity="1"  label="620m"/>
              <ColorMapEntry color="#c4a882" quantity="660"  opacity="1"  label="660m"/>
              <ColorMapEntry color="#f5f0e8" quantity="700"  opacity="1"  label="700m+"/>

            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>