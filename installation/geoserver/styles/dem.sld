<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">

  <NamedLayer>
    <Name>terrarium_dem</Name>
    <UserStyle>
      <Title>Terrarium RGB Elevation Encoding</Title>
      <Abstract>
        Encodes DEM elevation values as Terrarium RGB so OpenLayers can decode
        real-metre elevation with sub-metre precision using calculateElevation().
      </Abstract>

      <FeatureTypeStyle>
        <!--
          ras:Jiffle runs a per-pixel script on the source raster bands.
          src[0] = raw elevation value in metres from your DEM.
          dst[0], dst[1], dst[2] = R, G, B output channels (0-255 range).
        -->
        <Transformation>
          <ogc:Function name="ras:Jiffle">

            <!-- Output: 3-band floating-point raster (one band per RGB channel) -->
            <ogc:Function name="parameter">
              <ogc:Literal>outputType</ogc:Literal>
              <ogc:Literal>FLOAT</ogc:Literal>
            </ogc:Function>
            <ogc:Function name="parameter">
              <ogc:Literal>outputBands</ogc:Literal>
              <ogc:Literal>3</ogc:Literal>
            </ogc:Function>

            <!--
              Jiffle script — Terrarium encoding.

              Avoids the % operator for maximum compatibility across
              Jiffle versions (uses subtraction instead of modulo).

              pv  = pixelValue = (elevation + 32768) × 256
              r   = most-significant byte  [0-255]
              g   = middle byte            [0-255]
              b   = least-significant byte [0-255]
            -->
            <ogc:Function name="parameter">
              <ogc:Literal>script</ogc:Literal>
              <ogc:Literal>
                n = src[0];

                if (isnan(n) || n &lt; -32768 || n &gt; 32767) {
                  // No-data or out-of-range: output black (alpha handled by WMS)
                  dst[0] = 0;
                  dst[1] = 0;
                  dst[2] = 0;
                } else {
                  pv = (n + 32768) * 256;

                  // R: most-significant byte
                  r  = floor(pv / 65536);
                  // G: middle byte
                  g  = floor((pv - r * 65536) / 256);
                  // B: least-significant byte
                  b  = floor(pv - r * 65536 - g * 256);

                  dst[0] = r;
                  dst[1] = g;
                  dst[2] = b;
                }
              </ogc:Literal>
            </ogc:Function>

          </ogc:Function>
        </Transformation>

        <Rule>
          <RasterSymbolizer>
            <Opacity>1</Opacity>
            <!--
              Map the 3 Jiffle output bands to RGB display channels.
              Band 1 (dst[0]) → Red
              Band 2 (dst[1]) → Green
              Band 3 (dst[2]) → Blue
            -->
            <ChannelSelection>
              <RedChannel>
                <SourceChannelName>1</SourceChannelName>
              </RedChannel>
              <GreenChannel>
                <SourceChannelName>2</SourceChannelName>
              </GreenChannel>
              <BlueChannel>
                <SourceChannelName>3</SourceChannelName>
              </BlueChannel>
            </ChannelSelection>
          </RasterSymbolizer>
        </Rule>

      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>

</StyledLayerDescriptor>