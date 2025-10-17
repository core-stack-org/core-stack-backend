<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis hasScaleBasedVisibilityFlag="0" minScale="1e+08" version="3.22.4-Białowieża" maxScale="0" styleCategories="AllStyleCategories">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
    <Private>0</Private>
  </flags>
  <temporal fetchMode="0" mode="0" enabled="0">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <customproperties>
    <Option type="Map">
      <Option type="bool" name="WMSBackgroundLayer" value="false"/>
      <Option type="bool" name="WMSPublishDataSourceUrl" value="false"/>
      <Option type="int" name="embeddedWidgets/count" value="0"/>
      <Option type="QString" name="identify/format" value="Value"/>
    </Option>
  </customproperties>
  <pipe-data-defined-properties>
    <Option type="Map">
      <Option type="QString" name="name" value=""/>
      <Option name="properties"/>
      <Option type="QString" name="type" value="collection"/>
    </Option>
  </pipe-data-defined-properties>
  <pipe>
    <provider>
      <resampling maxOversampling="2" zoomedInResamplingMethod="nearestNeighbour" zoomedOutResamplingMethod="nearestNeighbour" enabled="false"/>
    </provider>
    <rasterrenderer type="singlebandpseudocolor" classificationMin="-2" classificationMax="5" nodataColor="" band="1" opacity="1" alphaBand="-1">
      <rasterTransparency/>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <rastershader>
        <colorrampshader colorRampType="INTERPOLATED" clip="0" classificationMode="3" maximumValue="5" minimumValue="-2" labelPrecision="4">
          <colorramp type="gradient" name="[source]">
            <Option type="Map">
              <Option type="QString" name="color1" value="255,0,0,255"/>
              <Option type="QString" name="color2" value="0,0,0,255"/>
              <Option type="QString" name="discrete" value="0"/>
              <Option type="QString" name="rampType" value="gradient"/>
              <Option type="QString" name="stops" value="0.142857;255,165,0,255:0.285714;255,255,255,255:0.428571;138,255,138,255:0.571429;0,117,0,255:0.714286;222,230,76,255:0.857143;222,230,76,255"/>
            </Option>
            <prop v="255,0,0,255" k="color1"/>
            <prop v="0,0,0,255" k="color2"/>
            <prop v="0" k="discrete"/>
            <prop v="gradient" k="rampType"/>
            <prop v="0.142857;255,165,0,255:0.285714;255,255,255,255:0.428571;138,255,138,255:0.571429;0,117,0,255:0.714286;222,230,76,255:0.857143;222,230,76,255" k="stops"/>
          </colorramp>
          <item alpha="255" value="-2" color="#ff0000" label="Deforestation"/>
          <item alpha="255" value="-1" color="#ffa500" label="Degradation"/>
          <item alpha="255" value="0" color="#ffffff" label="No Change"/>
          <item alpha="255" value="1" color="#8aff8a" label="Improvement"/>
          <item alpha="255" value="2" color="#007500" label="Afforestation"/>
          <item alpha="255" value="3" color="#dee64c" label="Partially Degraded"/>
          <item alpha="255" value="4" color="#dee64c" label="Partially Degraded"/>
          <item alpha="255" value="5" color="#000000" label="Missing Data"/>
          <rampLegendSettings maximumLabel="" minimumLabel="" orientation="2" suffix="" prefix="" direction="0" useContinuousLegend="1">
            <numericFormat id="basic">
              <Option type="Map">
                <Option type="QChar" name="decimal_separator" value=""/>
                <Option type="int" name="decimals" value="6"/>
                <Option type="int" name="rounding_type" value="0"/>
                <Option type="bool" name="show_plus" value="false"/>
                <Option type="bool" name="show_thousand_separator" value="true"/>
                <Option type="bool" name="show_trailing_zeros" value="false"/>
                <Option type="QChar" name="thousand_separator" value=""/>
              </Option>
            </numericFormat>
          </rampLegendSettings>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast gamma="1" brightness="0" contrast="0"/>
    <huesaturation colorizeStrength="100" saturation="0" grayscaleMode="0" colorizeOn="0" invertColors="0" colorizeGreen="128" colorizeBlue="128" colorizeRed="255"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
