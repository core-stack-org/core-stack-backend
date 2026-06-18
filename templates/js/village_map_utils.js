// ===== VILLAGE MAP UTILITY (Reusable Across All Sections) =====

// Shared color mapping
const VILLAGE_COLORS = {
    fill: {
        'red': 'rgba(239, 68, 68, 0.6)',
        'yellow': 'rgba(234, 179, 8, 0.6)',
        'green': 'rgba(34, 197, 94, 0.6)',
        'black': 'rgba(0, 0, 0, 0.3)',
        'gray': 'rgba(200, 200, 200, 0.3)'
    },
    stroke: {
        'red': '#000000',
        'yellow': '#000000',
        'green': '#000000',
        'black': '#000000',
        'gray': '#000000'
    },
    currentVillageStroke: '#000000',
    currentVillageStrokeWidth: 3.5,
    defaultStrokeWidth: 1.0
};

// Current village ID from context
const CURRENT_VILLAGE_ID = '{{ village_id }}';
const CURRENT_VILLAGE_NAME = '{{ village_name }}';

// ===== SHARED: Create location pin as vector feature =====
function createLocationPin(map, coordinate) {
    // SVG pin encoded for ol.style.Icon
    const svgPin = `
        <svg width="24" height="44" viewBox="0 0 24 44" xmlns="http://www.w3.org/2000/svg">
            <path d="M12 0C5.373 0 0 5.373 0 12c0 9 12 20 12 20S24 21 24 12C24 5.373 18.627 0 12 0z"
                  fill="#c17a4a" stroke="#3e2723" stroke-width="1.5"/>
            <circle cx="12" cy="12" r="4.5" fill="white"/>
        </svg>
    `;
    const svgBlob = new Blob([svgPin], { type: 'image/svg+xml' });
    const svgUrl = URL.createObjectURL(svgBlob);

    // Label feature
    const labelFeature = new ol.Feature({
        geometry: new ol.geom.Point(coordinate)
    });
    labelFeature.setStyle(new ol.style.Style({
        text: new ol.style.Text({
            text: 'You are here',
            offsetY: -38,
            font: '11px Georgia, serif',
            fill: new ol.style.Fill({ color: '#ffffff' }),
            backgroundFill: new ol.style.Fill({ color: 'rgba(62,39,35,0.85)' }),
            padding: [3, 6, 3, 6],
            backgroundStroke: new ol.style.Stroke({ color: 'transparent', width: 0 }),
            textBaseline: 'bottom'
        })
    }));

    // Pin icon feature
    const pinFeature = new ol.Feature({
        geometry: new ol.geom.Point(coordinate)
    });
    pinFeature.setStyle(new ol.style.Style({
        image: new ol.style.Icon({
            src: svgUrl,
            anchor: [0.5, 1.0],       // anchor at bottom-center of SVG
            anchorXUnits: 'fraction',
            anchorYUnits: 'fraction',
            scale: 1.2
        })
    }));

    const pinSource = new ol.source.Vector({
        features: [labelFeature, pinFeature]
    });

    const pinLayer = new ol.layer.Vector({
        source: pinSource,
        zIndex: 20      // above village layer
    });

    map.addLayer(pinLayer);
    return pinLayer;
}

// ===== SHARED: Create zoom toggle button overlay =====
function createZoomToggle(map, currentFeature, villageName) {
    if (!currentFeature) return;

    const tehsilExtent = null; // will be set after villages load — handled by caller
    let isZoomedIn = false;

    const btnEl = document.createElement('button');
    btnEl.style.cssText = `
        position: absolute;
        top: 10px;
        right: 10px;
        z-index: 100;
        background: white;
        border: 1px solid #ccc;
        border-radius: 4px;
        padding: 5px 10px;
        font-size: 11px;
        font-family: 'Georgia', serif;
        color: var(--primary-dark, #3e2723);
        cursor: pointer;
        box-shadow: 0 2px 6px rgba(0,0,0,0.2);
        white-space: nowrap;
        display: flex;
        align-items: center;
        gap: 5px;
    `;
    btnEl.innerHTML = `
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
            <line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/>
        </svg>
        Zoom to ${villageName}
    `;

    // Append to map container (which must have position:relative)
    map.getTargetElement().style.position = 'relative';
    map.getTargetElement().appendChild(btnEl);

    btnEl.addEventListener('click', function () {
        if (!isZoomedIn) {
            // Zoom in to current village
            const villageExtent = currentFeature.getGeometry().getExtent();
            map.getView().fit(villageExtent, {
                padding: [40, 40, 40, 40],
                maxZoom: 14,
                duration: 600
            });
            isZoomedIn = true;
            btnEl.innerHTML = `
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                    <line x1="8" y1="11" x2="14" y2="11"/>
                </svg>
                Show full tehsil
            `;
        } else {
            // Zoom out to full tehsil
            map.getView().fit(map._tehsilExtent, {
                padding: [30, 30, 30, 30],
                duration: 600
            });
            isZoomedIn = false;
            btnEl.innerHTML = `
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                    <line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/>
                </svg>
                Zoom to ${villageName}
            `;
        }
    });

    return btnEl;
}

// ===== SHARED: Get centroid of a geometry =====
function getGeometryCentroid(geometry) {
    const extent = geometry.getExtent();
    return [
        (extent[0] + extent[2]) / 2,
        (extent[1] + extent[3]) / 2
    ];
}

/**
 * Create a styled village map section
 */
function createVillageMap(config) {
    const container = document.getElementById(config.targetId);
    if (!container) return null;

    let mapData = {};
    try {
        mapData = JSON.parse(config.mapDataJson);
    } catch (e) {
        console.warn(config.label + ' Map: Could not parse map data:', e);
    }

    function styleFeature(feature) {
        const villageId = feature.get('vill_ID');
        const isCurrentVillage = (String(villageId) === String(CURRENT_VILLAGE_ID));

        let colorName = 'gray';
        if (mapData[villageId] && mapData[villageId][config.colorKey]) {
            colorName = mapData[villageId][config.colorKey];
        }

        const fillColor = VILLAGE_COLORS.fill[colorName] || VILLAGE_COLORS.fill['gray'];
        const strokeWidth = isCurrentVillage
            ? VILLAGE_COLORS.currentVillageStrokeWidth
            : VILLAGE_COLORS.defaultStrokeWidth;

        return new ol.style.Style({
            fill: new ol.style.Fill({ color: fillColor }),
            stroke: new ol.style.Stroke({
                color: '#000000',
                width: strokeWidth
            })
        });
    }

    const map = new ol.Map({
        target: config.targetId,
        layers: [
            new ol.layer.Tile({
                source: new ol.source.XYZ({
                    url: 'https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',
                    attributions: 'Google Satellite Hybrid Contributors'
                }),
                zIndex: 1
            })
        ],
        view: new ol.View({
            center: [0, 0],
            zoom: 12,
            projection: 'EPSG:4326'
        })
    });

    (async () => {
        try {
            const geoserverData = await waitForGlobalVillages();

            if (geoserverData && geoserverData.features) {
                const vectorSource = new ol.source.Vector();
                const features = new ol.format.GeoJSON().readFeatures(geoserverData, {
                    featureProjection: 'EPSG:4326'
                });

                vectorSource.addFeatures(features);

                const villageLayer = new ol.layer.Vector({
                    source: vectorSource,
                    style: styleFeature,
                    zIndex: 10
                });

                map.addLayer(villageLayer);

                // Fit to full tehsil on load
                const tehsilExtent = vectorSource.getExtent();
                map._tehsilExtent = tehsilExtent;
                map.getView().fit(tehsilExtent, {
                    padding: [30, 30, 30, 30],
                    duration: 500
                });

                console.log(config.label + ' Map: Added', features.length, 'village features');

                // Location pin + zoom toggle
                const currentFeature = features.find(
                    f => String(f.get('vill_ID')) === String(CURRENT_VILLAGE_ID)
                );

                if (currentFeature) {
                    const centroid = getGeometryCentroid(currentFeature.getGeometry());
                    createLocationPin(map, centroid);
                    createZoomToggle(map, currentFeature, CURRENT_VILLAGE_NAME);
                }

                map._villageLayer = villageLayer;
                map._vectorSource = vectorSource;

                console.log(config.label + ' Map: Fitted to tehsil');
            }
        } catch (error) {
            console.error(config.label + ' Map: Error loading villages:', error);
        }
    })();

    if (config.mwsesJson && config.villageJson) {
        addMWSesLayer(map, config.mwsesJson, config.villageJson, config.label);
    }

    return map;
}

/**
 * Create a tabbed village map
 */
function createTabbedVillageMap(config) {
    const container = document.getElementById(config.targetId);
    if (!container) return null;

    let mapData = {};
    try {
        mapData = JSON.parse(config.mapDataJson);
    } catch (e) {
        console.warn(config.label + ' Map: Could not parse map data:', e);
    }

    let currentTab = config.defaultTab;

    function styleFeature(feature) {
        const villageId = feature.get('vill_ID');
        const isCurrentVillage = (String(villageId) === String(CURRENT_VILLAGE_ID));
        const tabConfig = config.tabs[currentTab];

        let colorName = 'gray';
        if (mapData[villageId] && mapData[villageId][tabConfig.colorKey]) {
            colorName = mapData[villageId][tabConfig.colorKey];
        }

        const fillColor = VILLAGE_COLORS.fill[colorName] || VILLAGE_COLORS.fill['gray'];
        const strokeWidth = isCurrentVillage
            ? VILLAGE_COLORS.currentVillageStrokeWidth
            : VILLAGE_COLORS.defaultStrokeWidth;

        return new ol.style.Style({
            fill: new ol.style.Fill({ color: fillColor }),
            stroke: new ol.style.Stroke({
                color: '#000000',
                width: strokeWidth
            })
        });
    }

    const map = new ol.Map({
        target: config.targetId,
        layers: [
            new ol.layer.Tile({
                source: new ol.source.XYZ({
                    url: 'https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',
                    maxZoom: 30,
                    transition: 500,
                }),
                preload: 4,
            })
        ],
        view: new ol.View({
            center: [78.9, 23.6],
            zoom: 10,
            projection: 'EPSG:4326',
            constrainResolution: true,
            smoothExtentConstraint: true,
            smoothResolutionConstraint: true,
        }),
        interactions: new ol.Collection([
            new ol.interaction.DragPan(),
            new ol.interaction.KeyboardZoom(),
        ])
    });

    let villageLayer = null;

    (async () => {
        try {
            const geoserverData = await waitForGlobalVillages();

            if (geoserverData && geoserverData.features) {
                const vectorSource = new ol.source.Vector();
                const features = new ol.format.GeoJSON().readFeatures(geoserverData, {
                    featureProjection: 'EPSG:4326'
                });

                vectorSource.addFeatures(features);

                villageLayer = new ol.layer.Vector({
                    source: vectorSource,
                    style: styleFeature,
                    zIndex: 10
                });

                map.addLayer(villageLayer);

                // Fit to full tehsil on load
                const tehsilExtent = vectorSource.getExtent();
                map._tehsilExtent = tehsilExtent;
                map.getView().fit(tehsilExtent, {
                    padding: [30, 30, 30, 30],
                    duration: 500
                });

                console.log(config.label + ' Map: Added', features.length, 'village features');

                // Location pin + zoom toggle
                const currentFeature = features.find(
                    f => String(f.get('vill_ID')) === String(CURRENT_VILLAGE_ID)
                );

                if (currentFeature) {
                    const centroid = getGeometryCentroid(currentFeature.getGeometry());
                    createLocationPin(map, centroid);
                    createZoomToggle(map, currentFeature, CURRENT_VILLAGE_NAME);
                }
            }
        } catch (error) {
            console.error(config.label + ' Map: Error loading villages:', error);
        }
    })();

    if (config.mwsesJson && config.villageJson) {
        addMWSesLayer(map, config.mwsesJson, config.villageJson, config.label);
    }

    function switchTab(tabKey) {
        currentTab = tabKey;
        const tabConfig = config.tabs[tabKey];

        Object.entries(config.buttonIds).forEach(([key, btnId]) => {
            const btn = document.getElementById(btnId);
            if (btn) {
                btn.style.backgroundColor = (key === tabKey)
                    ? 'var(--primary-accent)'
                    : '#bbb';
            }
        });

        if (config.infoIds) {
            const titleEl = document.getElementById(config.infoIds.title);
            const descEl  = document.getElementById(config.infoIds.desc);
            const legendEl = document.getElementById(config.infoIds.legend);
            if (titleEl) titleEl.textContent = tabConfig.name;
            if (descEl)  descEl.textContent  = tabConfig.desc;
            if (legendEl) legendEl.innerHTML  = tabConfig.legend;
        }

        if (villageLayer) {
            villageLayer.setStyle(styleFeature);
            console.log(config.label + ' Map: Switched to', tabKey);
        }
    }

    return { map, switchTab };
}

/**
 * Add MWSes layer to a map
 */
function addMWSesLayer(map, mwsesJson, villageJson, label) {
    try {
        const mwsesPolygon = JSON.parse(mwsesJson);

        if (!mwsesPolygon || !mwsesPolygon.features || mwsesPolygon.features.length === 0) {
            console.warn(label + ' Map: No MWSes data available');
            return;
        }

        let filteredMWSes = mwsesPolygon.features;

        try {
            const villageData = JSON.parse(villageJson);
            if (villageData && villageData.features && villageData.features.length > 0) {
                const villageBoundary = villageData.features[0].geometry;
                filteredMWSes = mwsesPolygon.features.filter(mwsFeature => {
                    try {
                        const villageBbox = getBoundingBox(villageBoundary);
                        const mwsBbox = getBoundingBox(mwsFeature.geometry); 
                        return boxesIntersect(villageBbox, mwsBbox);
                    } catch (e) {
                        return true;
                    }
                }); 
            }
        } catch (filterError) {
            console.warn(label + ' Map: Could not filter MWSes:', filterError);
        }

        if (filteredMWSes.length > 0) {
            const mwsesFC = { type: 'FeatureCollection', features: filteredMWSes };
            const mwsesSource = new ol.source.Vector({
                features: new ol.format.GeoJSON().readFeatures(mwsesFC, {
                    featureProjection: 'EPSG:4326'
                })
            });

            const mwsesLayer = new ol.layer.Vector({
                source: mwsesSource,
                style: new ol.style.Style({
                    fill: new ol.style.Fill({ color: 'rgba(59, 130, 246, 0.08)' }),
                    stroke: new ol.style.Stroke({
                        color: '#2563eb',
                        width: 2,
                        lineDash: [5, 5]
                    })
                }),
                zIndex: 5
            });

            map.addLayer(mwsesLayer);
            console.log('✓ ' + label + ' Map: MWSes layer added (' + filteredMWSes.length + ' features)');
        }
    } catch (error) {
        console.warn(label + ' Map: MWSes error:', error);
    }
}