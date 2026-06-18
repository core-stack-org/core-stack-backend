// ===== GLOBAL VILLAGE BOUNDARIES FETCHER (Fetch Once, Reuse Everywhere) =====

// Global storage for villages
window.globalVillageData = {
    villages: {},           // Features by village ID
    geoserverData: null,    // Full GeoServer response
    isLoading: false,
    isLoaded: false,
    error: null
};

/**
 * Fetch all villages from GeoServer once at page load
 * This data is reused by all map sections
 */
async function initializeGlobalVillages() {
    if (window.globalVillageData.isLoading || window.globalVillageData.isLoaded) {
        return;
    }
    
    window.globalVillageData.isLoading = true;
    
    try {
        const geoserverUrl = `https://geoserver.core-stack.org:8443/geoserver/panchayat_boundaries/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=panchayat_boundaries:{{ district|lower }}_{{ block|lower }}&outputFormat=application/json`;
        
        console.log('🌐 Fetching villages from GeoServer (Global)...');
        
        const response = await fetch(geoserverUrl);
        if (!response.ok) {
            throw new Error(`GeoServer error: ${response.status}`);
        }
        
        const geoserverData = await response.json();
        console.log('✓ GeoServer response received:', geoserverData.features?.length, 'villages');
        
        // Store GeoServer response
        window.globalVillageData.geoserverData = geoserverData;
        
        // Parse features and store by village ID
        const features = new ol.format.GeoJSON().readFeatures(geoserverData, {
            featureProjection: 'EPSG:4326'
        });
        
        features.forEach(feature => {
            const villageId = feature.get('vill_ID');
            window.globalVillageData.villages[villageId] = feature;
        });
        
        console.log('✓ Stored', Object.keys(window.globalVillageData.villages).length, 'village features globally');
        
        window.globalVillageData.isLoaded = true;
        window.globalVillageData.isLoading = false;
        
        return geoserverData;
        
    } catch (error) {
        console.error('✗ Error fetching villages:', error);
        window.globalVillageData.error = error;
        window.globalVillageData.isLoading = false;
        return null;
    }
}

/**
 * Get parsed features for a specific map section
 * Parameters:
 *   - mapDataString: Context data (e.g., '{{ education_map|escapejs }}')
 *   - Returns: Array of OL features ready to add to map
 */
function getVillageFeatures(mapDataString) {
    if (!window.globalVillageData.geoserverData) {
        console.warn('Global villages not loaded yet');
        return [];
    }
    
    try {
        const mapData = JSON.parse(mapDataString);
        const features = new ol.format.GeoJSON().readFeatures(window.globalVillageData.geoserverData, {
            featureProjection: 'EPSG:4326'
        });
        
        return features;
    } catch (error) {
        console.warn('Error parsing map data:', error);
        return [];
    }
}

/**
 * Wait for global villages to load
 */
async function waitForGlobalVillages(maxWait = 10000) {
    const startTime = Date.now();
    
    while (!window.globalVillageData.isLoaded && !window.globalVillageData.error) {
        if (Date.now() - startTime > maxWait) {
            throw new Error('Timeout waiting for global villages');
        }
        await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    if (window.globalVillageData.error) {
        throw window.globalVillageData.error;
    }
    
    return window.globalVillageData.geoserverData;
}

// Initialize global villages when page loads
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeGlobalVillages);
} else {
    initializeGlobalVillages();
}