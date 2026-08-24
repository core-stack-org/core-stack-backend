(function () {
    const dataElement = document.getElementById('ceew-climate-data');
    const sectionElement = document.getElementById('district-climate-section') ||
        document.getElementById('district-drought-context');

    if (!dataElement || typeof Chart === 'undefined') {
        return;
    }

    let climateCharts;
    try {
        climateCharts = JSON.parse(dataElement.textContent);
    } catch (error) {
        console.error('Could not parse district climate chart data:', error);
        return;
    }

    const districtName = sectionElement ? sectionElement.dataset.districtName : '';
    const years = Array.from({ length: 119 }, (_, index) => String(1981 + index));
    const observedColor = '#2468a2';
    const rcp45Color = '#c78100';
    const rcp85Color = '#d94841';
    const observedFill = 'rgba(36, 104, 162, 0.16)';
    const rcp45Fill = 'rgba(199, 129, 0, 0.11)';
    const rcp85Fill = 'rgba(217, 72, 65, 0.11)';

    const projectionDivider = {
        id: 'ceewProjectionDivider',
        beforeDatasetsDraw(chart, args, options) {
            const startIndex = chart.data.labels.indexOf(String(options.startYear || 2025));
            if (startIndex < 0 || !chart.scales.x) return;

            const xPosition = chart.scales.x.getPixelForValue(startIndex);
            const { ctx, chartArea } = chart;
            ctx.save();
            ctx.fillStyle = 'rgba(87, 94, 107, 0.06)';
            ctx.fillRect(xPosition, chartArea.top, chartArea.right - xPosition, chartArea.bottom - chartArea.top);
            ctx.restore();
        },
        afterDraw(chart, args, options) {
            const startIndex = chart.data.labels.indexOf(String(options.startYear || 2025));
            if (startIndex < 0 || !chart.scales.x) return;

            const xPosition = chart.scales.x.getPixelForValue(startIndex);
            const { ctx, chartArea } = chart;
            ctx.save();
            ctx.strokeStyle = '#77818f';
            ctx.lineWidth = 1;
            ctx.setLineDash([5, 4]);
            ctx.beginPath();
            ctx.moveTo(xPosition, chartArea.top);
            ctx.lineTo(xPosition, chartArea.bottom);
            ctx.stroke();
            ctx.restore();
        }
    };

    const temperatureLabels = {
        id: 'ceewTemperatureLabels',
        afterDatasetsDraw(chart, args, options) {
            if (!options.labels) return;
            const { ctx } = chart;
            ctx.save();
            ctx.fillStyle = '#4b515b';
            ctx.font = '12px Arial';
            options.labels.forEach(label => {
                const metadata = chart.getDatasetMeta(label.datasetIndex);
                const point = metadata.data[metadata.data.length - 1];
                if (point) {
                    ctx.font = label.bold ? 'bold 12px Arial' : '12px Arial';
                    ctx.fillText(label.text, point.x + 7, point.y + 4);
                }
            });
            ctx.restore();
        }
    };

    if (!Chart.registry.plugins.get(projectionDivider.id)) {
        Chart.register(projectionDivider);
    }
    if (!Chart.registry.plugins.get(temperatureLabels.id)) {
        Chart.register(temperatureLabels);
    }

    function valuesFor(series) {
        const safeSeries = series || {};
        return years.map(year => Object.prototype.hasOwnProperty.call(safeSeries, year) ? safeSeries[year] : null);
    }

    function xAxis() {
        const shownYears = new Set(['1981', '2000', '2024', '2050', '2075', '2099']);
        return {
            grid: { display: false },
            ticks: {
                autoSkip: false,
                maxRotation: 0,
                callback(value) {
                    const year = this.getLabelForValue(value);
                    return shownYears.has(year) ? year : '';
                }
            }
        };
    }

    function lineDatasets(metric) {
        return [
            {
                label: 'Observed IMD',
                data: valuesFor(metric.observed),
                borderColor: observedColor,
                backgroundColor: observedFill,
                borderWidth: 1.8,
                pointRadius: 0,
                tension: 0,
                spanGaps: false
            },
            {
                label: 'RCP4.5 modelled',
                data: valuesFor(metric.rcp45),
                borderColor: rcp45Color,
                backgroundColor: rcp45Fill,
                borderWidth: 1.8,
                borderDash: [7, 4],
                pointRadius: 0,
                tension: 0,
                spanGaps: false
            },
            {
                label: 'RCP8.5 modelled',
                data: valuesFor(metric.rcp85),
                borderColor: rcp85Color,
                backgroundColor: rcp85Fill,
                borderWidth: 1.8,
                pointRadius: 0,
                tension: 0,
                spanGaps: false
            }
        ];
    }

    function createLineChart(canvasId, metric, title, yTitle, showLegend) {
        const canvas = document.getElementById(canvasId);
        if (!canvas || !metric) return;

        new Chart(canvas, {
            type: 'line',
            data: { labels: years, datasets: lineDatasets(metric) },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    title: {
                        display: true,
                        text: title,
                        align: 'start',
                        color: '#24272d',
                        font: { size: 16, weight: 'bold' }
                    },
                    legend: {
                        display: showLegend,
                        position: 'top',
                        align: 'start'
                    },
                    ceewProjectionDivider: { startYear: 2025 }
                },
                scales: {
                    x: xAxis(),
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: yTitle },
                        grid: { color: '#d8dee7' }
                    }
                }
            }
        });
    }

    function createTemperatureChart(metric) {
        const canvas = document.getElementById('ceewTemperatureRangeChart');
        if (!canvas || !metric) return;

        const datasets = [
            {
                label: '',
                data: valuesFor(metric.maximum.observed),
                borderColor: observedColor,
                borderWidth: 1.1,
                pointRadius: 0,
                tension: 0
            },
            {
                label: '',
                data: valuesFor(metric.minimum.observed),
                borderColor: observedColor,
                backgroundColor: observedFill,
                borderWidth: 1.1,
                pointRadius: 0,
                tension: 0,
                fill: { target: 0, above: observedFill }
            },
            {
                label: 'Observed IMD',
                data: valuesFor(metric.average.observed),
                borderColor: observedColor,
                borderWidth: 2.6,
                pointRadius: 0,
                tension: 0
            },
            {
                label: '',
                data: valuesFor(metric.maximum.rcp45),
                borderColor: rcp45Color,
                borderWidth: 1.1,
                borderDash: [7, 4],
                pointRadius: 0,
                tension: 0
            },
            {
                label: '',
                data: valuesFor(metric.minimum.rcp45),
                borderColor: rcp45Color,
                backgroundColor: rcp45Fill,
                borderWidth: 1.1,
                borderDash: [7, 4],
                pointRadius: 0,
                tension: 0,
                fill: { target: 3, above: rcp45Fill }
            },
            {
                label: 'RCP4.5 modelled',
                data: valuesFor(metric.average.rcp45),
                borderColor: rcp45Color,
                borderWidth: 2.4,
                borderDash: [7, 4],
                pointRadius: 0,
                tension: 0
            },
            {
                label: '',
                data: valuesFor(metric.maximum.rcp85),
                borderColor: rcp85Color,
                borderWidth: 1.1,
                pointRadius: 0,
                tension: 0
            },
            {
                label: '',
                data: valuesFor(metric.minimum.rcp85),
                borderColor: rcp85Color,
                backgroundColor: rcp85Fill,
                borderWidth: 1.1,
                pointRadius: 0,
                tension: 0,
                fill: { target: 6, above: rcp85Fill }
            },
            {
                label: 'RCP8.5 modelled',
                data: valuesFor(metric.average.rcp85),
                borderColor: rcp85Color,
                borderWidth: 2.6,
                pointRadius: 0,
                tension: 0
            }
        ];

        new Chart(canvas, {
            type: 'line',
            data: { labels: years, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { mode: 'index', intersect: false },
                layout: { padding: { right: 145 } },
                plugins: {
                    title: {
                        display: true,
                        text: `${districtName} district: annual temperature range and average`,
                        align: 'start',
                        color: '#24272d',
                        font: { size: 18, weight: 'bold' }
                    },
                    legend: {
                        position: 'top',
                        align: 'start',
                        labels: { filter: item => Boolean(item.text) }
                    },
                    ceewProjectionDivider: { startYear: 2025 },
                    ceewTemperatureLabels: {
                        labels: [
                            { datasetIndex: 6, text: 'Maximum temperature' },
                            { datasetIndex: 8, text: 'Average temperature', bold: true },
                            { datasetIndex: 7, text: 'Minimum temperature' }
                        ]
                    }
                },
                scales: {
                    x: xAxis(),
                    y: {
                        min: metric.y_min,
                        max: metric.y_max,
                        title: { display: true, text: 'Temperature (°C)' },
                        grid: { color: '#d8dee7' }
                    }
                }
            }
        });
    }

    const drought = climateCharts.drought;
    if (drought) {
        createLineChart(
            'ceewDroughtSeverityChart',
            drought.drought,
            `${districtName} district: normalised 6-month drought severity`,
            'Normalised index',
            true
        );
        createLineChart(
            'ceewLongestDrySpellChart',
            drought.dry_spell,
            'Longest dry spell',
            'Days',
            false
        );
    }

    createTemperatureChart(climateCharts.temperature);

    const unusualTemperature = climateCharts.unusual_temperature;
    if (unusualTemperature) {
        createLineChart('ceewHotDaysChart', unusualTemperature.hot, 'Unusually hot days', 'Days/year', true);
        createLineChart('ceewWarmNightsChart', unusualTemperature.warm, 'Unusually warm nights', 'Nights/year', false);
        createLineChart('ceewColdDaysChart', unusualTemperature.cold_day, 'Unusually cold days', 'Days/year', false);
        createLineChart('ceewColdNightsChart', unusualTemperature.cold_night, 'Unusually cold nights', 'Nights/year', false);
    }

    if (climateCharts.rainfall) {
        createLineChart(
            'ceewAnnualRainfallChart',
            climateCharts.rainfall,
            `${districtName} district: annual rainfall`,
            'Millimetres',
            true
        );
    }

    if (climateCharts.unusual_rainfall) {
        createLineChart(
            'ceewUnusualRainfallChart',
            climateCharts.unusual_rainfall,
            `${districtName} district: unusually heavy rainy days`,
            'Days/year',
            true
        );
    }
})();
