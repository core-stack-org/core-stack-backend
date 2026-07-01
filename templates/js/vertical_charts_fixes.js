// ===== VERTICAL BAR CHART FIXES =====
// Replace the chart initialization code with these updated versions

// 1. BASIC INFRASTRUCTURE CHART - VERTICAL
function initBasicInfraChart(basicInfraScores, basicInfraLabels) {
    const infraCtx = document.getElementById('basicInfraChart');
    if (!infraCtx) return;
    
    const ctx = infraCtx.getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: basicInfraLabels,
            datasets: [{
                label: 'Infrastructure Score',
                data: basicInfraScores,
                backgroundColor: basicInfraScores.map(value => {
                    if (value <= 0.33) return '#ef4444';
                    if (value <= 0.66) return '#eab308';
                    return '#22c55e';
                }),
                borderColor: basicInfraScores.map(value => {
                    if (value <= 0.33) return '#dc2626';
                    if (value <= 0.66) return '#ca8a04';
                    return '#16a34a';
                }),
                borderWidth: 1.5,
                borderRadius: 4,
                minBarLength: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            layout: {padding: {top: 10, right: 20, bottom: 10, left: 10}},
            plugins: {
                legend: {display: false},
                tooltip: {
                    backgroundColor: 'rgba(62, 39, 35, 0.9)',
                    titleFont: {family: "'Georgia', 'Garamond', serif", size: 12, weight: 'bold'},
                    bodyFont: {family: "'Georgia', 'Garamond', serif", size: 12},
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const value = context.parsed.y.toFixed(2);
                            let performance = 'Low';
                            if (value > 0.33 && value <= 0.66) performance = 'Medium';
                            if (value > 0.66) performance = 'High';
                            return 'Score: ' + value + ' (' + performance + ')';
                        }
                    }
                }
            },
            scales: {
                y: {
                    min: 0,
                    max: 1,
                    ticks: {
                        stepSize: 0.25,
                        font: {family: "'Georgia', 'Garamond', serif", size: 11},
                        color: '#6b7280',
                        callback: function(value) {
                            if (value === 0) return 'Low';
                            if (value === 0.5) return 'Medium';
                            if (value === 1) return 'High';
                            return '';
                        }
                    },
                    grid: {color: 'rgba(0, 0, 0, 0.1)', drawBorder: true, borderColor: '#000000'}
                },
                x: {
                    ticks: {
                        font: {family: "'Georgia', 'Garamond', serif", size: 11, weight: '500'},
                        color: '#3e2723'
                    },
                    grid: {display: false, drawBorder: false}
                }
            }
        }
    });
}

// 2. COMMUNITY CHART - VERTICAL
function initCommunityChart(communityScores, communityLabels) {
    const ctx = document.getElementById('communityChart');
    if (!ctx) return;
    
    const chartCtx = ctx.getContext('2d');
    new Chart(chartCtx, {
        type: 'bar',
        data: {
            labels: communityLabels,
            datasets: [{
                label: 'Community Score',
                data: communityScores,
                backgroundColor: communityScores.map(value => {
                    if (value <= 0.33) return '#ef4444';
                    if (value <= 0.66) return '#eab308';
                    return '#22c55e';
                }),
                borderColor: communityScores.map(value => {
                    if (value <= 0.33) return '#dc2626';
                    if (value <= 0.66) return '#ca8a04';
                    return '#16a34a';
                }),
                borderWidth: 1.5,
                borderRadius: 4,
                minBarLength: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {display: false},
                tooltip: {
                    backgroundColor: 'rgba(62, 39, 35, 0.9)',
                    titleFont: {family: "'Georgia', 'Garamond', serif", size: 12, weight: 'bold'},
                    bodyFont: {family: "'Georgia', 'Garamond', serif", size: 12},
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const value = context.parsed.y.toFixed(2);
                            let performance = 'Low';
                            if (value > 0.33 && value <= 0.66) performance = 'Medium';
                            if (value > 0.66) performance = 'High';
                            return 'Score: ' + value + ' (' + performance + ')';
                        }
                    }
                }
            },
            scales: {
                y: {
                    min: 0,
                    max: 1,
                    ticks: {
                        stepSize: 0.25,
                        font: {family: "'Georgia', 'Garamond', serif", size: 11},
                        color: '#6b7280',
                        callback: function(value) {
                            if (value === 0) return 'Low';
                            if (value === 0.5) return 'Medium';
                            if (value === 1) return 'High';
                            return '';
                        }
                    },
                    grid: {color: 'rgba(0, 0, 0, 0.1)', drawBorder: true, borderColor: '#000000'}
                },
                x: {
                    ticks: {
                        font: {family: "'Georgia', 'Garamond', serif", size: 11, weight: '500'},
                        color: '#3e2723'
                    },
                    grid: {display: false, drawBorder: false}
                }
            }
        }
    });
}

// 3. LIVELIHOOD CHART - VERTICAL
function initLivelihoodChart(livelihoodScores, livelihoodLabels) {
    const ctx = document.getElementById('livelihoodChart');
    if (!ctx) return;
    
    const chartCtx = ctx.getContext('2d');
    new Chart(chartCtx, {
        type: 'bar',
        data: {
            labels: livelihoodLabels,
            datasets: [{
                label: 'Livelihood Score',
                data: livelihoodScores,
                backgroundColor: livelihoodScores.map((value, index) => {
                    const isFarm = index === 0;
                    if (isFarm) {
                        if (value <= 0.33) return '#ef4444';
                        if (value <= 0.66) return '#eab308';
                        return '#22c55e';
                    } else {
                        if (value < 0.5) return '#ef4444';
                        return '#22c55e';
                    }
                }),
                borderColor: livelihoodScores.map((value, index) => {
                    const isFarm = index === 0;
                    if (isFarm) {
                        if (value <= 0.33) return '#dc2626';
                        if (value <= 0.66) return '#ca8a04';
                        return '#16a34a';
                    } else {
                        if (value < 0.5) return '#dc2626';
                        return '#16a34a';
                    }
                }),
                borderWidth: 1.5,
                borderRadius: 4,
                minBarLength: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {display: false},
                tooltip: {
                    backgroundColor: 'rgba(62, 39, 35, 0.9)',
                    titleFont: {family: "'Georgia', 'Garamond', serif", size: 12, weight: 'bold'},
                    bodyFont: {family: "'Georgia', 'Garamond', serif", size: 12},
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const value = context.parsed.y.toFixed(2);
                            const isFarm = context.dataIndex === 0;
                            let performance = 'Low';
                            if (isFarm) {
                                if (value > 0.33 && value <= 0.66) performance = 'Medium';
                                if (value > 0.66) performance = 'High';
                            } else {
                                if (value >= 0.5) performance = 'High';
                            }
                            return 'Score: ' + value + ' (' + performance + ')';
                        }
                    }
                }
            },
            scales: {
                y: {
                    min: 0,
                    max: 1,
                    ticks: {
                        stepSize: 0.25,
                        font: {family: "'Georgia', 'Garamond', serif", size: 11},
                        color: '#6b7280',
                        callback: function(value) {
                            if (value === 0) return 'Low';
                            if (value === 0.5) return 'High';
                            return '';
                        }
                    },
                    grid: {color: 'rgba(0, 0, 0, 0.1)', drawBorder: true, borderColor: '#000000'}
                },
                x: {
                    ticks: {
                        font: {family: "'Georgia', 'Garamond', serif", size: 10, weight: '500'},
                        color: '#3e2723'
                    },
                    grid: {display: false, drawBorder: false}
                }
            }
        }
    });
}

// 4. LIVESTOCK CHART - VERTICAL
function initLivestockChart(livestockScores, livestockLabels) {
    const ctx = document.getElementById('livestockChart');
    if (!ctx) return;
    
    const chartCtx = ctx.getContext('2d');
    new Chart(chartCtx, {
        type: 'bar',
        data: {
            labels: livestockLabels,
            datasets: [{
                label: 'Livestock Score',
                data: livestockScores,
                backgroundColor: livestockScores.map((value, index) => {
                    const isService = index === 0;
                    if (isService) {
                        if (value <= 0.33) return '#ef4444';
                        if (value <= 0.66) return '#eab308';
                        return '#22c55e';
                    } else {
                        if (value < 0.5) return '#ef4444';
                        return '#22c55e';
                    }
                }),
                borderColor: livestockScores.map((value, index) => {
                    const isService = index === 0;
                    if (isService) {
                        if (value <= 0.33) return '#dc2626';
                        if (value <= 0.66) return '#ca8a04';
                        return '#16a34a';
                    } else {
                        if (value < 0.5) return '#dc2626';
                        return '#16a34a';
                    }
                }),
                borderWidth: 1.5,
                borderRadius: 4,
                minBarLength: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {display: false},
                tooltip: {
                    backgroundColor: 'rgba(62, 39, 35, 0.9)',
                    titleFont: {family: "'Georgia', 'Garamond', serif", size: 12, weight: 'bold'},
                    bodyFont: {family: "'Georgia', 'Garamond', serif", size: 12},
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const value = context.parsed.y.toFixed(2);
                            const isService = context.dataIndex === 0;
                            let performance = 'Low';
                            if (isService) {
                                if (value > 0.33 && value <= 0.66) performance = 'Medium';
                                if (value > 0.66) performance = 'High';
                            } else {
                                if (value >= 0.5) performance = 'High';
                            }
                            return 'Score: ' + value + ' (' + performance + ')';
                        }
                    }
                }
            },
            scales: {
                y: {
                    min: 0,
                    max: 1,
                    ticks: {
                        stepSize: 0.25,
                        font: {family: "'Georgia', 'Garamond', serif", size: 11},
                        color: '#6b7280',
                        callback: function(value) {
                            if (value === 0) return 'Low';
                            if (value === 0.5) return 'High';
                            return '';
                        }
                    },
                    grid: {color: 'rgba(0, 0, 0, 0.1)', drawBorder: true, borderColor: '#000000'}
                },
                x: {
                    ticks: {
                        font: {family: "'Georgia', 'Garamond', serif", size: 11, weight: '500'},
                        color: '#3e2723'
                    },
                    grid: {display: false, drawBorder: false}
                }
            }
        }
    });
}

// 5. IRRIGATION CHART - VERTICAL
// 5a. LAND CULTIVATION CHART - VERTICAL
function initLandCultivationChart(scores, labels) {
    const ctx = document.getElementById('landCultivationChart');
    if (!ctx) return;

    const chartCtx = ctx.getContext('2d');
    new Chart(chartCtx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Score',
                data: scores,
                backgroundColor: scores.map((value) => {
                    if (value <= 0.33) return '#ef4444';
                    if (value <= 0.66) return '#eab308';
                    return '#22c55e';
                }),
                borderColor: scores.map((value) => {
                    if (value <= 0.33) return '#dc2626';
                    if (value <= 0.66) return '#ca8a04';
                    return '#16a34a';
                }),
                borderWidth: 1.5,
                borderRadius: 4,
                minBarLength: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {display: false},
                tooltip: {
                    backgroundColor: 'rgba(62, 39, 35, 0.9)',
                    titleFont: {family: "'Georgia', 'Garamond', serif", size: 12, weight: 'bold'},
                    bodyFont: {family: "'Georgia', 'Garamond', serif", size: 12},
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const value = context.parsed.y.toFixed(2);
                            let performance = 'Low';
                            if (value > 0.66) performance = 'High';
                            else if (value > 0.33) performance = 'Medium';
                            return 'Score: ' + value + ' (' + performance + ')';
                        }
                    }
                }
            },
            scales: {
                y: {
                    min: 0,
                    max: 1,
                    ticks: {
                        stepSize: 0.33,
                        font: {family: "'Georgia', 'Garamond', serif", size: 11},
                        color: '#6b7280',
                        callback: function(value) {
                            if (value === 0) return 'Low';
                            if (value === 0.33) return 'Medium';
                            if (value === 0.66) return 'High';
                            return '';
                        }
                    },
                    grid: {color: 'rgba(0, 0, 0, 0.1)', drawBorder: true, borderColor: '#000000'}
                },
                x: {
                    ticks: {
                        font: {family: "'Georgia', 'Garamond', serif", size: 11, weight: '500'},
                        color: '#3e2723'
                    },
                    grid: {display: false, drawBorder: false}
                }
            }
        }
    });
}

function initIrrigationChart(irrigationScores, irrigationLabels) {
    const ctx = document.getElementById('irrigationChart');
    if (!ctx) return;

    const chartCtx = ctx.getContext('2d');
    new Chart(chartCtx, {
        type: 'bar',
        data: {
            labels: irrigationLabels,
            datasets: [{
                label: 'Irrigation Score',
                data: irrigationScores,
                backgroundColor: irrigationScores.map((value) => {
                    if (value <= 0.33) return '#ef4444';
                    if (value <= 0.66) return '#eab308';
                    return '#22c55e';
                }),
                borderColor: irrigationScores.map((value) => {
                    if (value <= 0.33) return '#dc2626';
                    if (value <= 0.66) return '#ca8a04';
                    return '#16a34a';
                }),
                borderWidth: 1.5,
                borderRadius: 4,
                minBarLength: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {display: false},
                tooltip: {
                    backgroundColor: 'rgba(62, 39, 35, 0.9)',
                    titleFont: {family: "'Georgia', 'Garamond', serif", size: 12, weight: 'bold'},
                    bodyFont: {family: "'Georgia', 'Garamond', serif", size: 12},
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const value = context.parsed.y.toFixed(2);
                            let performance = 'Low';
                            if (value > 0.66) performance = 'High';
                            else if (value > 0.33) performance = 'Medium';
                            return 'Score: ' + value + ' (' + performance + ')';
                        }
                    }
                }
            },
            scales: {
                y: {
                    min: 0,
                    max: 1,
                    ticks: {
                        stepSize: 0.33,
                        font: {family: "'Georgia', 'Garamond', serif", size: 11},
                        color: '#6b7280',
                        callback: function(value) {
                            if (value === 0) return 'Low';
                            if (value === 0.33) return 'Medium';
                            if (value === 0.66) return 'High';
                            return '';
                        }
                    },
                    grid: {color: 'rgba(0, 0, 0, 0.1)', drawBorder: true, borderColor: '#000000'}
                },
                x: {
                    ticks: {
                        font: {family: "'Georgia', 'Garamond', serif", size: 11, weight: '500'},
                        color: '#3e2723'
                    },
                    grid: {display: false, drawBorder: false}
                }
            }
        }
    });
}

// 6. AGRICULTURAL SUPPORT CHART - VERTICAL
function initAgriSupportChart(agriScores, agriSupportLabels) {
    const ctx = document.getElementById('agriSupportChart');
    if (!ctx) return;
    
    const chartCtx = ctx.getContext('2d');
    new Chart(chartCtx, {
        type: 'bar',
        data: {
            labels: agriSupportLabels,
            datasets: [{
                label: 'Agricultural Support Score',
                data: agriScores,
                backgroundColor: agriScores.map((value, index) => {
                    const isSupport = index === 0;
                    if (isSupport) {
                        if (value <= 0.33) return '#ef4444';
                        if (value <= 0.66) return '#eab308';
                        return '#22c55e';
                    } else {
                        if (value < 0.5) return '#ef4444';
                        return '#22c55e';
                    }
                }),
                borderColor: agriScores.map((value, index) => {
                    const isSupport = index === 0;
                    if (isSupport) {
                        if (value <= 0.33) return '#dc2626';
                        if (value <= 0.66) return '#ca8a04';
                        return '#16a34a';
                    } else {
                        if (value < 0.5) return '#dc2626';
                        return '#16a34a';
                    }
                }),
                borderWidth: 1.5,
                borderRadius: 4,
                minBarLength: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {display: false},
                tooltip: {
                    backgroundColor: 'rgba(62, 39, 35, 0.9)',
                    titleFont: {family: "'Georgia', 'Garamond', serif", size: 12, weight: 'bold'},
                    bodyFont: {family: "'Georgia', 'Garamond', serif", size: 12},
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const value = context.parsed.y.toFixed(2);
                            const isSupport = context.dataIndex === 0;
                            let performance = 'Low';
                            if (isSupport) {
                                if (value > 0.33 && value <= 0.66) performance = 'Medium';
                                if (value > 0.66) performance = 'High';
                            } else {
                                if (value >= 0.5) performance = 'High';
                            }
                            return 'Score: ' + value + ' (' + performance + ')';
                        }
                    }
                }
            },
            scales: {
                y: {
                    min: 0,
                    max: 1,
                    ticks: {
                        stepSize: 0.25,
                        font: {family: "'Georgia', 'Garamond', serif", size: 11},
                        color: '#6b7280',
                        callback: function(value) {
                            if (value === 0) return 'Low';
                            if (value === 0.5) return 'High';
                            return '';
                        }
                    },
                    grid: {color: 'rgba(0, 0, 0, 0.1)', drawBorder: true, borderColor: '#000000'}
                },
                x: {
                    ticks: {
                        font: {family: "'Georgia', 'Garamond', serif", size: 11, weight: '500'},
                        color: '#3e2723'
                    },
                    grid: {display: false, drawBorder: false}
                }
            }
        }
    });
}