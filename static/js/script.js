// Common utility functions
function formatDate(dateStr) {
    try {
        if (!dateStr) return '';
        if (dateStr.match(/^\d{2}-\d{2}-\d{4}/)) return dateStr;
        const date = new Date(dateStr);
        if (isNaN(date.getTime())) return '';
        return date.toLocaleString('en-GB', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        });
    } catch (e) {
        console.error('Date formatting error:', e);
        return '';
    }
}

// Index page specific code
const IndexPage = {
    priceChart: null,
    fullChartData: null,

    updateChartData: function(start, end) {
        if (!this.fullChartData) return;
        
        const startIndex = Math.floor(start);
        const endIndex = Math.ceil(end);
        
        this.priceChart.data.labels = this.fullChartData.dates.slice(startIndex, endIndex);
        this.priceChart.data.datasets[0].data = this.fullChartData.prices.slice(startIndex, endIndex);
        this.priceChart.update('none');
    },

    init: function() {
        $('.crypto-row').click(function() {
            const cryptoId = $(this).data('crypto-id');
            const row = $(this).closest('tr');
            
            $('#coinDetailsModal').modal('show');
            $('.modal-title').text('Loading...');
            
            if (IndexPage.priceChart) {
                IndexPage.priceChart.destroy();
                IndexPage.priceChart = null;
            }
            
            const slider = document.getElementById('timeSlider');
            if (slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }

            // Fetch historical data and create chart
            fetch(`/api/coin_history/${cryptoId}`)
                .then(response => response.json())
                .then(data => {
                    IndexPage.fullChartData = data;
                    const totalDays = data.dates.length;
                    const initialStart = Math.max(0, totalDays - 365); // Show last year by default
                    const initialEnd = totalDays;

                    const ctx = document.getElementById('priceChart').getContext('2d');
                    IndexPage.priceChart = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: data.dates.slice(initialStart, initialEnd),
                            datasets: [{
                                data: data.prices.slice(initialStart, initialEnd),
                                borderColor: 'rgb(75, 192, 192)',
                                tension: 0.1,
                                fill: false
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                y: {
                                    beginAtZero: false,
                                    ticks: {
                                        callback: function(value) {
                                            return '$' + value.toLocaleString();
                                        }
                                    }
                                },
                                x: {
                                    ticks: {
                                        maxTicksLimit: 7,
                                        callback: function(value, index) {
                                            return formatDate(this.getLabelForValue(value));
                                        }
                                    }
                                }
                            },
                            plugins: {
                                legend: {
                                    display: false
                                }
                            }
                        }
                    });

                    // Initialize time range slider
                    noUiSlider.create(slider, {
                        start: [initialStart, initialEnd - 1],
                        connect: true,
                        range: {
                            'min': 0,
                            'max': data.dates.length - 1
                        },
                        step: 1
                    });

                    slider.noUiSlider.on('update', function(values, handle) {
                        const startIndex = Math.floor(values[0]);
                        const endIndex = Math.ceil(values[1]);
                        
                        $('#timeRangeStart').text(formatDate(data.dates[startIndex]));
                        $('#timeRangeEnd').text(formatDate(data.dates[endIndex]));
                        
                        IndexPage.updateChartData(startIndex, endIndex);
                    });
                })
                .catch(error => {
                    console.error('Error:', error);
                    $('.modal-title').text('Error');
                    $('.modal-body').html('<p class="text-danger">Failed to load chart data. Please try again later.</p>');
                });
        });

        // Clean up when modal is closed
        $('#coinDetailsModal').on('hidden.bs.modal', function() {
            if (IndexPage.priceChart) {
                IndexPage.priceChart.destroy();
                IndexPage.priceChart = null;
            }
            IndexPage.fullChartData = null;
            
            const slider = document.getElementById('timeSlider');
            if (slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }
        });
    }
};

// Trending page specific code
const TrendingPage = {
    priceChart: null,
    fullChartData: null,

    updateChartData: function(start, end) {
        if (!this.priceChart || !this.fullChartData) return;
        
        this.priceChart.data.labels = this.fullChartData.dates.slice(start, end);
        this.priceChart.data.datasets[0].data = this.fullChartData.prices.slice(start, end);
        this.priceChart.update('none');
    },

    init: function() {
        $('.crypto-row').click(function() {
            const cryptoId = $(this).data('crypto-id');
            const row = $(this).closest('tr');
            
            // Update modal title and image
            $('.modal-title').text(row.find('td:eq(1)').text());
            $('#modalCryptoImage').attr('src', row.find('img').attr('src'));
            
            // Update modal content
            $('#modalPrice').text(row.find('td:eq(2)').text());
            $('#modal24hChange').text(row.find('td:eq(3)').text());
            $('#modalMarketCap').text(row.find('td:eq(4)').text());
            $('#modalVolume').text(row.find('td:eq(5)').text());
            
            $('#coinDetailsModal').modal('show');

            // Clean up existing chart
            if (TrendingPage.priceChart) {
                TrendingPage.priceChart.destroy();
                TrendingPage.priceChart = null;
            }

            // Clean up existing slider
            const slider = document.getElementById('timeSlider');
            if (slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }

            // Fetch and display chart data
            fetch(`/api/coin_history/${cryptoId}`)
                .then(response => response.json())
                .then(data => {
                    TrendingPage.fullChartData = data;
                    const totalDays = data.dates.length;
                    const initialStart = Math.max(0, totalDays - 7); // Show last 7 days
                    const initialEnd = totalDays;

                    const ctx = document.getElementById('priceChart').getContext('2d');
                    TrendingPage.priceChart = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: data.dates.slice(initialStart, initialEnd),
                            datasets: [{
                                data: data.prices.slice(initialStart, initialEnd),
                                borderColor: 'rgb(75, 192, 192)',
                                tension: 0.1,
                                fill: false
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                y: {
                                    beginAtZero: false,
                                    ticks: {
                                        callback: function(value) {
                                            return '$' + value.toLocaleString();
                                        }
                                    }
                                },
                                x: {
                                    ticks: {
                                        maxTicksLimit: 7,
                                        callback: function(value, index) {
                                            return formatDate(this.getLabelForValue(value));
                                        }
                                    }
                                }
                            },
                            plugins: {
                                legend: {
                                    display: false
                                }
                            }
                        }
                    });

                    // Initialize time range slider
                    noUiSlider.create(slider, {
                        start: [initialStart, initialEnd - 1],
                        connect: true,
                        range: {
                            'min': 0,
                            'max': data.dates.length - 1
                        },
                        step: 1
                    });

                    slider.noUiSlider.on('update', function(values, handle) {
                        const startIndex = Math.floor(values[0]);
                        const endIndex = Math.ceil(values[1]);
                        
                        $('#timeRangeStart').text(formatDate(data.dates[startIndex]));
                        $('#timeRangeEnd').text(formatDate(data.dates[endIndex]));
                        
                        TrendingPage.updateChartData(startIndex, endIndex);
                    });
                })
                .catch(error => {
                    console.error('Error:', error);
                    $('.modal-title').text('Error');
                    $('.modal-body').html('<p class="text-danger">Failed to load chart data. Please try again later.</p>');
                });
        });

        // Clean up when modal is closed
        $('#coinDetailsModal').on('hidden.bs.modal', function() {
            if (TrendingPage.priceChart) {
                TrendingPage.priceChart.destroy();
                TrendingPage.priceChart = null;
            }
            TrendingPage.fullChartData = null;
            
            const slider = document.getElementById('timeSlider');
            if (slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }
        });
    }
};

// Predictions page specific code
const PredictionsPage = {
    priceChart: null,
    fullChartData: null,

    updateChartData: function(start, end) {
        if (!this.priceChart || !this.fullChartData) return;
        
        this.priceChart.data.labels = this.fullChartData.allDates.slice(start, end);
        this.priceChart.data.datasets[0].data = this.fullChartData.historicalPrices.slice(start, end);
        this.priceChart.data.datasets[1].data = this.fullChartData.predictedPrices.slice(start, end);
        this.priceChart.update('none');
    },

    init: function() {
        $('.crypto-row').click(function() {
            const cryptoId = $(this).data('crypto-id');
            const row = $(this).closest('tr');
            
            // Update modal title and image
            $('.modal-title').text(row.find('td:eq(1)').text());
            $('#modalCryptoImage').attr('src', row.find('img').attr('src'));
            
            $('#coinDetailsModal').modal('show');

            // Clean up existing chart
            if (PredictionsPage.priceChart) {
                PredictionsPage.priceChart.destroy();
                PredictionsPage.priceChart = null;
            }

            // Clean up existing slider
            const slider = document.getElementById('timeSlider');
            if (slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }

            // Get prediction data from the row
            const predictionData = {
                '24h': {
                    price: parseFloat(row.find('td:eq(3)').text().match(/\$([0-9,.]+)/)[1].replace(',', '')),
                    confidence: parseFloat(row.find('td:eq(3) .confidence-score').text().match(/([0-9.]+)%/)[1])
                },
                '48h': {
                    price: parseFloat(row.find('td:eq(4)').text().match(/\$([0-9,.]+)/)[1].replace(',', '')),
                    confidence: parseFloat(row.find('td:eq(4) .confidence-score').text().match(/([0-9.]+)%/)[1])
                },
                '3d': {
                    price: parseFloat(row.find('td:eq(5)').text().match(/\$([0-9,.]+)/)[1].replace(',', '')),
                    confidence: parseFloat(row.find('td:eq(5) .confidence-score').text().match(/([0-9.]+)%/)[1])
                },
                '7d': {
                    price: parseFloat(row.find('td:eq(6)').text().match(/\$([0-9,.]+)/)[1].replace(',', '')),
                    confidence: parseFloat(row.find('td:eq(6) .confidence-score').text().match(/([0-9.]+)%/)[1])
                }
            };

            // Fetch historical data and create chart
            fetch(`/api/coin_history/${cryptoId}`)
                .then(response => response.json())
                .then(data => {
                    // Create future dates and prices array
                    const lastDate = new Date(data.dates[data.dates.length - 1]);
                    const futureData = [
                        { date: new Date(lastDate.getTime() + 24 * 60 * 60 * 1000), price: predictionData['24h'].price },
                        { date: new Date(lastDate.getTime() + 48 * 60 * 60 * 1000), price: predictionData['48h'].price },
                        { date: new Date(lastDate.getTime() + 72 * 60 * 60 * 1000), price: predictionData['3d'].price },
                        { date: new Date(lastDate.getTime() + 7 * 24 * 60 * 60 * 1000), price: predictionData['7d'].price }
                    ];

                    PredictionsPage.fullChartData = {
                        allDates: [...data.dates, ...futureData.map(d => formatDate(d.date))],
                        historicalPrices: [...data.prices],
                        predictedPrices: [...Array(data.prices.length - 1).fill(null),
                            data.prices[data.prices.length - 1],
                            ...futureData.map(d => d.price)]
                    };

                    const totalDays = PredictionsPage.fullChartData.allDates.length;
                    const initialStart = Math.max(0, totalDays - 14); // Show last 7 days + next 7 days
                    const initialEnd = totalDays;

                    const ctx = document.getElementById('priceChart').getContext('2d');
                    PredictionsPage.priceChart = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: PredictionsPage.fullChartData.allDates.slice(initialStart, initialEnd),
                            datasets: [{
                                label: 'Historical Price',
                                data: PredictionsPage.fullChartData.historicalPrices.slice(initialStart, initialEnd),
                                borderColor: 'rgb(75, 192, 192)',
                                tension: 0.1,
                                fill: false
                            },
                            {
                                label: 'Predicted Price',
                                data: PredictionsPage.fullChartData.predictedPrices.slice(initialStart, initialEnd),
                                borderColor: 'rgb(255, 99, 132)',
                                tension: 0.1,
                                fill: false
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                y: {
                                    beginAtZero: false,
                                    ticks: {
                                        callback: function(value) {
                                            return '$' + value.toLocaleString();
                                        }
                                    }
                                },
                                x: {
                                    ticks: {
                                        maxTicksLimit: 7,
                                        callback: function(value, index) {
                                            return formatDate(this.getLabelForValue(value));
                                        }
                                    }
                                }
                            },
                            plugins: {
                                legend: {
                                    display: true,
                                    position: 'top'
                                }
                            }
                        }
                    });

                    // Initialize time range slider
                    noUiSlider.create(slider, {
                        start: [initialStart, initialEnd - 1],
                        connect: true,
                        range: {
                            'min': 0,
                            'max': PredictionsPage.fullChartData.allDates.length - 1
                        },
                        step: 1
                    });

                    slider.noUiSlider.on('update', function(values, handle) {
                        const startIndex = Math.floor(values[0]);
                        const endIndex = Math.ceil(values[1]);
                        
                        $('#timeRangeStart').text(formatDate(PredictionsPage.fullChartData.allDates[startIndex]));
                        $('#timeRangeEnd').text(formatDate(PredictionsPage.fullChartData.allDates[endIndex]));
                        
                        PredictionsPage.updateChartData(startIndex, endIndex);
                    });
                })
                .catch(error => {
                    console.error('Error:', error);
                    $('.modal-title').text('Error');
                    $('.modal-body').html('<p class="text-danger">Failed to load chart data. Please try again later.</p>');
                });
        });

        // Clean up when modal is closed
        $('#coinDetailsModal').on('hidden.bs.modal', function() {
            if (PredictionsPage.priceChart) {
                PredictionsPage.priceChart.destroy();
                PredictionsPage.priceChart = null;
            }
            PredictionsPage.fullChartData = null;
            
            const slider = document.getElementById('timeSlider');
            if (slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }
        });
    }
};

// Past Predictions page specific code
const PastPredictionsPage = {
    priceChart: null,
    fullChartData: null,

    updateChartData: function(start, end) {
        if (!this.priceChart || !this.fullChartData) return;
        
        this.priceChart.data.labels = this.fullChartData.allDates.slice(start, end);
        this.priceChart.data.datasets[0].data = this.fullChartData.historicalPrices.slice(start, end);
        this.priceChart.data.datasets[1].data = this.fullChartData.predictedPrices.slice(start, end);
        this.priceChart.data.datasets[2].data = this.fullChartData.actualPrices.slice(start, end);
        this.priceChart.update('none');
    },

    init: function() {
        $('.crypto-row').click(function() {
            const cryptoId = $(this).data('crypto-id');
            const predictionDate = $(this).data('prediction-date');
            const row = $(this).closest('tr');
            
            // Update modal title and image
            $('.modal-title').text(row.find('td:eq(1)').text());
            $('#modalCryptoImage').attr('src', row.find('img').attr('src'));
            
            $('#coinDetailsModal').modal('show');

            // Clean up existing chart
            if (PastPredictionsPage.priceChart) {
                PastPredictionsPage.priceChart.destroy();
                PastPredictionsPage.priceChart = null;
            }

            // Clean up existing slider
            const slider = document.getElementById('timeSlider');
            if (slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }

            // Get prediction and actual data from the row
            const predictionData = {
                date: predictionDate,
                predicted: parseFloat(row.find('td:eq(3)').text().match(/\$([0-9,.]+)/)[1].replace(',', '')),
                actual: parseFloat(row.find('td:eq(4)').text().match(/\$([0-9,.]+)/)[1].replace(',', '')),
                accuracy: parseFloat(row.find('td:eq(5)').text().match(/([0-9.]+)%/)[1])
            };

            // Fetch historical data and create chart
            fetch(`/api/past_prediction/${cryptoId}/${predictionDate}`)
                .then(response => response.json())
                .then(data => {
                    PastPredictionsPage.fullChartData = {
                        allDates: data.dates,
                        historicalPrices: data.historical_prices,
                        predictedPrices: data.predicted_prices,
                        actualPrices: data.actual_prices
                    };

                    const ctx = document.getElementById('priceChart').getContext('2d');
                    PastPredictionsPage.priceChart = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: data.dates,
                            datasets: [{
                                label: 'Historical Price',
                                data: data.historical_prices,
                                borderColor: 'rgb(75, 192, 192)',
                                tension: 0.1,
                                fill: false
                            },
                            {
                                label: 'Predicted Price',
                                data: data.predicted_prices,
                                borderColor: 'rgb(255, 99, 132)',
                                tension: 0.1,
                                fill: false
                            },
                            {
                                label: 'Actual Price',
                                data: data.actual_prices,
                                borderColor: 'rgb(255, 159, 64)',
                                tension: 0.1,
                                fill: false
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                y: {
                                    beginAtZero: false,
                                    ticks: {
                                        callback: function(value) {
                                            return '$' + value.toLocaleString();
                                        }
                                    }
                                },
                                x: {
                                    ticks: {
                                        maxTicksLimit: 7,
                                        callback: function(value, index) {
                                            return formatDate(this.getLabelForValue(value));
                                        }
                                    }
                                }
                            },
                            plugins: {
                                legend: {
                                    display: true,
                                    position: 'top'
                                }
                            }
                        }
                    });

                    // Initialize time range slider
                    noUiSlider.create(slider, {
                        start: [0, data.dates.length - 1],
                        connect: true,
                        range: {
                            'min': 0,
                            'max': data.dates.length - 1
                        },
                        step: 1
                    });

                    slider.noUiSlider.on('update', function(values, handle) {
                        const startIndex = Math.floor(values[0]);
                        const endIndex = Math.ceil(values[1]);
                        
                        $('#timeRangeStart').text(formatDate(data.dates[startIndex]));
                        $('#timeRangeEnd').text(formatDate(data.dates[endIndex]));
                        
                        PastPredictionsPage.updateChartData(startIndex, endIndex);
                    });
                })
                .catch(error => {
                    console.error('Error:', error);
                    $('.modal-title').text('Error');
                    $('.modal-body').html('<p class="text-danger">Failed to load chart data. Please try again later.</p>');
                });
        });

        // Clean up when modal is closed
        $('#coinDetailsModal').on('hidden.bs.modal', function() {
            if (PastPredictionsPage.priceChart) {
                PastPredictionsPage.priceChart.destroy();
                PastPredictionsPage.priceChart = null;
            }
            PastPredictionsPage.fullChartData = null;
            
            const slider = document.getElementById('timeSlider');
            if (slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }
        });
    }
};

// Initialize when document is ready
$(document).ready(function() {
    if ($('.crypto-row').length) {
        const path = window.location.pathname;
        if (path === '/') {
            IndexPage.init();
        } else if (path === '/trending') {
            TrendingPage.init();
        } else if (path === '/predictions') {
            PredictionsPage.init();
        } else if (path === '/past_predictions') {
            PastPredictionsPage.init();
        }
    }
}); 