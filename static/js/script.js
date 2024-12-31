// Common utility functions
function formatDate(dateStr) {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-GB'); // DD/MM/YYYY format
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
        $('.crypto-row').on('click', function() {
            const cryptoId = $(this).data('crypto-id');
            
            // Get the data from the row
            const row = $(this);
            const name = row.find('td:eq(1)').text().trim();
            const price = row.find('td:eq(2)').text().trim();
            const change = row.find('td:eq(3)').text().trim();
            const marketCap = row.find('td:eq(4)').text().trim();
            const volume = row.find('td:eq(5)').text().trim();
            const imageUrl = row.find('img').attr('src');

            // Update modal content
            $('#modalCryptoImage').attr('src', imageUrl);
            $('.modal-title').text(name);
            $('#modalPrice').text(price);
            $('#modal24hChange').text(change);
            $('#modalMarketCap').text(marketCap);
            $('#modalVolume').text(volume);

            // Fetch sentiment data from the server
            fetch(`/api/sentiment/${cryptoId}`)
                .then(response => {
                    if (!response.ok) {
                        throw new Error('Network response was not ok');
                    }
                    return response.json();
                })
                .then(data => {
                    // Format the sentiment values with proper decimal places
                    const formatPercentage = (value) => {
                        return ((value || 0) + '%');
                    };

                    $('#modalSentimentUp').text(formatPercentage(data.positive));
                    $('#modalSentimentDown').text(formatPercentage(data.negative));
                    $('#modalInterestScore').text(data.interest_score || '0');

                    // Clear any previous error messages
                    $('.sentiment-error').remove();
                })
                .catch(error => {
                    console.error('Error fetching sentiment data:', error);
                    // Set default values if there's an error
                    $('#modalSentimentUp').text('0%');
                    $('#modalSentimentDown').text('0%');
                    $('#modalInterestScore').text('0');
                    
                    // Optionally show error message to user
                    $('.modal-body').prepend(
                        `<div class="alert alert-warning sentiment-error">
                            Unable to load sentiment data
                        </div>`
                    );
                });

            // Show the modal
            $('#coinDetailsModal').modal('show');

            // Load price chart data
            loadPriceChart(cryptoId);
        });

        // Function to load price chart
        function loadPriceChart(cryptoId) {
            fetch(`/api/price_history/${cryptoId}`)
                .then(response => {
                    if (!response.ok) {
                        throw new Error('Network response was not ok');
                    }
                    return response.json();
                })
                .then(data => {
                    // Store full data for slider use
                    window.fullChartData = {
                        dates: data.dates,
                        prices: data.prices
                    };

                    // Get the chart context
                    const ctx = document.getElementById('priceChart').getContext('2d');
                    
                    // Destroy existing chart if it exists
                    if (window.currentChart instanceof Chart) {
                        window.currentChart.destroy();
                    }

                    // Create new chart
                    window.currentChart = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: data.dates,
                            datasets: [{
                                label: 'Price (USD)',
                                data: data.prices,
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
                                    type: 'category',
                                    ticks: {
                                        maxTicksLimit: 10
                                    }
                                }
                            }
                        }
                    });

                    // Initialize time range slider
                    const slider = document.getElementById('timeSlider');
                    if (slider.noUiSlider) {
                        slider.noUiSlider.destroy();
                    }

                    noUiSlider.create(slider, {
                        start: [0, data.dates.length - 1],
                        connect: true,
                        range: {
                            'min': 0,
                            'max': data.dates.length - 1
                        },
                        step: 1
                    });

                    // Update chart when slider changes
                    slider.noUiSlider.on('update', function(values, handle) {
                        const startIndex = Math.floor(values[0]);
                        const endIndex = Math.ceil(values[1]);
                        
                        // Update date range labels
                        $('#timeRangeStart').text(formatDate(data.dates[startIndex]));
                        $('#timeRangeEnd').text(formatDate(data.dates[endIndex]));
                        
                        // Update chart data
                        window.currentChart.data.labels = data.dates.slice(startIndex, endIndex + 1);
                        window.currentChart.data.datasets[0].data = data.prices.slice(startIndex, endIndex + 1);
                        window.currentChart.update('none');
                    });
                })
                .catch(error => {
                    console.error('Error loading chart:', error);
                    const container = document.querySelector('.chart-container');
                    container.innerHTML = '<p class="text-danger">Failed to load price chart. Please try again later.</p>';
                });
        }

        // Add modal cleanup
        $('#coinDetailsModal').on('hidden.bs.modal', function() {
            // Destroy chart if it exists
            if (window.currentChart instanceof Chart) {
                window.currentChart.destroy();
                window.currentChart = null;
            }
            
            // Clean up slider if it exists
            const slider = document.getElementById('timeSlider');
            if (slider && slider.noUiSlider) {
                slider.noUiSlider.destroy();
            }

            // Clear stored chart data
            window.fullChartData = null;
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
            fetch(`/api/price_history/${cryptoId}`)
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
            fetch(`/api/price_history/${cryptoId}`)
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

// Function to format numbers with commas
function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Function to format percentage
function formatPercentage(value) {
    if (value === null || value === undefined) return '0%';
    return value.toFixed(2) + '%';
}

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