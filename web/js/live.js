var defaultData = window.location.origin + '/data/live.csv'
var urlInput = document.getElementById('fetchURL');
var pollingCheckbox = document.getElementById('enablePolling');
var pollingInput = document.getElementById('pollingTime');

function createChart() {
    Highcharts.chart('container_live', {
        chart: {
            type: 'spline',
            zoomType: 'x'
        },
        title: {
            text: 'Live Data'
        },
        yAxis: {
          title: {
            text: 'Rate'
          }
        },
        data: {
            csvURL: urlInput.value,
            enablePolling: pollingCheckbox.checked === true,
            dataRefreshRate: parseInt(pollingInput.value, 10)
        },
        tooltip: {
          valueDecimals: 1
        }
    });

    if (pollingInput.value < 1 || !pollingInput.value) {
        pollingInput.value = 1;
    }
}

urlInput.value = defaultData;

// We recreate instead of using chart update to make sure the loaded CSV
// and such is completely gone.
pollingCheckbox.onchange = urlInput.onchange = pollingInput.onchange = createChart;

// Create the chart
createChart();
