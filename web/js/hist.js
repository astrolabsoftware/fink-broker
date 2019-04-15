var defaultData = window.location.origin + '/data/live.csv'
var urlInput = document.getElementById('fetchURL');
var horizontalMarker = document.getElementById('horizontalMarker');

function createChart() {
    Highcharts.stockChart('container_hist', {
      chart: {
          type: 'spline',
          zoomType: 'x'
      },
      title: {
          text: 'History Live Data'
      },
      yAxis: {
        title: {
          text: 'Rate'
        },
        plotLines: [{
        value: horizontalMarker.value,
        color: 'red',
        dashStyle: 'shortdash',
        width: 2,
        label: {
          text: 'Rate = ' +  horizontalMarker.value + ' row/s'
        }
      }]
      },
      rangeSelector: {
        text: "Last",
      buttons: [{
        type: 'minute',
        count: 1,
        text: '1min'
      }, {
        type: 'hour',
        count: 1,
        text: '1h'
      }, {
        type: 'day',
        count: 1,
        text: '1d'
      }, {
        type: 'month',
        count: 1,
        text: '1M'
      }, {
        type: 'year',
        count: 1,
        text: '1y'
      }, {
        type: 'all',
        text: 'All'
      }],
      buttonTheme: { // styles for the buttons
            fill: 'none',
            stroke: 'none',
            'stroke-width': 0,
            r: 8,
            style: {
                color: '#039',
                fontWeight: 'bold'
            },
            states: {
                hover: {
                },
                select: {
                    fill: '#039',
                    style: {
                        color: 'white'
                    }
                }
                // disabled: { ... }
            }
        },
        inputBoxBorderColor: 'gray',
        inputBoxWidth: 120,
        inputBoxHeight: 18,
        inputStyle: {
            color: '#039',
            fontWeight: 'bold'
        },
        labelStyle: {
            color: 'silver',
            fontWeight: 'bold'
        },
      selected: 5 // all
    },
      data: {
          csvURL: urlInput.value
      },
      tooltip: {
        valueDecimals: 1
      },
    });
}

urlInput.value = defaultData;
horizontalMarker.onchange = createChart;
// Create the chart
createChart();
