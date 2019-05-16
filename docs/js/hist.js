// Copyright 2019 AstroLab Software
// Author: Julien Peloton
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// This function serves the Fink documentation.
// Please change the csvURL if data does not display (hardcoded...)
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
        value: 30,
        color: 'red',
        dashStyle: 'shortdash',
        width: 2,
        label: {
          text: 'Rate = ' +  30 + ' row/s'
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
    xAxis: {
      ordinal: false
    },
      data: {
          csvURL: "https://fink-broker.readthedocs.io/en/latest/data/history.csv"
      },
      tooltip: {
        valueDecimals: 1
      },
    });
}

// Create the chart
createChart();
