// Copyright 2018 AstroLab Software
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
var chart = Highcharts.chart('container_bar', {
    chart: {
        type: 'bar',
        polar: true,
        height: 600
    },
    title: {
        text: 'xMatch found with Simbad catalogs'
    },
    legend: {
        enabled: false
    },
    subtitle: {
        text: 'Example on ZTF stream'
    },
    data: {
        csvURL: 'https://fink-broker.readthedocs.io/en/latest/data/simbadtype.csv',
        enablePolling: true,
        dataRefreshRate: 1
    },
    plotOptions: {
        bar: {
            colorByPoint: true
        },
        series: {
            zones: [{
                color: '#4CAF50',
                value: 0
            }, {
                color: '#8BC34A',
                value: 10
            }, {
                color: '#CDDC39',
                value: 20
            }, {
                color: '#CDDC39',
                value: 30
            }, {
                color: '#FFEB3B',
                value: 40
            }, {
                color: '#FFEB3B',
                value: 50
            }, {
                color: '#FFC107',
                value: 60
            }, {
                color: '#FF9800',
                value: 70
            }, {
                color: '#FF5722',
                value: 80
            }, {
                color: '#F44336',
                value: 90
            }, {
                color: '#F44336',
                value: Number.MAX_VALUE
            }],
            dataLabels: {
                enabled: true,
                format: '{point.y:.0f}'
            }
        }
    },
    tooltip: {
        valueDecimals: 0,
        valueSuffix: ''
    },
    xAxis: {
        type: 'category',
        labels: {
            style: {
                fontSize: '10px'
            }
        }
    },
    yAxis: {
        // max: dataMax,
        type: 'logarithmic',
        title: false,
        plotBands: [{
            from: 0,
            to: 10,
            color: '#E8F5E9'
        }, {
            from: 10,
            to: 100,
            color: '#FFFDE7'
        }, {
            from: 100,
            to: 1000,
            color: "#FFEBEE"
        }, {
            from: 1000,
            to: 10000,
            color: "#FFEBEE"
        }]
    }
});
