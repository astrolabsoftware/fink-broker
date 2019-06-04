function choose(projection){
    var polarval = false;
    if (projection == "polar") {
      polarval = true;
    }

    if (projection == "pyramid"){
      createPyramid();
    } else {
      createChart(polarval);
    }
}

function createChart(polarval) {
  Highcharts.chart('container_bar', {
      chart: {
          type: 'bar',
          polar: polarval,
          height: 600
      },
      title: {
          text: 'xMatch found with Simbad catalogs'
      },
      legend: {
          enabled: false
      },
      subtitle: {
          text: 'Refreshed every 10 seconds'
      },
      data: {
          csvURL: window.location.origin + '/data/simbadtype.csv',
          enablePolling: true,
          dataRefreshRate: 10
      },
      plotOptions: {
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
}

function createPyramid() {
  Highcharts.chart('container_bar', {
      chart: {
          type: 'pyramid',
          height: 600
      },
      title: {
          text: 'xMatch found with Simbad catalogs'
      },
      legend: {
          enabled: false
      },
      subtitle: {
          text: 'Refreshed every 10 seconds'
      },
      data: {
          csvURL: window.location.origin + '/data/simbadtype.csv',
          enablePolling: true,
          dataRefreshRate: 30
      },
      yAxis: {
          // max: dataMax,
          type: 'logarithmic'
      },
      plotOptions: {
        series: {
            dataLabels: {
                enabled: true,
                format: '<b>{point.name}</b> ({point.y:,.0f})',
                color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black',
                softConnector: true
            },
            center: ['40%', '50%'],
            width: '80%'
        }
    },
  });
}

createChart(false);
