let lineChartData = {}; //declare an object
lineChartData.labels = []; //add 'labels' element to object (X axis)
lineChartData.datasets = []; //add 'datasets' array element to object

let lineChartOptions = {};
lineChartOptions.scales = {};
lineChartOptions.scales.xAxes = []

function addLine(xydata, lineName){
    lineChartData.datasets.push({});                  //Create a new line dataset.
    let lineNum = (lineChartData.datasets).length - 1   //The position of the new line in our array of lines.
    let dataset = lineChartData.datasets[lineNum]       //This dataset is at point "lineNum" in the array.
    dataset.background_color = 'rgba(255, 99, 132, 0.2)';
    dataset.borderColor = 'rgba(255,99,132,1)'
    dataset.borderWidth = 1
    dataset.data = [];                              //contains the 'Y; axis data
    dataset.label = lineName
    lineChartData.datasets[lineNum].data = xydata; //send new line data to dataset

    lineChartOptions.scales.xAxes.push({})
    let chartOptions = lineChartOptions.scales.xAxes[lineNum]
    chartOptions.type = 'time'
    chartOptions.time = {
        displayFormats: {
                    day: 'MMM D',
                    hour: 'MMM D HH:MM' // The displayFormat for the given unit
                        },
                    unit: 'hour'
                    }
    lineChartOptions.scales.xAxes[lineNum] = chartOptions
    return [lineChartData, lineChartOptions]
}

function buildChart(xydata) {
    var ctx = document.getElementById('canvas').getContext('2d');
    var myChart = new Chart(ctx, {
      type: 'line',
      data: {
        //labels: xdata,
        datasets: [{
          label: 'R30 Flows',
          data: xydata,
          backgroundColor: [
            'rgba(255, 99, 132, 0.2)',
            'rgba(54, 162, 235, 0.2)',
            'rgba(255, 206, 86, 0.2)',
            'rgba(75, 192, 192, 0.2)',
            'rgba(153, 102, 255, 0.2)',
            'rgba(255, 159, 64, 0.2)'
          ],
          borderColor: [
            'rgba(255,99,132,1)',
            'rgba(54, 162, 235, 1)',
            'rgba(255, 206, 86, 1)',
            'rgba(75, 192, 192, 1)',
            'rgba(153, 102, 255, 1)',
            'rgba(255, 159, 64, 1)'
          ],
          borderWidth: 1
        }]
      },
      options: {
        scales: {
          xAxes: [{
              type: 'time',
              time:{
                displayFormats:{
                    day: 'MMM D',
                    hour: 'MMM D HH:MM' // The displayFormat for the given unit
                },
                  unit: 'hour'
              }
          }]
        }
      }
    });
}

export default class chartConstruction {
    final_url = ''
    chartData = {}
    chartOptions = {}
    async urlBuilder(gType, gName, sTime, eTime, interval) {
        const BASE_URL = new URL('https://flows.pcwa.net/piwebapi/attributes/')
        const params = {"path": `\\\\BUSINESSPI2\\OPS\\${gType}\\${gName}`}
        BASE_URL.search = new URLSearchParams(params).toString()
        console.log("Requesting URL for " + gName)
        const url_options = {
            method: 'GET', // *GET, POST, PUT, DELETE, etc.
            mode: 'cors', // no-cors, *cors, same-origin
            cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
            credentials: 'same-origin', // include, *same-origin, omit
        }

        const flow_params = {
            "startTime": sTime,
            "endTime": eTime,
            "interval": interval
        }
        await fetch(BASE_URL, url_options)
            .then(data => {
                return data.json()
            })
            .then(resp => {
                const url = new URL(resp["Links"]["InterpolatedData"])
                url.search = new URLSearchParams(flow_params)
                this.final_url = url
            })
            .catch(error => console.log("Can't Get URL:", error))
        return this.final_url
    }

    async dataGrabber(url, gauge_name) {
        let xdata = []
        let ydata = []
        let xydata = []
        let url_options = null
        await fetch(url, url_options)
            .then(data => {
                return data.json()
            })
            .then(resp => {
                const jdata = resp["Items"]
                for (let i = 0; i < jdata.length; i++) {
                    xydata.push({'t': new Date(jdata[i]["Timestamp"]).toString(), 'y': jdata[i]["Value"]})
                    ydata.push(jdata[i]["Value"]);
                    xdata.push(new Date(jdata[i]["Timestamp"]).toString());
                }
                [this.chartData, this.chartOptions] = addLine(xydata, gauge_name)
            })
            .catch(error => console.log("ERROR HERE:", error))
        return [this.chartData, this.chartOptions]
    }

    chartInit(ctx){
        var test =  new Chart(ctx, {
                        type:'line',
                        plugins: {zoom: {
                // Container for pan options
                pan: {
                    // Boolean to enable panning
                    enabled: true,

                    // Panning directions. Remove the appropriate direction to disable
                    // Eg. 'y' would only allow panning in the y direction
                    // A function that is called as the user is panning and returns the
                    // available directions can also be used:
                    //   mode: function({ chart }) {
                    //     return 'xy';
                    //   },
                    mode: 'xy',

                    rangeMin: {
                        // Format of min pan range depends on scale type
                        x: null,
                        y: null
                    },
                    rangeMax: {
                        // Format of max pan range depends on scale type
                        x: null,
                        y: null
                    },

                    // On category scale, factor of pan velocity
                    speed: 20,

                    // Minimal pan distance required before actually applying pan
                    threshold: 10,

                    // Function called while the user is panning
                    onPan: function({chart}) { console.log(`I'm panning!!!`); },
                    // Function called once panning is completed
                    onPanComplete: function({chart}) { console.log(`I was panned!!!`); }
                },

                // Container for zoom options
                zoom: {
                    // Boolean to enable zooming
                    enabled: true,

                    // Enable drag-to-zoom behavior
                    drag: true,

                    // Drag-to-zoom effect can be customized
                    // drag: {
                    // 	 borderColor: 'rgba(225,225,225,0.3)'
                    // 	 borderWidth: 5,
                    // 	 backgroundColor: 'rgb(225,225,225)',
                    // 	 animationDuration: 0
                    // },

                    // Zooming directions. Remove the appropriate direction to disable
                    // Eg. 'y' would only allow zooming in the y direction
                    // A function that is called as the user is zooming and returns the
                    // available directions can also be used:
                    //   mode: function({ chart }) {
                    //     return 'xy';
                    //   },
                    mode: 'xy',

                    rangeMin: {
                        // Format of min zoom range depends on scale type
                        x: null,
                        y: null
                    },
                    rangeMax: {
                        // Format of max zoom range depends on scale type
                        x: null,
                        y: null
                    },

                    // Speed of zoom via mouse wheel
                    // (percentage of zoom on a wheel event)
                    speed: 0.1,

                    // Minimal zoom distance required before actually applying zoom
                    threshold: 2,

                    // On category scale, minimal zoom level before actually applying zoom
                    sensitivity: 3,

                    // Function called while the user is zooming
                    onZoom: function({chart}) { console.log(`I'm zooming!!!`); },
                    // Function called once zooming is completed
                    onZoomComplete: function({chart}) { console.log(`I was zoomed!!!`); }
                }
            }}
                        })
        console.log(test)
        return test
    }
}

function addPlugin(plugin) {
    if (plugin === 'zoom') {

    }
}
