import chartBuilder from "./chartBuilder.js"
const buildChartData = new chartBuilder()
var ctx = document.getElementById('canvas').getContext('2d');
let chart = buildChartData.chartInit(ctx)
let piTimeFormat = d3.timeFormat("%Y-%m-%dT%H:%M:00%Z")
//var startTime = "2020-11-15T00:00:00-08:00"
//var endTime = "2020-12-01T09:23:00-08:00"
let interval = "1h"
const abayFloat = document.getElementById("addAbayFloat")
let endTime = piTimeFormat(new Date())
let startTime = piTimeFormat(addDays(new Date(), -7))


//https://flows.pcwa.net/piwebapi/elements/?path=\\BUSINESSPI2\OPS\Reservoirs
async function makeRequest(gType, gName, startTime, endTime, interval){
    return buildChartData.urlBuilder(gType, gName, startTime, endTime, interval)
}

function graphPI(gauge_type, gauge_name, graph_type, startTime, endTime, interval){
    makeRequest(gauge_type, gauge_name, startTime, endTime, interval)
        .then(result=> {
            return buildChartData.dataGrabber(result, gauge_name)
        })
        .then(data=> {
            let [chartData, chartOptions] = [data[0], data[1]]
            chart.type = 'line'
            chart.data.datasets = chartData.datasets
            if ((chart.data.datasets).length <= 1){
                chart.options = chartOptions
            }

            chart.update()
        })
}

function addDays(date, days) {
  const copy = new Date(Number(date))
  copy.setDate(date.getDate() + days)
  return copy
}


graphPI("Reservoirs", "Afterbay|Elevation", "line", startTime, endTime, interval)
abayFloat.addEventListener("click", (elem) => {
    graphPI("Reservoirs", "Afterbay|Elevation Setpoint", "line", startTime, endTime, interval)
})

