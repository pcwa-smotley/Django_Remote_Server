import requests
from datetime import datetime, timedelta


class PiRequest:
    #
    # https://flows.pcwa.net/piwebapi/assetdatabases/D0vXCmerKddk-VtN6YtBmF5A8lsCue2JtEm2KAZ4UNRKIwQlVTSU5FU1NQSTJcT1BT/elements
    def __init__(self, meter_name, attribute):
        self.meter_name = meter_name  # R4, Afterbay, Ralston
        self.attribute = attribute  # Flow, Elevation, Lat, Lon, Storage, Elevation Setpoint, Gate 1 Position, Generation
        self.baseURL = 'https://flows.pcwa.net/piwebapi/attributes'
        self.meter_element_type = self.meter_element_type()  # Gauging Stations, Reservoirs, Generation Units
        self.url = self.url()
        self.data = self.grab_data()

    def url(self):
        try:
            response = requests.get(
                url="https://flows.pcwa.net/piwebapi/attributes",
                params={"path": f"\\\\BUSINESSPI2\\OPS\\{self.meter_element_type}\\{self.meter_name}|{self.attribute}",
                        },
            )
            j = response.json()
            url_flow = j['Links']['InterpolatedData']
            return url_flow

        except requests.exceptions.RequestException:
            print('HTTP Request failed')
            return None

    def grab_data(self):
        # Now that we have the url for the PI data, this request is for the actual data. We will
        # download data from the beginning of the water year to the current date. (We can't download data
        # past today's date, if we do we'll get an error.
        try:
            response = requests.get(
                url=self.url,
                params={"startTime": (datetime.utcnow() + timedelta(hours=-24)).strftime("%Y-%m-%dT%H:00:00-00:00"),
                        "endTime": datetime.utcnow().strftime("%Y-%m-%dT%H:00:00-00:00"),
                        "interval": "1h",
                        },
            )
            print('Response HTTP Status Code: {status_code}'.format(status_code=response.status_code))
            j = response.json()
            # We only want the "Items" object.
            return j["Items"]
        except requests.exceptions.RequestException:
            print('HTTP Request failed')
            return None