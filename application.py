from database import Database
import os
"""j
- min/max/average energy consumption for a given city over
  - year
  - overall
- min/max/average weather event for a given city over
  - year
  - overall

valid cities:
  - chicago: AEP
  - pittsburgh: duquesne
  - philadelphia: ???
  - new york: ???
  - detroit: AEP
  - indianapolis: AEP
"""


class Application(object):
    availableData = [
        {"city": "Chicago", "station": "American_Electric_Power"},
        {"city": "Pittsburgh", "station": "Duquesne_Light_Co"},
        {"city": "Philadelphia", "station": "PJM_WEST"},
        {"city": "New York", "station": "PJM_WEST"},
        {"city": "Detroit", "station": "American_Electric_Power"},
        {"city": "Indianapolis", "station": "American_Electric_Power"}
    ]

    currentCity = "Chicago"
    # currentQueryType = "weather"
    currentTimespan = "all"
    currentWeatherType = "temperature"
    currentValue = "avg"
    results = {}

    # curren state of the query string
    def printInfoStrings(self):
        print()
        print("-----Currently selected options-----")
        print("City: " + self.currentCity)
        # print("Weather/Power: " + self.currentQueryType)
        print("Weather type: " + self.currentWeatherType)
        print("Value to retreive: " + self.currentValue)
        print("Timespan: " + str(self.currentTimespan))
        print("------------------------------------")
        print()

    # print out all available commands
    def printCommands(self):
        print("Commands:\n[c] to edit city\t[s] to select time span\t[w] to select weather type\t[v] to select min/max/avg\t[r] to run query\t[q] to exit the program")

    # select one of the available cities
    def cityInput(self):
        print()
        print("Select a city to query")
        for i in range(len(self.availableData)):
            print(self.availableData[i]['city'] + " (" + str(i+1) + ")")
        print("or 'All'")

        cityStr = input("City: ")
        if(cityStr.lower() == 'all'):
            self.currentCity = "All"

        else:
            try:
                if int(cityStr) > len(self.availableData) or int(cityStr) <= 0:
                    raise Exception("---Invalid option---")

                self.currentCity = self.availableData[int(cityStr)-1]['city']

            except Exception as e:
                print(e)
                print()
                self.currentCity = -1
                self.cityInput()
            except ValueError as e:
                self.currentCity = -1
                print("Not a number")
                print()
                self.cityInput()

    # # select weather/power as the primary query paramater
    # def queryTypeInput(self):
    #     print("Select what data you want to query (either 'weather' or 'power')")
    #     type = input("Data to query: ")

    #     if type.lower() != "weather" and type.lower() != "power":
    #         print("---Invalid option---")
    #         print()
    #         self.queryTypeInput()

    #     else:
    #         self.currentQueryType = type

    # select overall/specific year
    def timespanInput(self):
        print("Select the timespan to view")
        print("This can be 'Yearly' or 'All'")
        span = input("Timespan: ")

        try:
            if span.lower() != 'all' and span.lower() != 'yearly':
                raise Exception("--Invalid option---")

            self.currentTimespan = span.lower()

        except Exception as e:
            print(e)
            print()
            self.currentTimespan = "all"
            self.timespanInput()

    # weather option to look at ("humidity", "pressure", etc)
    def weatherTypeInput(self):
        options = [
            "humidity",
            "pressure",
            "temperature",
            "winddirection",
            "windspeed"
        ]
        print("Select the type of weather to view")
        for i in range(len(options)):
            print("\t" + options[i] + " (" + str(i+1) + ")")

        type = input("Weather Type: ")
        try:
            if int(type) > len(options) or int(type) <= 0:
                raise Exception("---Invalid option---")

            self.currentWeatherType = options[int(type)-1]

        except Exception as e:
            print(e)
            print()
            self.currentWeatherType = "temperature"
            self.weatherTypeInput()
        except ValueError as e:
            self.currentCity = "temperature"
            print("Not a number")
            print()
            self.weatherTypeInput()

    def valueInput(self):
        print("Enter the value to view")
        print("This can be one of min/max/avg")

        val = input("Value (min/max/avg): ")
        if val.lower() != 'min' and val.lower() != 'max' and val.lower() != 'avg':
            print("---Invalid Option---")
            print()
            self.valueInput()

        else:
            self.currentValue = val.lower()

    def runQuery(self):
        d = Database()
        self.results = {}
        weather = self.currentWeatherType
        byYearly = (self.currentTimespan == 'yearly')
        if self.currentCity.lower() == "all":
            for c in self.availableData:
                cityName = c['city'].lower()
                if cityName == "new york":
                    cityName = "newyork"
                region = c['station']

                self.results[cityName] = {
                    weather: [],
                    'power': []
                }

                self.results[cityName][weather] = d.get_weather_details_by_city(
                    cityName, byYearly, weather)[weather]

                self.results[cityName]['power'] = d.get_consumption_details_by_city("dummy", region, byYearly)

        else:
            cityName = self.currentCity

            region = next((x['station'] for x in self.availableData if x['city'] == cityName), None)

            if cityName.lower() == "new york":
                cityName = "newyork"

            self.results[cityName] = {
                weather: [],
                'power': []
            }

            self.results[cityName][weather] = d.get_weather_details_by_city(
                cityName, byYearly, weather)[weather]

            self.results[cityName]['power'] = d.get_consumption_details_by_city("dummy", region, byYearly)

        self.printResults()

    def printResults(self):
        print()
        yearStr = 'Year'
        valStr = self.currentValue
        typeStr = self.currentWeatherType
        heading = "City, {}, {} {}, {} power".format(yearStr, valStr, typeStr, valStr)
        print("RESULTS" + "-"*(len(heading)-7))
        print(heading)
        print("_"*len(heading))

        for key, value in self.results.items():
            weatherData = value[self.currentWeatherType]
            powerData = value['power']
            powerData = [r for r in powerData if ((len(r) == 4 and int(r[0]) >= 2012 and int(r[0] <= 2017)) or len(r) == 3)]

            if len(powerData) != len(weatherData):
                print("oopsie")
                print(powerData)
                print(weatherData)
                return

            for i in range(max(len(powerData), len(weatherData))):
                rowStr = key + ", "
                powerRow = powerData[i]
                weatherRow = weatherData[i]

                offset = 0
                if len(weatherRow) == 4:
                    rowStr += str(weatherRow[0]) + ", "
                    offset = 1
                else:
                    rowStr += "All, "

                if self.currentValue.lower() == 'avg':
                    weatherVal = float(weatherRow[0 + offset]) * (9/5) - 459.67
                    rowStr += str(int(weatherVal)) + ", "
                    rowStr += str(powerRow[0 + offset])
                if self.currentValue.lower() == 'min':
                    weatherVal = float(weatherRow[1 + offset]) * (9/5) - 459.67
                    rowStr += str(int(weatherVal)) + ", "
                    rowStr += str(powerRow[1 + offset])
                if self.currentValue.lower() == 'max':
                    weatherVal = float(weatherRow[2 + offset]) * (9/5) - 459.67
                    rowStr += str(int(weatherVal)) + ", "
                    rowStr += str(powerRow[2 + offset])

                print(rowStr)

        print()
        input("Press any key to continue...")

if __name__ == "__main__":
    a = Application()
    cmd = ""
    while(1):
        a.printInfoStrings()
        a.printCommands()
        cmd = input("-->")
        if cmd == "c":
            a.cityInput()
        elif cmd == "t":
            a.queryTypeInput()
        elif cmd == "s":
            a.timespanInput()
        elif cmd == "w":
            a.weatherTypeInput()
        elif cmd == "v":
            a.valueInput()
        elif cmd == "r":
            a.runQuery()
        elif cmd == "q":
            break
