import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

"""
This script assumes the database "final" exists, and that user "final" has full permissions
on the database
"""

class DataLoader(object):
  connection_string = "host='localhost' dbname='final' user='final' password='final'"
  conn = psycopg2.connect(connection_string, cursor_factory=psycopg2.extras.DictCursor)
  conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

  def cleanDB(self):
    # initialize database
    with self.conn.cursor() as cursor:
      cursor.execute("""
      TRUNCATE TABLE cityattributes, humidity, pressure, temperature, description, winddirection, windspeed, megawattphourly;
      """)

      self.conn.commit()

  def createSchema(self):
    # set up schemas
    with self.conn.cursor() as cursor:
      with open('./groupprojectschema.sql', 'r') as project_schema:
        setup_queries = project_schema.read()
        cursor.execute(setup_queries)

      self.conn.commit()

  def loadData(self):
    with self.conn.cursor() as cursor:
      #=================================================================================================
      # WEATHER DATA

      # read in cityattributes data
      # with open('./raw_data/historical-hourly-weather-data/city_attributes.csv', 'r') as cityattributes:
      print("loading city attributes")
      with open('./city_attributes.csv', 'r') as cityattributes:
        for line in cityattributes.read().split("\n"):
          if line != '':
            data = line.split(",")
            if data[0] != "City":
              cursor.execute("""
              INSERT INTO cityattributes(city, country, latitude, longitude)
              VALUES(\'%s\', \'%s\', %s, %s);
              """ %(data[0], data[1], data[2], data[3]))

      # humidity data
      # with open('./raw_data/historical-hourly-weather-data/humidity.csv', 'r') as humidity:
      print("loading humidity")
      with open('./humidity.csv', 'r') as humidity:
        for line in humidity.read().split("\n"):
          if line != '':
            data = line.split(",")
            if data[0] != "datetime":
              corrected_data = []
              for a in data:
                if a == '':
                  corrected_data.append(0)
                else:
                  corrected_data.append(a)

              cursor.execute("""
              INSERT INTO humidity VALUES(""" + (str(corrected_data))[1:-1]+ ");")

      # pressure data
      # with open('./raw_data/historical-hourly-weather-data/pressure.csv', 'r') as pressure:
      print("loading pressure")
      with open('./pressure.csv', 'r') as pressure:
        for line in pressure.read().split("\n"):
          if line != '':
            data = line.split(",")
            if data[0] != "datetime":
              corrected_data = []
              for a in data:
                if a == '':
                  corrected_data.append(None)
                else:
                  corrected_data.append(a)

              cursor.execute("""
              INSERT INTO pressure VALUES(""" + (str(corrected_data)).replace("None", "NULL")[1:-1]+ ");")

      # temperature data
      # with open('./raw_data/historical-hourly-weather-data/temperature.csv', 'r') as temperature:
      print("loading temperature")
      with open('./temperature.csv', 'r') as temperature:
        for line in temperature.read().split("\n"):
          if line != '':
            data = line.split(",")
            if data[0] != "datetime":
              corrected_data = []
              for a in data:
                if a == '':
                  corrected_data.append(None)
                else:
                  corrected_data.append(a)

              cursor.execute("""
              INSERT INTO temperature VALUES(""" + (str(corrected_data)).replace("None", "NULL")[1:-1]+ ");")

      # weather description
      # with open('./raw_data/historical-hourly-weather-data/weather_description.csv', 'r') as description:
      print("loading descriptions")
      with open('./weather_description.csv', 'r') as description:
        for line in description.read().split("\n"):
          if line != '':
            data = line.split(",")
            if data[0] != "datetime":
              corrected_data = []
              for a in data:
                if a == '':
                  corrected_data.append(None)
                else:
                  corrected_data.append(a)

              cursor.execute("""
              INSERT INTO description VALUES(""" + (str(corrected_data)).replace("None", "NULL")[1:-1]+ ");")

      # wind direction data
      # with open('./raw_data/historical-hourly-weather-data/wind_direction.csv', 'r') as winddirection:
      print("loading wind direction")
      with open('./wind_direction.csv', 'r') as winddirection:
        for line in winddirection.read().split("\n"):
          if line != '':
            data = line.split(",")
            if data[0] != "datetime":
              corrected_data = []
              for a in data:
                if a == '':
                  corrected_data.append(None)
                else:
                  corrected_data.append(a)

              cursor.execute("""
              INSERT INTO winddirection VALUES(""" + (str(corrected_data)).replace("None", "NULL")[1:-1]+ ");")

      # wind speed data
      # with open('./raw_data/historical-hourly-weather-data/wind_speed.csv', 'r') as windspeed:
      print("loading wind speed")
      with open('./wind_speed.csv', 'r') as windspeed:
        for line in windspeed.read().split("\n"):
          if line != '':
            data = line.split(",")
            if data[0] != "datetime":
              corrected_data = []
              for a in data:
                if a == '':
                  corrected_data.append(None)
                else:
                  corrected_data.append(a)

              cursor.execute("""
              INSERT INTO windspeed VALUES(""" + (str(corrected_data)).replace("None", "NULL")[1:-1]+ ");")
      #=================================================================================================

      #=================================================================================================
      # POWER DATA
      # with open('./raw_data/hourly-energy-consumption/pjm_hourly_est.csv', 'r') as power:
      print("loading hourly energy estimates")
      with open('./pjm_hourly_est.csv', 'r') as power:
        for line in power.read().split("\n"):
          if line != '':
            data = line.split(",")
            if data[0] != "Datetime":
              corrected_data = []
              for a in data:
                if a == '':
                  corrected_data.append(None)
                else:
                  corrected_data.append(a)

              cursor.execute("""
              INSERT INTO megawattphourly VALUES(""" + (str(corrected_data)).replace("None", "NULL")[1:-1]+ ");")
      #=================================================================================================

      self.conn.commit()

if __name__ == "__main__":
  loader = DataLoader()
  loader.createSchema()
  # loader.cleanDB()
  loader.loadData()