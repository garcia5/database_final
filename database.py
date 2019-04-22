import psycopg2
from psycopg2.extensions import AsIs


class Database:

  # Initialize connection + vars
  def __init__(self):
    self.conn = psycopg2.connect(
        "host='localhost' dbname='final' user='final' password='final'")
    self.weather_tables = ["humidity", "pressure",
        "temperature", "winddirection", "windspeed"]

  # city_name - matching city name within weather tables
  # region - matching column name within region table
  # byYearly - 0/1 depending on overall/yearly
  # Assumes city_name and region are associated (regardless of true or not)
  # RETURNS: Ordered array of tuples containing [(YYYY*, AVG, MIN, MAX),...]
  # * field is only included when byYearly = 1
  def get_consumption_details_by_city(self, city_name, region, byYearly):
    cursor = self.conn.cursor()
    techniqueSquidward = ["", "CAST(extract(year from dt) as INT) as year, "]
    bringItAroundTime = ["", " GROUP BY extract(year from dt) ORDER BY year ASC"]
    # Gather consumption data
    cursor.execute("SELECT %sROUND(AVG(%s),1), MIN(%s), MAX(%s) FROM megawattphourly%s", (AsIs(
        techniqueSquidward[byYearly]), AsIs(region), AsIs(region), AsIs(region), AsIs(bringItAroundTime[byYearly]),))
    return cursor.fetchall()

  # city_name - matching city name within weather tables
  # byYearly - 0/1 depending on overall/yearly
  # additional preset parameters that can be toggled off while retrieving data
  # RETURNS: Dictionary pairing keys with array containing tuples {humidity: [(YYYY*, AVG, MIN, MAX),...],...}
  # * field is only included when byYearly = 1
  # def get_weather_details_by_city(self, city_name, byYearly, humidity = False, pressure = False, temperature = False, winddirection = False, windspeed = False, all_details = False, = False):
  def get_weather_details_by_city(self, city_name, byYearly, weather_type):
    cursor = self.conn.cursor()
    youForgotTheTechnique = ["","CAST(extract(year from dt) as INT) as year,"]
    techniqueTechnique = [""," GROUP BY extract(year from dt) ORDER BY year ASC"]

    ret_obj = {}
    if weather_type == 'humidity' or weather_type == 'all':
      cursor.execute("SELECT %sROUND(AVG(%s),1), ROUND(MIN(%s),1), ROUND(MAX(%s),1) FROM humidity%s", (AsIs(youForgotTheTechnique[byYearly]),AsIs(city_name),AsIs(city_name),AsIs(city_name),AsIs(techniqueTechnique[byYearly]),))
      ret_obj["humidity"] = cursor.fetchall()

    if weather_type == 'pressure' or weather_type == 'all':
      cursor.execute("SELECT %sROUND(AVG(%s),1), ROUND(MIN(%s),1), ROUND(MAX(%s),1) FROM pressure%s", (AsIs(youForgotTheTechnique[byYearly]),AsIs(city_name),AsIs(city_name),AsIs(city_name),AsIs(techniqueTechnique[byYearly]),))
      ret_obj["pressure"] = cursor.fetchall()

    if weather_type == "temperature" or weather_type == 'all':
      cursor.execute("SELECT %sROUND(AVG(%s),1), ROUND(MIN(%s),1), ROUND(MAX(%s),1) FROM temperature%s", (AsIs(youForgotTheTechnique[byYearly]),AsIs(city_name),AsIs(city_name),AsIs(city_name),AsIs(techniqueTechnique[byYearly]),))
      ret_obj["temperature"] = cursor.fetchall()

    if weather_type == "winddirection" or weather_type == 'all':
      cursor.execute("SELECT %sROUND(AVG(%s),1), ROUND(MIN(%s),1), ROUND(MAX(%s),1) FROM winddirection%s", (AsIs(youForgotTheTechnique[byYearly]),AsIs(city_name),AsIs(city_name),AsIs(city_name),AsIs(techniqueTechnique[byYearly]),))
      ret_obj["winddirection"] = cursor.fetchall()

    if weather_type == "windspeed" or weather_type == 'all':
      cursor.execute("SELECT %sROUND(AVG(%s),1), ROUND(MIN(%s),1), ROUND(MAX(%s),1) FROM windspeed%s", (AsIs(youForgotTheTechnique[byYearly]),AsIs(city_name),AsIs(city_name),AsIs(city_name),AsIs(techniqueTechnique[byYearly]),))
      ret_obj["windspeed"] = cursor.fetchall()
    return ret_obj

  # Close connection
  def close(self):
    self.conn.cursor().close()
    self.conn.close()

def main():
  print("-->REMOVE WHEN USING WITH APPLICATION.PY")
  print("== RUNNING QUICK TESTS  ==")
  print("Test #1: Overall Consumption Details")
  consumption = Database()
  print(consumption.get_consumption_details_by_city("philadelphia","pjm_load", 0))
  print("\nTest #2: Consumption Details by Year")
  consumption = Database()
  print(consumption.get_consumption_details_by_city("philadelphia","pjm_east", 1))
  print("\nTest #3: All Weather Details by Year")
  print(consumption.get_weather_details_by_city("philadelphia", 1, weather_type = 'all'))
  print("\nTest #4: Overall Temperature Details")
  print(consumption.get_weather_details_by_city("philadelphia", 0, weather_type = "temperature"))
  consumption.close()

if __name__ == '__main__': main()
