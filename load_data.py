import psycopg2
import psycopg2.extras

"""
This script assumes the database "final" exists, and that user "final" has full permissions
on the database
"""

class DataLoader:
  connection_string = "host='localhost' dbmame='final' user='final' password='final'"
  data_file = "final-data.csv"
  conn = psycopg2.connect(connection_string, cursor_factory=psycopg2.extras.DictCursor)

  def setUpDb(self):
    # initialize database
    with self.conn.cursor() as cursor:
      with open('groupprojectschema.sql', 'r') as project_schema:
        setup_queries = project_schema.read()
        cursor.execute(setup_queries)

      cursor.commit()

  def importData(self):
    # read in data here
    return

if __name__ == "__main__":
  print("hello world")