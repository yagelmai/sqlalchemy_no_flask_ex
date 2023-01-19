
from sqlalchemycollector import setup, MetisInstrumentor, PlanCollectType
import time
from numpy import genfromtxt
import csv
from datetime import datetime
from sqlalchemy.sql.expression import text
from sqlalchemy import Column, Integer, Float, Date, String, and_, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
import pandas as pd




# Create the engine
engine = create_engine("postgresql://yagel:postgres@localhost:5432/sql_no_flask")
Session = sessionmaker(bind=engine, autoflush=True)
db = Session()


def createDB():
    try:
        csv_tablename= "par_exe_csv"
        csv_columns = ["id", "cell_name", "test", "flopcnt"]
        # Connect to the database
        connection = engine.connect()

        # Perform a query
        sqlQueryCreate = 'DROP TABLE IF EXISTS '+ csv_tablename + ";\n"
        sqlQueryCreate += 'CREATE TABLE ' + csv_tablename + "("

        for column in csv_columns:
            sqlQueryCreate += column + " TEXT,\n"

        sqlQueryCreate = sqlQueryCreate[:-2]
        sqlQueryCreate += ");"

        result = connection.execute(sqlQueryCreate)

        mapfile_tablename= "par_exe_mapfile"
        mapfile_columns = ["id", "dlvrloadgndvsense0", "dlvrloadgndvsense02"]
        # Connect to the database

        # Perform a query
        sqlQueryCreate = 'DROP TABLE IF EXISTS '+ mapfile_tablename + ";\n"
        sqlQueryCreate += 'CREATE TABLE ' + mapfile_tablename + "("

        for column in mapfile_columns:
            sqlQueryCreate += column + " TEXT,\n"

        sqlQueryCreate = sqlQueryCreate[:-2]
        sqlQueryCreate += ");"
        result = connection.execute(sqlQueryCreate)

        joined_tablename= "csvjoinmap"
        joined_columns = ["id", "cell_name", "dlvrloadgndvsense0"]
        # Connect to the database

        # Perform a query
        sqlQueryCreate = 'DROP TABLE IF EXISTS '+ joined_tablename + ";\n"
        sqlQueryCreate += 'CREATE TABLE ' + joined_tablename + "("

        for column in joined_columns:
            sqlQueryCreate += column + " TEXT,\n"

        sqlQueryCreate = sqlQueryCreate[:-2]
        sqlQueryCreate += ");"
        result = connection.execute(sqlQueryCreate)

        # Close the connection
        connection.close()
        print("created tables successfully!")
    except Exception as e:
        print(e)

# app = Flask(__name__)
# with app.app_context():
#     app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://yagel:postgres@localhost:5432/sqlalchemydata"
#     app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#
#     db = SQLAlchemy(app)
#     migrate = Migrate(app, db)

    # instrumentation: MetisInstrumentor = setup('power_csvs-web-server',
    #                                            api_key='<API_KEY>',
    #                                            service_version='1.1'
    #                                            )
    #
    # instrumentation.instrument_app(app, db.get_engine())

Base = declarative_base()

class PowerCsv(Base):
    __tablename__ = 'par_exe_csv'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cell_name = Column(String)
    test = Column(String)
    flopcnt = Column(Integer())
    # map_file = db.relationship('PowerMapfile', primaryjoin='PowerMapfile.csv_id==PowerCsv.id', backref='par_exe_csv')

    def __init__(self, cell_name, test, flopcnt):
        self.cell_name = cell_name
        self.test = test
        self.flopcnt = flopcnt
        self.id = PowerCsv.get_next_id()

    _current_id = 0

    @classmethod
    def get_next_id(self):
        self._current_id += 1
        return self._current_id

    def __repr__(self):
        return f"<par_exe_csv-{self.cell_name}>"

class PowerMapfile(Base):
    __tablename__ = 'par_exe_mapfile'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dlvrloadgndvsense0 = Column(String)
    dlvrloadgndvsense02 = Column(String)
    # csv_id = Column(Integer, db.ForeignKey('par_exe_csv.id'))

    def __init__(self, dlvrloadgndvsense0, dlvrloadgndvsense02):
        self.dlvrloadgndvsense0 = dlvrloadgndvsense0
        self.dlvrloadgndvsense02 = dlvrloadgndvsense02
        self.id = PowerCsv.get_next_id()

    _current_id = 0

    @classmethod
    def get_next_id(self):
        self._current_id += 1
        return self._current_id

    def __repr__(self):
        return f"<par_exe_mapfile-{self.cell_name}>"

class csvjoinmap(Base):
    __tablename__ = 'csvjoinmap'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cell_name = Column(String)
    dlvrloadgndvsense0 = Column(String)
    # par_exe_csv_id = Column(Integer, ForeignKey('par_exe_csv.id'))
    # par_exe_csv = relationship("ParExeCsv", back_populates="par_exe_mapfile")

    def __init__(self, cell_name, dlvrloadgndvsense0):
        self.cell_name = cell_name
        self.dlvrloadgndvsense0 = dlvrloadgndvsense0
        self.id = PowerCsv.get_next_id()

    _current_id = 0

    @classmethod
    def get_next_id(self):
        self._current_id += 1
        return self._current_id

    def __repr__(self):
        return f"<csvjoinmap-{self.id}>"

# PowerCsv.par_exe_mapfile = relationship("PowerMapfile", order_by=PowerMapfile.id, back_populates="PowerCsv")

# @app.route('/')
def hello():
    return {"hello": "world"}

def Load_Data(file_name):
    # we can add , max_rows=100000 to the function below for testing
    data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda s: str(s)}, max_rows=1000000)
    return data.tolist()

# def Load_Data(file_name):
#     from itertools import islice, chain
#     from operator import itemgetter
#     with open(file_name) as f:
#         r = csv.reader(f)
#         data = np.fromiter(chain.from_iterable(map(itemgetter(0, 4, 10),
#                                                   islice(r, 4, 10))), dtype=float).reshape(6, -1)
#     return data.tolist()

# @app.route('/load_csv')
def loadCsv():
    t=time.time()
    try:
        file_name = '../out/par_exe.power.csv'
        data = Load_Data(file_name)
        for i in data:
            record = PowerCsv(**{
                # 'date': datetime.strptime(i[0], '%d-%b-%y').date(),
                'cell_name': i[0][2:-1],
                'test': i[1],
                'flopcnt': i[4],
            })
            db.add(record)  # Add all the records
            # db.flush()
            # db.refresh(record)

        db.commit()  # Attempt to commit all the records
        return {"res": "sucess"}
    except Exception as e:
        print(e)
        db.rollback()  # Rollback the changes on error
        return {"res": "failed"}
    finally:
        db.close()  # Close the connection
        print("Time elapsed: " + str(time.time() - t) + " s.")

# @app.route('/load_mapfile')
def loadMapfile():
    t = time.time()
    try:
        file_name = '../out/par_exe.rtl.mapfile'
        data = Load_Data(file_name)
        for i in data:
            record = PowerMapfile(**{
                # 'date': datetime.strptime(i[0], '%d-%b-%y').date(),
                'dlvrloadgndvsense0': i[0][2:-1],
                'dlvrloadgndvsense02': i[1]
            })
            db.add(record)  # Add all the records
            # db.flush()
            # db.refresh(record)

        db.commit()  # Attempt to commit all the records
        return {"res": "sucess"}
    except Exception as e:
        print(e)
        db.rollback()  # Rollback the changes on error
        return {"res": "failed"}
    finally:
        db.close()  # Close the connection
        print("Time elapsed: " + str(time.time() - t) + " s.")

# @app.route('/join')
def joinTables():
    t = time.time()
    try:
        # Join the two tables
        result = db.query(PowerCsv.cell_name, PowerMapfile.dlvrloadgndvsense0).filter(
            PowerCsv.cell_name.like('%' + PowerMapfile.dlvrloadgndvsense0 + '%')).limit(100).with_labels().all()

        # Insert the results into the "res" table
        for res in result:
            db.add(csvjoinmap(cell_name=res[0], dlvrloadgndvsense0=res[1]))
        db.commit()
        return {"res": "sucess"}
    except Exception as e:
        print(e)
        db.rollback()  # Rollback the changes on error
        return {"res": "failed"}
    finally:
        db.close()
        print("Time elapsed: " + str(time.time() - t) + " s.")

# @app.route('/createDF')
def CreateDf():
    try:
        df = pd.read_sql_table('csvjoinmap', engine)
        return df.to_json()
    except Exception as e:
        print(e)
        return {"res": "failed", "reas": str(e)}

# @app.route('/run_all')
def runAll():
    f_time = time.time()
    createDB()
    print("load csv")
    loadCsv()
    print("load mapfile")
    loadMapfile()
    s_time = time.time()
    print("load join tables")
    joinTables()
    t_time = time.time()
    print("create DF")
    df = CreateDf()
    l_time = time.time()
    print("successful finished! at time: ")
    print(l_time - f_time)
    print("copy csv and mapfile to tables: ")
    print(s_time - f_time)
    print("join: ")
    print(t_time - s_time)
    print("create DF: ")
    print(l_time - t_time)
    return df

if __name__ == '__main__':
    df = runAll()
    print("the df", str(df))