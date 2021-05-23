import numpy as np
import datetime as dt
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

Base = automap_base()
Base.prepare(engine, reflect=True)

Measurement = Base.classes.measurement
Station = Base.classes.station

app = Flask(__name__)

@app.route("/")
def home():
    return (
        f"Available routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date<br/>"
        f"/api/v1.0/start_date/end_date"
    )

@app.route("/api/v1.0/precipitation")
def precip():
    session = Session(engine)
    recent_date = session.query(func.max(Measurement.date)).first()[0]       
    query_date = dt.datetime.strptime(recent_date, '%Y-%m-%d') - dt.timedelta(days=365)
    p_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date>=query_date).all()
    session.close()

    precipitation = []
    for date, prcp in p_data:
    	prcp_dict = {}
    	prcp_dict["date"] = date
    	prcp_dict["prcp"] = prcp
    	precipitation.append(prcp_dict)

    return jsonify(precipitation)


    
@app.route("/api/v1.0/stations")
def stations():
	session = Session(engine)
	results = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()
	session.close()

	results = pd.DataFrame(results)

	return jsonify(results.to_dict(orient='records'))



@app.route("/api/v1.0/tobs")
def tobs():
	session = Session(engine)

	recent_date = session.query(func.max(Measurement.date)).first()[0]
	query_date = dt.datetime.strptime(recent_date, '%Y-%m-%d') - dt.timedelta(days=365)

	station_count = session.query(Station.id, Measurement.station, Station.name, func.count(Measurement.station)).\
	filter(Measurement.station == Station.station).\
	group_by(Measurement.station).\
	order_by(func.count(Measurement.station).desc()).all()

	stat_name = station_count[0][1]

	results = session.query(Measurement.date, Measurement.tobs).\
	filter(Measurement.date>=query_date).\
	filter(Measurement.station == stat_name).all()

	session.close()

	results = pd.DataFrame(results)

	return jsonify(results.to_dict(orient='records'))
    


@app.route("/api/v1.0/<start>")
def start(start):
	session = Session(engine)

	recent_date = session.query(func.max(Measurement.date)).first()[0]

	results = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
	filter(Measurement.date >= dt.datetime.strptime(start, '%Y-%m-%d')).\
	filter(Measurement.date <= dt.datetime.strptime(recent_date, '%Y-%m-%d')).all()

	session.close()

	result_list = list(np.ravel(results))
	result_dict = {"min_temp": result_list[0], "max_temp": result_list[1], "avg_temp": round(result_list[2],2)}

	return jsonify(result_dict)

    


@app.route("/api/v1.0/<begin>/<end>")
def start_end(begin, end):
	session = Session(engine)

	results = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
	filter(Measurement.date >= dt.datetime.strptime(begin, '%Y-%m-%d')).\
	filter(Measurement.date <= dt.datetime.strptime(end, '%Y-%m-%d')).all()

	session.close()

	result_list = list(np.ravel(results))
	result_dict = {"min_temp": result_list[0], "max_temp": result_list[1], "avg_temp": round(result_list[2],2)}

	return jsonify(result_dict)

    
if __name__ == "__main__":
    app.run(debug=True)