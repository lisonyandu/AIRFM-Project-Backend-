import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin


@app.route("/api")
@cross_origin()
def users():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM jse_index")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp


@app.route("/comparison")
@cross_origin()
def comparison():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("Select Beta, DATE_FORMAT(Date,'%d/%m/%Y') as Date, Instrument from ba_beta_output order by  Instrument;")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp


@app.route("/api/beta")
@cross_origin()
def betaTable():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("Select id,DATE_FORMAT(Date,'%d/%m/%Y') as Date,Instrument,`Data Points`, Beta,`p-Value Beta`,`Total Risk`,`Unique Risk`, R2 FROM ba_beta_output where `Index` = 'J200';")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp

@app.route("/api/betaJ203")
@cross_origin()
def betaJ203():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("Select id,DATE_FORMAT(Date,'%d/%m/%Y') as Date,Instrument,`Data Points`, Beta,`p-Value Beta`,`Total Risk`,`Unique Risk`, R2 FROM ba_beta_output where `Index` = 'J203';")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp

@app.route("/api/betaJ250")
@cross_origin()
def betaJ250():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("Select id,DATE_FORMAT(Date,'%d/%m/%Y') as Date,Instrument,`Data Points`, Beta,`p-Value Beta`,`Total Risk`,`Unique Risk`, R2 FROM ba_beta_output where `Index` = 'J250';")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp

@app.route("/api/betaJ257")
@cross_origin()
def betaJ257():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("Select id,DATE_FORMAT(Date,'%d/%m/%Y') as Date,Instrument,`Data Points`, Beta,`p-Value Beta`,`Total Risk`,`Unique Risk`, R2 FROM ba_beta_output where `Index` = 'J257';")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp

@app.route("/api/timeplot1")
@cross_origin()
def timeplot1():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("Select DATE_FORMAT(Date,'%d/%m/%Y') as Date,`R2`as Rate, Beta,`Unique Risk` as Risk FROM ba_beta_output where `Instrument` = 'ACE' AND `Index` = 'J200';")
    rows = cursor.fetchall()
    resp = jsonify(rows)
    resp.status_code = 200

    return resp


@app.route("/api/synthetic")
@cross_origin()
def synthetic():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM dashboard_backend.synthetic_data")
    rows = cursor.fetchall()
    resp = jsonify(rows)
    resp.status_code = 200

    return resp



@app.route("/api/timeplot2")
@cross_origin()
def timeplot2():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("Select DATE_FORMAT(Date,'%d/%m/%Y') as Date, Beta, Instrument FROM ba_beta_output where  `Index` = 'J200';")
    rows = cursor.fetchall()
    resp = jsonify(rows)
    resp.status_code = 200

    return resp

    
@app.route("/api/timeplot3")
@cross_origin()
def timeplot3():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("Select DATE_FORMAT(Date,'%d/%m/%Y') as Date, `Beta` FROM ba_beta_output where `Instrument` = 'ACG' AND `Index` = 'J200';")
    rows = cursor.fetchall()
    resp = jsonify(rows)
    resp.status_code = 200

    return resp


@app.route("/api/betaJ258")
@cross_origin()
def betaJ258():

    query_parameters = request.args
    date = query_parameters.get("date")

    query = "Select id,DATE_FORMAT(Date,'%d/%m/%Y') as Date,Instrument,`Data Points`, Beta,`p-Value Beta`,`Total Risk`,`Unique Risk`, R2 FROM ba_beta_output where `Index` = 'J258';"

    if date:
        query += "where date='{0}'".format(date)

    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
