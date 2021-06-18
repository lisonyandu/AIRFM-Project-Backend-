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




@app.route("/api/top40")
@cross_origin()
def getTop40():
    query_parameters = request.args
    date = query_parameters.get("date")

    query = "select instrument as `name`, `Gross Market Capitalisation` as value from `index_constituents`"

    if date:
        query += "where date='{0}'".format(date)

    query += " order by `Gross Market Capitalisation` desc limit 40;"
    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp


@app.route("/api/shares")
@cross_origin()
def getAShare():
    query_parameters = request.args
    share = query_parameters.get("share")

    query = "select `date` as name, price as value from equity_data"

    if share:
        query += " where instrument='{0}';".format(share)

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp


@app.route("/api/index-constituents")
@cross_origin()
def indexConstituents():
    query_parameters = request.args
    date = query_parameters.get("date")
    indexCode = query_parameters.get("indexCode")

    query = "select instrument as `name`, `Gross Market Capitalisation` as value from `index_constituents`"

    if date:
        query += " where date='{0}'".format(date)
    if indexCode:
        query += " AND `{0} New` is not null".format(indexCode)

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp


def index():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM index_constituents")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
