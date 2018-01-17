/* jshint node: true */
"use strict";
var mysql = require("mysql");

var pool;

var db = {

  getPool: function(conf) {
    if (pool) {
      return pool;
    }
    //create a connection pool, assign and return
    //
    var conn = mysql.createPool(conf);
    conn.on("error", function(err) {
      console.log("dberror: " + err);
    });
    conn.on("connection", function(err) {
      console.log("pulling out a connection now");
    });
    pool = conn;
    return pool;
  },

  insert: function(conf, sql, params, callback) {
    var conn = db.getPool(conf).getConnection(function(err, connection) {
      if (err) {
        if (connection) {
          connection.release();
        }
        throw err;
      }
      if (err)
        return callback(true, "insert: db connection failed " + err.message);

      connection.query(sql, params, function(err, res) {
        connection.release();
        if (err) return callback(err, err.message);
        return callback(err, res.insertId);
      });
    });
  },

  query: function(conf, sql, params, callback) {
    var conn = db.getPool(conf).getConnection(function(err, connection) {
      if (err) {
        if (connection) {
          connection.release();
        }

        throw err;
      }
      connection.query(sql, params, function(err, rows, fields) {
        connection.release();
        return callback(err, rows, fields);
      });
      connection.on("error", function(err) {
        throw err;
        return;
      });
    });
  },

  queryArray: function(conf, sql, params, callback) {
    db.query(conf, sql, params, function(err, rows, fields) {
      var arr = [];
      if (!err) for (var i in rows) arr.push(rows[i][fields[0].name]);
      return callback(err, arr);
    });
  },

  getColumnNames: function(conf, tableName, callback) {
    var sql = "show columns from " + tableName;
    db.query(conf, sql, [], function(err, rows, fields) {
      if (err) return callback(err, rows);
      var arr = [];
      for (var i in rows) arr.push(rows[i].Field);
      return callback(err, arr);
    });
  }
};

module.exports = db;
