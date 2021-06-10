// https://node-postgres.com/

require('dotenv').config();
const _ = require('underscore');
const { Pool } = require('pg');

const connectionString = process.env.POSTGRESQL_URI;

const pool = new Pool({
  connectionString,
});


(async () => {
  let tables = await getTables();

  if(!_.any(tables, (t) => t.table_name === 'films')){
    await createTable();
    console.log('Created table films')
  }
  console.log(tables);

  console.log('----------');
  await insert('UA' + Math.floor(Math.random()*1000));

  let q = await query();
  console.log(q);

  await pool.end();
})();

function getTables(){
  return new Promise((resolve, reject) => {
    pool.query( 
      'SELECT * FROM information_schema."tables" ' +
      'where table_type = \'BASE TABLE\' and table_schema not in (\'pg_catalog\', \'information_schema\');',
      (err, res) => {
        if(err){
          return reject(err);
        }
        resolve(res.rows);
      });
  });
}

function createTable(){
  return new Promise((resolve, reject) => {
    pool.query( 
      'CREATE TABLE films (' +
      '  code        char(5) CONSTRAINT firstkey PRIMARY KEY,' +
      '  title       varchar(40) NOT NULL,' +
      '  did         integer NOT NULL,' +
      '  date_prod   date,' +
      '  kind        varchar(10),' +
      '  len         interval hour to minute' +
    ');',
      (err, res) => {
        if(err){
          return reject(err);
        }
        resolve(res.rows);
      });
  });
}

function insert(code){
  return new Promise((resolve, reject) => {
    pool.query( 
      'INSERT INTO films VALUES' +
      "($1, 'Bananas', 105, '1971-07-13', 'Comedy', '82 minutes');",
      [code],
      (err, res) => {
        if(err){
          return reject(err);
        }
        resolve(res.rows);
      });
  });
}

function query(){
  return new Promise((resolve, reject) => {
    pool.query( 
      'SELECT * FroM films',
      (err, res) => {
        if(err){
          return reject(err);
        }
        resolve(res.rows);
      });
  });
}