require('dotenv').config();
const express = require('express');
const multer = require('multer');
const csvtojson = require('csvtojson');
const { Pool } = require('pg');

const app = express();
const upload = multer({ dest: 'uploads/' });
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

// Convert a CSV file to JSON and save to PostgreSQL
app.post('/upload', upload.single('csvfile'), async (req, res) => {
  try {
    const jsonArray = await csvtojson().fromFile(process.env.CSV_FILE_LOCATION);
    const client = await pool.connect();
    await client.query('BEGIN');
    const insertQuery = 'INSERT INTO users(name, age, address, additional_info) VALUES($1, $2, $3, $4) RETURNING id';
    for (let i = 0; i < jsonArray.length; i++) {
      const { firstName, lastName, age, ...rest } = jsonArray[i];
      const address = {
        line1: rest['address.line1'],
        line2: rest['address.line2'],
        city: rest['address.city'],
        state: rest['address.state'],
      };
      const additionalInfo = { ...rest };
      delete additionalInfo['address.line1'];
      delete additionalInfo['address.line2'];
      delete additionalInfo['address.city'];
      delete additionalInfo['address.state'];
      const { rows } = await client.query(insertQuery, [`${firstName} ${lastName}`, parseInt(age), address, additionalInfo]);
      console.log(`Inserted user with ID ${rows[0].id}`);
    }
    await client.query('COMMIT');
    res.send('Uploaded successfully');
  } catch (err) {
    console.error(err);
    res.status(500).send('Internal server error');
  }
});

// Calculate age distribution of all users and print report on console
app.get('/report', async (req, res) => {
  try {
    const client = await pool.connect();
    const query = 'SELECT COUNT(*) as count, age FROM users GROUP BY age ORDER BY age';
    const { rows } = await client.query(query);
    console.log('Age distribution report:');
    console.log('Age\tCount');
    rows.forEach(row => console.log(`${row.age}\t${row.count}`));
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Internal server error');
  }
});

app.listen(3000, () => {
  console.log('Server started on port 3000');
});
