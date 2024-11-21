const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
const PORT = 3001;

const NYC_TAXI_DATA_URL = 'https://data.cityofnewyork.us/resource/gkne-dk5s.json';

app.use(cors({
  origin: 'http://localhost:3000'
}));

app.get('/api/taxi-trips', async (req, res) => {
  try {
    const { vendor_id, payment_type, page = 0, pageSize = 10, passenger_count, pickup_datetime_start, pickup_datetime_end} = req.query;

    let query = `
      SELECT 
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude,
        pickup_latitude,
        store_and_fwd_flag,
        dropoff_longitude,
        dropoff_latitude,
        payment_type,
        fare_amount,
        mta_tax,
        tip_amount,
        tolls_amount,
        total_amount,
        imp_surcharge,
        rate_code
    `;

    if (vendor_id || payment_type || passenger_count || pickup_datetime_start || pickup_datetime_end) {
      query += " WHERE ";

      const conditions = [];
      if (vendor_id) {
        conditions.push(`caseless_one_of(vendor_id, "${vendor_id}")`);
      }
      if (payment_type) {
        conditions.push(`caseless_one_of(payment_type, "${payment_type}")`);
      }
      if (passenger_count) {
        const passengerCountInt = parseInt(passenger_count, 10);
        if (!isNaN(passengerCountInt)) {
          conditions.push(`passenger_count = ${passengerCountInt}`);
        } else {
          return res.status(400).json({
            message: "Invalid passenger_count value",
            statusCode: 400
          });
        }
      }

    if (pickup_datetime_start && pickup_datetime_end) {
      const startDate = new Date(pickup_datetime_start);
      const endDate = new Date(pickup_datetime_end);
      
      if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
        conditions.push(`pickup_datetime BETWEEN "${pickup_datetime_start}" AND "${pickup_datetime_end}"`);
      } else {
        return res.status(400).json({
          message: "Invalid pickup_datetime format",
          statusCode: 400
        });
      }
    }

      query += conditions.join(" AND ");
    }

    const offset = page * pageSize;
    query += ` LIMIT ${pageSize} OFFSET ${offset}`;

    const queryParams = { $query: query };

    const response = await axios.get(NYC_TAXI_DATA_URL, { params: queryParams });

    const totalRecords = response.data.length; 
    const totalPages = Math.ceil(totalRecords / pageSize);

    res.json({
      data: response.data,
      pagination: {
        currentPage: parseInt(page, 10),
        pageSize: parseInt(pageSize, 10),
        totalPages
      },
      message: "Success",
      messageError: null,
      statusCode: response.status
    });
  } catch (error) {
    console.error(`Error fetching taxi trip data: ${error}`);
    res.status(500).json({
      data: null,
      pagination: null,
      message: "Failed",
      messageError: error.message,
      statusCode: 500
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
