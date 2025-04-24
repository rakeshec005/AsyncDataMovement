
# 📄 **Async CDR Data Movement Script Documentation**

This Python script is designed to:
- Fetch CDR (Call Detail Record) data from a PostgreSQL database.
- Send the data in batches to a specified API endpoint.
- Handle failures, retries, and send alert emails using SMTP if any domain is not configured.

---

## 🧰 **Tech Stack & Dependencies**
- Python 3.8+
- `asyncio`, `aiosmtplib`, `aiohttp`, `asyncpg`, `multiprocessing`, `loguru`
- PostgreSQL for data storage
- External SMTP server (e.g., Gmail) for email notifications

---

## ⚙️ **Configuration File (`config.json`)**
Expected fields in the `config.json` file:

```json
{
  "Database": {
    "User": "",
    "Password": "",
    "Database": "",
    "Host": "",
    "Port": ""
  },
  "lite_api": {
    "base_url": "",
    "auth_key": ""
  },
  "sendOneDayData": "true/false",
  "timerange": "true/false",
  "startdatetime": "YYYY-MM-DD HH:MM:SS",
  "enddatetime": "YYYY-MM-DD HH:MM:SS",
  "isBroadcast": "true/false",
  "sendDataToCustomDomain": "true/false",
  "customDomain": "optional_domain_name",
  "servicename": "service_name",
  "smtpconfig": {
    "host": "",
    "port": 587,
    "username": "",
    "password": "",
    "receiveremail": ["team@example.com"]
  }
}
```

---

## 📦 **Project Structure**
```
├── script.py
├── config.json
└── logs/
    └── data_movement_YYYY-MM-DD.log
```

---

## 🛠️ **Main Functionalities**

### 🔄 `main()`
- Entrypoint of the script.
- Determines time range from config.
- Fetches total CDRs within the range.
- Triggers data batching and transmission.

### 🧠 `fetch_data()`
- Executes asynchronous SQL query to fetch CDRs from the database.
- Returns a list of JSON records.

### 📊 `get_total_records()`
- Returns the total number of records based on the selected time window.

### 📤 `send_to_api()`
- Sends each batch of data to a remote API.
- Handles retry logic (max 3 attempts).
- Sends alert email if a domain is not configured.

### 📧 `send_email()`
- Sends an alert email using SMTP with TLS via `aiosmtplib`.

### 🧪 `process_batches()`
- Handles batching and parallel API submissions using asyncio and aiohttp.

---

## 🚨 **Logging**
- Logs are written to `logs/data_movement_<date>.log`
- Includes detailed logs for:
  - Config load
  - Batch processing
  - API responses
  - Email status

---

## 🐞 **Error Handling**
- Database errors include line number and filename.
- Email or SMTP failures are logged with complete exception details.
- API retry logic includes exponential backoff.

---

## 📬 **SMTP Notes**
If using Gmail SMTP:
- Ensure App Passwords or OAuth2 is configured.
- Avoid sending bulk mails too frequently (Gmail throttles).
- Errors like `451 4.3.0` are temporary; implement retry logic.

---

## ▶️ **Running the Script**

```bash
python script.py
```

Ensure:
- Python 3.8+
- All dependencies installed via pip
- `config.json` is correctly populated

---

## 📌 **Tips**
- Test SMTP and API separately before full runs.
- Tune `BATCH_SIZE`, `NUM_WORKERS`, and `API_CONCURRENCY` based on your system and endpoint performance.
- Monitor the logs for any failed batches or unconfigured domains.

---
