

---

# URLhaus ETL Data Connector

This project extracts threat intelligence data from the **URLhaus API**, transforms it, and loads it into **MongoDB**.
It is designed for ingesting malicious URL data for cybersecurity monitoring and analysis.

---

## 1. Clone or Download the Project

```bash
git clone https://github.com/your-username/urlhaus-etl.git
cd urlhaus-etl
```

*(If you don’t have a repository, place your `etl.py` file and `.env` file in the same folder.)*

---

## 2. Create and Activate a Virtual Environment

**Windows (CMD or PowerShell)**:

```bash
python -m venv venv
venv\Scripts\activate
```

**macOS / Linux**:

```bash
python3 -m venv venv
source venv/bin/activate
```

---

## 3. Install Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

If you don’t yet have `requirements.txt`, create it with:

```
pymongo==4.10.1
python-dotenv==1.0.1
requests==2.32.3
```

---
Replace your section **4** with this:

---

## 4. Create and Configure `.env` File

The `.env` file stores **private configuration values** used by the ETL script.
Place it in the **root folder** of the project (next to `etl.py`).

**Steps to create it:**

1. Create a new file named `.env` in the project folder.
2. Open it with a text editor.
3. Add the following **variables** (fill in your own values locally; do **not** commit them):

```env
API_URL=<YOUR_URLHAUS_API_ENDPOINT>
MONGO_URI=<YOUR_MONGODB_CONNECTION_STRING>
DB_NAME=<YOUR_DATABASE_NAME>
COLLECTION=<YOUR_COLLECTION_NAME>
```



## 5. Run the ETL Script

```bash
python etl.py
```

The script will:

1. Request the **latest malicious URL data** from the URLhaus API
2. Transform the JSON into a clean structured format
3. Insert (or update) the records into your MongoDB collection

---

## 6. Verify Data in MongoDB

If MongoDB is running locally:

```bash
mongo
use threatintel
db.url_intel.find().pretty()
```



