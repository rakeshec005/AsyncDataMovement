from email.message import EmailMessage
import json
import os
import math
import sys
import aiosmtplib
import asyncpg
import asyncio
import aiohttp
import multiprocessing
from datetime import datetime, timedelta
from loguru import logger
import time

# Logger Configuration
logger.remove()
logger.add(
    "logs/data_movement_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="7 days",
    level="DEBUG",
    format="{time} [{level}] {message}",
)

# Load Config
with open('config.json', 'r') as f:
    CONFIG = json.load(f)

DB_CONFIG = CONFIG["Database"]
API_DETAILS = CONFIG["lite_api"]
SEND_ONE_DAY_DATA = CONFIG["sendOneDayData"]
TIME_RANGE = CONFIG["timerange"]
BATCH_SIZE = 100  # Adjust batch size
NUM_WORKERS = min(10, multiprocessing.cpu_count() * 2)
API_CONCURRENCY = 10  # API requests in parallel
IS_BROADCAST = CONFIG.get("isBroadcast", "false")  
SEND_DATA_TO_CUSTOM_DOMAIN = CONFIG.get("sendDataToCustomDomain", "false")
SERVICE_NAME = CONFIG.get("servicename")
SMTP_CONFIG = CONFIG.get("smtpconfig")

logger.debug(f"Config Data loaded successfully ::  {CONFIG}")
# Create PostgreSQL Async Connection Pool
async def create_db_pool():
    return await asyncpg.create_pool(
        user=DB_CONFIG["User"],
        password=DB_CONFIG["Password"],
        database=DB_CONFIG["Database"],
        host=DB_CONFIG["Host"],
        port=DB_CONFIG["Port"],
        min_size=1,
        max_size=20,
    )

async def send_email(message: EmailMessage):
    
    """ Sends an email using SMTP """
    if SMTP_CONFIG["host"] == "" or SMTP_CONFIG["username"] == "" or SMTP_CONFIG["password"] == "" or SMTP_CONFIG["port"] == "":
        logger.error("SMTP configuration is incomplete. Please update smtpconfig.")
        return

    if SMTP_CONFIG["receiveremail"] == "":
        logger.error("Receiver email is not configured. Please update smtpconfig.")
        return

    receiver_email = SMTP_CONFIG["receiveremail"]
    # Join all the emails into a single string separated by commas
    message["To"] = ", ".join(receiver_email)
    
    logger.debug(f"Email message: {message}")

    try:
        await aiosmtplib.send(
            message,
            hostname=SMTP_CONFIG["host"],
            port=SMTP_CONFIG["port"],
            username=SMTP_CONFIG["username"],
            password=SMTP_CONFIG["password"],
            use_tls=False,
            start_tls=True
        )
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(f"Error sending email: error::{e}, line::{exc_tb.tb_lineno}")
        

# Async API Call with Retry Mechanism
# @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=1, max=5))
async def send_to_api(payloads, session):
    """ Asynchronously sends data to API with retry logic """
    logger.debug(f"Sending {payloads} records to API")
    url = f"{API_DETAILS['base_url']}insertcdr?auth_key={API_DETAILS['auth_key']}"
    for attempt in range(1, 4):
        # async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            async with session.post(url, json={"cdrdata": payloads, "servicename": SERVICE_NAME}) as response:
                domain_list = []
                if response.status == 200:
                    resp_json = await response.json()
                    if resp_json.get("msg", "").lower() == "domain not configured":
                        logger.warning("API returned 'Domain not configured', sending alert email...")
                        logger.info(f"Domain not configured for Domainname, please configure the domain & re-run the script. \n DOMAINNAME==>{resp_json.get('domain')}")
                        
                        if resp_json.get("domain") not in domain_list:
                            domain_list.append(resp_json.get("domain"))
                            message = EmailMessage()
                            message["From"] = SMTP_CONFIG["username"]
                            message["Subject"] = "Error Alert - VERVE-ContaqueLite Data Movement"
                            message.set_content(
                                f"""\
                            Dear Team,

                            {resp_json.get('domain')} is not configured. Please create the domain using the Create Domain API and re-run the script.
                            
                            Regards,  
                            Dev Team
                            """
                            )
                            
                            await send_email(message)
                            logger.debug(f"Batch failed due to domain not configured {resp_json.get('domain')}")
                            return True
                            
                    logger.info(f"Successfully sent {len(payloads)} records")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Attempt {attempt}: API request failed with status {response.status}: {error_text}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"Attempt {attempt}: Network error: {e}")
            
        if attempt < 3:
            logger.info(f"Retrying batch... Attempt {attempt + 1}/3")
            await asyncio.sleep(2**attempt)  # Exponential backoff (2s, 4s)
            
    logger.error(f"Failed to send batch after 3 attempts. Moving to next batch ===> Failed Batch :::: {payloads}")
    return False
 

async def fetch_data(pool, start_time, end_time, offset, limit, is_broadcast):
    if SEND_DATA_TO_CUSTOM_DOMAIN.lower() == "true":
        CUSTOM_DOMAIN = CONFIG.get("customDomain", None)
        if not CUSTOM_DOMAIN:
            logger.error("Custom domain is not provided in the configuration")
            return
        
    """ Fetches data from PostgreSQL asynchronously using asyncpg """
    try:
        async with pool.acquire() as connection:
                # Base query
                query = """
                    SELECT cd.name,
                        ccc.accountcode,
                        ccc.dnis,
                        ccc.phonenumber,
                        ccc.callstatus,
                        ccc.callduration,
                        ccc.hangupcause,
                        ccc.calltype,
                        ccc.recordentrydate
                    FROM cr_conn_cdr AS ccc
                    LEFT JOIN ct_domain AS cd ON ccc.domainid = cd.id
                    WHERE ccc.recordentrydate BETWEEN $1 AND $2
                """

                params = [start_time, end_time]

                if is_broadcast.lower() == "true":
                    query += " AND ccc.accountcode LIKE 'IT%' AND ccc.linkedaccountcode LIKE 'BR%'"

                query += " ORDER BY ccc.recordentrydate ASC LIMIT $3 OFFSET $4;"
                params.extend([limit, offset])

                # Execute the query
                records = await connection.fetch(query, *params)

        return [{
        "domainname": CUSTOM_DOMAIN if SEND_DATA_TO_CUSTOM_DOMAIN.lower() == "true" and CUSTOM_DOMAIN else row['name'],
        "callid": row['accountcode'],
        "cli": row['dnis'],
        "phonenumber": row['phonenumber'],
        "status": row['callstatus'],
        "callduration": row['callduration'],
        "hangupcause": row['hangupcause'],
        "calltype": row['calltype'],
        "entrydate": row['recordentrydate'].strftime("%Y-%m-%d %H:%M:%S")
        } for row in records]
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        connection.rollback()
        logger.error(f"Error fetching data: error::{e}, line::{exc_tb.tb_lineno}")

# Fetch Total Records Count
async def get_total_records(pool, start_time, end_time, is_broadcast):
    """ Returns the total number of records in the given time range """
    async with pool.acquire() as connection:
        query = "SELECT COUNT(*) FROM cr_conn_cdr WHERE recordentrydate BETWEEN $1 AND $2"
        params = [start_time, end_time]

        if is_broadcast.lower() == "true":
            query += " AND accountcode LIKE 'IT%' AND linkedaccountcode LIKE 'BR%'"

        total_records = await connection.fetchval(query, *params)
    return total_records


async def process_batches(pool, start_time, end_time, total_records):
    """ Fetches data in chunks & sends it to API asynchronously """
    tasks = []
    logger.debug(f"pool size: {pool}")
    for offset in range(0, total_records, BATCH_SIZE):
        logger.debug(f"Fetching records from {offset} to {offset + BATCH_SIZE}")
        # tasks.append(limited_fetch(pool, start_time, end_time, offset, BATCH_SIZE))
        task = fetch_data(pool, start_time, end_time, offset, BATCH_SIZE, IS_BROADCAST)
        tasks.append(task)

    for task in asyncio.as_completed(tasks):
        logger.debug(f"Processing task {task}")
        try:
            batch = await task
            if batch:
                logger.debug(f"Sending {len(batch)} records to API and {task} is completed")
                connector = aiohttp.TCPConnector(ssl=False)  # disable SSL verification for testing
                timeout = aiohttp.ClientTimeout(total=30)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    try:
                        await send_to_api(batch, session)
                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout error while sending batch {task}. Skipping to next batch.")
        except Exception as e:
            logger.error(f"Error processing task {task}: {e}")
  
# Main Execution
async def main():
    """ Main entry point for script execution """
    # Create DB connection pool
    pool = await create_db_pool()

    enddatetime = datetime.now()

    if SEND_ONE_DAY_DATA.lower() == 'true' and TIME_RANGE.lower() == 'false':
        fromdatetime = (enddatetime - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        enddatetime = enddatetime.replace(hour=0, minute=0, second=0, microsecond=0)
    elif SEND_ONE_DAY_DATA.lower() == 'false' and TIME_RANGE.lower() == 'true':
        if not CONFIG["startdatetime"] or not CONFIG["enddatetime"]:
                logger.error("Invalid configuration :: startdatetime or enddatetime is missing")
                return
            
        if datetime.strptime(CONFIG["startdatetime"], '%Y-%m-%d %H:%M:%S') > datetime.strptime(CONFIG["enddatetime"], '%Y-%m-%d %H:%M:%S'):
            logger.error("Invalid configuration :: startdatetime cannot be greater than enddatetime")
            return
        fromdatetime = datetime.strptime(CONFIG["startdatetime"], '%Y-%m-%d %H:%M:%S')
        enddatetime = datetime.strptime(CONFIG["enddatetime"], '%Y-%m-%d %H:%M:%S')
    else:
        logger.error("Invalid configuration :: SEND_ONE_DAY_DATA and TIME_RANGE cannot be true/false at the same time")
        return

    # Fetch total records
    total_records = await get_total_records(pool, fromdatetime, enddatetime, IS_BROADCAST)
    logger.info(f"Total Records: {total_records} :: from {fromdatetime} to {enddatetime}")

    if total_records > 0:
        # Process the batches of data asynchronously
        await process_batches(pool, fromdatetime, enddatetime, total_records)
    else:
        logger.info("No data to process")

    logger.info("Data processing completed")

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
