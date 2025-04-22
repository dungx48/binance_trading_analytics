#!/usr/bin/env python3
import os
import logging
import psycopg2
from datetime import date, timedelta

# ---- Cấu hình kết nối PostgreSQL qua biến môi trường ----
DB_HOST     = os.environ.get('DB_HOST')
DB_PORT     = os.environ.get('DB_PORT')
DB_NAME     = os.environ.get('DB_NAME')
DB_USER     = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

# Thiết lập logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')

def main():
    start = date.today()
    end   = date(2025, 6, 1)
    delta = timedelta(days=1)
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        with conn, conn.cursor() as cur:
            current = start
            inserted = 0
            while current <= end:
                cur.execute("""
                    INSERT INTO dim_date (
                      date_id, day, month, year,
                      quarter, day_of_week, day_name,
                      month_name, is_weekend
                    )
                    SELECT
                      %(d)s,
                      EXTRACT(DAY   FROM %(d)s)::int,
                      EXTRACT(MONTH FROM %(d)s)::int,
                      EXTRACT(YEAR  FROM %(d)s)::int,
                      EXTRACT(QUARTER FROM %(d)s)::int,
                      EXTRACT(ISODOW FROM %(d)s)::int,
                      to_char(%(d)s, 'FMDay'),
                      to_char(%(d)s, 'FMMonth'),
                      (EXTRACT(ISODOW FROM %(d)s) IN (6,7))
                    WHERE NOT EXISTS (
                      SELECT 1 FROM dim_date WHERE date_id = %(d)s
                    );
                """, {'d': current})
                
                if cur.rowcount:
                    inserted += 1
                current += delta
            
            logging.info(f"Hoàn thành: đã chèn {inserted} bản ghi từ {start} đến {end}.")
    except Exception as e:
        logging.error("Lỗi khi chèn dữ liệu dim_date", exc_info=e)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    main()
