from database.db_connection import get_db_connection

def create_partition_if_not_exists(timestamp):
    """Tạo partition tự động nếu chưa tồn tại"""
    partition_date = timestamp.strftime('%Y%m%d')
    partition_name = f"binance_prices_{partition_date}"
    
    start_date = timestamp.strftime('%Y-%m-%d 00:00:00')
    end_date = timestamp.strftime('%Y-%m-%d 23:59:59')

    partition_sql = f"""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_tables WHERE tablename = '{partition_name}'
            ) THEN
                EXECUTE 'CREATE TABLE {partition_name} PARTITION OF binance_prices
                        FOR VALUES FROM (''{start_date}'') TO (''{end_date}'');';
            END IF;
        END $$;
    """

    # Mở kết nối riêng để tránh khóa bảng chính
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(partition_sql)
            conn.commit()
