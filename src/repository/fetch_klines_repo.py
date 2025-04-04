from repository.db_connection import DatabaseConnection


class FetchKlinesRepository():
    def __init__(self):
        pass
    # Hàm lưu dữ liệu vào database
    def save_to_db(data, symbol):
        db = DatabaseConnection()
        conn = db.connection
        cursor = conn.cursor()

        # Chuyển dữ liệu về dạng tuple để chèn vào DB
        formatted_data = [(symbol, row[0], row[1], row[2], row[3], row[4], row[5]) for row in data]

        # Chèn vào DB
        sql = """
        INSERT INTO binance_prices_historical (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
        """

        cursor.executemany(sql, formatted_data)
        conn.commit()

        print(f"✅ Đã lưu {len(formatted_data)} dòng vào database!")

        cursor.close()
        conn.close()