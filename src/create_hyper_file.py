import csv
from pathlib import Path
from datetime import datetime
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName, SqlType, TableDefinition, Inserter, CreateMode

# Cấu hình đường dẫn tương đối cho cấu trúc thư mục mới
base_dir = Path(__file__).parent.parent # Thư mục gốc dự án
path_to_csv = base_dir / "data" / "fcs_pro_telemetry_log.csv"
path_to_hyper = base_dir / "flight_data.hyper" # File kết quả nằm ở root dự án
table_name = TableName("flight_logs")

print(f"Đang tìm file dữ liệu tại: {path_to_csv}")

# Bắt đầu Hyper process
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint,
                    database=path_to_hyper,
                    create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

        # Định nghĩa cấu trúc bảng
        table_definition = TableDefinition(
            table_name,
            [
                TableDefinition.Column('timestamp', SqlType.timestamp()),
                TableDefinition.Column('flight_mode', SqlType.text()),
                TableDefinition.Column('is_armed', SqlType.bool()),
                TableDefinition.Column('roll', SqlType.double()),
                TableDefinition.Column('pitch', SqlType.double()),
                TableDefinition.Column('yaw', SqlType.double()),
                TableDefinition.Column('groundspeed', SqlType.double()),
                TableDefinition.Column('heading', SqlType.int()),
                TableDefinition.Column('alt_agl', SqlType.double()),
                TableDefinition.Column('ekf_flags_raw', SqlType.big_int()),
                TableDefinition.Column('ekf_health_status', SqlType.text()),
            ]
        )
        connection.catalog.create_table(table_definition)

        # Chèn dữ liệu
        print(f"Bắt đầu đọc và xử lý...")
        with open(path_to_csv, "r", encoding="utf-8") as csv_file:
            header = next(csv_file) 
            with Inserter(connection, table_name) as inserter:
                csv_reader = csv.reader(csv_file)
                count = 0
                for row in csv_reader:
                    try:
                        timestamp = datetime.fromtimestamp(float(row[0]))
                        flight_mode = row[1]
                        is_armed = (row[2].lower() == 'true')
                        roll = float(row[3])
                        pitch = float(row[4])
                        yaw = float(row[5])
                        groundspeed = float(row[6])
                        heading = int(row[7])
                        alt_agl = float(row[8])
                        ekf_flags_raw = int(row[9])
                        ekf_health_status = row[10]

                        inserter.add_row([timestamp, flight_mode, is_armed, roll, pitch, yaw, groundspeed, heading, alt_agl, ekf_flags_raw, ekf_health_status])
                        count += 1
                    except Exception as e:
                        print(f"Lỗi dòng {count + 2}: {e}")

        print(f"✅ Đã chèn thành công {count} dòng.")

print(f"🎉 Tạo file '{path_to_hyper}' hoàn tất!")
