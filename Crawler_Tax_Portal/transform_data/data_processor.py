# transform_data/data_processor.py

import pandas as pd
import pymongo
import os
import logging
import traceback
import io
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Đọc cấu hình từ .env file ---
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://thaian:thaian123@taxanalyses.qxevmke.mongodb.net/?retryWrites=true&w=majority&appName=TaxAnalyses')
DATABASE_NAME = os.getenv('MONGO_DB_NAME', 'MolaDatabase')

if not MONGO_URI:
    raise ValueError("MONGO_URI not found in environment variables")

logging.info(f"✅ Loaded MongoDB config from .env: {DATABASE_NAME}")
logging.info(f"🔗 Atlas URI: {MONGO_URI[:50]}...")

if not MONGO_URI or not DATABASE_NAME:
     logging.error("Không thể lấy MONGO_URI hoặc DATABASE_NAME từ file secret.")
     raise ValueError("MONGO_URI or DATABASE_NAME missing in secret file.")


# --- Cấu hình xử lý file (Không cần DATA_DIR và base_name nữa) ---
FILES_CONFIG = {
    "KhachHang": {
        # "base_name": "Danhmuckhachhang", # Bỏ đi
        "collection": "KhachHang",
        "sheet_name": "Sheet1",
        "header_keyword": "Là tổ chức/cá nhân",
        "column_mapping": {
            "Mã khách hàng (*)": "ma_khach_hang",
            "Tên khách hàng (*)": "ten_khach_hang",
            "Địa chỉ": "dia_chi",
            "Mã số thuế": "ma_so_thue"
        },
        "required_columns": ["Mã khách hàng (*)", "Tên khách hàng (*)"]
    },
    "NhaCungCap": {
        # "base_name": "Danhmucnhacungcap", # Bỏ đi
        "collection": "NhaCungCap",
        "sheet_name": "Sheet1",
        "header_keyword": "Là tổ chức/cá nhân",
        "column_mapping": {
            "Mã nhà cung cấp (*)": "ma_ncc_goc",
            "Tên nhà cung cấp (*)": "ten_ncc",
            "Địa chỉ": "dia_chi",
            "Mã số thuế": "ma_so_thue"
        },
         "required_columns": ["Mã nhà cung cấp (*)", "Tên nhà cung cấp (*)"]
    },
    "DanhMucVatTu": {
        # "base_name": "Danhmucvattu", # Bỏ đi
        "collection": "DanhMucVatTu",
        "sheet_name": "Sheet1",
        "header_keyword": "Mã", # Hoặc "Mã (*)" tùy file
        "column_mapping": {
            "Mã (*)": "ma_vt",
            "Tên (*)": "ten_vat_tu",
            "Tính chất": "tinh_chat",
            "Đơn vị tính chính": "don_vi_tinh",
            "Nhóm VTHH": "nhom_VTHH",
            "Kho ngầm định": "kho_ngam_dinh",
            "TK kho": "TK_kho"
        },
         "required_columns": ["Mã (*)", "Tên (*)"]
    }
}

# --- Hàm tìm header (Giữ nguyên) ---
def find_header_row(df, keyword):
    """Tìm index của dòng đầu tiên chứa keyword trong cột đầu tiên."""
    if df.empty or 0 not in df.columns: # Kiểm tra df rỗng hoặc không có cột 0
        return -1
    try:
        # Đảm bảo cột đầu tiên là string và loại bỏ khoảng trắng đầu/cuối
        first_col_str = df[0].astype(str).str.strip()
        # Tìm không phân biệt hoa thường
        matching_rows = first_col_str[first_col_str.str.contains(keyword, na=False, case=False)]
        if not matching_rows.empty:
            return matching_rows.index[0]
    except Exception as e:
        logging.error(f"Lỗi khi tìm header với keyword '{keyword}': {e}")
    return -1

# --- Hàm xử lý dữ liệu được tải lên ---
def process_uploaded_data(file_content_bytes, data_type_key, username, is_csv):
    """
    Xử lý nội dung file (bytes) được tải lên, ghi vào MongoDB.

    Args:
        file_content_bytes (bytes): Nội dung của file dưới dạng bytes.
        data_type_key (str): Key trong FILES_CONFIG (ví dụ: "KhachHang").
        username (str): Username của người dùng liên quan đến dữ liệu này.
        is_csv (bool): True nếu là file CSV, False nếu là XLSX.

    Returns:
        dict: Kết quả xử lý {'success': bool, 'message': str, 'inserted': int, 'skipped': int}
    """
    if data_type_key not in FILES_CONFIG:
        return {'success': False, 'message': f"Loại dữ liệu không hợp lệ: {data_type_key}", 'inserted': 0, 'skipped': 0}

    if not username:
         # Quyết định: Có cho phép username rỗng không? Nếu không thì:
         # return {'success': False, 'message': "Username không được để trống", 'inserted': 0, 'skipped': 0}
         logging.warning("Username rỗng được cung cấp.") # Nếu cho phép thì cảnh báo

    config = FILES_CONFIG[data_type_key]
    collection_name = config["collection"]
    sheet_name = config.get("sheet_name", 0) # Dùng cho Excel
    header_keyword = config["header_keyword"]
    column_mapping = config["column_mapping"]
    required_columns = config["required_columns"]

    mongo_client = None
    inserted_count = 0
    skipped_rows = 0
    total_rows_in_data = 0 # Để đếm tổng số dòng dữ liệu sau header

    try:
        logging.info(f"Bắt đầu xử lý dữ liệu '{data_type_key}' cho username: {username}")
        # Tạo file-like object trong bộ nhớ từ bytes
        file_stream = io.BytesIO(file_content_bytes)

        df_raw = None
        # Đọc file với header=None từ stream
        if is_csv:
            try:
                # Đọc CSV, cố gắng dùng utf-8 trước
                df_raw = pd.read_csv(file_stream, delimiter=';', header=None, encoding='utf-8', low_memory=False, skipinitialspace=True, dtype=str)
            except UnicodeDecodeError:
                logging.warning(f"Lỗi đọc CSV dạng '{data_type_key}' bằng utf-8, thử lại với utf-8-sig.")
                file_stream.seek(0) # Quay lại đầu stream để đọc lại
                df_raw = pd.read_csv(file_stream, delimiter=';', header=None, encoding='utf-8-sig', low_memory=False, skipinitialspace=True, dtype=str)
            except Exception as csv_err:
                 raise ValueError(f"Lỗi khi đọc file CSV: {csv_err}")
        else: # XLSX
            try:
                # Đọc Excel, đọc tất cả thành string
                df_raw = pd.read_excel(file_stream, sheet_name=sheet_name, header=None, engine='openpyxl', dtype=str)
            except ImportError:
                 raise ImportError("Cần cài đặt 'openpyxl' để đọc .xlsx. Chạy: pip install openpyxl")
            except Exception as xlsx_err:
                 raise ValueError(f"Lỗi khi đọc file XLSX: {xlsx_err}")
            # Xử lý trường hợp sheet trống
            if df_raw is None:
                df_raw = pd.DataFrame()

        if df_raw.empty:
            logging.warning(f"Không có dữ liệu trong file tải lên cho '{data_type_key}'.")
            return {'success': True, 'message': "File tải lên trống hoặc không đọc được dữ liệu.", 'inserted': 0, 'skipped': 0}

        # --- Phần xử lý DataFrame (gần như giữ nguyên từ get_data) ---
        header_row_index = find_header_row(df_raw, header_keyword)

        if header_row_index == -1:
            logging.error(f"Không tìm thấy header với từ khóa '{header_keyword}' cho '{data_type_key}'.")
            return {'success': False, 'message': f"Không tìm thấy dòng header với từ khóa '{header_keyword}'.", 'inserted': 0, 'skipped': 0}

        logging.info(f"Tìm thấy header cho '{data_type_key}' tại dòng index: {header_row_index}")

        # Lấy header và dữ liệu
        new_columns = df_raw.iloc[header_row_index].astype(str).str.strip().replace(r'^\.+|\.+$', '', regex=True)
        if new_columns.duplicated().any():
            logging.warning(f"Phát hiện tên cột trùng lặp trong header của '{data_type_key}': {new_columns[new_columns.duplicated()].tolist()}.")
            # Có thể thêm logic xử lý trùng lặp nếu cần

        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = new_columns
        df.reset_index(drop=True, inplace=True)
        total_rows_in_data = df.shape[0] # Số dòng dữ liệu sau header

        logging.info(f"Header sau xử lý cho '{data_type_key}': {list(df.columns)}")

        # Kiểm tra cột bắt buộc
        current_file_missing_required = set()
        for req_col in required_columns:
            if req_col not in df.columns:
                current_file_missing_required.add(req_col)

        if current_file_missing_required:
            msg = f"Thiếu các cột bắt buộc {list(current_file_missing_required)} trong header tìm được của '{data_type_key}'. Header: {list(df.columns)}."
            logging.error(msg)
            return {'success': False, 'message': msg, 'inserted': 0, 'skipped': total_rows_in_data} # Bỏ qua toàn bộ nếu thiếu cột

        # Kết nối DB với SSL fix
        logging.info(f"Đang kết nối tới MongoDB để ghi dữ liệu '{data_type_key}'...")
        mongo_client = pymongo.MongoClient(
            MONGO_URI,
            tls=True,
            tlsAllowInvalidCertificates=True,
            tlsAllowInvalidHostnames=True,
            serverSelectionTimeoutMS=60000,
            connectTimeoutMS=60000,
            socketTimeoutMS=60000,
            maxPoolSize=10,
            retryWrites=True
        )
        # Test kết nối
        mongo_client.admin.command('ping')
        db = mongo_client[DATABASE_NAME]
        collection = db[collection_name]

        documents_to_insert = []
        missing_cols_reported = set()

        # Lặp qua các dòng dữ liệu
        for index, row in df.iterrows():
            is_valid_row = True
            for req_col in required_columns:
                cell_value = row.get(req_col, None)
                if pd.isna(cell_value) or str(cell_value).strip() == "":
                    is_valid_row = False
                    break

            if not is_valid_row:
                skipped_rows += 1
                continue

            # Tạo document
            doc = {"username": username} # THÊM USERNAME VÀO DOCUMENT
            for map_col, mongo_field in column_mapping.items():
                if map_col in df.columns:
                    value = row[map_col]
                    doc[mongo_field] = "" if pd.isna(value) else str(value).strip()
                else:
                    if map_col not in missing_cols_reported:
                        logging.warning(f"Cột được map '{map_col}' không có trong header của '{data_type_key}'. Sẽ dùng giá trị rỗng.")
                        missing_cols_reported.add(map_col)
                    doc[mongo_field] = ""

            documents_to_insert.append(doc)

        # Ghi vào DB
        if documents_to_insert:
            logging.info(f"Chuẩn bị ghi {len(documents_to_insert)} documents từ '{data_type_key}' vào collection '{collection_name}'...")
            try:
                # Cân nhắc: Xóa dữ liệu cũ của username này trước khi insert?
                # collection.delete_many({"username": username})
                # logging.info(f"Đã xóa dữ liệu cũ của username '{username}' trong collection '{collection_name}'.")

                result = collection.insert_many(documents_to_insert, ordered=False)
                inserted_count = len(result.inserted_ids)
                logging.info(f"Ghi thành công {inserted_count} documents vào collection '{collection_name}'.")
            except pymongo.errors.BulkWriteError as bwe:
                inserted_count = bwe.details.get('nInserted', 0)
                error_count = len(bwe.details.get('writeErrors', []))
                logging.warning(f"Lỗi BulkWriteError khi ghi '{data_type_key}'. Thành công: {inserted_count}. Lỗi: {error_count}.")
                if bwe.details.get('writeErrors'):
                     first_error = bwe.details['writeErrors'][0]
                     logging.warning(f"Ví dụ lỗi đầu tiên: Code {first_error.get('code', 'N/A')} - {first_error.get('errmsg', 'N/A')}")
            except Exception as insert_err:
                logging.error(f"Lỗi không xác định khi ghi vào '{collection_name}': {insert_err}")
                logging.error(traceback.format_exc())
                # Trả về lỗi ngay lập tức nếu insert gặp vấn đề nghiêm trọng
                return {'success': False, 'message': f"Lỗi khi ghi vào DB: {insert_err}", 'inserted': 0, 'skipped': total_rows_in_data}

        # Tạo thông báo kết quả
        final_message = f"Xử lý '{data_type_key}' hoàn tất."
        if skipped_rows > 0:
            final_message += f" Bỏ qua {skipped_rows}/{total_rows_in_data} dòng do thiếu dữ liệu bắt buộc."
        if inserted_count == 0 and skipped_rows == total_rows_in_data and total_rows_in_data > 0:
            final_message = f"Tất cả {total_rows_in_data} dòng dữ liệu trong '{data_type_key}' không hợp lệ."
        elif inserted_count == 0 and total_rows_in_data == 0 :
             final_message = f"Không có dữ liệu hợp lệ nào trong '{data_type_key}' để ghi."

        logging.info(f"--- Kết thúc xử lý dữ liệu '{data_type_key}' cho username: {username} ---")
        return {'success': True, 'message': final_message, 'inserted': inserted_count, 'skipped': skipped_rows}

    except (pymongo.errors.ConnectionFailure, pymongo.errors.ConfigurationError) as db_err:
        logging.error(f"Lỗi MongoDB khi xử lý '{data_type_key}': {db_err}")
        return {'success': False, 'message': f"Lỗi kết nối hoặc cấu hình DB: {db_err}", 'inserted': 0, 'skipped': total_rows_in_data}
    except (ValueError, ImportError, Exception) as e:
        logging.error(f"Lỗi khi xử lý dữ liệu '{data_type_key}': {e}")
        logging.error(traceback.format_exc())
        return {'success': False, 'message': f"Lỗi xử lý file '{data_type_key}': {e}", 'inserted': 0, 'skipped': total_rows_in_data}
    finally:
        if mongo_client:
            mongo_client.close()
            logging.info(f"Đã đóng kết nối MongoDB cho '{data_type_key}'.")

# --- Bỏ hoặc comment phần chạy thử nghiệm cũ ---
# if __name__ == "__main__":
#     # Phần này không còn dùng trực tiếp với file nữa
#     pass