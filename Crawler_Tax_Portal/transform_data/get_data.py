# transform_data/get_data.py

import pandas as pd
import pymongo
from bson.objectid import ObjectId
import os
import logging
import traceback
from dotenv import load_dotenv

# Load environment variables
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

# Validate configuration
if not MONGO_URI or not DATABASE_NAME:
    logging.error("Không thể lấy MONGO_URI hoặc DATABASE_NAME từ environment variables.")
    exit(1)



# --- Cấu hình đường dẫn và file (với header_keyword) ---

DATA_DIR = os.path.join(BASE_DIR, "data")



# ***** Cập nhật cấu hình với header_keyword *****

FILES_CONFIG = {

    "KhachHang": {

        "base_name": "Danhmuckhachhang",

        "collection": "KhachHang",

        "sheet_name": "Sheet1",

        "header_keyword": "Là tổ chức/cá nhân", # Tìm dòng có từ này ở cột đầu tiên

        "column_mapping": {

            "Mã khách hàng (*)": "ma_khach_hang", # Tên cột này PHẢI khớp với header tìm được trong file

            "Tên khách hàng (*)": "ten_khach_hang",

            "Địa chỉ": "dia_chi",

            "Mã số thuế": "ma_so_thue"

        },

        "required_columns": ["Mã khách hàng (*)", "Tên khách hàng (*)"] # Tên cột này PHẢI khớp

    },

    "NhaCungCap": {

        "base_name": "Danhmucnhacungcap",

        "collection": "NhaCungCap",

        "sheet_name": "Sheet1",

        "header_keyword": "Là tổ chức/cá nhân", # Tìm dòng có từ này ở cột đầu tiên

        "column_mapping": {

            "Mã nhà cung cấp (*)": "ma_ncc_goc",

            "Tên nhà cung cấp (*)": "ten_ncc",

            "Địa chỉ": "dia_chi",

            "Mã số thuế": "ma_so_thue"

        },

         "required_columns": ["Mã nhà cung cấp (*)", "Tên nhà cung cấp (*)"]

    },

    "DanhMucVatTu": {

        "base_name": "Danhmucvattu",

        "collection": "DanhMucVatTu",

        "sheet_name": "Sheet1",

        "header_keyword": "Mã", # Tìm dòng có từ này ở cột đầu tiên (Lưu ý: có thể cần chính xác hơn "Mã (*)" tùy file)

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





def find_header_row(df, keyword):

    """Tìm index của dòng đầu tiên chứa keyword trong cột đầu tiên."""

    if 0 not in df.columns: # Kiểm tra xem cột đầu tiên (index 0) có tồn tại không

        return -1



    # Chuyển cột đầu tiên thành string, xử lý lỗi nếu có kiểu dữ liệu không phù hợp

    try:

        first_col_str = df[0].astype(str).str.strip()

    except Exception as e:

        logging.error(f"Không thể chuyển đổi cột đầu tiên thành chuỗi để tìm header: {e}")

        return -1



    # Tìm các dòng chứa keyword

    matching_rows = first_col_str[first_col_str.str.contains(keyword, na=False, case=False)] # case=False để không phân biệt hoa thường



    if not matching_rows.empty:

        # Lấy index của dòng đầu tiên khớp

        header_index = matching_rows.index[0]

        return header_index

    return -1 # Không tìm thấy



def get_data(username):

    """

    Đọc dữ liệu từ file CSV/XLSX, tự động tìm header, ghi vào MongoDB.

    Sử dụng username trực tiếp làm giá trị cho trường 'username'.



    Args:

        username (str): Chuỗi string bất kỳ đại diện cho client/user.

    """

    # Chỉ cần log thông tin bắt đầu, không cần kiểm tra hay chuyển đổi username

    logging.info(f"Bắt đầu quá trình nhập dữ liệu cho client (username): {username}")



    # Kiểm tra nếu username rỗng (tùy chọn, có thể bỏ nếu muốn chấp nhận cả username rỗng)

    if not username:

         logging.warning("Username được cung cấp là rỗng. Có thể ảnh hưởng đến việc truy vấn sau này.")

         # Bạn có thể quyết định return ở đây nếu không muốn xử lý username rỗng

         # return



    mongo_client = None

    try:

        logging.info(f"Đang kết nối tới MongoDB...")

        mongo_client = pymongo.MongoClient(MONGO_URI)

        mongo_client.admin.command('ping') # Kiểm tra kết nối

        db = mongo_client[DATABASE_NAME]

        logging.info(f"Kết nối thành công tới database: {DATABASE_NAME}")



        for config_key, config in FILES_CONFIG.items():

            base_name = config["base_name"]

            collection_name = config["collection"]

            sheet_name = config.get("sheet_name", 0)

            header_keyword = config["header_keyword"]

            column_mapping = config["column_mapping"]

            required_columns = config["required_columns"]

            collection = db[collection_name]



            base_path = os.path.join(DATA_DIR, base_name)

            csv_path = base_path + ".csv"

            xlsx_path = base_path + ".xlsx"



            actual_file_path = None

            is_csv = None



            if os.path.exists(csv_path):

                actual_file_path = csv_path

                is_csv = True

            elif os.path.exists(xlsx_path):

                actual_file_path = xlsx_path

                is_csv = False

            else:

                logging.warning(f"Không tìm thấy file '{base_name}.csv' hoặc '{base_name}.xlsx' cho '{config_key}'. Bỏ qua.")

                continue



            file_type_str = "CSV" if is_csv else "XLSX"

            logging.info(f"--- Bắt đầu xử lý file {file_type_str}: {os.path.basename(actual_file_path)} ---")



            try:

                df_raw = None

                # Đọc file với header=None

                if is_csv:

                    try:

                        df_raw = pd.read_csv(actual_file_path, delimiter=';', header=None, encoding='utf-8', low_memory=False, skipinitialspace=True, dtype=str) # Thêm dtype=str

                    except UnicodeDecodeError:

                        logging.warning(f"Lỗi đọc file {os.path.basename(actual_file_path)} bằng utf-8, thử lại với utf-8-sig.")

                        df_raw = pd.read_csv(actual_file_path, delimiter=';', header=None, encoding='utf-8-sig', low_memory=False, skipinitialspace=True, dtype=str) # Thêm dtype=str

                else: # XLSX

                    try:

                        # Đọc tất cả dữ liệu dưới dạng string để tránh lỗi kiểu dữ liệu và giữ nguyên định dạng

                        df_raw = pd.read_excel(actual_file_path, sheet_name=sheet_name, header=None, engine='openpyxl', dtype=str)

                    except ImportError:

                        logging.error("Lỗi: Cần cài đặt 'openpyxl' để đọc .xlsx. Chạy: pip install openpyxl")

                        continue

                    # Xử lý trường hợp sheet trống trả về None thay vì DataFrame rỗng

                    if df_raw is None:

                        df_raw = pd.DataFrame()





                if df_raw.empty:

                    logging.warning(f"File {os.path.basename(actual_file_path)} trống hoặc không đọc được dữ liệu.")

                    continue



                # Tìm header và xử lý DataFrame

                header_row_index = find_header_row(df_raw, header_keyword)



                if header_row_index == -1:

                    logging.error(f"Không tìm thấy dòng header với từ khóa '{header_keyword}' trong file {os.path.basename(actual_file_path)}. Bỏ qua file này.")

                    continue



                logging.info(f"Tìm thấy header tại dòng index: {header_row_index}")



                # Lấy dòng header làm tên cột mới (chuyển thành str và strip whitespace)

                new_columns = df_raw.iloc[header_row_index].astype(str).str.strip().replace(r'^\.+|\.+$', '', regex=True) # Thêm replace để xóa dấu chấm thừa

                # Kiểm tra tên cột trùng lặp và xử lý nếu cần (ví dụ thêm hậu tố)

                if new_columns.duplicated().any():

                    logging.warning(f"Phát hiện tên cột trùng lặp trong header file {os.path.basename(actual_file_path)}: {new_columns[new_columns.duplicated()].tolist()}. Cân nhắc chỉnh sửa file gốc hoặc xử lý đổi tên.")

                    # Có thể thêm logic đổi tên tự động ở đây nếu muốn



                df = df_raw.iloc[header_row_index + 1:].copy()

                df.columns = new_columns

                df.reset_index(drop=True, inplace=True)



                logging.info(f"Header sau khi xử lý: {list(df.columns)}")



                documents_to_insert = []

                skipped_rows = 0

                missing_cols_reported = set() # Dùng để báo lỗi 1 lần cho mỗi cột thiếu



                # Kiểm tra xem các cột yêu cầu có tồn tại trong header mới không

                current_file_missing_required = set()

                for req_col in required_columns:

                    # Kiểm tra bằng cách chuẩn hóa (ví dụ: lower case, strip) nếu cần

                    # req_col_normalized = str(req_col).strip().lower()

                    # found = any(str(col).strip().lower() == req_col_normalized for col in df.columns)

                    if req_col not in df.columns: # Kiểm tra trực tiếp trước

                         current_file_missing_required.add(req_col)



                if current_file_missing_required:

                    logging.error(f"Thiếu các cột bắt buộc {list(current_file_missing_required)} trong header tìm được của file {os.path.basename(actual_file_path)}. Header: {list(df.columns)}. Bỏ qua xử lý file này.")

                    continue # Bỏ qua toàn bộ file nếu thiếu cột bắt buộc



                # Lặp qua các dòng dữ liệu đã có header đúng

                for index, row in df.iterrows():

                    is_valid_row = True

                    # Kiểm tra giá trị các cột bắt buộc không được rỗng

                    for req_col in required_columns:

                         # Dùng row.get(req_col, None) để tránh lỗi nếu cột bất ngờ bị thiếu dù đã check ở trên

                        cell_value = row.get(req_col, None)

                        # Coi None, NaN, chuỗi rỗng, chuỗi chỉ chứa khoảng trắng là không hợp lệ

                        if pd.isna(cell_value) or str(cell_value).strip() == "":

                            is_valid_row = False

                            break # Chỉ cần 1 cột bắt buộc thiếu là bỏ qua dòng



                    if not is_valid_row:

                        skipped_rows += 1

                        continue # Bỏ qua dòng này



                    # Tạo document để insert

                    # ***** THAY ĐỔI QUAN TRỌNG Ở ĐÂY *****

                    doc = {"username": username} # Sử dụng username gốc trực tiếp



                    # Map các cột từ config sang document

                    for map_col, mongo_field in column_mapping.items():

                        if map_col in df.columns:

                            value = row[map_col]

                            # Xử lý giá trị NaN hoặc None thành chuỗi rỗng, còn lại thành string và strip

                            if pd.isna(value):

                                doc[mongo_field] = ""

                            else:

                                doc[mongo_field] = str(value).strip()

                        else:

                            # Chỉ log cảnh báo nếu cột mapping không có trong header tìm được

                            if map_col not in missing_cols_reported:

                                logging.warning(f"Cột được map '{map_col}' (từ config) không có trong header tìm được của file {os.path.basename(actual_file_path)}. Header: {list(df.columns)}. Sẽ dùng giá trị mặc định (chuỗi rỗng).")

                                missing_cols_reported.add(map_col)

                            doc[mongo_field] = "" # Gán giá trị mặc định



                    documents_to_insert.append(doc)



                # Log và Ghi vào DB

                if skipped_rows > 0:

                    logging.warning(f"Đã bỏ qua {skipped_rows} dòng trong file {os.path.basename(actual_file_path)} do thiếu dữ liệu ở các cột bắt buộc.")



                if documents_to_insert:

                    logging.info(f"Chuẩn bị ghi {len(documents_to_insert)} documents từ file {os.path.basename(actual_file_path)} vào collection '{collection_name}'...")

                    try:

                        result = collection.insert_many(documents_to_insert, ordered=False)

                        logging.info(f"Ghi thành công {len(result.inserted_ids)} documents vào collection '{collection_name}'.")

                    except pymongo.errors.BulkWriteError as bwe:

                        success_count = bwe.details.get('nInserted', 0)

                        error_count = len(bwe.details.get('writeErrors', []))

                        logging.warning(f"Lỗi BulkWriteError khi ghi vào '{collection_name}'. "

                                        f"Số lượng ghi thành công: {success_count}. "

                                        f"Số lượng lỗi: {error_count}.")

                        # Log chi tiết lỗi đầu tiên nếu có

                        if bwe.details.get('writeErrors'):

                            first_error = bwe.details['writeErrors'][0]

                            logging.warning(f"Ví dụ lỗi đầu tiên (index {first_error.get('index', 'N/A')}): Code {first_error.get('code', 'N/A')} - {first_error.get('errmsg', 'N/A')}")

                    except Exception as insert_err:

                        logging.error(f"Lỗi không xác định khi ghi vào '{collection_name}': {insert_err}")

                        logging.error(traceback.format_exc()) # In traceback để debug

                else:

                    # Chỉ log info nếu không có document nào được tạo ra (và không có lỗi trước đó)

                    if skipped_rows == df.shape[0]: # Nếu tất cả các dòng đều bị bỏ qua

                         logging.info(f"Tất cả {df.shape[0]} dòng dữ liệu trong file {os.path.basename(actual_file_path)} không hợp lệ (thiếu cột bắt buộc).")

                    else:

                        logging.info(f"Không có dữ liệu hợp lệ nào được tìm thấy trong file {os.path.basename(actual_file_path)} để ghi vào collection '{collection_name}'.")



            # Bắt lỗi chung khi đọc/xử lý file

            except Exception as read_err:

                logging.error(f"Lỗi nghiêm trọng khi đọc hoặc xử lý file {os.path.basename(actual_file_path)}: {read_err}")

                logging.error(traceback.format_exc()) # In traceback để debug



            logging.info(f"--- Kết thúc xử lý file: {os.path.basename(actual_file_path)} ---")

        # Kết thúc vòng lặp for config_key, config...



        logging.info(f"Hoàn tất quá trình nhập dữ liệu cho client (username): {username}")



    except pymongo.errors.ConfigurationError as ce:

        logging.error(f"Lỗi cấu hình MongoDB URI: {ce}")

    except pymongo.errors.ConnectionFailure as cf:

        logging.error(f"Lỗi kết nối MongoDB: {cf}")

    except Exception as e:

        logging.error(f"Đã xảy ra lỗi không mong muốn trong quá trình xử lý chính: {e}")

        logging.error(traceback.format_exc()) # In traceback để debug

    finally:

        if mongo_client:

            mongo_client.close()

            logging.info("Đã đóng kết nối MongoDB.")



# --- Chạy thử nghiệm (Giữ nguyên) ---

if __name__ == "__main__":

    test_client_id = "0302147168" # ID VÍ DỤ - CẦN THAY THẾ!

    get_data(test_client_id)

