import pymongo
import logging
import os
import re
# import argparse # Không cần argparse nữa
from bson import ObjectId

# --- Cấu hình Logging NGAY ĐẦU ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Đọc cấu hình từ file secret ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SECRET_FILE = os.path.join(BASE_DIR, "secret")
MONGO_URI = None
DATABASE_NAME = None

try:
    # (Giữ nguyên phần đọc file secret của bạn)
    with open(SECRET_FILE, 'r', encoding='utf-8') as f:
        line1 = f.readline().strip()
        line2 = f.readline().strip()
        if '=' in line1: MONGO_URI = line1.split('=', 1)[1].strip().strip('"')
        if '=' in line2: DATABASE_NAME = line2.split('=', 1)[1].strip().strip('"')
    if not MONGO_URI or not DATABASE_NAME:
        raise ValueError("Không thể trích xuất MONGO_URI hoặc DATABASE_NAME.")
    logging.info(f"Đã đọc cấu hình từ {SECRET_FILE}.")
except FileNotFoundError:
    logging.error(f"Lỗi nghiêm trọng: Không tìm thấy file cấu hình '{SECRET_FILE}'.")
    exit(1)
except Exception as e:
    logging.error(f"Lỗi nghiêm trọng khi đọc file cấu hình '{SECRET_FILE}': {e}.")
    exit(1)

# --- Các cấu hình khác ---
# Không cần HOADON_COLLECTION ở đây nữa
DANHMUC_COLLECTION = "DanhMucVatTu"
DEFAULT_MAVT_PREFIX = "HH"
DEFAULT_MAVT_LENGTH = 5

# --- Hàm sinh Mã Vật Tư duy nhất ---
# (Giữ nguyên hàm generate_unique_ma_vt không đổi)
def generate_unique_ma_vt(db, prefix=DEFAULT_MAVT_PREFIX, length=DEFAULT_MAVT_LENGTH):
    dmvt_collection = db[DANHMUC_COLLECTION]
    numeric_part_len = length
    regex_pattern = f"^{re.escape(prefix)}(\\d{{{numeric_part_len}}})$"
    last_entry = dmvt_collection.find_one(
        {"ma_vt": {"$regex": regex_pattern}},
        sort=[("ma_vt", pymongo.DESCENDING)]
    )
    new_numeric = 1
    if last_entry and last_entry.get("ma_vt"):
        match = re.match(regex_pattern, last_entry["ma_vt"])
        if match:
            new_numeric = int(match.group(1)) + 1
        else:
            logging.warning(f"Mã VT cuối {last_entry['ma_vt']} không khớp mẫu '{regex_pattern}', bắt đầu lại từ 1.")
    while True:
        new_ma_vt = f"{prefix}{new_numeric:0{length}d}"
        exists = dmvt_collection.find_one({"ma_vt": new_ma_vt})
        if not exists:
            return new_ma_vt
        logging.warning(f"Mã {new_ma_vt} đã tồn tại, thử số tiếp theo.")
        new_numeric += 1


# --- Hàm chính xử lý - Chấp nhận tên collection làm tham số ---
# (Giữ nguyên hàm này từ lần sửa trước)
def update_and_sync_ma_vat_tu(target_username, hoadon_collection_name):
    """
    Cập nhật Mã VT trong collection hóa đơn được chỉ định và đồng bộ vật tư
    mới vào DanhMucVatTu CHỈ CHO MỘT USERNAME CỤ THỂ.

    Args:
        target_username (str): Username cần xử lý hóa đơn.
        hoadon_collection_name (str): Tên của collection hóa đơn cần xử lý
                                      (ví dụ: "HoaDonBanRa", "HoaDonMuaVao").
    """
    if not target_username:
        logging.error("Username mục tiêu không được cung cấp.")
        return
    if not hoadon_collection_name:
        logging.error("Tên collection hóa đơn không được cung cấp.")
        return

    logging.info(f"--- Bắt đầu quá trình xử lý cho username: {target_username} trong collection: {hoadon_collection_name} ---")

    mongo_client = None
    updated_count = 0
    synced_new_count = 0
    found_existing_count = 0
    error_count = 0
    processed_count = 0 # Khởi tạo ở đây

    try:
        logging.info(f"Đang kết nối tới MongoDB...")
        mongo_client = pymongo.MongoClient(MONGO_URI)
        mongo_client.admin.command('ping') # Kiểm tra kết nối
        db = mongo_client[DATABASE_NAME]
        logging.info(f"Kết nối thành công tới database: {DATABASE_NAME}")

        # Sử dụng tên collection được truyền vào
        hoadon_collection = db[hoadon_collection_name]
        dmvt_collection = db[DANHMUC_COLLECTION]

        # Kiểm tra collection hóa đơn có tồn tại không
        if hoadon_collection_name not in db.list_collection_names():
             logging.error(f"Lỗi: Collection '{hoadon_collection_name}' không tồn tại trong database '{DATABASE_NAME}'.")
             return

        # --- Truy vấn hóa đơn ---
        query = {
            "Mã VT": {"$in": ["", None]},
            "username": target_username,
            "Tên hàng hóa, dịch vụ": {"$exists": True, "$ne": ""}
        }
        total_to_process = hoadon_collection.count_documents(query)
        logging.info(f"Tìm thấy {total_to_process} hóa đơn trong '{hoadon_collection_name}' cần cập nhật Mã VT cho username: {target_username}.")

        if total_to_process == 0:
            logging.info(f"Không có hóa đơn nào cần xử lý trong '{hoadon_collection_name}' cho username: {target_username}.")
            return

        cursor = hoadon_collection.find(query)

        # Vòng lặp xử lý từng hóa đơn
        for hoadon in cursor:
            processed_count += 1
            hoadon_id = hoadon.get("_id")
            ten_hang_hoa = str(hoadon.get("Tên hàng hóa, dịch vụ", "")).strip()

            if processed_count % 50 == 0 or processed_count == total_to_process:
                logging.info(f"Đang xử lý hóa đơn {processed_count}/{total_to_process} (ID: {hoadon_id}) trong '{hoadon_collection_name}' cho username: {target_username}...")

            if not ten_hang_hoa:
                logging.warning(f"Bỏ qua hóa đơn ID {hoadon_id} (User: {target_username}, Collection: {hoadon_collection_name}) do Tên hàng hóa rỗng.")
                error_count += 1
                continue

            try:
                # 1. Tìm trong DanhMucVatTu
                found_vat_tu = dmvt_collection.find_one({
                    "username": target_username,
                    "ten_vat_tu": ten_hang_hoa
                })

                ma_vt_to_update = None

                if found_vat_tu and found_vat_tu.get("ma_vt"):
                    ma_vt_to_update = found_vat_tu["ma_vt"]
                    found_existing_count += 1
                    logging.debug(f"Tìm thấy mã VT '{ma_vt_to_update}' cho '{ten_hang_hoa}' (User: {target_username}) trong DanhMucVatTu.")
                else:
                    # Sinh mã mới và thêm vào DMVT
                    ma_vt_to_update = generate_unique_ma_vt(db)
                    logging.info(f"Không tìm thấy '{ten_hang_hoa}' trong DMVT cho user {target_username}. Sinh mã mới: {ma_vt_to_update}")

                    new_dmvt_doc = {
                        "username": target_username,
                        "ma_vt": ma_vt_to_update,
                        "ten_vat_tu": ten_hang_hoa,
                        "tinh_chat": "",
                        "don_vi_tinh": hoadon.get("Đơn vị tính", "Cái"),
                        "nhom_VTHH": "Chưa phân loại",
                        "kho_ngam_dinh": "",
                        "TK_kho": ""
                    }
                    try:
                        insert_result = dmvt_collection.insert_one(new_dmvt_doc)
                        synced_new_count += 1
                        logging.info(f"Đã đồng bộ vật tư mới '{ma_vt_to_update}' - '{ten_hang_hoa}' vào DanhMucVatTu (ID: {insert_result.inserted_id}).")
                    except pymongo.errors.DuplicateKeyError:
                        logging.error(f"Lỗi DuplicateKeyError khi đồng bộ '{ma_vt_to_update}' cho '{ten_hang_hoa}' (User: {target_username}). Thử tìm lại mã VT.")
                        retry_found_vt = dmvt_collection.find_one({"username": target_username, "ten_vat_tu": ten_hang_hoa})
                        if retry_found_vt and retry_found_vt.get("ma_vt"):
                             ma_vt_to_update = retry_found_vt["ma_vt"]
                             logging.info(f"Tìm thấy mã VT '{ma_vt_to_update}' sau lỗi DuplicateKeyError.")
                        else:
                             logging.error(f"Không thể tìm lại mã VT cho '{ten_hang_hoa}' sau lỗi DuplicateKeyError. Bỏ qua cập nhật hóa đơn {hoadon_id}.")
                             error_count += 1
                             continue
                    except Exception as sync_err:
                        logging.error(f"Lỗi khi đồng bộ vật tư mới '{ma_vt_to_update}' (User: {target_username}) vào DMVT: {sync_err}", exc_info=True)
                        error_count += 1
                        continue

                # 2. Cập nhật collection hóa đơn nguồn
                if ma_vt_to_update:
                    try:
                        update_result = hoadon_collection.update_one(
                            {"_id": hoadon_id},
                            {"$set": {"Mã VT": ma_vt_to_update}}
                        )
                        if update_result.modified_count > 0:
                            updated_count += 1
                            logging.debug(f"Đã cập nhật Mã VT '{ma_vt_to_update}' cho HĐ ID {hoadon_id} trong '{hoadon_collection_name}'.")
                        elif update_result.matched_count == 0:
                            logging.error(f"Lỗi: Không tìm thấy HĐ ID {hoadon_id} (User: {target_username}, Collection: {hoadon_collection_name}) để cập nhật.")
                            error_count += 1
                    except Exception as update_err:
                        logging.error(f"Lỗi khi cập nhật HĐ ID {hoadon_id} (User: {target_username}, Collection: {hoadon_collection_name}): {update_err}", exc_info=True)
                        error_count += 1
                else:
                     logging.warning(f"Không có Mã VT để cập nhật cho HĐ ID {hoadon_id} (Tên: '{ten_hang_hoa}').")
                     error_count += 1

            except Exception as process_err:
                logging.error(f"Lỗi không xác định khi xử lý HĐ ID {hoadon_id} (User: {target_username}, Collection: {hoadon_collection_name}): {process_err}", exc_info=True)
                error_count += 1

        # Đóng cursor (nếu cần)
        if 'cursor' in locals() and cursor:
             cursor.close()

        # --- Log Kết quả Cuối cùng ---
        logging.info(f"--- Hoàn thành quá trình cho username: {target_username} trong collection: {hoadon_collection_name} ---")
        logging.info(f"Tổng số hóa đơn đã quét / cần xử lý ban đầu: {processed_count}/{total_to_process}")
        logging.info(f"Số hóa đơn được cập nhật Mã VT thành công: {updated_count}")
        logging.info(f"   (Trong đó, số lần tìm thấy mã VT có sẵn trong DMVT: {found_existing_count})")
        logging.info(f"Số vật tư mới được sinh và đồng bộ vào DanhMucVatTu: {synced_new_count}")
        logging.info(f"Số lỗi/vấn đề gặp phải trong quá trình xử lý: {error_count}")

    except pymongo.errors.ConnectionFailure as cf:
        logging.error(f"Lỗi kết nối MongoDB: {cf}")
    except Exception as e:
        logging.error(f"Đã xảy ra lỗi không mong muốn khi xử lý cho user '{target_username}' trong collection '{hoadon_collection_name}': {e}", exc_info=True)
    finally:
        if mongo_client:
            mongo_client.close()
            logging.info(f"Đã đóng kết nối MongoDB (sau khi xử lý user: {target_username}, collection: {hoadon_collection_name}).")


# --- Chạy hàm chính - Chọn collection bằng cách sửa code ở đây ---
if __name__ == "__main__":
    # --- Cấu hình chạy ---
    specific_user_to_process = "0302147168" # Đặt username bạn muốn xử lý ở đây

    # CHỌN 1 TRONG 2 DÒNG DƯỚI ĐÂY ĐỂ CHẠY:
    collection_to_process = "HoaDonMuaVao"  # <-- Chạy cho Hóa Đơn Mua Vào
    # collection_to_process = "HoaDonBanRa"    # <-- Chạy cho Hóa Đơn Bán Ra

    # --- Thực thi ---
    logging.info(f"===== Bắt đầu chạy xử lý cho username: {specific_user_to_process}, collection: {collection_to_process} =====")
    if collection_to_process in ["HoaDonBanRa", "HoaDonMuaVao"]:
        update_and_sync_ma_vat_tu(specific_user_to_process, collection_to_process)
    else:
        logging.error(f"Lỗi: Tên collection '{collection_to_process}' không hợp lệ. Vui lòng chọn 'HoaDonBanRa' hoặc 'HoaDonMuaVao'.")
    logging.info(f"===== Kết thúc xử lý cho username: {specific_user_to_process}, collection: {collection_to_process} =====")