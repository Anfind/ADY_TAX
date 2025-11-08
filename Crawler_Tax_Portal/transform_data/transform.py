# transform_data/transform.py

import pymongo
import logging
import os
import re
from bson import ObjectId
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Cấu hình Logging NGAY ĐẦU ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Đọc cấu hình từ .env file ---
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://thaian:thaian123@taxanalyses.qxevmke.mongodb.net/?retryWrites=true&w=majority&appName=TaxAnalyses')
DATABASE_NAME = os.getenv('MONGO_DB_NAME', 'MolaDatabase')

if not MONGO_URI:
    raise ValueError("MONGO_URI not found in environment variables")

logging.info(f"✅ Loaded MongoDB config from .env: {DATABASE_NAME}")
logging.info(f"🔗 Atlas URI: {MONGO_URI[:50]}...")

# --- Các cấu hình khác ---
DANHMUC_COLLECTION = "DanhMucVatTu"
DEFAULT_MAVT_PREFIX = "HH"
DEFAULT_MAVT_LENGTH = 5

# --- Hàm sinh Mã Vật Tư duy nhất ---
# (Giữ nguyên hàm generate_unique_ma_vt như bạn đã cung cấp - nó đã đúng)
def generate_unique_ma_vt(db, prefix=DEFAULT_MAVT_PREFIX, length=DEFAULT_MAVT_LENGTH):
    dmvt_collection = db[DANHMUC_COLLECTION]
    numeric_part_len = length
    regex_pattern = f"^{re.escape(prefix)}(\\d{{{numeric_part_len}}})$"
    last_entry = dmvt_collection.find_one(
        {"ma_vt": {"$regex": regex_pattern}},
        sort=[("ma_vt", pymongo.DESCENDING)]
    )
    new_numeric = 1
    last_key = last_entry.get("ma_vt") if last_entry else None
    if last_key:
        match = re.match(regex_pattern, last_key)
        if match:
            try:
                new_numeric = int(match.group(1)) + 1
            except (ValueError, IndexError):
                logging.warning(f"Không thể phân tích số từ mã VT cuối {last_key}. Bắt đầu lại từ 1.")
                new_numeric = 1
        else:
            logging.warning(f"Mã VT cuối {last_key} không khớp mẫu '{regex_pattern}', bắt đầu lại từ 1.")
            new_numeric = 1
    while True:
        new_ma_vt = f"{prefix}{new_numeric:0{length}d}"
        exists = dmvt_collection.find_one({"ma_vt": new_ma_vt})
        if not exists:
            return new_ma_vt
        logging.warning(f"Mã {new_ma_vt} đã tồn tại, thử số tiếp theo.")
        new_numeric += 1

# --- Hàm chính xử lý - **ĐÃ SỬA LẠI CÁC ĐIỂM RETURN** ---
def update_and_sync_ma_vat_tu(target_username, hoadon_collection_name):
    """
    Cập nhật Mã VT và đồng bộ vật tư, **luôn trả về dict kết quả**.
    """
    # Khởi tạo dictionary kết quả mặc định
    summary = {
        'success': False, 'message': 'Lỗi chưa xác định', # Mặc định là lỗi
        'collection_processed': hoadon_collection_name,
        'total_items_queried': 0, 'items_processed': 0, 'invoices_updated': 0,
        'found_existing_master': 0, 'new_master_items_synced': 0, 'errors_encountered': 0
    }

    # === SỬA 1: Thêm return summary khi lỗi tham số ===
    if not target_username:
        summary['message'] = "Username mục tiêu không được cung cấp."
        summary['errors_encountered'] = 1
        logging.error(summary['message'])
        return summary # Trả về dict lỗi
    if not hoadon_collection_name:
        summary['message'] = "Tên collection hóa đơn không được cung cấp."
        summary['errors_encountered'] = 1
        logging.error(summary['message'])
        return summary # Trả về dict lỗi

    logging.info(f"--- Bắt đầu quá trình xử lý cho username: {target_username} trong collection: {hoadon_collection_name} ---")

    mongo_client = None
    updated_count = 0
    synced_new_count = 0
    found_existing_count = 0
    error_count = 0
    processed_count = 0
    total_to_process = 0
    success_flag = True # Giả định thành công ban đầu

    try:
        logging.info(f"Đang kết nối tới MongoDB...")
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
        mongo_client.admin.command('ping')
        db = mongo_client[DATABASE_NAME]
        logging.info(f"Kết nối thành công tới database: {DATABASE_NAME}")

        hoadon_collection = db[hoadon_collection_name]
        dmvt_collection = db[DANHMUC_COLLECTION]

        # === SỬA 2: Thêm return summary khi collection không tồn tại ===
        if hoadon_collection_name not in db.list_collection_names():
             msg = f"Lỗi: Collection '{hoadon_collection_name}' không tồn tại trong database '{DATABASE_NAME}'."
             logging.error(msg)
             success_flag = False
             summary.update({'success': success_flag, 'message': msg, 'errors_encountered': 1})
             # Đóng kết nối trước khi return nếu đã mở
             if mongo_client: mongo_client.close()
             return summary # Trả về dict lỗi

        query = {
            "Mã VT": {"$in": ["", None]},
            "username": target_username,
            "Tên hàng hóa, dịch vụ": {"$exists": True, "$ne": ""}
        }
        total_to_process = hoadon_collection.count_documents(query)
        summary['total_items_queried'] = total_to_process
        logging.info(f"Tìm thấy {total_to_process} hóa đơn cần xử lý...")

        # === SỬA 3: Thêm return summary khi không có gì để xử lý ===
        if total_to_process == 0:
            msg = f"Không có hóa đơn nào cần xử lý trong '{hoadon_collection_name}' cho username: {target_username}."
            logging.info(msg)
            summary.update({'success': True, 'message': msg}) # Success = True vì không lỗi
            # Đóng kết nối trước khi return nếu đã mở
            if mongo_client: mongo_client.close()
            return summary # Trả về dict thành công

        cursor = hoadon_collection.find(query)

        for hoadon in cursor:
            processed_count += 1
            hoadon_id = hoadon.get("_id")
            ten_hang_hoa = str(hoadon.get("Tên hàng hóa, dịch vụ", "")).strip()

            # ... (log tiến trình giữ nguyên) ...
            if processed_count % 50 == 0 or processed_count == total_to_process:
                logging.info(f"Đang xử lý hóa đơn {processed_count}/{total_to_process} (ID: {hoadon_id})...")


            if not ten_hang_hoa:
                logging.warning(f"Bỏ qua HĐ ID {hoadon_id} do Tên hàng hóa rỗng.")
                error_count += 1
                continue

            try:
                # --- Logic tìm kiếm, sinh mã, insert, update giữ nguyên như code bạn cung cấp ---
                # (Đảm bảo logic này không có lỗi tiềm ẩn khác)
                 # 1. Tìm trong DanhMucVatTu
                found_vat_tu = dmvt_collection.find_one({
                    "username": target_username,
                    "ten_vat_tu": ten_hang_hoa
                })
                ma_vt_to_update = None

                if found_vat_tu and found_vat_tu.get("ma_vt"):
                     ma_vt_to_update = found_vat_tu["ma_vt"]
                     found_existing_count += 1
                     logging.debug(f"Tìm thấy mã VT '{ma_vt_to_update}' cho '{ten_hang_hoa}'.")
                else:
                    # Sinh mã mới và thêm vào DMVT
                    try:
                        ma_vt_to_update = generate_unique_ma_vt(db)
                        logging.info(f"Không tìm thấy '{ten_hang_hoa}'. Sinh mã mới: {ma_vt_to_update}")
                    except Exception as gen_err:
                         logging.error(f"Lỗi khi sinh mã VT cho '{ten_hang_hoa}': {gen_err}", exc_info=True)
                         error_count += 1
                         continue # Bỏ qua hóa đơn này

                    new_dmvt_doc = {
                        "username": target_username, "ma_vt": ma_vt_to_update,
                        "ten_vat_tu": ten_hang_hoa, "tinh_chat": "",
                        "don_vi_tinh": hoadon.get("Đơn vị tính", "Cái"),
                        "nhom_VTHH": "Chưa phân loại", "kho_ngam_dinh": "", "TK_kho": ""
                    }
                    try:
                        insert_result = dmvt_collection.insert_one(new_dmvt_doc)
                        synced_new_count += 1
                        logging.info(f"Đã đồng bộ vật tư mới '{ma_vt_to_update}' vào DMVT (ID: {insert_result.inserted_id}).")
                    except pymongo.errors.DuplicateKeyError:
                        logging.error(f"Lỗi DuplicateKeyError khi đồng bộ '{ma_vt_to_update}'. Thử tìm lại.")
                        retry_found_vt = dmvt_collection.find_one({"username": target_username, "ten_vat_tu": ten_hang_hoa})
                        if retry_found_vt and retry_found_vt.get("ma_vt"):
                             ma_vt_to_update = retry_found_vt["ma_vt"]
                             logging.info(f"Tìm thấy mã VT '{ma_vt_to_update}' sau lỗi.")
                        else:
                             logging.error(f"Không thể tìm lại mã VT sau lỗi. Bỏ qua HĐ {hoadon_id}.")
                             error_count += 1
                             continue
                    except Exception as sync_err:
                        logging.error(f"Lỗi khi đồng bộ vật tư mới '{ma_vt_to_update}': {sync_err}", exc_info=True)
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
                            logging.debug(f"Đã cập nhật Mã VT '{ma_vt_to_update}' cho HĐ ID {hoadon_id}.")
                        elif update_result.matched_count == 0:
                             logging.error(f"Lỗi: Không tìm thấy HĐ ID {hoadon_id} để cập nhật.")
                             error_count += 1
                    except Exception as update_err:
                         logging.error(f"Lỗi khi cập nhật HĐ ID {hoadon_id}: {update_err}", exc_info=True)
                         error_count += 1
                else:
                    logging.warning(f"Không có Mã VT để cập nhật cho HĐ ID {hoadon_id} (Tên: '{ten_hang_hoa}').")
                    error_count += 1

            except Exception as process_err:
                logging.error(f"Lỗi không xác định khi xử lý HĐ ID {hoadon_id}: {process_err}", exc_info=True)
                error_count += 1

        if 'cursor' in locals() and cursor:
             cursor.close()

        # --- Log Kết quả Cuối cùng ---
        logging.info(f"--- Hoàn thành quá trình cho username: {target_username} trong collection: {hoadon_collection_name} ---")
        # ... (các log khác giữ nguyên) ...
        logging.info(f"Tổng số hóa đơn đã quét / cần xử lý ban đầu: {processed_count}/{total_to_process}")
        logging.info(f"Số hóa đơn được cập nhật Mã VT thành công: {updated_count}")
        logging.info(f"  (Số lần tìm thấy mã VT có sẵn trong DMVT: {found_existing_count})")
        logging.info(f"Số vật tư mới được sinh và đồng bộ vào DanhMucVatTu: {synced_new_count}")
        logging.info(f"Số lỗi/vấn đề gặp phải: {error_count}")

        # === SỬA 4: Thêm return summary ở cuối khối try thành công ===
        summary.update({
            'success': success_flag, # True nếu không có lỗi nghiêm trọng nào xảy ra
            'message': f"Xử lý hoàn tất cho '{hoadon_collection_name}'. Quét {processed_count}/{total_to_process}.",
            'items_processed': processed_count,
            'invoices_updated': updated_count,
            'found_existing_master': found_existing_count,
            'new_master_items_synced': synced_new_count,
            'errors_encountered': error_count
        })
        logging.info(f"Returning summary (end of try): {summary}") # Log trước khi return
        return summary

    # === SỬA 5: Thêm return summary trong các khối except ngoài cùng ===
    except pymongo.errors.ConnectionFailure as cf:
        error_msg = f"Lỗi kết nối MongoDB: {cf}"
        logging.error(error_msg)
        summary.update({'success': False, 'message': error_msg, 'errors_encountered': summary['errors_encountered'] + (total_to_process - processed_count)})
        logging.info(f"Returning error summary (ConnectionFailure): {summary}") # Log trước khi return
        return summary
    except Exception as e:
        error_msg = f"Lỗi không mong muốn trong quá trình xử lý: {e}"
        logging.error(error_msg, exc_info=True)
        summary.update({'success': False, 'message': error_msg, 'errors_encountered': summary['errors_encountered'] + (total_to_process - processed_count)})
        logging.info(f"Returning error summary (Outer Exception): {summary}") # Log trước khi return
        return summary
    finally:
        if mongo_client:
            mongo_client.close()
            logging.info(f"Đã đóng kết nối MongoDB.")


# --- Chạy hàm chính khi thực thi file trực tiếp ---
# (Phần này giữ nguyên như code bạn cung cấp, nó dùng để test thủ công)
if __name__ == "__main__":
    # ... (code chạy thủ công giữ nguyên) ...
    specific_user_to_process = "0302147168"
    collection_to_process = "HoaDonMuaVao"
    # collection_to_process = "HoaDonBanRa"
    print(f"\n===== BẮT ĐẦU CHẠY THỬ NGHIỆM TỪ DÒNG LỆNH =====")
    logging.info(f"===== Chạy xử lý cho username: {specific_user_to_process}, collection: {collection_to_process} =====")
    if collection_to_process in ["HoaDonBanRa", "HoaDonMuaVao"]:
        result_summary = update_and_sync_ma_vat_tu(specific_user_to_process, collection_to_process)
        print("\n----- KẾT QUẢ TÓM TẮT -----")
        import json
        print(json.dumps(result_summary, indent=4, ensure_ascii=False))
        print("---------------------------\n")
    else:
        logging.error(f"Lỗi: Tên collection '{collection_to_process}' không hợp lệ.")
    logging.info(f"===== Kết thúc xử lý cho username: {specific_user_to_process}, collection: {collection_to_process} =====")
    print(f"===== KẾT THÚC CHẠY THỬ NGHIỆM TỪ DÒNG LỆNH =====\n")