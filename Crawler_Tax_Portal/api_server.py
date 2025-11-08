from flask import Flask, request, jsonify
from logic import InvoiceLogic
import os
import shutil
import openpyxl
from openpyxl.styles import Font, Border, Side, Alignment
import time
from flask_cors import CORS
import json
from bson import ObjectId
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import MongoDB processor
from invoice_mongo_processor import save_invoice_data_to_mongo

# transform
import io # Thêm io
from transform_data.data_processor import process_uploaded_data 
from transform_data.transform import update_and_sync_ma_vat_tu
import logging

# Custom JSON encoder to handle MongoDB ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

def clean_mongodb_data(data):
    """Recursively convert ObjectId instances to strings in data structures"""
    if isinstance(data, dict):
        return {key: clean_mongodb_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [clean_mongodb_data(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    else:
        return data

app = Flask(__name__)
app.json_encoder = JSONEncoder  # Set custom JSON encoder
logic = InvoiceLogic()

CORS(app, origins=["http://localhost:5173"], supports_credentials=True)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint for Docker"""
    return jsonify({
        'status': 'healthy',
        'version': '1.0.0',
        'mongodb': os.getenv('MONGO_DB_NAME', 'MolaDatabase')
    })

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    logic.user = data.get('username')
    logic.pass_ = data.get('password')
    logic.ckey = data.get('ckey')
    logic.captcha_inp = data.get('captcha')
    
    if logic.login_web():
        return jsonify({
            'success': True,
            'token': logic.token_
        })
    return jsonify({
        'success': False,
        'message': 'Invalid credentials'
    })

@app.route('/api/get-captcha', methods=['GET'])
def get_captcha():
    getCaptcha = logic.ssl_supressed_session().get('https://hoadondientu.gdt.gov.vn:30000/captcha', verify=False).json()
    logic.ckey = getCaptcha['key']
    return jsonify(getCaptcha)

@app.route('/api/process-invoices', methods=['POST'])
def process_invoices():
    data = request.json
    invoice_type = data.get('type')  # 1 for banra, 2 for muavao
    process_type = data.get('process_type')  # tongquat, chitiet, xml_html
    date_range = data.get('date_range')
    
    # Set up date range
    logic.begin_ = date_range['start']
    logic.end_ = date_range['end']
    logic.range_DAY_const = f' Thời gian : {logic.begin_}=>{logic.end_}'
    logic.arr_ed = logic.day_dow(logic.begin_, logic.end_)
    
    # Kiểm tra logic.arr_ed có phải là mảng rỗng không
    if not logic.arr_ed:
        return jsonify({
            'success': False,
            'message': 'Không thể tạo khoảng thời gian, vui lòng kiểm tra lại ngày bắt đầu và ngày kết thúc'
        })
    
    print(f"Processing invoices for date range: {logic.begin_} -> {logic.end_}")
    
    # Process based on type
    if process_type == 'tongquat':
        result = process_tongquat(invoice_type)
    elif process_type == 'chitiet':
        result = process_chitiet(invoice_type)
    elif process_type == 'xml_html':
        result = process_xml_html(invoice_type)
    else:
        return jsonify({
            'success': False,
            'message': 'Invalid process type'
        })
    
    # Lưu vào MongoDB nếu xử lý thành công
    if result.get('success') and result.get('json_data'):
        try:
            # Get MongoDB config from .env
            mongo_uri = os.getenv('MONGO_URI', 'mongodb+srv://thaian:thaian123@taxanalyses.qxevmke.mongodb.net/?retryWrites=true&w=majority&appName=TaxAnalyses')
            db_name = os.getenv('MONGO_DB_NAME', 'MolaDatabase')
            
            # Lưu dữ liệu vào MongoDB
            mongo_result = save_invoice_data_to_mongo(
                result['json_data'],
                mongo_uri=mongo_uri,
                db_name=db_name
            )
            
            # Thêm thông tin MongoDB vào response
            result['mongodb_result'] = {
                'saved': mongo_result.get('success', False),
                'message': mongo_result.get('message', ''),
                'summary': mongo_result.get('summary', {})
            }
            
            if mongo_result.get('success'):
                print(f"✅ Đã lưu vào MongoDB: {mongo_result.get('message')}")
            else:
                print(f"❌ Lỗi lưu MongoDB: {mongo_result.get('message')}")
                
        except Exception as e:
            print(f"❌ Lỗi kết nối MongoDB: {str(e)}")
            result['mongodb_result'] = {
                'saved': False,
                'message': f'Lỗi kết nối MongoDB: {str(e)}',
                'summary': {}
            }

    # Clean any MongoDB ObjectIds from the result before returning
    cleaned_result = clean_mongodb_data(result)
    return jsonify(cleaned_result)

def process_tongquat(type):
    try:
        # Thiết lập các biến cần thiết nhưng không tạo file
        day__ = logic.range_DAY_const.replace("Thời gian : ","").replace("/","-").replace("=>","_")
        us = logic.user.replace("\r","")
        br = 1
        
        if type == 1:
            br = 2
            mst = 1
            type_mb = logic.user
            type_hoadon = 'sold'
        
        d = ""
        e = ""
        type_list = {""}
        if type == 2:
            mst = 2
            type_mb = "nbmst"
            type_list = {'5','6','8'}
            d = f';ttxly=='
            type_hoadon = 'purchase'

        # Xử lý dữ liệu (giữ nguyên logic API calls)
        logic.len_a = len(logic.arr_ed)
        logic.n_pro = int(100/logic.len_a)
        logic.a = 0
        datas_first = ""
        n_collect = 100/len(logic.arr_ed)
        
        for logic.i in range(len(logic.arr_ed)):
            print("RUN")
            logic.a += n_collect
            begin_day = logic.arr_ed[logic.i][0]
            end_day = logic.arr_ed[logic.i][1]
            print(begin_day)
            print(end_day)
            spec = ""
            
            for e in type_list:
                for i in range(br):
                    if e == "8":
                        spec = "sco-"
                    else:
                        spec = ""
                    if i == 1:
                        spec = "sco-"
                        print(2)    
                    while True:
                        try:
                            res = logic.ssl_supressed_session().get(f'https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/{type_hoadon}?sort=tdlap:desc,khmshdon:asc,shdon:desc&size=50&search=tdlap=ge={begin_day}T00:00:00;tdlap=le={end_day}T23:59:59{d}{e}',headers=logic.headers,verify=False,timeout=0.5)
                            if res.status_code == 200:
                                break
                        except:
                            print("error")
                    data = res.json()
                    if datas_first == "":
                        print("first")
                        datas_first = data
                    else:
                        print("add ")
                        datas_first["datas"].extend(data["datas"])
                    while True:
                        if data["state"] != None and data != "":
                            print("chèn")
                            print(data['state'])
                            while True:
                                try:
                                    res = logic.ssl_supressed_session().get(f'https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/{type_hoadon}?sort=tdlap:desc,khmshdon:asc,shdon:desc&size=50&state={data["state"]}&search=tdlap=ge={begin_day}T00:00:00;tdlap=le={end_day}T23:59:59',headers=logic.headers,verify=False,timeout=0.5)
                                    if res.status_code == 200:
                                        break
                                    else:
                                        pass
                                except:
                                    print("error")
                            data = res.json()
                            if "state" in data:
                                datas_first["datas"].extend(data["datas"])
                        else:
                            break

        # Xử lý dữ liệu và tạo JSON response
        datas_first["datas"] = logic.remove_duplicate_elements(datas_first["datas"])
        count = len(datas_first["datas"])
        
        if count == 0:
            return {
                'success': False,
                'message': 'Không có hóa đơn !'
            }

        # Tạo JSON data thay vì Excel
        json_data = []
        if type == 1:
            nm = "nmmst"
            nmten = "nmten"
        elif type == 2:
            nm = "nbmst"
            nmten = "nbten"
            
        hdon = {1: "Hóa đơn mới", 2: "Hóa đơn thay thế", 3: "Hóa đơn điều chỉnh", 4: "Hóa đơn đã bị thay thế", 5: "Hóa đơn đã bị điều chỉnh", 6: "Hóa đơn đã bị hủy"}
        ttxly = {0: "Tổng cục Thuế đã nhận", 1: "Đang tiến hành kiểm tra điều kiện cấp mã", 2: "CQT từ chối hóa đơn theo từng lần phát sinh", 3: "Hóa đơn đủ điều kiện cấp mã", 4: "Hóa đơn không đủ điều kiện cấp mã", 5: "Đã cấp mã hóa đơn", 6: "Tổng cục thuế đã nhận không mã", 7: "Đã kiểm tra định kỳ HĐĐT không có mã", 8: "Tổng cục thuế đã nhận hóa đơn có mã khởi tạo từ máy tính tiền"}
        
        # Xử lý từng hóa đơn và tạo JSON object
        for index, data in enumerate(datas_first["datas"], 1):
            spec = "sco-" if data["ttxly"] == 8 else ""
            
            # Tạo object theo format mẫu
            invoice_obj = {
                "_id": str(data.get("id", f"generated_{index}")),
                "username": logic.user,
                "Mẫu số HD": data.get("khmshdon", ""),
                "Ký hiệu hóa  đơn": data.get("khhdon", ""),
                "Số hóa đơn": data.get("shdon", ""),
                "MST người bán": data.get(nm, ""),
                "Tên người bán": data.get(nmten, ""),
                "MST người mua": data.get("nbmst" if type == 1 else "nmmst", ""),
                "Tên người mua": data.get("nbten" if type == 1 else "nmten", ""),
                "Thành tiền chưa thuế": data.get("tgtcthue", 0),
                "Tiền thuế": data.get("tgtthue", 0),
                "Tổng tiền CKTM": data.get("ttcktmai", 0),
                "Tổng tiền thanh toán": data.get("tgtttbso", 0),
                "Đơn vị tiền tệ": data.get("dvtte", "VND"),
                "Trạng thái hóa đơn": hdon.get(data.get("tthai"), ""),
                "Kết quả kiểm tra hóa đơn": ttxly.get(data.get("ttxly"), "")
            }
            
            # Xử lý ngày lập hóa đơn
            try:
                tdlap = data.get("tdlap", "")
                if tdlap:
                    date_part = tdlap.split("T")[0]
                    date_parts = date_part.split("-")
                    formatted_date = f"{date_parts[2]}/{date_parts[1]}/{date_parts[0]}"
                    invoice_obj["Ngày lập hóa đơn"] = formatted_date
            except:
                invoice_obj["Ngày lập hóa đơn"] = ""
            
            # Xử lý tổng tiền phí
            try:
                new_p = 0
                for phi in data.get("thttlphi", []):
                    new_p += phi.get("tphi", 0)
                invoice_obj["Tổng tiền phí"] = new_p
            except:
                invoice_obj["Tổng tiền phí"] = 0
                
            json_data.append(invoice_obj)

        return {
            'success': True,
            'json_data': json_data,
            'message': f'Đã xử lý thành công {count}/{count} hóa đơn'
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': str(e)
        }

def print_workbook_as_json(wb):
    """Convert workbook data to JSON and print it"""
    sheet = wb.active
    headers = []
    data = []
    
    # Get headers from first row
    for cell in sheet[6]:
        headers.append(cell.value)
        
    # Get data from remaining rows
    for row in sheet.iter_rows(min_row=2):
        row_data = {}
        for idx, cell in enumerate(row):
            if idx < len(headers):
                row_data[headers[idx]] = cell.value
        data.append(row_data)
        
    # Convert to JSON and print
    json_data = json.dumps(data, ensure_ascii=False, indent=2)
    print("\n=== Workbook Data as JSON ===")
    print(json_data)
    print("===========================\n")

def process_chitiet(type):
    try:
        # Thiết lập các biến cần thiết nhưng không tạo file  
        day__ = logic.range_DAY_const.replace("Thời gian : ","").replace("/","-").replace("=>","_")
        us = logic.user.replace("\r","")
        br = 1
        
        if type == 1:
            br = 2
            mst = 1
            type_mb = logic.user
            type_hoadon = 'sold'
            logic.banra = True
            logic.muavao = False
        
        d = ""
        e = ""
        type_list = {""}
        if type == 2:
            mst = 2
            type_mb = "nbmst"
            type_list = {'5','6','8'}
            d = f';ttxly=='
            type_hoadon = 'purchase'
            logic.banra = False
            logic.muavao = True

        # Xử lý dữ liệu (giữ nguyên logic API calls)
        logic.len_a = len(logic.arr_ed)
        logic.n_pro = int(100/logic.len_a)
        logic.a = 0
        datas_first = ""
        n_collect = 100/len(logic.arr_ed)
        
        for logic.i in range(len(logic.arr_ed)):
            print("RUN")
            logic.a += n_collect
            begin_day = logic.arr_ed[logic.i][0]
            end_day = logic.arr_ed[logic.i][1]
            print(begin_day)
            print(end_day)
            spec = ""
            
            for e in type_list:
                for i in range(br):
                    if e == "8":
                        spec = "sco-"
                    else:
                        spec = ""
                    if i == 1:
                        spec = "sco-"
                        print(2)    
                    while True:
                        try:
                            res = logic.ssl_supressed_session().get(f'https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/{type_hoadon}?sort=tdlap:desc,khmshdon:asc,shdon:desc&size=50&search=tdlap=ge={begin_day}T00:00:00;tdlap=le={end_day}T23:59:59{d}{e}',headers=logic.headers,verify=False,timeout=0.5)
                            if res.status_code == 200:
                                break
                        except:
                            print("error")
                    data = res.json()
                    if datas_first == "":
                        print("first")
                        datas_first = data
                    else:
                        print("add ")
                        datas_first["datas"].extend(data["datas"])
                    while True:
                        if data["state"] != None and data != "":
                            print("chèn")
                            print(data['state'])
                            while True:
                                try:
                                    res = logic.ssl_supressed_session().get(f'https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/{type_hoadon}?sort=tdlap:desc,khmshdon:asc,shdon:desc&size=50&state={data["state"]}&search=tdlap=ge={begin_day}T00:00:00;tdlap=le={end_day}T23:59:59',headers=logic.headers,verify=False,timeout=0.5)
                                    if res.status_code == 200:
                                        break
                                    else:
                                        pass
                                except:
                                    print("error")
                            data = res.json()
                            if "state" in data:
                                datas_first["datas"].extend(data["datas"])
                        else:
                            break

        # Xử lý dữ liệu và tạo JSON response
        datas_first["datas"] = logic.remove_duplicate_elements(datas_first["datas"])
        count = len(datas_first["datas"])
        
        if count == 0:
            return {
                'success': False,
                'message': 'Không có hóa đơn !'
            }

        # Tạo JSON data chi tiết theo format mẫu
        json_data = []
        hdon = {1: "Hóa đơn mới", 2: "Hóa đơn thay thế", 3: "Hóa đơn điều chỉnh", 4: "Hóa đơn đã bị thay thế", 5: "Hóa đơn đã bị điều chỉnh", 6: "Hóa đơn đã bị hủy"}
        ttxly = {0: "Tổng cục Thuế đã nhận", 1: "Đang tiến hành kiểm tra điều kiện cấp mã", 2: "CQT từ chối hóa đơn theo từng lần phát sinh", 3: "Hóa đơn đủ điều kiện cấp mã", 4: "Hóa đơn không đủ điều kiện cấp mã", 5: "Đã cấp mã hóa đơn", 6: "Tổng cục thuế đã nhận không mã", 7: "Đã kiểm tra định kỳ HĐĐT không có mã", 8: "Tổng cục thuế đã nhận hóa đơn có mã khởi tạo từ máy tính tiền"}
        
        # Xử lý từng hóa đơn và lấy chi tiết
        for index, data in enumerate(datas_first["datas"], 1):
            spec = "sco-" if data["ttxly"] == 8 else ""
            
            # Lấy chi tiết hóa đơn
            nbmst = data["nbmst"]
            khhdon = data["khhdon"]
            shd = data["shdon"]
            khmshdon = data["khmshdon"]
            
            try:
                while True:    
                    try:                     
                        res1 = logic.ssl_supressed_session().get(f'https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/detail?nbmst={nbmst}&khhdon={khhdon}&shdon={shd}&khmshdon={khmshdon}',headers=logic.headers,verify=False,timeout=1)
                        if res1.status_code == 200:
                            break
                    except:
                        print("ERROR getting detail")
                        time.sleep(0.5)
                
                data_ct = res1.json()
                
                # Kiểm tra xem hóa đơn có chi tiết hàng hóa không
                if "hdhhdvu" in data_ct and data_ct["hdhhdvu"]:
                    # Có chi tiết hàng hóa - tạo 1 record cho mỗi dòng hàng
                    for item_index, sp in enumerate(data_ct["hdhhdvu"]):
                        invoice_obj = create_detailed_invoice_object(data, data_ct, sp, logic.user, hdon, ttxly, item_index == 0)
                        json_data.append(invoice_obj)
                else:
                    # Không có chi tiết hàng hóa - tạo 1 record tổng hợp
                    invoice_obj = create_summary_invoice_object(data, data_ct, logic.user, hdon, ttxly)
                    json_data.append(invoice_obj)
                    
            except Exception as detail_error:
                print(f"Error getting detail for invoice {shd}: {detail_error}")
                # Tạo record cơ bản nếu không lấy được chi tiết
                invoice_obj = create_basic_invoice_object(data, logic.user, hdon, ttxly)
                json_data.append(invoice_obj)

        return {
            'success': True,
            'json_data': json_data,
            'message': f'Đã xử lý thành công {len(json_data)}/{count} hóa đơn'
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': str(e)
        }


def process_xml_html(type):
    try:
        # Thiết lập các biến cần thiết nhưng không tạo file
        day__ = logic.range_DAY_const.replace("Thời gian : ","").replace("/","-").replace("=>","_")
        us = logic.user.replace("\r","")
        br = 1
        
        if type == 1:
            br = 2
            mst = 1
            type_mb = logic.user
            type_hoadon = 'sold'
        
        d = ""
        e = ""
        type_list = {""}
        if type == 2:
            mst = 2
            type_mb = "nbmst"
            type_list = {'5','6','8'}
            d = f';ttxly=='
            type_hoadon = 'purchase'

        # Xử lý dữ liệu (giữ nguyên logic API calls)
        logic.len_a = len(logic.arr_ed)
        logic.n_pro = int(100/logic.len_a)
        logic.a = 0
        datas_first = ""
        n_collect = 100/len(logic.arr_ed)
        
        for logic.i in range(len(logic.arr_ed)):
            print("RUN")
            logic.a += n_collect
            begin_day = logic.arr_ed[logic.i][0]
            end_day = logic.arr_ed[logic.i][1]
            print(begin_day)
            print(end_day)
            spec = ""
            
            for e in type_list:
                for i in range(br):
                    if e == "8":
                        spec = "sco-"
                    else:
                        spec = ""
                    if i == 1:
                        spec = "sco-"
                        print(2)    
                    while True:
                        try:
                            res = logic.ssl_supressed_session().get(f'https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/{type_hoadon}?sort=tdlap:desc,khmshdon:asc,shdon:desc&size=50&search=tdlap=ge={begin_day}T00:00:00;tdlap=le={end_day}T23:59:59{d}{e}',headers=logic.headers,verify=False,timeout=0.5)
                            if res.status_code == 200:
                                break
                        except:
                            print("error")
                    data = res.json()
                    if datas_first == "":
                        print("first")
                        datas_first = data
                    else:
                        print("add ")
                        datas_first["datas"].extend(data["datas"])
                    while True:
                        if data["state"] != None and data != "":
                            print("chèn")
                            print(data['state'])
                            while True:
                                try:
                                    res = logic.ssl_supressed_session().get(f'https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/{type_hoadon}?sort=tdlap:desc,khmshdon:asc,shdon:desc&size=50&state={data["state"]}&search=tdlap=ge={begin_day}T00:00:00;tdlap=le={end_day}T23:59:59',headers=logic.headers,verify=False,timeout=0.5)
                                    if res.status_code == 200:
                                        break
                                    else:
                                        pass
                                except:
                                    print("error")
                            data = res.json()
                            if "state" in data:
                                datas_first["datas"].extend(data["datas"])
                        else:
                            break

        # Xử lý dữ liệu và tạo JSON response với file links
        datas_first["datas"] = logic.remove_duplicate_elements(datas_first["datas"])
        count = len(datas_first["datas"])
        
        if count == 0:
            return {
                'success': False,
                'message': 'Không có hóa đơn !'
            }

        # Tạo JSON data với file download links
        json_data = []
        
        for index, data in enumerate(datas_first["datas"], 1):
            spec = "sco-" if data["ttxly"] == 8 else ""
            
            # Lấy thông tin cơ bản
            nbmst = data["nbmst"]
            khhdon = data["khhdon"] 
            shd = data["shdon"]
            khmshdon = data["khmshdon"]
            nbd = data["ntao"].split("T")[0] if data.get("ntao") else ""
            
            # Tạo object với thông tin file links
            file_obj = {
                "_id": str(data.get("id", f"xml_html_{shd}")),
                "username": logic.user,
                "Mẫu số HD": data.get("khmshdon", ""),
                "Ký hiệu hóa  đơn": data.get("khhdon", ""),
                "Số hóa đơn": f" {data.get('shdon', '')}",
                "Ngày lập hóa đơn": format_date_simple(data.get("tdlap", "")),
                "MST người bán": data.get("nbmst", ""),
                "Tên người bán": data.get("nbten", ""),
                "MST người mua": data.get("nmmst", ""),
                "Tên người mua": data.get("nmten", ""),
                
                # File download URLs
                "xml_download_url": f"https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/detail?nbmst={nbmst}&khhdon={khhdon}&shdon={shd}&khmshdon={khmshdon}",
                "pdf_download_url": f"https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/pdf?nbmst={nbmst}&khhdon={khhdon}&shdon={shd}&khmshdon={khmshdon}",
                "html_preview_url": f"https://hoadondientu.gdt.gov.vn:30000/{spec}query/invoices/html?nbmst={nbmst}&khhdon={khhdon}&shdon={shd}&khmshdon={khmshdon}",
                
                # File name suggestions
                "suggested_xml_filename": f"{khmshdon}_{khhdon}_{shd}_{nbd}_{nbmst}.xml",
                "suggested_html_filename": f"{khmshdon}_{khhdon}_{shd}_{nbd}_{nbmst}.html",
                "suggested_pdf_filename": f"{khmshdon}_{khhdon}_{shd}_{nbd}_{nbmst}.pdf",
                
                # Additional info
                "file_type": "xml_html",
                "date_range": f"{logic.begin_} -> {logic.end_}",
                "spec_prefix": spec
            }
            
            json_data.append(file_obj)

        return {
            'success': True,
            'json_data': json_data,
            'message': f'Đã xử lý thành công {count}/{count} file links'
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': str(e)
        }

def format_date_simple(date_str):
    """Simple date formatter"""
    if not date_str: return ""
    try:
        date_part = date_str.split('T')[0]
        parts = date_part.split('-')
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
        return date_part
    except:
        return date_str

@app.route('/api/get-invoices', methods=['GET'])
def get_invoices():
    try:
        # Get query parameters
        invoice_type = request.args.get('type')  # 'sale' or 'purchase'
        date = request.args.get('date')  # Format: DD/MM/YYYY
        username = request.args.get('username')
        
        # Validate required parameters
        if not all([invoice_type, date, username]):
            return jsonify({
                'success': False,
                'message': 'Missing required parameters: type, date, or username'
            })
            
        # Validate invoice type
        if invoice_type not in ['sale', 'purchase']:
            return jsonify({
                'success': False,
                'message': 'Invalid invoice type. Must be either "sale" or "purchase"'
            })
            
        # Select the appropriate collection
        collection = logic.sales_collection if invoice_type == 'sale' else logic.purchase_collection
        
        # Query MongoDB
        invoices = list(collection.find({
            'username': username,
            'Ngày lập hóa đơn': date
        }, {'_id': 0}))  # Exclude MongoDB _id field
        
        return jsonify({
            'success': True,
            'data': invoices,
            'count': len(invoices)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

# transform push data
@app.route('/api/upload-master-data', methods=['POST'])
def upload_master_data():
    results = {}
    files_processed_count = 0

    # --- Lấy Username từ logic object ---
    # **CẢNH BÁO**: Cách này có thể không đáng tin cậy nếu trạng thái logic object không được duy trì.
    username = logic.user
    if not username:
        return jsonify({'success': False, 'message': 'User identifier not found in server state. Please login again.'}), 401
    print(f"Upload request received for user (from logic state): {username}") # Log kiểm tra

    # Định nghĩa các key file mong đợi và ánh xạ tới data_type_key
    file_mapping = {
        'khachhang_file': 'KhachHang',
        'nhacungcap_file': 'NhaCungCap',
        'vattu_file': 'DanhMucVatTu'
    }

    # Kiểm tra xem request có files không
    if not request.files:
         return jsonify({'success': False, 'message': 'No files found in the request.'}), 400

    # Xử lý từng loại file mong đợi
    for form_key, data_type_key in file_mapping.items():
        if form_key in request.files:
            file = request.files[form_key]
            # Đảm bảo file thực sự được gửi lên (không phải trường trống)
            if file and file.filename:
                files_processed_count += 1
                filename = file.filename
                print(f"Processing uploaded file '{filename}' for type '{data_type_key}' and user '{username}'") # Log

                try:
                    # Đọc nội dung file
                    file_content_bytes = file.read()
                    if not file_content_bytes:
                         print(f"Warning: File '{filename}' is empty.")
                         results[data_type_key] = {'success': False, 'message': f"File '{filename}' trống."}
                         continue

                    # Xác định loại file
                    is_csv = filename.lower().endswith('.csv')
                    is_xlsx = filename.lower().endswith('.xlsx')

                    if not is_csv and not is_xlsx:
                        results[data_type_key] = {'success': False, 'message': f"Định dạng file không hỗ trợ: {filename}. Chỉ chấp nhận .csv hoặc .xlsx."}
                        continue

                    # Gọi hàm xử lý từ data_processor
                    process_result = process_uploaded_data(
                        file_content_bytes=file_content_bytes,
                        data_type_key=data_type_key,
                        username=username, # Truyền username lấy từ logic object
                        is_csv=is_csv
                    )
                    results[data_type_key] = process_result

                except Exception as e:
                    print(f"Error processing uploaded file '{filename}': {e}") # Log lỗi chi tiết trên server
                    # Không lộ chi tiết lỗi ra client
                    results[data_type_key] = {'success': False, 'message': f"Lỗi server khi xử lý file '{filename}'."}
            else:
                 # Có key form nhưng không có file -> bỏ qua, không báo lỗi
                 print(f"No file uploaded for key '{form_key}'.")

    # Nếu không có file nào hợp lệ được xử lý
    if files_processed_count == 0:
        return jsonify({'success': False, 'message': 'Không có file nào hợp lệ được tải lên trong request.', 'details': results}), 400

    # Kiểm tra kết quả tổng thể
    overall_success = all(res.get('success', False) for res in results.values() if isinstance(res, dict))

    return jsonify({
        'success': overall_success,
        'message': 'Quá trình xử lý file upload hoàn tất.' if overall_success else 'Có lỗi xảy ra trong quá trình xử lý một hoặc nhiều file.',
        'details': results # Trả về kết quả chi tiết cho từng loại file đã xử lý
    }), 200 if overall_success else 400


@app.route('/api/transform-data', methods=['POST'])
def transform_data():
    results = {}
    overall_success = True

    # --- Lấy Username từ logic object ---
    username = logic.user
    if not username:
        return jsonify({'success': False, 'message': 'User identifier not found in server state. Please login again.'}), 401
    print(f"Transform request received for user (from logic state): {username}")

    collections_to_process = ["HoaDonMuaVao", "HoaDonBanRa"]

    for collection_name in collections_to_process:
        print(f"--- Triggering transform for collection: {collection_name}, user: {username} ---")
        try:
            # Gọi hàm xử lý từ file transform.py
            summary = update_and_sync_ma_vat_tu(username, collection_name)
            results[collection_name] = summary
            if not summary.get('success', False):
                overall_success = False
        except Exception as e:
            error_message = f"Lỗi nghiêm trọng khi gọi xử lý cho {collection_name}: {e}"
            print(error_message)
            logging.error(error_message, exc_info=True)
            results[collection_name] = {'success': False, 'message': error_message, 'errors_encountered': -1} # Đánh dấu lỗi gọi hàm
            overall_success = False

    final_message = "Quá trình transform hoàn tất." if overall_success else "Có lỗi xảy ra trong quá trình transform."
    status_code = 200 if overall_success else 500 # 500 nếu có lỗi nghiêm trọng

    return jsonify({
        'success': overall_success,
        'message': final_message,
        'details': results
    }), status_code

def create_detailed_invoice_object(data, data_ct, sp, username, hdon, ttxly, is_first_item):
    """Tạo object cho hóa đơn có chi tiết hàng hóa"""
    
    def format_date(date_str):
        if not date_str: return ""
        try:
            date_part = date_str.split('T')[0]
            parts = date_part.split('-')
            if len(parts) == 3:
                return f"{parts[2]}/{parts[1]}/{parts[0]}"
            return date_part
        except:
            return date_str
    
    # Base object với thông tin chung
    invoice_obj = {
        "_id": create_unique_invoice_id(data, data_ct, sp),
        "username": username,
        "Mẫu số HD": data.get("khmshdon", ""),
        "Ký hiệu hóa  đơn": data.get("khhdon", ""),
        "Số hóa đơn": f" {data.get('shdon', '')}",
        "Ngày lập hóa đơn": format_date(data.get("tdlap", "")),
        "Ngày người bán ký số": format_date(data.get("nky", "")),
        "Ngày CQT ký số": format_date(data_ct.get("nky", "")),
        "MST người bán": data_ct.get("nbmst", ""),
        "Tên người bán": data_ct.get("nbten", ""),
        "Địa chỉ người bán": data_ct.get("nbdchi", ""),
        "MST người mua": data_ct.get("nmmst", ""),
        "Tên người mua": data_ct.get("nmten", ""),
        "Địa chỉ người mua": data_ct.get("nmdchi", ""),
        "Trạng thái hóa đơn": hdon.get(data.get("tthai"), ""),
        "Kết quả kiểm tra hóa đơn": ttxly.get(data.get("ttxly"), ""),
        "Đơn vị tiền tệ": data_ct.get("dvtte", "VND"),
        "Tỷ giá": data_ct.get("tgia", 1.0),
        
        # Chi tiết hàng hóa
        "Mã VT": sp.get("m_VT", ""),
        "Tên hàng hóa, dịch vụ": sp.get("ten", ""),
        "Đơn vị tính": sp.get("dvtinh", ""),
        "Số lượng": sp.get("sluong", 0),
        "Đơn giá": sp.get("dgia", 0),
        "Chiết khấu": sp.get("stckhau", 0),
        "Thành tiền chưa thuế": sp.get("thtien", 0),
        "Thuế suất": format_tax_rate(sp.get("tsuat", 0), sp.get("ltsuat", "")),
        "Tiền thuế": sp.get("tthue") if sp.get("tthue") is not None else calculate_tax_amount(sp.get("thtien", 0), sp.get("tsuat", 0)),
        "Tính chất": format_product_type(sp.get("tchat", 1))
    }
    
    # Chỉ ghi thông tin tổng ở dòng đầu tiên
    if is_first_item:
        invoice_obj.update({
            "Tổng tiền CKTM": data_ct.get("ttcktmai", 0),
            "Tổng tiền phí": data_ct.get("tgtphi", 0),
            "Tổng tiền thanh toán": data_ct.get("tgtttbso", 0),
            "Hình  thức thanh toán": get_payment_method_from_data(data_ct),
        })
    else:
        invoice_obj.update({
            "Tổng tiền CKTM": 0,
            "Tổng tiền phí": None,
            "Tổng tiền thanh toán": 0,
            "Hình  thức thanh toán": "",
        })
        
    # Thêm các fields khác từ mẫu
    invoice_obj.update({
        "Ghi chú 1": "  ",
        "Ghi chú 2": " ",
        "Hạn dùng ": None,
        "MCCQT": get_mccqt_from_data(data_ct),
        "Mã tra cứu": get_lookup_code_from_data(data_ct),
        "Số lô ": None,
        "url  tra cứu hóa đọn": "",
    })
    
    return invoice_obj

def create_summary_invoice_object(data, data_ct, username, hdon, ttxly):
    """Tạo object cho hóa đơn không có chi tiết hàng hóa"""
    
    def format_date(date_str):
        if not date_str: return ""
        try:
            date_part = date_str.split('T')[0]
            parts = date_part.split('-')
            if len(parts) == 3:
                return f"{parts[2]}/{parts[1]}/{parts[0]}"
            return date_part
        except:
            return date_str
    
    return {
        "_id": create_unique_invoice_id(data, data_ct),
        "username": username,
        "Mẫu số HD": data.get("khmshdon", ""),
        "Ký hiệu hóa  đơn": data.get("khhdon", ""),
        "Số hóa đơn": f" {data.get('shdon', '')}",
        "Ngày lập hóa đơn": format_date(data.get("tdlap", "")),
        "Ngày người bán ký số": format_date(data.get("nky", "")),
        "Ngày CQT ký số": format_date(data_ct.get("nky", "")),
        "MST người bán": data_ct.get("nbmst", ""),
        "Tên người bán": data_ct.get("nbten", ""),
        "Địa chỉ người bán": data_ct.get("nbdchi", ""),
        "MST người mua": data_ct.get("nmmst", ""),
        "Tên người mua": data_ct.get("nmten", ""),
        "Địa chỉ người mua": data_ct.get("nmdchi", ""),
        "Trạng thái hóa đơn": hdon.get(data.get("tthai"), ""),
        "Kết quả kiểm tra hóa đơn": ttxly.get(data.get("ttxly"), ""),
        "Đơn vị tiền tệ": data_ct.get("dvtte", "VND"),
        "Tỷ giá": data_ct.get("tgia", 1.0),
        "Tổng tiền CKTM": data_ct.get("ttcktmai", 0),
        "Tổng tiền phí": data_ct.get("tgtphi", 0),
        "Tổng tiền thanh toán": data_ct.get("tgtttbso", 0),
        "Hình  thức thanh toán": get_payment_method_from_data(data_ct),
        
        # Các trường chi tiết để trống
        "Mã VT": "",
        "Tên hàng hóa, dịch vụ": "",
        "Đơn vị tính": "",
        "Số lượng": 0,
        "Đơn giá": 0,
        "Chiết khấu": 0,
        "Thành tiền chưa thuế": data_ct.get("tgtcthue", 0),
        "Thuế suất": "",
        "Tiền thuế": data_ct.get("tgtthue") if data_ct.get("tgtthue") is not None else 0,
        "Tính chất": "",
        
        # Thêm các fields khác
        "Ghi chú 1": "  ",
        "Ghi chú 2": " ",
        "Hạn dùng ": None,
        "MCCQT": get_mccqt_from_data(data_ct),
        "Mã tra cứu": get_lookup_code_from_data(data_ct),
        "Số lô ": None,
        "url  tra cứu hóa đơn": "",
    }

def create_basic_invoice_object(data, username, hdon, ttxly):
    """Tạo object cơ bản khi không lấy được chi tiết - với cấu trúc đầy đủ"""
    
    def format_date(date_str):
        if not date_str: return ""
        try:
            date_part = date_str.split('T')[0]
            parts = date_part.split('-')
            if len(parts) == 3:
                return f"{parts[2]}/{parts[1]}/{parts[0]}"
            return date_part
        except:
            return date_str
    
    return {
        "_id": create_unique_invoice_id(data, {}),
        "username": username,
        "Mẫu số HD": data.get("khmshdon", ""),
        "Ký hiệu hóa  đơn": data.get("khhdon", ""),
        "Số hóa đơn": f" {data.get('shdon', '')}",
        "Ngày lập hóa đơn": format_date(data.get("tdlap", "")),
        "Ngày người bán ký số": format_date(data.get("nky", "")),
        "Ngày CQT ký số": format_date(data.get("nky", "")),  # Fallback to main data
        "MST người bán": data.get("nbmst", ""),
        "Tên người bán": data.get("nbten", ""),
        "Địa chỉ người bán": data.get("nbdchi", ""),
        "MST người mua": data.get("nmmst", ""),
        "Tên người mua": data.get("nmten", ""),
        "Địa chỉ người mua": data.get("nmdchi", ""),
        "Trạng thái hóa đơn": hdon.get(data.get("tthai"), ""),
        "Kết quả kiểm tra hóa đơn": ttxly.get(data.get("ttxly"), ""),
        "Đơn vị tiền tệ": data.get("dvtte", "VND"),
        "Tỷ giá": data.get("tgia", 1.0),
        "Tổng tiền CKTM": data.get("ttcktmai", 0),
        "Tổng tiền phí": data.get("tgtphi", 0),
        "Tổng tiền thanh toán": data.get("tgtttbso", 0),
        "Hình  thức thanh toán": data.get("thtttoan", ""),  # Fallback to main data
        
        # Các trường chi tiết để trống
        "Mã VT": "",
        "Tên hàng hóa, dịch vụ": "",
        "Đơn vị tính": "",
        "Số lượng": 0,
        "Đơn giá": 0,
        "Chiết khấu": 0,
        "Thành tiền chưa thuế": data.get("tgtcthue", 0),
        "Thuế suất": "",
        "Tiền thuế": data.get("tgtthue", 0),
        "Tính chất": "",
        
        # Thêm các fields khác
        "Ghi chú 1": "  ",
        "Ghi chú 2": " ",
        "Hạn dùng ": None,
        "MCCQT": data.get("mhdon", ""),  # Fallback to main data
        "Mã tra cứu": "",  # Cannot extract without data_ct
        "Số lô ": None,
        "url  tra cứu hóa đơn": "",
    }

def format_tax_rate(tsuat, ltsuat):
    """Format thuế suất theo quy tắc"""
    if ltsuat == "KHAC":
        return "KHAC"
    elif ltsuat == "KKKNT":
        return "KKKNT"
    elif isinstance(tsuat, (int, float)) and tsuat == 0.0 and ltsuat == "KCT":
        return "KCT"
    elif isinstance(tsuat, (int, float)) and tsuat == 0.0:
        return "0%"
    elif isinstance(tsuat, (int, float)):
        return f"{tsuat * 100}%"
    else:
        return str(tsuat)

def format_product_type(tchat):
    """Format tính chất sản phẩm"""
    if tchat == 1:
        return "Hàng hóa, dịch vụ"
    elif tchat == 2:
        return "Khuyến mại"
    elif tchat == 3:
        return "Chiết khấu"
    elif tchat == 4:
        return "Ghi chú, diễn giải"
    else:
        return ""

def get_lookup_code(data_ct):
    """Lấy mã tra cứu từ data chi tiết"""
    try:
        for i in data_ct.get("ttkhac", []):
            if i.get("ttruong") in ["Mã số bí mật", "KeySearch", "Mã TC", "TransactionID", "Fkey", "MNHDon", "QuanLy_SoBaoMat", "Mã bảo mật", "Số bảo mật", "chungTuLienQuan", "Mã tra cứu hóa đơn", "InvoiceId", "MaTraCuu", "MTCuu", "SearchCode"]:
                return i.get("dlieu", "")
        return ""
    except:
        return ""

def get_payment_method_from_data(data_ct):
    """Lấy hình thức thanh toán từ API response"""
    # Trường chính xác từ debug output: thtttoan
    if 'thtttoan' in data_ct and data_ct['thtttoan']:
        return str(data_ct['thtttoan']).strip()
    
    # Các trường backup khác (Vietnamese tax system fields)
    payment_fields = [
        'httttoan',     # Hình thức thanh toán (most common)
        'htthanhtoan',  # Hình thức thanh toán (variant)
        'hinhthucthanhtoan',
        'phuongthucthanhtoan', 
        'paymentmethod',
        'methodpayment',
        'pttt',         # Phương thức thanh toán (short form)
        'ptttoan',      # Phương thức thanh toán
        'ttoan'         # Thanh toán (short)
    ]
    
    for field in payment_fields:
        value = data_ct.get(field)
        if value and str(value).strip():
            return str(value).strip()
    
    # Kiểm tra trong các trường bổ sung (ttkhac)
    try:
        ttkhac_list = data_ct.get("ttkhac", [])
        if isinstance(ttkhac_list, list):
            for item in ttkhac_list:
                if isinstance(item, dict):
                    ttruong = item.get("ttruong", "").lower()
                    if any(keyword in ttruong for keyword in [
                        "hình thức thanh toán", "httt", "phương thức thanh toán",
                        "pttt", "thanh toán", "payment"
                    ]):
                        dlieu = item.get("dlieu", "").strip()
                        if dlieu:
                            return dlieu
    except:
        pass
    
    return ""

def get_mccqt_from_data(data_ct):
    """Lấy mã CQT từ API response"""
    # Trường chính xác từ debug output: mhdon (chứa chuỗi hex dài)
    if 'mhdon' in data_ct and data_ct['mhdon']:
        return str(data_ct['mhdon']).strip()
    
    # Các trường backup khác (Vietnamese tax system fields)
    cqt_fields = [
        'mccqt',        # Mã CQT (most common)
        'macqt',        # Mã CQT (variant)
        'cqt',          # Mã CQT (numeric, fallback)
        'securitycode', # Security code
        'macuacqt',     # Mã của CQT
        'macanq',       # Mã cơ quan
        'cqtcode'       # CQT code
    ]
    
    for field in cqt_fields:
        value = data_ct.get(field)
        if value and str(value).strip():
            return str(value).strip()
    
    # Kiểm tra trong ttkhac
    try:
        ttkhac_list = data_ct.get("ttkhac", [])
        if isinstance(ttkhac_list, list):
            for item in ttkhac_list:
                if isinstance(item, dict):
                    ttruong = item.get("ttruong", "").lower()
                    if any(keyword in ttruong for keyword in [
                        'mã cqt', 'mccqt', 'ma cqt', 'security', 'mcc', 'code'
                    ]):
                        dlieu = item.get("dlieu", "").strip()
                        if dlieu:
                            return dlieu
    except:
        pass
    
    return ""
    
    # Kiểm tra trong ttkhac với nhiều pattern
    try:
        ttkhac_list = data_ct.get("ttkhac", [])
        if isinstance(ttkhac_list, list):
            for item in ttkhac_list:
                if isinstance(item, dict):
                    ttruong = item.get("ttruong", "").lower()
                    if any(keyword in ttruong for keyword in [
                        'mã cqt', 'mccqt', 'security', 'mcc', 'mã cơ quan thuế',
                        'cqt', 'mã kiểm tra', 'verification code'
                    ]):
                        dlieu = item.get("dlieu", "").strip()
                        if dlieu:
                            return dlieu
    except:
        pass
    
    return ""

def get_lookup_code_from_data(data_ct):
    """Lấy mã tra cứu từ data chi tiết - version cải tiến"""
    # Kiểm tra các trường thường gặp trước
    lookup_fields = [
        'matracuu',
        'tracuu', 
        'lookupcode',
        'searchcode'
    ]
    
    for field in lookup_fields:
        value = data_ct.get(field)
        if value:
            return str(value)
    
    # Kiểm tra trong ttkhac với nhiều pattern hơn
    try:
        for item in data_ct.get("ttkhac", []):
            ttruong = item.get("ttruong", "").lower()
            if any(keyword in ttruong for keyword in [
                "mã số bí mật", "keysearch", "mã tc", "transactionid", 
                "fkey", "mnhdon", "quanly_sobaomat", "mã bảo mật", 
                "số bảo mật", "chungtulienquan", "mã tra cứu hóa đơn",
                "invoiceid", "matracuu", "mtcuu", "searchcode", "tra cứu"
            ]):
                return item.get("dlieu", "")
    except:
        pass
    
    return ""

def create_unique_invoice_id(data, data_ct, sp=None):
    """Tạo ID unique cho invoice với format ngắn hơn"""
    import hashlib
    
    # Thử lấy ID từ API response trước - với format ngắn hơn
    id_candidates = [
        # Từ data_ct (detail response)
        data_ct.get('id'),
        data_ct.get('_id'), 
        data_ct.get('invoiceid'),
        data_ct.get('hdonid'),
        data_ct.get('identifier'),
        # Từ data (main response)
        data.get('id'),
        data.get('_id'),
        data.get('invoiceid'),
        data.get('hdonid')
    ]
    
    for candidate in id_candidates:
        if candidate and str(candidate).strip():
            # Chuyển về format ngắn hơn để match với form.json
            candidate_str = str(candidate).strip()
            if len(candidate_str) > 24:  # Nếu là UUID dài, hash lại
                hash_obj = hashlib.md5(candidate_str.encode('utf-8'))
                return hash_obj.hexdigest()[:24]
            return candidate_str
    
    # Tạo ID từ các thông tin unique của hóa đơn
    unique_parts = [
        str(data.get('nbmst', '')),      # MST người bán
        str(data.get('khhdon', '')),     # Ký hiệu hóa đơn
        str(data.get('shdon', '')),      # Số hóa đơn
        str(data.get('khmshdon', '')),   # Mẫu số hóa đơn
        str(data.get('tdlap', '')),      # Thời điểm lập
    ]
    
    # Thêm tên sản phẩm nếu có (để phân biệt các dòng trong cùng hóa đơn)
    if sp and sp.get('ten'):
        unique_parts.append(str(sp.get('ten', '')))
    
    # Loại bỏ các phần tử rỗng
    unique_parts = [part for part in unique_parts if part.strip()]
    
    if not unique_parts:
        # Fallback: sử dụng timestamp hiện tại
        import time
        unique_parts = [str(int(time.time() * 1000))]
    
    unique_string = '|'.join(unique_parts)
    hash_id = hashlib.md5(unique_string.encode('utf-8')).hexdigest()[:24]
    
    return hash_id

def calculate_tax_amount(base_amount, tax_rate):
    """Tính tiền thuế từ thành tiền và thuế suất"""
    try:
        if not base_amount or not tax_rate:
            return 0
        
        # Chuyển đổi tax_rate từ phần trăm sang decimal nếu cần
        if isinstance(tax_rate, str):
            # Xử lý string như "8%" hoặc "0.08"
            rate_str = tax_rate.replace('%', '').strip()
            rate = float(rate_str)
            if rate > 1:  # Nếu là phần trăm (8 thay vì 0.08)
                rate = rate / 100
        else:
            rate = float(tax_rate)
            if rate > 1:  # Nếu là phần trăm
                rate = rate / 100
                
        tax_amount = float(base_amount) * rate
        # Làm tròn về số nguyên như hệ thống thuế Việt Nam
        return float(round(tax_amount))
    except:
        return 0

# ================================
# MongoDB Query APIs
# ================================

@app.route('/api/invoices/<unique_key>', methods=['GET'])
def get_invoice_detail(unique_key):
    """Lấy chi tiết hóa đơn với danh sách sản phẩm"""
    try:
        from invoice_mongo_processor import InvoiceDataProcessor
        
        processor = InvoiceDataProcessor()
        invoice_data = processor.get_invoice_with_items(unique_key)
        
        if not invoice_data:
            return jsonify({
                'success': False,
                'message': 'Không tìm thấy hóa đơn'
            }), 404
        
        # Clean MongoDB ObjectIds
        cleaned_data = clean_mongodb_data(invoice_data)
        
        return jsonify({
            'success': True,
            'data': cleaned_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Lỗi truy vấn dữ liệu: {str(e)}'
        }), 500

@app.route('/api/invoices/search', methods=['POST'])
def search_invoices():
    """Tìm kiếm hóa đơn theo nhiều tiêu chí"""
    try:
        from invoice_mongo_processor import InvoiceDataProcessor
        data = request.json
        
        # Build query từ parameters
        query = {}
        
        if data.get('seller_tax_code'):
            query['seller.tax_code'] = data['seller_tax_code']
            
        if data.get('buyer_tax_code'):
            query['buyer.tax_code'] = data['buyer_tax_code']
            
        if data.get('start_date') and data.get('end_date'):
            query['issue_date'] = {
                '$gte': data['start_date'],
                '$lte': data['end_date']
            }
        
        if data.get('invoice_number'):
            query['invoice_number'] = data['invoice_number']
            
        if data.get('template_code'):
            query['template_code'] = str(data['template_code'])
        
        # Pagination
        page = int(data.get('page', 1))
        limit = int(data.get('limit', 20))
        skip = (page - 1) * limit
        
        processor = InvoiceDataProcessor()
        
        # Query with pagination
        cursor = processor.invoices.find(query).sort('issue_date', -1).skip(skip).limit(limit)
        invoices = list(cursor)
        
        # Get total count
        total = processor.invoices.count_documents(query)
        
        # Clean data
        cleaned_invoices = clean_mongodb_data(invoices)
        
        return jsonify({
            'success': True,
            'data': cleaned_invoices,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total,
                'pages': (total + limit - 1) // limit
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Lỗi tìm kiếm: {str(e)}'
        }), 500

@app.route('/api/analytics/revenue', methods=['POST'])
def get_revenue_analytics():
    """Phân tích doanh thu theo thời gian"""
    try:
        from invoice_mongo_processor import InvoiceDataProcessor
        data = request.json
        
        required_fields = ['seller_tax_code', 'start_date', 'end_date']
        for field in required_fields:
            if not data.get(field):
                return jsonify({
                    'success': False,
                    'message': f'Thiếu trường bắt buộc: {field}'
                }), 400
        
        processor = InvoiceDataProcessor()
        analytics_data = processor.get_revenue_analysis(
            data['seller_tax_code'],
            data['start_date'], 
            data['end_date']
        )
        
        return jsonify({
            'success': True,
            'data': analytics_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Lỗi phân tích doanh thu: {str(e)}'
        }), 500

@app.route('/api/analytics/summary', methods=['POST'])
def get_business_summary():
    """Tổng hợp thống kê kinh doanh"""
    try:
        from invoice_mongo_processor import InvoiceDataProcessor
        data = request.json
        
        processor = InvoiceDataProcessor()
        
        # Build match query
        match_query = {}
        if data.get('seller_tax_code'):
            match_query['seller_tax_code'] = data['seller_tax_code']
        if data.get('start_date') and data.get('end_date'):
            match_query['date'] = {
                '$gte': data['start_date'],
                '$lte': data['end_date']
            }
        
        # Aggregation pipeline
        pipeline = [
            {'$match': match_query},
            {
                '$group': {
                    '_id': None,
                    'total_revenue': {'$sum': '$total_revenue'},
                    'total_tax': {'$sum': '$total_tax'},
                    'total_invoices': {'$sum': '$total_invoices'},
                    'total_items': {'$sum': '$total_items'},
                    'unique_buyers': {'$addToSet': '$buyer_tax_code'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'total_revenue': 1,
                    'total_tax': 1,
                    'total_invoices': 1,
                    'total_items': 1,
                    'unique_buyers_count': {'$size': '$unique_buyers'}
                }
            }
        ]
        
        result = list(processor.invoice_analytics.aggregate(pipeline))
        summary = result[0] if result else {
            'total_revenue': 0,
            'total_tax': 0,
            'total_invoices': 0,
            'total_items': 0,
            'unique_buyers_count': 0
        }
        
        return jsonify({
            'success': True,
            'data': summary
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Lỗi thống kê tổng hợp: {str(e)}'
        }), 500

@app.route('/api/products/top-selling', methods=['POST'])
def get_top_selling_products():
    """Lấy danh sách sản phẩm bán chạy"""
    try:
        from invoice_mongo_processor import InvoiceDataProcessor
        data = request.json
        
        processor = InvoiceDataProcessor()
        
        # Build match query cho items
        match_query = {}
        if data.get('start_date') and data.get('end_date'):
            # Cần join với invoices để filter theo ngày
            pipeline = [
                {
                    '$lookup': {
                        'from': 'invoices',
                        'localField': 'invoice_id',
                        'foreignField': '_id',
                        'as': 'invoice'
                    }
                },
                {'$unwind': '$invoice'},
                {
                    '$match': {
                        'invoice.issue_date': {
                            '$gte': data['start_date'],
                            '$lte': data['end_date']
                        },
                        'item_type': 'product_service'  # Chỉ lấy sản phẩm/dịch vụ thực
                    }
                }
            ]
        else:
            pipeline = [
                {
                    '$match': {
                        'item_type': 'product_service'
                    }
                }
            ]
        
        # Group by product
        pipeline.extend([
            {
                '$group': {
                    '_id': '$item_name',
                    'total_quantity': {'$sum': '$quantity'},
                    'total_revenue': {'$sum': '$subtotal'},
                    'total_tax': {'$sum': '$tax_amount'},
                    'avg_price': {'$avg': '$unit_price'},
                    'invoice_count': {'$sum': 1}
                }
            },
            {'$sort': {'total_quantity': -1}},
            {'$limit': int(data.get('limit', 10))}
        ])
        
        result = list(processor.invoice_items.aggregate(pipeline))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Lỗi thống kê sản phẩm: {str(e)}'
        }), 500

if __name__ == '__main__':
    # Get port from environment or default to 5000
    port = int(os.getenv('PORT', 5000))
    host = os.getenv('HOST', '0.0.0.0')  # Use 0.0.0.0 for Docker
    
    print(f"🚀 Starting server on {host}:{port}")
    print(f"📊 MongoDB: {os.getenv('MONGO_DB_NAME', 'MolaDatabase')}")
    print(f"🔗 Atlas URI: {os.getenv('MONGO_URI', 'Not configured')[:50]}...")
    
    app.run(host=host, port=port, debug=False)