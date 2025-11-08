from pymongo import MongoClient
from datetime import datetime, date
import json
from typing import List, Dict, Any
from dataclasses import dataclass
import hashlib

class InvoiceDataProcessor:
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017", db_name: str = "invoice_system"):
        """
        Khởi tạo processor để lưu dữ liệu hóa đơn vào MongoDB
        """
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        
        # Collections
        self.invoices = self.db.invoices
        self.invoice_items = self.db.invoice_items
        self.invoice_analytics = self.db.invoice_analytics
        
        # Tạo indexes
        self._create_indexes()
    
    def _create_indexes(self):
        """Tạo các indexes cần thiết"""
        try:
            # Invoices indexes
            self.invoices.create_index([("unique_key", 1)], unique=True)
            self.invoices.create_index([("seller.tax_code", 1), ("issue_date", -1)])
            self.invoices.create_index([("buyer.tax_code", 1), ("issue_date", -1)])
            self.invoices.create_index([("issue_date", -1)])
            self.invoices.create_index([("processing_info.lookup_code", 1)])
            
            # Invoice items indexes
            self.invoice_items.create_index([("invoice_id", 1)])
            self.invoice_items.create_index([("invoice_unique_key", 1)])
            self.invoice_items.create_index([("item_name", "text")])
            
            # Analytics indexes
            self.invoice_analytics.create_index([("date", -1)])
            self.invoice_analytics.create_index([("seller_tax_code", 1), ("date", -1)])
            self.invoice_analytics.create_index([("buyer_tax_code", 1), ("date", -1)])
            
            print("✅ Indexes created successfully")
        except Exception as e:
            print(f"⚠️  Index creation warning: {e}")
    
    def process_and_save_invoices(self, raw_invoice_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Xử lý và lưu dữ liệu hóa đơn từ API response
        """
        try:
            # Group items by invoice
            invoice_groups = self._group_items_by_invoice(raw_invoice_data)
            
            saved_invoices = 0
            saved_items = 0
            updated_invoices = 0
            
            for unique_key, items in invoice_groups.items():
                # Lấy thông tin hóa đơn từ item đầu tiên (vì thông tin hóa đơn giống nhau)
                first_item = items[0]
                
                # Tạo invoice document
                invoice_doc = self._create_invoice_document(first_item, items)
                
                # Lưu hoặc update invoice
                result = self.invoices.replace_one(
                    {"unique_key": unique_key},
                    invoice_doc,
                    upsert=True
                )
                
                if result.upserted_id:
                    saved_invoices += 1
                    invoice_id = result.upserted_id
                else:
                    updated_invoices += 1
                    invoice_id = self.invoices.find_one({"unique_key": unique_key})["_id"]
                
                # Xóa items cũ của invoice này
                self.invoice_items.delete_many({"invoice_unique_key": unique_key})
                
                # Lưu invoice items
                item_docs = self._create_item_documents(items, invoice_id, unique_key)
                if item_docs:
                    self.invoice_items.insert_many(item_docs)
                    saved_items += len(item_docs)
                
                # Update analytics
                self._update_analytics(invoice_doc, len(item_docs))
            
            return {
                "success": True,
                "message": f"Đã lưu thành công {saved_invoices} hóa đơn mới, cập nhật {updated_invoices} hóa đơn, {saved_items} sản phẩm",
                "summary": {
                    "new_invoices": saved_invoices,
                    "updated_invoices": updated_invoices,
                    "total_items": saved_items,
                    "unique_invoices": len(invoice_groups)
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Lỗi khi lưu dữ liệu: {str(e)}"
            }
    
    def _group_items_by_invoice(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group các items theo hóa đơn dựa vào Số hóa đơn, Mẫu số HD và MST người bán
        """
        groups = {}
        
        for item in raw_data:
            # Tạo unique key cho hóa đơn
            template_code = str(item.get("Mẫu số HD", ""))
            invoice_number = str(item.get("Số hóa đơn", "")).strip()
            seller_tax_code = str(item.get("MST người bán", ""))
            
            unique_key = f"{template_code}_{invoice_number}_{seller_tax_code}"
            
            if unique_key not in groups:
                groups[unique_key] = []
            
            groups[unique_key].append(item)
        
        return groups
    
    def _create_invoice_document(self, first_item: Dict[str, Any], all_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Tạo document cho collection invoices
        """
        # Tính tổng tài chính từ tất cả items
        total_subtotal = sum(float(item.get("Thành tiền chưa thuế", 0) or 0) for item in all_items)
        total_tax = sum(float(item.get("Tiền thuế", 0) or 0) for item in all_items)
        
        # Lấy thông tin tổng từ item đầu tiên (chỉ item đầu có thông tin tổng)
        total_amount = float(first_item.get("Tổng tiền thanh toán", 0) or 0)
        total_discount = float(first_item.get("Tổng tiền CKTM", 0) or 0)
        total_fees = float(first_item.get("Tổng tiền phí", 0) or 0)
        
        # Parse dates
        issue_date = self._parse_date(first_item.get("Ngày lập hóa đơn", ""))
        seller_signature_date = self._parse_date(first_item.get("Ngày người bán ký số", ""))
        tax_office_signature_date = self._parse_date(first_item.get("Ngày CQT ký số", ""))
        
        invoice_doc = {
            "invoice_number": str(first_item.get("Số hóa đơn", "")).strip(),
            "template_code": str(first_item.get("Mẫu số HD", "")),
            "symbol": first_item.get("Ký hiệu hóa  đơn", ""),
            
            # Thông tin ngày tháng
            "issue_date": issue_date,
            "seller_signature_date": seller_signature_date,
            "tax_office_signature_date": tax_office_signature_date,
            
            # Người bán
            "seller": {
                "tax_code": first_item.get("MST người bán", ""),
                "name": first_item.get("Tên người bán", ""),
                "address": first_item.get("Địa chỉ người bán", "")
            },
            
            # Người mua
            "buyer": {
                "tax_code": first_item.get("MST người mua", ""),
                "name": first_item.get("Tên người mua", ""),
                "address": first_item.get("Địa chỉ người mua", "")
            },
            
            # Thông tin tài chính
            "financial_summary": {
                "subtotal_before_tax": total_subtotal,
                "total_tax": total_tax,
                "total_discount": total_discount,
                "total_fees": total_fees,
                "total_amount": total_amount,
                "currency": first_item.get("Đơn vị tiền tệ", "VND"),
                "exchange_rate": float(first_item.get("Tỷ giá", 1.0))
            },
            
            # Thông tin xử lý
            "processing_info": {
                "status": first_item.get("Trạng thái hóa đơn", ""),
                "verification_result": first_item.get("Kết quả kiểm tra hóa đơn", ""),
                "tax_office_code": first_item.get("MCCQT", ""),
                "lookup_code": first_item.get("Mã tra cứu", ""),
                "payment_method": first_item.get("Hình  thức thanh toán", "")
            },
            
            # Metadata
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "username": first_item.get("username", ""),
            "unique_key": f"{first_item.get('Mẫu số HD', '')}_{str(first_item.get('Số hóa đơn', '')).strip()}_{first_item.get('MST người bán', '')}",
            
            # Thống kê
            "item_count": len(all_items),
            "has_product_details": any(item.get("Tên hàng hóa, dịch vụ", "").strip() and 
                                     item.get("Tính chất", "") == "Hàng hóa, dịch vụ" for item in all_items)
        }
        
        return invoice_doc
    
    def _create_item_documents(self, items: List[Dict[str, Any]], invoice_id, unique_key: str) -> List[Dict[str, Any]]:
        """
        Tạo documents cho collection invoice_items
        """
        item_docs = []
        
        for sequence, item in enumerate(items, 1):
            # Chỉ lưu items có nội dung thực sự
            item_name = item.get("Tên hàng hóa, dịch vụ", "").strip()
            if not item_name:
                continue
                
            # Parse tax rate
            tax_rate_str = item.get("Thuế suất", "")
            tax_type = self._determine_tax_type(tax_rate_str)
            
            # Parse item type
            item_type, item_type_display = self._parse_item_type(item.get("Tính chất", ""))
            
            item_doc = {
                "invoice_id": invoice_id,
                "invoice_unique_key": unique_key,
                
                # Thông tin sản phẩm
                "item_code": item.get("Mã VT", ""),
                "item_name": item_name,
                "unit": item.get("Đơn vị tính", ""),
                "quantity": float(item.get("Số lượng", 0) or 0),
                "unit_price": float(item.get("Đơn giá", 0) or 0),
                "discount": float(item.get("Chiết khấu", 0) or 0),
                "subtotal": float(item.get("Thành tiền chưa thuế", 0) or 0),
                
                # Thuế
                "tax_rate": tax_rate_str,
                "tax_amount": float(item.get("Tiền thuế", 0) or 0),
                "tax_type": tax_type,
                
                # Tính chất
                "item_type": item_type,
                "item_type_display": item_type_display,
                
                # Metadata
                "notes_1": item.get("Ghi chú 1", ""),
                "notes_2": item.get("Ghi chú 2", ""),
                "expiry_date": item.get("Hạn dùng ", None),
                "batch_number": item.get("Số lô ", None),
                
                "created_at": datetime.now(),
                "sequence": sequence
            }
            
            item_docs.append(item_doc)
        
        return item_docs
    
    def _parse_date(self, date_str: str) -> str:
        """
        Parse date từ format DD/MM/YYYY sang YYYY-MM-DD
        """
        if not date_str or date_str.strip() == "":
            return None
            
        try:
            # Format từ API: "05/01/2023"
            parts = date_str.strip().split("/")
            if len(parts) == 3:
                day, month, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        except:
            pass
        
        return None
    
    def _determine_tax_type(self, tax_rate_str: str) -> str:
        """
        Xác định loại thuế
        """
        if not tax_rate_str:
            return "unknown"
        
        tax_rate_str = tax_rate_str.upper()
        
        if "KHAC" in tax_rate_str:
            return "other"
        elif "KKKNT" in tax_rate_str:
            return "not_subject"
        elif "KCT" in tax_rate_str or tax_rate_str == "0%":
            return "exempt"
        elif "%" in tax_rate_str and tax_rate_str not in ["0%"]:
            return "standard"
        else:
            return "unknown"
    
    def _parse_item_type(self, tinh_chat: str) -> tuple:
        """
        Parse tính chất sản phẩm
        """
        type_mapping = {
            "Hàng hóa, dịch vụ": ("product_service", "Hàng hóa, dịch vụ"),
            "Khuyến mại": ("promotion", "Khuyến mại"),
            "Chiết khấu": ("discount", "Chiết khấu"),
            "Ghi chú, diễn giải": ("note", "Ghi chú, diễn giải")
        }
        
        return type_mapping.get(tinh_chat, ("other", tinh_chat))
    
    def _update_analytics(self, invoice_doc: Dict[str, Any], item_count: int):
        """
        Cập nhật bảng analytics
        """
        try:
            issue_date = invoice_doc.get("issue_date")
            if not issue_date:
                return
                
            # Parse date components
            date_obj = datetime.strptime(issue_date, "%Y-%m-%d")
            month = date_obj.strftime("%Y-%m")
            year = str(date_obj.year)
            
            # Tạo analytics key
            analytics_key = {
                "date": issue_date,
                "seller_tax_code": invoice_doc["seller"]["tax_code"],
                "buyer_tax_code": invoice_doc["buyer"]["tax_code"]
            }
            
            # Dữ liệu analytics
            analytics_data = {
                **analytics_key,
                "month": month,
                "year": year,
                "seller_name": invoice_doc["seller"]["name"],
                "buyer_name": invoice_doc["buyer"]["name"],
                "total_invoices": 1,
                "total_items": item_count,
                "total_revenue": invoice_doc["financial_summary"]["total_amount"],
                "total_tax": invoice_doc["financial_summary"]["total_tax"],
                "payment_method": invoice_doc["processing_info"]["payment_method"],
                "invoice_status": invoice_doc["processing_info"]["status"],
                "updated_at": datetime.now()
            }
            
            # Upsert analytics
            self.invoice_analytics.replace_one(
                analytics_key,
                analytics_data,
                upsert=True
            )
            
        except Exception as e:
            print(f"⚠️  Analytics update warning: {e}")
    
    def get_invoice_with_items(self, unique_key: str) -> Dict[str, Any]:
        """
        Lấy hóa đơn với chi tiết sản phẩm
        """
        pipeline = [
            {"$match": {"unique_key": unique_key}},
            {
                "$lookup": {
                    "from": "invoice_items",
                    "localField": "unique_key",
                    "foreignField": "invoice_unique_key",
                    "as": "items"
                }
            }
        ]
        
        result = list(self.invoices.aggregate(pipeline))
        return result[0] if result else None
    
    def get_revenue_analysis(self, seller_tax_code: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Phân tích doanh thu theo thời gian
        """
        pipeline = [
            {
                "$match": {
                    "seller_tax_code": seller_tax_code,
                    "date": {"$gte": start_date, "$lte": end_date}
                }
            },
            {
                "$group": {
                    "_id": "$month",
                    "total_revenue": {"$sum": "$total_revenue"},
                    "total_invoices": {"$sum": "$total_invoices"},
                    "total_items": {"$sum": "$total_items"},
                    "total_tax": {"$sum": "$total_tax"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        return list(self.invoice_analytics.aggregate(pipeline))

# Hàm helper để sử dụng trong API
def save_invoice_data_to_mongo(raw_invoice_data: List[Dict[str, Any]], 
                              mongo_uri: str = "mongodb://localhost:27017",
                              db_name: str = "invoice_system") -> Dict[str, Any]:
    """
    Hàm tiện ích để lưu dữ liệu hóa đơn vào MongoDB
    """
    processor = InvoiceDataProcessor(mongo_uri, db_name)
    return processor.process_and_save_invoices(raw_invoice_data)

if __name__ == "__main__":
    # Test với dữ liệu mẫu
    sample_data = [
        {
            "Chiết khấu": None,
            "Ghi chú 1": "  ",
            "Ghi chú 2": " ",
            "Hình  thức thanh toán": "TM/CK",
            "Hạn dùng ": None,
            "Ký hiệu hóa  đơn": "C23TVC",
            "Kết quả kiểm tra hóa đơn": "Đã cấp mã hóa đơn",
            "MCCQT": "00D81E5836999A4CED92BAB781DE8ABBDD",
            "MST người bán": "0302147168",
            "MST người mua": "3603024990",
            "Mã VT": "",
            "Mã tra cứu": "2301061540407153603024990",
            "Mẫu số HD": 1,
            "Ngày CQT ký số": "06/01/2023",
            "Ngày lập hóa đơn": "05/01/2023",
            "Ngày người bán ký số": "06/01/2023",
            "Số hóa đơn": " 4",
            "Số lô ": None,
            "Số lượng": 1.0,
            "Thuế suất": "10.0%",
            "Thành tiền chưa thuế": 545454.54,
            "Tiền thuế": 54545.0,
            "Trạng thái hóa đơn": "Hóa đơn mới",
            "Tên hàng hóa, dịch vụ": "Kiểm định đầu khoan (CT: 230112)",
            "Tên người bán": "CÔNG TY CỔ PHẦN GIÁM ĐỊNH & TƯ VẤN VIỆT",
            "Tên người mua": "CÔNG TY TNHH MÁY XÂY DỰNG VIỆT NHẬT",
            "Tính chất": "Hàng hóa, dịch vụ",
            "Tổng tiền CKTM": 0.0,
            "Tổng tiền phí": None,
            "Tổng tiền thanh toán": 600000.0,
            "Tỷ giá": 1.0,
            "_id": "d8c53c5a2ea347129894b975",
            "url  tra cứu hóa đọn": "",
            "username": "0302147168",
            "Đơn giá": 545454.54,
            "Đơn vị tiền tệ": "VND",
            "Đơn vị tính": "Bộ",
            "Địa chỉ người bán": "SAV.8-20.13 Tầng 20, The Sun Avenue, 28 Mai Chí Thọ, Phường An Phú, Thành phố Thủ Đức, Thành phố Hồ Chí Minh",
            "Địa chỉ người mua": "Số 10, đường 2A, KCN Biên Hòa 2, phường An Bình, thành phố Biên Hòa, tỉnh Đồng Nai"
        }
    ]
    
    # Test lưu dữ liệu
    result = save_invoice_data_to_mongo(sample_data)
    print(json.dumps(result, indent=2, ensure_ascii=False))