# Thiết kế Database MongoDB cho Hệ thống Hóa đơn điện tử

## 1. Phân tích yêu cầu
- Lưu trữ dữ liệu hóa đơn và chi tiết sản phẩm
- Hỗ trợ phân tích kinh doanh
- Tránh trùng lặp dữ liệu
- Truy vấn nhanh cho cả hóa đơn tổng hợp và chi tiết

## 2. Cấu trúc Collections

### Collection 1: `invoices` (Hóa đơn chính)
```javascript
{
  _id: ObjectId,
  invoice_number: "4",           // Số hóa đơn (unique key với mẫu số)
  template_code: "1",            // Mẫu số HD
  symbol: "C23TVC",              // Ký hiệu hóa đơn
  
  // Thông tin cơ bản
  issue_date: "2023-01-05",      // Ngày lập (ISO date)
  seller_signature_date: "2023-01-06",
  tax_office_signature_date: "2023-01-06",
  
  // Người bán
  seller: {
    tax_code: "0302147168",
    name: "CÔNG TY CỔ PHẦN GIÁM ĐỊNH & TƯ VẤN VIỆT",
    address: "SAV.8-20.13 Tầng 20, The Sun Avenue..."
  },
  
  // Người mua
  buyer: {
    tax_code: "3603024990",
    name: "CÔNG TY TNHH MÁY XÂY DỰNG VIỆT NHẬT",
    address: "Số 10, đường 2A, KCN Biên Hòa 2..."
  },
  
  // Thông tin tài chính tổng hợp
  financial_summary: {
    subtotal_before_tax: 545454.54,    // Tổng thành tiền chưa thuế
    total_tax: 54545.0,                // Tổng tiền thuế
    total_discount: 0.0,               // Tổng chiết khấu
    total_fees: 0.0,                   // Tổng phí
    total_amount: 600000.0,            // Tổng tiền thanh toán
    currency: "VND",
    exchange_rate: 1.0
  },
  
  // Thông tin xử lý
  processing_info: {
    status: "Hóa đơn mới",             // Trạng thái hóa đơn
    verification_result: "Đã cấp mã hóa đơn",
    tax_office_code: "00D81E5836999A4CED92BAB781DE8ABBDD",
    lookup_code: "2301061540407153603024990",
    payment_method: "TM/CK"
  },
  
  // Metadata
  created_at: ISODate,
  updated_at: ISODate,
  username: "0302147168",              // User đã crawl
  
  // Indexes
  unique_key: "1_4_0302147168"         // template_code + invoice_number + seller_tax_code
}
```

### Collection 2: `invoice_items` (Chi tiết sản phẩm)
```javascript
{
  _id: ObjectId,
  invoice_id: ObjectId,              // Reference to invoices collection
  invoice_unique_key: "1_4_0302147168",  // Để query nhanh
  
  // Thông tin sản phẩm
  item_code: "",                     // Mã VT
  item_name: "Kiểm định đầu khoan (CT: 230112)",
  unit: "Bộ",
  quantity: 1.0,
  unit_price: 545454.54,
  discount: 0,
  subtotal: 545454.54,               // Thành tiền chưa thuế
  
  // Thuế
  tax_rate: "10.0%",
  tax_amount: 54545.0,
  tax_type: "standard",              // standard, exempt, zero, other
  
  // Tính chất
  item_type: "product_service",      // product_service, promotion, discount, note
  item_type_display: "Hàng hóa, dịch vụ",
  
  // Metadata
  notes_1: "  ",
  notes_2: " ",
  expiry_date: null,
  batch_number: null,
  
  created_at: ISODate,
  sequence: 1                        // Thứ tự sản phẩm trong hóa đơn
}
```

### Collection 3: `invoice_analytics` (Bảng phân tích - tự động tính toán)
```javascript
{
  _id: ObjectId,
  date: "2023-01-05",               // Ngày (để group by)
  month: "2023-01",                 // Tháng
  year: "2023",                     // Năm
  
  // Theo người bán
  seller_tax_code: "0302147168",
  seller_name: "CÔNG TY CỔ PHẦN GIÁM ĐỊNH & TƯ VẤN VIỆT",
  
  // Theo người mua
  buyer_tax_code: "3603024990",
  buyer_name: "CÔNG TY TNHH MÁY XÂY DỰNG VIỆT NHẬT",
  
  // Thống kê
  total_invoices: 1,
  total_items: 1,
  total_revenue: 600000.0,
  total_tax: 54545.0,
  
  // Phân loại
  payment_method: "TM/CK",
  invoice_status: "Hóa đơn mới",
  
  updated_at: ISODate
}
```

## 3. Indexes quan trọng

### Collection `invoices`:
```javascript
// Unique constraint
db.invoices.createIndex(
  { "unique_key": 1 }, 
  { unique: true }
)

// Query thường dùng
db.invoices.createIndex({ "seller.tax_code": 1, "issue_date": -1 })
db.invoices.createIndex({ "buyer.tax_code": 1, "issue_date": -1 })
db.invoices.createIndex({ "issue_date": -1 })
db.invoices.createIndex({ "processing_info.lookup_code": 1 })
```

### Collection `invoice_items`:
```javascript
db.invoice_items.createIndex({ "invoice_id": 1 })
db.invoice_items.createIndex({ "invoice_unique_key": 1 })
db.invoice_items.createIndex({ "item_name": "text" })
```

### Collection `invoice_analytics`:
```javascript
db.invoice_analytics.createIndex({ "date": -1 })
db.invoice_analytics.createIndex({ "seller_tax_code": 1, "date": -1 })
db.invoice_analytics.createIndex({ "buyer_tax_code": 1, "date": -1 })
```

## 4. Ưu điểm của cấu trúc này

1. **Tránh trùng lặp**: Thông tin hóa đơn chỉ lưu 1 lần
2. **Flexible**: Dễ dàng thêm/sửa sản phẩm mà không ảnh hưởng hóa đơn
3. **Analytics-ready**: Collection analytics sẵn sàng cho báo cáo
4. **Performance**: Indexes tối ưu cho các query thường dùng
5. **Scalability**: Có thể partition theo năm/tháng nếu cần

## 5. Các query mẫu

### Lấy hóa đơn với chi tiết:
```javascript
// Aggregation pipeline
db.invoices.aggregate([
  { $match: { "unique_key": "1_4_0302147168" } },
  {
    $lookup: {
      from: "invoice_items",
      localField: "unique_key",
      foreignField: "invoice_unique_key",
      as: "items"
    }
  }
])
```

### Phân tích doanh thu theo tháng:
```javascript
db.invoice_analytics.aggregate([
  { $match: { "seller_tax_code": "0302147168", "date": { $gte: "2023-01-01" } } },
  {
    $group: {
      _id: "$month",
      total_revenue: { $sum: "$total_revenue" },
      total_invoices: { $sum: "$total_invoices" }
    }
  }
])
```