# Hướng dẫn sử dụng hệ thống MongoDB cho Hóa đơn điện tử

## 1. Cài đặt

### Cài đặt MongoDB
```bash
# Windows - Download và cài đặt từ: https://www.mongodb.com/try/download/community
# Hoặc dùng Chocolatey:
choco install mongodb

# Linux/Ubuntu:
sudo apt update
sudo apt install -y mongodb-org

# macOS:
brew tap mongodb/brew
brew install mongodb-community
```

### Cài đặt Python dependencies
```bash
pip install -r requirements.txt
```

### Khởi động MongoDB
```bash
# Windows (nếu không tự khởi động):
net start MongoDB

# Linux/macOS:
sudo systemctl start mongod
```

## 2. Cấu trúc dữ liệu

### Collections:
1. **`invoices`** - Thông tin hóa đơn chính
2. **`invoice_items`** - Chi tiết sản phẩm/dịch vụ 
3. **`invoice_analytics`** - Dữ liệu thống kê (tự động cập nhật)

### Unique Key Format:
`{template_code}_{invoice_number}_{seller_tax_code}`

Ví dụ: `1_4_0302147168`

## 3. API Endpoints

### Crawl và lưu hóa đơn
```bash
POST /api/process-invoices
Content-Type: application/json

{
  "type": 1,  // 1=bán ra, 2=mua vào
  "process_type": "chitiet",  // tongquat, chitiet, xml_html
  "date_range": {
    "start": "2023-01-01",
    "end": "2023-01-31"
  }
}
```

**Response:**
```json
{
  "success": true,
  "json_data": [...],  // Dữ liệu hóa đơn
  "message": "Đã xử lý thành công 8/4 hóa đơn",
  "mongodb_result": {
    "saved": true,
    "message": "Đã lưu thành công 3 hóa đơn mới, cập nhật 0 hóa đơn, 8 sản phẩm",
    "summary": {
      "new_invoices": 3,
      "updated_invoices": 0,
      "total_items": 8,
      "unique_invoices": 3
    }
  }
}
```

### Lấy chi tiết hóa đơn
```bash
GET /api/invoices/{unique_key}

# Ví dụ:
GET /api/invoices/1_4_0302147168
```

### Tìm kiếm hóa đơn
```bash
POST /api/invoices/search
Content-Type: application/json

{
  "seller_tax_code": "0302147168",
  "start_date": "2023-01-01",
  "end_date": "2023-01-31",
  "page": 1,
  "limit": 20
}
```

### Phân tích doanh thu
```bash
POST /api/analytics/revenue
Content-Type: application/json

{
  "seller_tax_code": "0302147168",
  "start_date": "2023-01-01",
  "end_date": "2023-12-31"
}
```

### Thống kê tổng hợp
```bash
POST /api/analytics/summary
Content-Type: application/json

{
  "seller_tax_code": "0302147168",
  "start_date": "2023-01-01",
  "end_date": "2023-12-31"
}
```

### Top sản phẩm bán chạy
```bash
POST /api/products/top-selling
Content-Type: application/json

{
  "start_date": "2023-01-01",
  "end_date": "2023-12-31",
  "limit": 10
}
```

## 4. Truy vấn MongoDB trực tiếp

### Kết nối MongoDB
```javascript
// Mở MongoDB Compass hoặc mongo shell
use invoice_system
```

### Các truy vấn hữu ích

#### Lấy hóa đơn với chi tiết:
```javascript
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

#### Thống kê doanh thu theo tháng:
```javascript
db.invoice_analytics.aggregate([
  { 
    $match: { 
      "seller_tax_code": "0302147168",
      "date": { $gte: "2023-01-01", $lte: "2023-12-31" }
    }
  },
  {
    $group: {
      _id: "$month",
      total_revenue: { $sum: "$total_revenue" },
      total_invoices: { $sum: "$total_invoices" }
    }
  },
  { $sort: { "_id": 1 } }
])
```

#### Top khách hàng:
```javascript
db.invoice_analytics.aggregate([
  { $match: { "seller_tax_code": "0302147168" } },
  {
    $group: {
      _id: {
        tax_code: "$buyer_tax_code",
        name: "$buyer_name"
      },
      total_revenue: { $sum: "$total_revenue" },
      total_invoices: { $sum: "$total_invoices" }
    }
  },
  { $sort: { "total_revenue": -1 } },
  { $limit: 10 }
])
```

#### Sản phẩm bán chạy:
```javascript
db.invoice_items.aggregate([
  { $match: { "item_type": "product_service" } },
  {
    $group: {
      _id: "$item_name",
      total_quantity: { $sum: "$quantity" },
      total_revenue: { $sum: "$subtotal" },
      avg_price: { $avg: "$unit_price" }
    }
  },
  { $sort: { "total_quantity": -1 } },
  { $limit: 10 }
])
```

## 5. Backup và Restore

### Backup
```bash
# Backup toàn bộ database
mongodump --db invoice_system --out ./backup/

# Backup collection cụ thể
mongodump --db invoice_system --collection invoices --out ./backup/
```

### Restore
```bash
# Restore toàn bộ database
mongorestore --db invoice_system ./backup/invoice_system/

# Restore collection cụ thể
mongorestore --db invoice_system --collection invoices ./backup/invoice_system/invoices.bson
```

## 6. Performance Tips

### Indexing
Hệ thống đã tự động tạo các indexes quan trọng:
- `invoices.unique_key` (unique)
- `invoices.seller.tax_code + issue_date`
- `invoice_items.invoice_unique_key`
- `invoice_analytics.date`

### Query Optimization
```javascript
// Sử dụng projection để chỉ lấy fields cần thiết
db.invoices.find(
  { "seller.tax_code": "0302147168" },
  { 
    "invoice_number": 1, 
    "issue_date": 1, 
    "financial_summary.total_amount": 1 
  }
)

// Sử dụng limit cho danh sách
db.invoices.find().sort({"issue_date": -1}).limit(20)
```

## 7. Monitoring

### Kiểm tra kích thước database
```javascript
db.stats()
db.invoices.stats()
db.invoice_items.stats()
```

### Kiểm tra indexes
```javascript
db.invoices.getIndexes()
db.invoice_items.getIndexes()
```

## 8. Troubleshooting

### Lỗi kết nối MongoDB
```python
# Kiểm tra MongoDB đã chạy chưa:
# Windows: net start MongoDB
# Linux: sudo systemctl status mongod

# Kiểm tra port:
netstat -an | findstr 27017
```

### Lỗi duplicate key
```javascript
// Tìm duplicate records
db.invoices.aggregate([
  { $group: { _id: "$unique_key", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } }
])
```

### Clear dữ liệu để test lại
```javascript
// ⚠️ CẢNH BÁO: Xóa toàn bộ dữ liệu
db.invoices.deleteMany({})
db.invoice_items.deleteMany({})
db.invoice_analytics.deleteMany({})
```