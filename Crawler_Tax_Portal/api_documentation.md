# API Documentation

## Authentication

### Login
- **Endpoint**: `/api/login`
- **Method**: POST
- **Description**: Authenticates user and returns a token
- **Request Body**:
  ```json
  {
    "username": "string",
    "password": "string",
    "ckey": "string",
    "captcha": "string"
  }
  ```
- **Response**:
  ```json
  {
    "success": boolean,
    "token": "string" // if successful
  }
  ```

## Captcha

### Get Captcha
- **Endpoint**: `/api/get-captcha`
- **Method**: GET
- **Description**: Retrieves a new captcha for authentication
- **Response**: Returns captcha data including key

## Invoice Processing

### Process Invoices
- **Endpoint**: `/api/process-invoices`
- **Method**: POST
- **Description**: Processes invoices based on specified type and date range
- **Request Body**:
  ```json
  {
    "type": number, // 1 for banra, 2 for muavao
    "process_type": "string", // tongquat, chitiet, xml_html
    "date_range": {
      "start": "string", // DD/MM/YYYY
      "end": "string"    // DD/MM/YYYY
    },
    "output_path": "string"
  }
  ```
- **Response**:
  ```json
  {
    "success": boolean,
    "message": "string",
    "file_path": "string" // if successful
  }
  ```

### Get Invoices
- **Endpoint**: `/api/get-invoices`
- **Method**: GET
- **Description**: Retrieves invoices based on type, date, and username
- **Query Parameters**:
  - `type`: "sale" or "purchase"
  - `date`: "DD/MM/YYYY"
  - `username`: string
- **Response**:
  ```json
  {
    "success": boolean,
    "data": array,
    "count": number
  }
  ```

## Master Data Management

### Upload Master Data
- **Endpoint**: `/api/upload-master-data`
- **Method**: POST
- **Description**: Uploads and processes master data files (KhachHang, NhaCungCap, DanhMucVatTu)
- **Request Body** (multipart/form-data):
  - `khachhang_file`: File CSV/XLSX chứa dữ liệu khách hàng
  - `nhacungcap_file`: File CSV/XLSX chứa dữ liệu nhà cung cấp
  - `vattu_file`: File CSV/XLSX chứa danh mục vật tư
- **Response**:
  ```json
  {
    "success": boolean,
    "message": "string",
    "details": {
      "KhachHang": {
        "success": boolean,
        "message": "string"
      },
      "NhaCungCap": {
        "success": boolean,
        "message": "string"
      },
      "DanhMucVatTu": {
        "success": boolean,
        "message": "string"
      }
    }
  }
  ```

### Transform Data
- **Endpoint**: `/api/transform-data`
- **Method**: POST
- **Description**: Transforms and syncs data between collections
- **Request Body**: None (uses current user context)
- **Response**:
  ```json
  {
    "success": boolean,
    "message": "string",
    "details": {
      "HoaDonMuaVao": {
        "success": boolean,
        "message": "string",
        "errors_encountered": number
      },
      "HoaDonBanRa": {
        "success": boolean,
        "message": "string",
        "errors_encountered": number
      }
    }
  }
  ```

## Notes
- All endpoints are prefixed with `/api`
- The server runs on port 5000
- CORS is enabled for `http://localhost:5173`
- Authentication is required for most endpoints (except get-captcha)
- Date formats should be in DD/MM/YYYY
- File uploads support both CSV and XLSX formats
- Error messages are returned in Vietnamese 