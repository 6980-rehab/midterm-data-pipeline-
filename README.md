# Hybrid Big Data Pipeline — Final Phase

هذا المستودع هو **استكمال للمشروع النصفي داخل نفس المشروع**. المتطلبات النهائية تضيف 7 درجات فقط: Queries/Indexes/Explain، Aggregations، Materialized Views، Scheduled Jobs، FastAPI موحدة، وتحديث README/GitHub.

## 1. المتطلبات الجديدة المنفذة

### Queries + Indexes + Explain
- 5 استعلامات عملية في `src/final_queries.py`.
- 3 فهارس على الأقل، منها Compound Index:
  - `idx_customer_id`
  - `idx_order_date`
  - `idx_city_status_compound`
- `executionStats` للمقارنة قبل وبعد الفهارس عبر `/explain/{name}`.

### Aggregation Reports
خمسة تقارير مستقلة:
1. `sales_by_city`
2. `top_products`
3. `top_customers`
4. `sales_by_period`
5. `orders_by_status`

### Materialized Views
- `daily_sales_summary`
- `top_products_summary`

يتم أول مرة بناء الملخصات، وبعدها تستخدم آلية watermark وdelta لتحديث الجزء المتأثر بدل إعادة بناء كل البيانات في كل تشغيل.

### Scheduled Jobs
مهمتان يوميتان:
- `daily_sales_summary`
- `top_products_summary`

يمكن تشغيل كل مهمة يدويًا أثناء المناقشة، ويتم تسجيل البداية والنهاية والحالة في نتيجة التنفيذ المعادة من الـAPI، مع استمرار السجل في MongoDB من خلال سجل التشغيل.

### FastAPI
تشغيل:
```bash
uvicorn src.api:app --reload
```

Swagger:
`http://127.0.0.1:8000/docs`

Endpoints المطلوبة:
- `GET /health`
- `POST /ingest`
- `GET /indexes`
- `POST /indexes`
- `GET /queries`
- `GET /queries/{name}`
- `GET /aggregations`
- `GET /aggregations/{name}`
- `POST /refresh-mv`
- `POST /jobs`
- `POST /jobs/{name}/run`

`POST /ingest` يعيد استخدام Router والـPipeline الموجودين في المشروع النصفي، ولا ينشئ مسار إدخال جديد.

## 2. التثبيت

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

انسخ `example.env` إلى `.env` وعدّل القيم عند الحاجة، بدون وضع أي بيانات سرية حقيقية في GitHub.

## 3. تشغيل المشروع النصفي

```bash
python src/main.py --file data/sample_orders.csv
```

## 4. تهيئة فهارس المرحلة النهائية

```bash
python -c "from src.final_queries import create_indexes; print(create_indexes())"
```

## 5. تشغيل API

```bash
uvicorn src.api:app --host 127.0.0.1 --port 8000
```

ثم افتح `/docs`.

## 6. أمثلة للاختبار

إنشاء الفهارس:
```bash
curl -X POST http://127.0.0.1:8000/indexes
```

قائمة الاستعلامات:
```bash
curl http://127.0.0.1:8000/queries
```

تشغيل استعلام عميل:
```bash
curl "http://127.0.0.1:8000/queries/customer_orders?customer_id=CUST-500"
```

تشغيل تقرير تجميعي:
```bash
curl http://127.0.0.1:8000/aggregations/sales_by_city
```

تحديث الـMaterialized Views:
```bash
curl -X POST http://127.0.0.1:8000/refresh-mv
```

تشغيل مهمة يدويًا:
```bash
curl -X POST http://127.0.0.1:8000/jobs/daily_sales_summary/run
```

Explain قبل وبعد الفهرس:
```bash
curl "http://127.0.0.1:8000/explain/customer_orders?customer_id=CUST-500"
```

## 7. ملاحظات الاختبار

- لا يعتمد الكود على اسم ملف تدريب ثابت أو عدد سجلات ثابت.
- البيانات المصدرية تستمر في المرور عبر `orders_raw` ثم الـELT النصفي.
- الـAPI طبقة تشغيل واختبار للوظائف الأصلية، وليست Backend منفصلًا.
- الـDashboard غير مطلوب.

## 8. بنية الإضافات

```text
src/
├── final_queries.py
├── aggregations.py
├── materialized_views.py
├── scheduled_jobs.py
├── api.py
├── final_common.py
├── ... ملفات المشروع النصفي ...
config/
└── settings.py
tests/
└── test_final_features.py
requirements.txt
example.env
README.md
```
