"file_size_mb"| حجم الملف
"engine_used"| المحرك المستخدم
"rows_read"| عدد السجلات المقروءة
"raw_loaded"| السجلات التي وصلت إلى Raw
"valid_count"| السجلات السليمة
"corrected_count"| السجلات المصححة
"quarantine_count"| السجلات المعزولة
"elapsed_seconds"| زمن التنفيذ
"throughput"| معدل المعالجة
"count_inserted"| السجلات الجديدة
"count_updated"| السجلات التي تم تحديثها
"count_unchanged"| السجلات التي لم تتغير

وتُستخدم هذه البيانات لاحقًا لتحليل أداء Python Batch وPySpark.

«ملاحظة: يتم وضع الأرقام النهائية هنا بعد اكتمال تشغيل المشروع فعليًا، حتى تكون النتائج موثقة من جهاز التنفيذ وليست أرقامًا افتراضية.»

---

🛠 6. قواعد جودة البيانات

من أمثلة القواعد المستخدمة:

- "DATE_STANDARDIZED" — توحيد صيغة التاريخ.
- "CURRENCY_NORMALIZED" — توحيد العملة.
- "ARABIC_DIGIT_CONVERSION" — تحويل الأرقام العربية إلى أرقام لاتينية.
- "PRICE_PARSER" — معالجة صيغ الأسعار.
- "PHONE_SANITIZATION" — تنظيف وتوحيد أرقام الهاتف.
- "EMAIL_SYNTAX_REPAIR" — إصلاح الأخطاء الواضحة في البريد.
- "FATAL_CORRUPTION_ISOLATION" — عزل الأخطاء الجوهرية.
- قواعد Trim وتوحيد القيم المعروفة.

---

📸 7. أدلة التشغيل

سيتم وضع لقطات الشاشة الناتجة من التشغيل الفعلي داخل:

reports/screenshots/

1. تشغيل العينة باستخدام Python Batch

reports/screenshots/python_batch_result.png

يظهر فيها حجم الملف، المحرك المختار، الدفعات، الزمن ومعدل المعالجة.

2. إثبات نتائج MongoDB

reports/screenshots/mongo_collections.png

وتوضح Collections:

orders_raw
orders_validated
orders_quarantine

3. سجل مصحح مع Audit Trail

reports/screenshots/validated_sample.png

4. سجل معزول مع سبب العزل

reports/screenshots/quarantine_sample.png

5. نتائج PySpark

reports/screenshots/spark_result.png

6. Spark UI

reports/screenshots/spark_ui.png

وتُستخدم لإظهار Jobs وStages وTasks وPartitions أثناء تشغيل الملف الكبير، وفق متطلبات العرض العملي.

7. اختبار Idempotency وUpsert

reports/screenshots/idempotency_test.png

ويظهر إعادة تشغيل نفس البيانات وعدم إنشاء سجلات Business مكررة.

---

💻 8. طريقة التشغيل

تثبيت المتطلبات

pip install -r requirements.txt

إنشاء عينة من الملف الكبير

python src/create_small_sample.py --input data/orders_huge_mixed_quality.csv --rows 100000

تشغيل العينة

python src/main.py --file data/sample_orders.csv

تشغيل الملف الكبير

python src/main.py --file data/orders_huge_mixed_quality.csv

فحص MongoDB

python check_mongo.py

تشغيل الاختبارات

pytest

---

📁 9. بنية المشروع

piplineBigData/
│
├── README.md
├── requirements.txt
│
├── config/
│   └── settings.py
│
├── data/
│   ├── orders_huge_mixed_quality.csv
│   └── sample_orders.csv
│
├── src/
│   ├── main.py
│   ├── file_router.py
│   ├── create_small_sample.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── quality_rules.py
│   ├── elt_pipeline.py
│   ├── incremental_loader.py
│   ├── mongo_setup.py
│   └── metrics.py
│
├── tests/
│   ├── test_cleaning_rules.py
│   └── test_classification.py
│
├── reports/
│   ├── results.json
│   └── screenshots/
│
└── docs/
    └── architecture.md

---

🎯 10. الهدف من التنفيذ

لا يقتصر الهدف على نقل البيانات من ملف CSV إلى MongoDB، وإنما بناء خط بيانات يمكنه:

- اختيار محرك المعالجة المناسب حسب حجم البيانات.
- المحافظة على السجلات الأصلية.
- تطبيق قواعد جودة واضحة.
- توثيق عمليات التصحيح.
- عزل السجلات غير القابلة للتصحيح.
- قياس أداء المعالجة.
- إعادة التشغيل بأمان دون إنشاء تكرارات.
- توفير نتائج يمكن تحليلها وتوثيقها.

وهذه هي الفكرة الأساسية للمشروع كما وردت في التكليف: بناء خط بيانات يمكن تفسير قراراته وتتبع سجلاته وقياس أدائه وعدم فقدان البيانات السيئة.

---

👩‍💻 إعداد

رحاب بشير الخطيب

جامعة الرازي — كلية الحاسوب وتكنولوجيا المعلومات

بكالوريوس ذكاء اصطناعي — المستوى الرابع

مقرر البيانات الضخمة - العملي

إشراف: م. عمر أبوسند

---