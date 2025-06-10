#Personalized pagerank
- bước 1
Cài Java JDK 8 trở lên
Cài Apache Spark (đã cấu hình biến môi trường SPARK_HOME và PATH)
- bước 2
Tạo file graph.txt trong thư mục src/data/ (nếu chưa có)
- bước 3: đảm bảo mã nguồn nằm đúng vị trí
- personallized-pagerank/
├── src/
│   └── data/
│       └── graph.txt
├── com/
│   └── example/
│       └── personalized/
│           └── SparkPi.java

- bước 4: chạy 
1. compile file java
2. chạy bằng spark submit
