--start-date 2004-01-01 --end-date 2025-11-30

keyword_normalizer.py -> keyword_mapping.csv

    cd "gg trends"
    python keyword_normalizer.py --input ../tourism.csv --delimiter ';' --output keyword_mapping.csv


fetch_trends_data.py -> gg trends/dest_trends_raw/*.csv
    # Fetch tất cả với anchor normalization (4 keywords + 1 anchor/group)
    # Anchor có thể là BẤT KỲ từ khóa phổ biến nào, không cần có trong destinations
    python fetch_trends_data.py --keywords-file keyword_mapping.csv --start-date 2004-01-01 --end-date 2025-11-30 --group-delay 1 --anchor "Rau má"
    
    # Nếu có groups fail, retry specific groups:
    python fetch_trends_data.py --keywords-file keyword_mapping.csv --start-date 2004-01-01 --end-date 2025-11-30 --group-delay 1 --anchor "Rau má" --start-group 10 --end-group 15
    
    # Failed groups sẽ được lưu vào: dest_trends_raw/failed_groups.txt

analyze_trends_data.py -> destination_monthly_trends.csv + destination_summary_stats.csv
    # Analyze với anchor normalization (interest có thể so sánh được)
    python analyze_trends_data.py --normalize --anchor "Rau má"
    
    # Analyze không normalize (interest gốc, không so sánh được)
    python analyze_trends_data.py










bỏ:

destination_monthly_trends.py -> 

    cd "gg trends"
    python destination_monthly_trends.py \
    --start-date 2004-01-01 --end-date 2025-11-30 \
    --batch-size 5 --group-delay 4 \
    --keywords-file keyword_mapping.csv