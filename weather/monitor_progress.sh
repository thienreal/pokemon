#!/bin/bash
# Script theo dõi tiến trình lấy dữ liệu thời tiết

LOG_FILE="/workspaces/pokemon/weather/weather_log.txt"

echo "=== THEO DÕI TIẾN TRÌNH LẤY DỮ LIỆU THỜI TIẾT ==="
echo ""

while true; do
    clear
    echo "🕐 $(date '+%H:%M:%S')"
    echo "================================================================================  "
    
    if [ -f "$LOG_FILE" ]; then
        # Đếm số requests thành công
        SUCCESS_COUNT=$(grep -c "✅" "$LOG_FILE" 2>/dev/null || echo "0")
        FAIL_COUNT=$(grep -c "❌" "$LOG_FILE" 2>/dev/null || echo "0")
        TOTAL=$((SUCCESS_COUNT + FAIL_COUNT))
        
        echo "📊 Thống kê:"
        echo "   ✅ Thành công: $SUCCESS_COUNT"
        echo "   ❌ Thất bại: $FAIL_COUNT"
        echo "   📈 Tổng: $TOTAL / 510"
        if [ $TOTAL -gt 0 ]; then
            PERCENT=$((SUCCESS_COUNT * 100 / TOTAL))
            echo "   🎯 Tỷ lệ thành công: $PERCENT%"
        fi
        
        echo ""
        echo "📝 30 dòng cuối của log:"
        echo "--------------------------------------------------------------------------------"
        tail -30 "$LOG_FILE"
    else
        echo "⏳ Đang chờ script bắt đầu..."
    fi
    
    echo ""
    echo "================================================================================  "
    echo "Nhấn Ctrl+C để thoát | Tự động cập nhật sau 5s..."
    
    sleep 5
done
