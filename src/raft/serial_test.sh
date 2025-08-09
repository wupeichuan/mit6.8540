# !/bin/bash
echo "===== the test start ====="

start_time=$(date +%s.%N)
result=()
for i in {0..9}; do
    go test -run 2C > "test/testlog_$i" 2>&1
    if [ $? -eq 0 ]; then
        result[$i]=1
    else
        result[$i]=2
    fi
done

end_time=$(date +%s.%N)
success_count=0
fail_count=0
error_count=0
for element in ${result[@]}; do
    if [ $element -eq 1 ]; then
        ((success_count++))
    elif [ $element -eq 2 ]; then
        ((fail_count++))
    else
        ((error_count++))
    fi
done

echo "SUCCESS $success_count"
echo "FAIL $fail_count"
echo "ERROR $error_count"
echo "elasped time: $(echo "$end_time - $start_time" | bc) secs"
echo "===== the test end ====="