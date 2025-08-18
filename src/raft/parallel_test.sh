# !/bin/bash
echo "===== the test start ====="

start_time=$(date +%s.%N)
tmpdir=$(mktemp -d)
for i in {0..29}; do
    (
        time go test -run 2C
        if [ $? -eq 0 ]; then
            echo "1" > "$tmpdir/$i"
        else
            echo "2" > "$tmpdir/$i"
        fi
    ) > "test/testlog_$i" 2>&1 &
done

wait
end_time=$(date +%s.%N)
result=()
for i in {0..29}; do
    if [ -f "$tmpdir/$i" ]; then
        result[$i]=$(<"$tmpdir/$i")
    else
        result[$i]="2"
    fi
done

success_count=0
fail_count=0
error_count=0
for element in ${result[@]}; do
    if [ $element -eq "1" ]; then
        ((success_count++))
    elif [ $element -eq "2" ]; then
        ((fail_count++))
    else
        ((error_count++))
    fi
done

echo "SUCCESS $success_count"
echo "FAIL $fail_count"
echo "ERROR $error_count"
echo "elasp time: $(echo "$end_time - $start_time" | bc) secs"