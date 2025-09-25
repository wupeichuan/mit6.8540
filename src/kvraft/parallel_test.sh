# !/bin/bash
echo "===== the test start ====="

start_time=$(date +%s.%N)
tmpdir=$(mktemp -d)
iternum=({0..0})
process_num=({0..14})
for x in ${iternum[@]}; do
    for i in ${process_num[@]}; do
        (
            time go test -run 3A
            if [ $? -eq 0 ]; then
                echo "1" > "$tmpdir/`expr $x \* ${#process_num[@]} \+ $i`"
            else
                echo "2" > "$tmpdir/`expr $x \* ${#process_num[@]} \+ $i`"
            fi
        ) > "test/testlog_`expr $x \* ${#process_num[@]} \+ $i`" 2>&1 &
    done
    wait
    echo "iter $x OK"
done

end_time=$(date +%s.%N)
result=()
for x in ${iternum[@]}; do
    for i in ${process_num[@]}; do
        if [ -f "$tmpdir/`expr $x \* ${#process_num[@]} \+ $i`" ]; then
            result[`expr $x \* ${#process_num[@]} \+ $i`]=$(<"$tmpdir/`expr $x \* ${#process_num[@]} \+ $i`")
        else
            result[`expr $x \* ${#process_num[@]} \+ $i`]="0"
        fi
    done
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

echo "===== the test end ====="