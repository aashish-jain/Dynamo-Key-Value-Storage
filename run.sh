for i in {1..20}
do
    echo $i, $(./simpledynamo-grading.linux app/build/outputs/apk/debug/app-debug.apk | tail -n 1)
done

